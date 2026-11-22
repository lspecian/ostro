package ostro

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// The frozen ready line (D-P10-9), printed on stdout.
var readyLine = regexp.MustCompile(`^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`)

// repoRoot: the test runs in clients/go/ostro.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func ostroBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("OSTRO_BIN")
	if bin == "" {
		bin = filepath.Join(repoRoot(t), "target", "debug", "ostro")
	}
	if _, err := os.Stat(bin); err != nil {
		// Never a silent skip: the e2e scenario is part of the suite.
		t.Fatalf("the ostro binary %s does not exist: build it with `cargo build -p ostro` "+
			"and/or point OSTRO_BIN at it (scripts/ci-local.sh sets OSTRO_BIN)", bin)
	}
	return bin
}

func guestWasm(t *testing.T, name string) []byte {
	t.Helper()
	if p := os.Getenv("OSTRO_E2E_WASM"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	root := repoRoot(t)
	target := filepath.Join(root, "target", "guests")
	cargo := os.Getenv("CARGO")
	if cargo == "" {
		cargo = "cargo"
	}
	cmd := exec.Command(cargo, "build", "--manifest-path", filepath.Join(root, "guests", "Cargo.toml"),
		"-p", name, "--target", "wasm32-wasip2", "--release", "--target-dir", target)
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		switch k {
		case "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR", "RUSTC_WRAPPER":
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("building guests/%s failed: %v\n%s", name, err, stderr.String())
	}
	b, err := os.ReadFile(filepath.Join(target, "wasm32-wasip2", "release", strings.ReplaceAll(name, "-", "_")+".wasm"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// spawnServer starts `ostro dev --ephemeral --listen 127.0.0.1:0 <extra>` and returns its
// URL and whether the ready line says ephemeral; the server is killed at cleanup.
func spawnServer(t *testing.T, extra ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command(ostroBin(t), append([]string{"dev", "--ephemeral", "--listen", "127.0.0.1:0"}, extra...)...)
	cmd.Dir = t.TempDir()
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "OSTRO_") && !strings.HasPrefix(kv, "RUST_LOG=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	stderr := &syncBuf{}
	cmd.Stderr = stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	ready := make(chan []string, 1)
	go func() {
		sc := bufio.NewScanner(out)
		sent := false
		for sc.Scan() {
			if m := readyLine.FindStringSubmatch(sc.Text()); m != nil && !sent {
				ready <- m
				sent = true
			}
		}
		if !sent {
			close(ready)
		}
	}()
	select {
	case m, ok := <-ready:
		if !ok {
			t.Fatalf("ostro dev exited before its ready line; stderr: %s", stderr.String())
		}
		return "http://" + m[2], m[4] != ""
	case <-time.After(60 * time.Second):
		t.Fatalf("no ready line from ostro dev within 60 s; stderr: %s", stderr.String())
	}
	return "", false
}

func unknownID(id string) string {
	b := []byte(id)
	if b[10] == '0' {
		b[10] = '1'
	} else {
		b[10] = '0'
	}
	return string(b)
}

func hasKind(h *HistoryPage, kind string) bool {
	for _, e := range h.Events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func kinds(h *HistoryPage) []string {
	var k []string
	for _, e := range h.Events {
		k = append(k, e.Kind)
	}
	return k
}

const dayMs = 24 * 3600 * 1000

var liveStatus = map[string]bool{"running": true, "scheduled": true, "ready": true}

func TestE2ESharedScenario(t *testing.T) {
	wasm := guestWasm(t, "p10-time")
	t0 := time.Now()
	url, ephemeral := spawnServer(t, "--time-skipping=manual")
	if !ephemeral {
		t.Fatal("ready line is not ephemeral")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := NewClient(url)

	up, err := c.UploadModule(ctx, wasm, "p10-time", "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range up.Module.Workflows {
		found = found || w.Name == "signal_then_sleep"
	}
	if !found {
		t.Fatalf("no signal_then_sleep in %+v", up.Module)
	}
	wf, err := c.Activate(ctx, "signal_then_sleep", 1)
	if err != nil {
		t.Fatal(err)
	}
	if wf.ActiveVersion == nil || *wf.ActiveVersion != 1 {
		t.Fatalf("activate: %+v", wf)
	}

	started, err := c.StartRun(ctx, StartRunRequest{Workflow: "signal_then_sleep", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if !started.Created {
		t.Fatalf("start: %+v", started)
	}
	runID := started.RunID
	// The run is live: `running`, or `scheduled`/`ready` between activations (a run parked
	// on its signal wait is reported either way).
	run, err := c.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if !liveStatus[run.Status] || run.Workflow != "signal_then_sleep" {
		t.Fatalf("get run: %+v", run)
	}

	sig, err := c.Signal(ctx, runID, "go", map[string]any{"ok": true}, "")
	if err != nil || !sig.Accepted {
		t.Fatalf("signal: %+v %v", sig, err)
	}

	// Wait until the run is parked on its 1-day timer.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		h, err := c.History(ctx, runID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if hasKind(h, "timer_started") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never parked: %v", kinds(h))
		}
	}

	skip, err := c.DevSkipBy(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if skip.SkippedMs < dayMs {
		t.Fatalf("skip: %+v", skip)
	}

	res, err := c.Result(ctx, runID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Completed || res.Status != "completed" {
		t.Fatalf("result: %+v", res)
	}
	var payload map[string]any
	if err := json.Unmarshal(res.Result, &payload); err != nil || payload["ok"] != true {
		t.Fatalf("result payload %s: %v", res.Result, err)
	}

	h, err := c.History(ctx, runID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasKind(h, "timer_fired") {
		t.Fatalf("no timer_fired: %v", kinds(h))
	}
	dt, err := c.DevTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dt.OffsetMs < dayMs {
		t.Fatalf("dev time: %+v", dt)
	}

	_, err = c.GetRun(ctx, unknownID(runID))
	var e *Error
	if !errors.As(err, &e) || e.Status != 404 {
		t.Fatalf("unknown run: %v", err)
	}

	t.Logf("[wall] go e2e scenario: %.2fs", time.Since(t0).Seconds())
}
