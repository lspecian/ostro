package ostrotest

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const dayMs = int64(24 * time.Hour / time.Millisecond)

// repo: the test runs in clients/go/ostrotest.
func repo(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func bin(t *testing.T) string {
	t.Helper()
	b := os.Getenv("OSTRO_BIN")
	if b == "" {
		b = filepath.Join(repo(t), "target", "debug", "ostro")
	}
	if _, err := os.Stat(b); err != nil {
		t.Fatalf("the ostro binary %s does not exist: build it with `cargo build -p ostro` or point OSTRO_BIN at it", b)
	}
	return b
}

func guest(t *testing.T, name string) string {
	t.Helper()
	if name == "p10-time" && os.Getenv("OSTRO_E2E_WASM") != "" {
		return os.Getenv("OSTRO_E2E_WASM")
	}
	target := filepath.Join(repo(t), "target", "guests")
	cargo := os.Getenv("CARGO")
	if cargo == "" {
		cargo = "cargo"
	}
	cmd := exec.Command(cargo, "build", "--manifest-path", filepath.Join(repo(t), "guests", "Cargo.toml"),
		"-p", name, "--target", "wasm32-wasip2", "--release", "--target-dir", target)
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		switch k {
		case "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR", "RUSTC_WRAPPER":
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building guests/%s failed: %v\n%s", name, err, out)
	}
	return filepath.Join(target, "wasm32-wasip2", "release", strings.ReplaceAll(name, "-", "_")+".wasm")
}

func TestReadyLinePattern(t *testing.T) {
	if ReadyLinePattern != `^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$` {
		t.Fatal("ready-line pattern changed")
	}
	m := ReadyLine.FindStringSubmatch("ostro dev listening on http://127.0.0.1:1 (data: /tmp/a b, ephemeral) ready in 5 ms")
	if m == nil || m[2] != "127.0.0.1:1" || m[3] != "/tmp/a b" || m[4] == "" {
		t.Fatalf("%q", m)
	}
}

func TestThreeDaySleepAndSignal(t *testing.T) {
	env := Start(t, WithBinary(bin(t)))
	if !env.Ephemeral {
		t.Fatal("not ephemeral")
	}
	env.DeployWasm(guest(t, "p10-time"), "p10-time")

	t0 := time.Now()
	run := env.StartRun("sleep_for", map[string]any{"ms": 3 * dayMs})
	res := env.Result(run, 30*time.Second)
	wall := time.Since(t0)
	if !res.Completed || res.Status != "completed" {
		t.Fatalf("%+v", res)
	}
	if wall > 10*time.Second {
		t.Fatalf("wall %v", wall)
	}
	if d := env.NowMs() - run.AcceptedAtMs; d < 3*dayMs {
		t.Fatalf("only %d ms of server time", d)
	}
	timers := env.Timers(run)
	if len(timers) != 1 || !timers[0].Fired || timers[0].FireAtMs != run.AcceptedAtMs+3*dayMs {
		t.Fatalf("%+v", timers)
	}
	ev := env.History(run)
	if ev[0].Kind != "run_started" || ev[len(ev)-1].Kind != "run_completed" {
		t.Fatalf("%v .. %v", ev[0].Kind, ev[len(ev)-1].Kind)
	}
	env.AssertCompleted(run, map[string]any{"slept": 3 * dayMs})
	t.Logf("kit wall time: 3-day sleep_for %v", wall)

	sig := env.StartRun("signal_then_sleep", map[string]any{})
	env.Signal(sig, "go", map[string]any{"ok": 7})
	env.AssertCompleted(sig, map[string]any{"ok": 7})
	if len(env.Steps(sig)) != 0 {
		t.Fatal("unexpected steps")
	}

	before := env.NowMs()
	if s := env.Skip(72 * time.Hour); s.SkippedMs < 72*3_600_000 {
		t.Fatalf("%+v", s)
	}
	if env.NowMs()-before < 72*3_600_000 {
		t.Fatal("skip did not advance the clock")
	}
}

func TestLoopbackHTTPMock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/hello" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("hello from the mock"))
	}))
	defer srv.Close()
	env := Start(t, WithBinary(bin(t)), WithEgressLoopback())
	env.DeployWasm(guest(t, "fx-http")) // module "fx-http" (file stem)
	env.AllowEgress("fx-http", srv.URL)
	run := env.StartRun("fx-http", map[string]any{
		"requests": []any{map[string]any{"method": "GET", "url": srv.URL + "/hello"}},
		"parallel": false,
	})
	out := env.AssertCompleted(run, nil).(map[string]any)
	r := out["results"].([]any)[0].(map[string]any)
	body, _ := base64.StdEncoding.DecodeString(r["body_b64"].(string))
	if r["status"].(float64) != 200 || string(body) != "hello from the mock" {
		t.Fatalf("%v %q", r["status"], body)
	}
}

func TestCloseKillsTheServer(t *testing.T) {
	env, err := StartE(WithBinary(bin(t)))
	if err != nil {
		t.Fatal(err)
	}
	pid := env.Pid()
	env.Close()
	env.Close() // idempotent
	if runtime.GOOS != "linux" {
		return
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat("/proc/" + strconv.Itoa(pid)); err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still alive", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMissingOstroBin(t *testing.T) {
	t.Setenv("OSTRO_BIN", "/nonexistent/ostro-kit-test")
	_, err := Bin("")
	if err == nil || !strings.Contains(err.Error(), "OSTRO_BIN=/nonexistent/ostro-kit-test") {
		t.Fatalf("%v", err)
	}
	if _, err := StartE(); err == nil || !strings.Contains(err.Error(), "OSTRO_BIN") {
		t.Fatalf("%v", err)
	}
}
