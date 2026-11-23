// Package ostrotest is the Go test kit for Ostro (SDK-06): Start spawns a throwaway
// `ostro dev --ephemeral --time-skipping` for one test and drives it through the REST
// client (package ostro).
//
// The server skips its dev clock forward whenever it is idle and a result long-poll is
// waiting, so Result on a workflow that sleeps for days returns in milliseconds of wall
// time:
//
//	func TestHello(t *testing.T) {
//		env := ostrotest.Start(t)
//		env.DeployWasm("hello.wasm")
//		run := env.StartRun("hello", map[string]any{"name": "ada"})
//		env.Signal(run, "approve", map[string]any{"ok": true})
//		res := env.Result(run, 30*time.Second) // skips the 3-day sleep
//		_ = res
//	}
//
// Go has no workflow SDK: the kit deploys components built in another language.
// The binary is $OSTRO_BIN, else `ostro` on PATH (WithBinary overrides both). Its
// `ostro build-info --json` version must be at least Version. Every method fails the
// test (t.Fatalf) on error; the server is killed by t.Cleanup.
package ostrotest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lspecian/ostro/clients/go/ostro"
)

// ReadyLinePattern is the frozen ready line (D-P10-9, amended); the same in every kit.
const ReadyLinePattern = `^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`

// ReadyLine is ReadyLinePattern compiled.
var ReadyLine = regexp.MustCompile(ReadyLinePattern)

// Retention is the run retention given to the server: skipped days must not expire runs
// under test.
const Retention = "87600h"

// Version is the kit version; the binary must be at least this version.
const Version = "0.1.0"

const readyTimeout = 30 * time.Second

// Option configures Start.
type Option func(*options)

type options struct {
	binary         string
	egressLoopback bool
	timeSkipping   string
	env            []string
}

// WithBinary uses this ostro binary instead of $OSTRO_BIN / PATH.
func WithBinary(path string) Option { return func(o *options) { o.binary = path } }

// WithEgressLoopback lets effects reach 127.0.0.0/8 ([egress] allow_private) for loopback
// HTTP mocks. A module still needs AllowEgress.
func WithEgressLoopback() Option { return func(o *options) { o.egressLoopback = true } }

// WithTimeSkipping sets the mode: "auto" (default) or "manual" (skips only through Skip).
func WithTimeSkipping(mode string) Option { return func(o *options) { o.timeSkipping = mode } }

// WithEnv adds a server environment variable.
func WithEnv(key, value string) Option {
	return func(o *options) { o.env = append(o.env, key+"="+value) }
}

// Run is a started run; AcceptedAtMs is server (dev clock) time.
type Run struct {
	ID           string
	Workflow     string
	AcceptedAtMs int64
}

// Timer is one durable timer of a run.
type Timer struct {
	Seq       uint64
	StartedMs int64
	FireAtMs  int64
	Fired     bool
	FiredSeq  uint64
	FiredMs   int64
}

// Env is a running `ostro dev --ephemeral --time-skipping`.
type Env struct {
	t         testing.TB
	cmd       *exec.Cmd
	done      chan struct{}
	closeOnce sync.Once
	cwd       string
	stderr    *tail

	// Binary is the ostro binary in use.
	Binary string
	// Addr is the listen address, URL "http://"+Addr.
	Addr, URL string
	// DataDir and Ephemeral come from the ready line.
	DataDir   string
	Ephemeral bool
	// Client is the underlying REST client.
	Client *ostro.Client

	mu      sync.Mutex
	modules map[string]string
}

type tail struct {
	mu  sync.Mutex
	buf []byte
}

func (b *tail) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > 128<<10 {
		b.buf = append([]byte(nil), b.buf[len(b.buf)-(64<<10):]...)
	}
	return len(p), nil
}

func (b *tail) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// Bin resolves the binary: explicit, else $OSTRO_BIN, else `ostro` on PATH.
func Bin(explicit string) (string, error) {
	isFile := func(p string) bool { st, err := os.Stat(p); return err == nil && st.Mode().IsRegular() }
	if explicit != "" {
		if !isFile(explicit) {
			return "", fmt.Errorf("the ostro binary %s does not exist", explicit)
		}
		return explicit, nil
	}
	if p := os.Getenv("OSTRO_BIN"); p != "" {
		if !isFile(p) {
			return "", fmt.Errorf("OSTRO_BIN=%s does not exist: point OSTRO_BIN at a built ostro binary (cargo build -p ostro) or unset it to use `ostro` on PATH", p)
		}
		return p, nil
	}
	p, err := exec.LookPath("ostro")
	if err != nil {
		return "", errors.New("no `ostro` on PATH and OSTRO_BIN is not set: install ostro or point OSTRO_BIN at the binary")
	}
	return p, nil
}

func triple(v string) ([3]int, bool) {
	var out [3]int
	core := strings.FieldsFunc(v, func(r rune) bool { return r == '-' || r == '+' })
	if len(core) == 0 {
		return out, false
	}
	parts := strings.Split(core[0], ".")
	if len(parts) < 2 {
		return out, false
	}
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

// CheckVersion runs `ostro build-info --json` and returns its version; an error when it
// is older than Version.
func CheckVersion(bin string) (string, error) {
	out, err := exec.Command(bin, "build-info", "--json").Output()
	if err != nil {
		return "", fmt.Errorf("`%s build-info --json` failed: %w", bin, err)
	}
	var info struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return "", fmt.Errorf("`%s build-info --json` printed no JSON: %w", bin, err)
	}
	have, ok1 := triple(info.Version)
	need, _ := triple(Version)
	if !ok1 || have[0] < need[0] || (have[0] == need[0] && (have[1] < need[1] || (have[1] == need[1] && have[2] < need[2]))) {
		return "", fmt.Errorf("%s is version %q; this test kit needs ostro >= %s", bin, info.Version, Version)
	}
	return info.Version, nil
}

func serverEnv(o *options) []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		// Inherited server configuration (OSTRO_<SECTION>__*) and RUST_LOG are dropped.
		if k == "RUST_LOG" || k == "OSTRO_LISTEN" || (strings.HasPrefix(k, "OSTRO_") && strings.Contains(k, "__")) {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "OSTRO_RUNS__RETENTION="+Retention)
	if o.egressLoopback {
		env = append(env, "OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8")
	}
	return append(env, o.env...)
}

// StartE spawns the server and waits (30 s) for its ready line. Close stops it.
func StartE(opts ...Option) (*Env, error) {
	o := &options{timeSkipping: "auto"}
	for _, f := range opts {
		f(o)
	}
	bin, err := Bin(o.binary)
	if err != nil {
		return nil, err
	}
	if _, err := CheckVersion(bin); err != nil {
		return nil, err
	}
	cwd, err := os.MkdirTemp("", "ostro-testenv-")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, "dev", "--ephemeral", "--listen", "127.0.0.1:0", "--time-skipping="+o.timeSkipping)
	cmd.Dir = cwd
	cmd.Env = serverEnv(o)
	stderr := &tail{}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.RemoveAll(cwd)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		os.RemoveAll(cwd)
		return nil, fmt.Errorf("cannot spawn %s dev: %w", bin, err)
	}
	ready := make(chan []string, 1)
	go func() {
		// Drained to the end: the server logs on stdout.
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		sent := false
		for sc.Scan() {
			if !sent {
				if m := ReadyLine.FindStringSubmatch(strings.TrimSuffix(sc.Text(), "\r")); m != nil {
					ready <- m
					sent = true
				}
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
		close(ready)
	}()
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	var m []string
	select {
	case m = <-ready:
	case <-time.After(readyTimeout):
	}
	if m == nil {
		_ = cmd.Process.Kill()
		<-done
		os.RemoveAll(cwd)
		return nil, fmt.Errorf("ostro dev printed no ready line (%s); stderr:\n%s", bin, stderr.String())
	}
	addr := m[2]
	return &Env{
		cmd: cmd, done: done, cwd: cwd, stderr: stderr,
		Binary: bin, Addr: addr, URL: "http://" + addr, DataDir: m[3], Ephemeral: m[4] != "",
		Client: ostro.NewClient("http://" + addr), modules: map[string]string{},
	}, nil
}

// Start is StartE for a test: it fails t on error and registers Close with t.Cleanup.
func Start(t testing.TB, opts ...Option) *Env {
	t.Helper()
	e, err := StartE(opts...)
	if err != nil {
		t.Fatalf("ostrotest: %v", err)
	}
	e.t = t
	t.Cleanup(e.Close)
	return e
}

// Close kills and reaps the server and removes its temp directory. Idempotent.
func (e *Env) Close() {
	e.closeOnce.Do(func() {
		_ = e.cmd.Process.Kill()
		<-e.done
		os.RemoveAll(e.cwd)
	})
}

// Pid is the server's process id.
func (e *Env) Pid() int { return e.cmd.Process.Pid }

// Stderr is the tail of the server's stderr.
func (e *Env) Stderr() string { return e.stderr.String() }

func (e *Env) fatal(err error, what string) {
	if err == nil {
		return
	}
	if e.t == nil {
		panic(fmt.Sprintf("ostrotest: %s: %v", what, err))
	}
	e.t.Helper()
	e.t.Fatalf("ostrotest: %s: %v", what, err)
}

func (e *Env) remember(module string, workflows []string) {
	if module == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, w := range workflows {
		e.modules[w] = module
	}
	e.modules[module] = module
}

// BuildAndDeploy runs `ostro deploy <dir> --activate --json --server <url>` (build if
// stale, upload as module = project name, activate new versions) and returns its JSON.
// The server refuses a changed module under an existing workflow name and version: bump
// the version in the source to redeploy a change into the same server.
func (e *Env) BuildAndDeploy(dir string) map[string]any {
	if e.t != nil {
		e.t.Helper()
	}
	cmd := exec.Command(e.Binary, "deploy", dir, "--activate", "--json", "--server", e.URL)
	var errb strings.Builder
	cmd.Stderr = &errb
	out, err := cmd.Output()
	if err != nil {
		e.fatal(fmt.Errorf("%w\n%s%s", err, errb.String(), out), "ostro deploy "+dir)
	}
	var v map[string]any
	e.fatal(json.Unmarshal(out, &v), "ostro deploy --json output")
	if b, ok := v["build"].(map[string]any); ok {
		name, _ := b["name"].(string)
		var wfs []string
		if ws, ok := b["workflows"].([]any); ok {
			for _, w := range ws {
				if m, ok := w.(map[string]any); ok {
					if n, ok := m["name"].(string); ok {
						wfs = append(wfs, n)
					}
				}
			}
		}
		e.remember(name, wfs)
	}
	return v
}

// DeployWasm uploads a prebuilt component as module name (default: the file stem with
// "_" -> "-") and activates every workflow version it declares.
func (e *Env) DeployWasm(path string, name ...string) *ostro.UploadOutcome {
	if e.t != nil {
		e.t.Helper()
	}
	wasm, err := os.ReadFile(path)
	e.fatal(err, "read "+path)
	n := strings.ReplaceAll(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)), "_", "-")
	if len(name) > 0 && name[0] != "" {
		n = name[0]
	}
	return e.DeployWasmBytes(wasm, n)
}

// DeployWasmBytes uploads component bytes as module name and activates its workflows.
func (e *Env) DeployWasmBytes(wasm []byte, name string) *ostro.UploadOutcome {
	if e.t != nil {
		e.t.Helper()
	}
	ctx := context.Background()
	up, err := e.Client.UploadModule(ctx, wasm, name, "")
	e.fatal(err, "upload "+name)
	var wfs []string
	for _, w := range up.Module.Workflows {
		_, err := e.Client.Activate(ctx, w.Name, w.Version)
		e.fatal(err, "activate "+w.Name)
		wfs = append(wfs, w.Name)
	}
	e.remember(name, wfs)
	return up
}

// StartRun starts a run of the active version of workflow.
func (e *Env) StartRun(workflow string, input any) Run {
	if e.t != nil {
		e.t.Helper()
	}
	return e.StartRunWith(ostro.StartRunRequest{Workflow: workflow, Input: input})
}

// StartRunWith starts a run with every StartRunRequest option.
func (e *Env) StartRunWith(r ostro.StartRunRequest) Run {
	if e.t != nil {
		e.t.Helper()
	}
	ctx := context.Background()
	s, err := e.Client.StartRun(ctx, r)
	e.fatal(err, "start "+r.Workflow)
	info, err := e.Client.GetRun(ctx, s.RunID)
	e.fatal(err, "get run "+s.RunID)
	var at int64
	if info.CreatedAtMs != nil {
		at = *info.CreatedAtMs
	}
	return Run{ID: s.RunID, Workflow: info.Workflow, AcceptedAtMs: at}
}

// Signal sends signal name with payload to run.
func (e *Env) Signal(run Run, name string, payload any) {
	if e.t != nil {
		e.t.Helper()
	}
	_, err := e.Client.Signal(context.Background(), run.ID, name, payload, "")
	e.fatal(err, "signal "+name)
}

// Result waits up to wait for the run's outcome; the wait lets the server skip idle time.
// Completed is false when the wait ended first.
func (e *Env) Result(run Run, wait time.Duration) *ostro.RunResult {
	if e.t != nil {
		e.t.Helper()
	}
	r, err := e.Client.Result(context.Background(), run.ID, wait)
	e.fatal(err, "result "+run.ID)
	return r
}

// AssertCompleted waits (30 s) for run, fails the test unless it completed and returns its
// output decoded as generic JSON. want (when non-nil) must deep-equal that output after a
// JSON round trip.
func (e *Env) AssertCompleted(run Run, want any) any {
	if e.t != nil {
		e.t.Helper()
	}
	r := e.Result(run, 30*time.Second)
	if !r.Completed || r.Status != "completed" {
		e.fatal(fmt.Errorf("status %q (completed %v): %s", r.Status, r.Completed, r.Failure), "run "+run.ID+" did not complete")
	}
	var got any
	if len(r.Result) > 0 {
		e.fatal(json.Unmarshal(r.Result, &got), "decode output")
	}
	if want != nil {
		b, err := json.Marshal(want)
		e.fatal(err, "encode want")
		var w any
		e.fatal(json.Unmarshal(b, &w), "decode want")
		if !reflect.DeepEqual(got, w) {
			e.fatal(fmt.Errorf("got %s, want %s", r.Result, b), "output of run "+run.ID)
		}
	}
	return got
}

// Skip skips the dev clock forward by d (503 busy while activations are in flight).
func (e *Env) Skip(d time.Duration) *ostro.DevSkip {
	if e.t != nil {
		e.t.Helper()
	}
	s, err := e.Client.DevSkipBy(context.Background(), d)
	e.fatal(err, "skip")
	return s
}

// NowMs is the server (dev clock) time in ms since the epoch.
func (e *Env) NowMs() int64 {
	if e.t != nil {
		e.t.Helper()
	}
	t, err := e.Client.DevTime(context.Background())
	e.fatal(err, "dev time")
	return t.NowMs
}

// History returns every history event of run (all pages).
func (e *Env) History(run Run) []ostro.HistoryEvent {
	if e.t != nil {
		e.t.Helper()
	}
	var events []ostro.HistoryEvent
	var from uint64
	for {
		p, err := e.Client.History(context.Background(), run.ID, from, 0)
		e.fatal(err, "history "+run.ID)
		events = append(events, p.Events...)
		if p.NextFromSeq == nil || *p.NextFromSeq == from {
			return events
		}
		from = *p.NextFromSeq
	}
}

func num(v any) int64 {
	f, _ := v.(float64)
	return int64(f)
}

func details(ev ostro.HistoryEvent) map[string]any {
	d, _ := ev.Raw["details"].(map[string]any)
	return d
}

// Steps returns the names of the completed steps (step_completed with an ok outcome).
func (e *Env) Steps(run Run) []string {
	if e.t != nil {
		e.t.Helper()
	}
	names := []string{}
	for _, ev := range e.History(run) {
		if ev.Kind != "step_completed" {
			continue
		}
		d := details(ev)
		if o, ok := d["outcome"].(map[string]any); ok {
			if _, ok := o["ok"]; ok {
				n, _ := d["name"].(string)
				names = append(names, n)
			}
		}
	}
	return names
}

// Timers returns each timer_started with its timer_fired.
func (e *Env) Timers(run Run) []Timer {
	if e.t != nil {
		e.t.Helper()
	}
	events := e.History(run)
	var timers []Timer
	for _, ev := range events {
		if ev.Kind == "timer_started" {
			timers = append(timers, Timer{Seq: ev.Seq, StartedMs: num(ev.Raw["ts_ms"]), FireAtMs: num(details(ev)["fire_at_ms"])})
		}
	}
	for _, ev := range events {
		if ev.Kind != "timer_fired" {
			continue
		}
		op := uint64(num(details(ev)["op"]))
		for i := range timers {
			if timers[i].Seq == op {
				timers[i].Fired, timers[i].FiredSeq, timers[i].FiredMs = true, ev.Seq, num(ev.Raw["ts_ms"])
			}
		}
	}
	return timers
}

// AllowEgress lets a module (or a workflow deployed through this kit) call urlPrefix (an
// egress rule such as "http://127.0.0.1:8080") through PUT /v1/egress/{module}, keeping
// its existing rules. Loopback targets also need WithEgressLoopback.
func (e *Env) AllowEgress(moduleOrWorkflow, urlPrefix string) {
	if e.t != nil {
		e.t.Helper()
	}
	e.mu.Lock()
	module, ok := e.modules[moduleOrWorkflow]
	e.mu.Unlock()
	if !ok {
		module = moduleOrWorkflow
	}
	u := e.URL + "/v1/egress/" + url.PathEscape(module)
	var cur struct {
		Allow []string `json:"allow"`
	}
	resp, err := e.Client.HTTP.Get(u)
	e.fatal(err, "GET egress")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		e.fatal(json.Unmarshal(b, &cur), "decode egress")
	case http.StatusNotFound:
	default:
		e.fatal(fmt.Errorf("HTTP %d: %s", resp.StatusCode, b), "GET egress")
	}
	rule := strings.TrimRight(urlPrefix, "/")
	found := false
	for _, r := range cur.Allow {
		found = found || r == rule
	}
	if !found {
		cur.Allow = append(cur.Allow, rule)
	}
	body, _ := json.Marshal(map[string]any{"allow": cur.Allow})
	req, _ := http.NewRequest(http.MethodPut, u, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = e.Client.HTTP.Do(req)
	e.fatal(err, "PUT egress")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		e.fatal(fmt.Errorf("HTTP %d: %s", resp.StatusCode, b), "PUT egress")
	}
}
