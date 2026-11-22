package ostro

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorded struct {
	Method, Path, RawPath string
	Query                 url.Values
	Header                http.Header
	Body                  []byte
}

type stub struct {
	mu        sync.Mutex
	reqs      []recorded
	responses []struct {
		status int
		body   string
	}
	delay time.Duration
	srv   *httptest.Server
}

func newStub(t *testing.T) *stub {
	s := &stub{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, recorded{r.Method, r.URL.Path, r.URL.EscapedPath(), r.URL.Query(), r.Header, b})
		status, body := 200, "{}"
		if len(s.responses) > 0 {
			status, body = s.responses[0].status, s.responses[0].body
			s.responses = s.responses[1:]
		}
		delay := s.delay
		s.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *stub) reply(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, struct {
		status int
		body   string
	}{status, body})
}

func (s *stub) last(t *testing.T) recorded {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reqs) == 0 {
		t.Fatal("no request recorded")
	}
	return s.reqs[len(s.reqs)-1]
}

func jsonBody(t *testing.T, r recorded) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Body, &m); err != nil {
		t.Fatalf("body %q: %v", r.Body, err)
	}
	return m
}

func eq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

var ctx = context.Background()

func TestStartRunPostsJSON(t *testing.T) {
	s := newStub(t)
	s.reply(201, `{"run_id":"r1","created":true}`)
	c := NewClient(s.srv.URL)
	out, err := c.StartRun(ctx, StartRunRequest{
		Workflow: "order", Input: map[string]any{"id": 7}, IdempotencyKey: "k1",
		Tags: []string{"a=b"}, Extra: map[string]any{"priority": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, *out, StartedRun{RunID: "r1", Created: true})
	r := s.last(t)
	eq(t, r.Method, "POST")
	eq(t, r.Path, "/v1/workflows/order/runs")
	eq(t, r.Header.Get("Content-Type"), "application/json")
	eq(t, jsonBody(t, r), map[string]any{
		"input": map[string]any{"id": 7.0}, "idempotency_key": "k1", "tags": []any{"a=b"}, "priority": 3.0,
	})
}

func TestTenantQualifiedSegmentIsEscaped(t *testing.T) {
	s := newStub(t)
	c := NewClient(s.srv.URL)
	if _, err := c.StartRun(ctx, StartRunRequest{Workflow: "acme/order"}); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last(t).RawPath, "/v1/workflows/acme%2Forder/runs")
	if _, err := c.Signal(ctx, "r 1", "go/now", map[string]any{"ok": true}, "q"); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last(t).RawPath, "/v1/runs/r%201/signals/go%2Fnow")
	eq(t, jsonBody(t, s.last(t)), map[string]any{"payload": map[string]any{"ok": true}, "request_id": "q"})
}

func TestResultSendsWaitAndExtendsTheTimeout(t *testing.T) {
	s := newStub(t)
	s.delay = 400 * time.Millisecond
	s.reply(200, `{"run_id":"r1","completed":true,"status":"completed","result":1}`)
	c := NewClient(s.srv.URL)
	c.Timeout = 200 * time.Millisecond // the 1 s wait is added on top
	r, err := c.Result(ctx, "r1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(r.Result), "1")
	eq(t, s.last(t).Path, "/v1/runs/r1/result")
	eq(t, s.last(t).Query.Get("wait"), "1000ms")
}

func TestResultWithoutWaitPollsOnce(t *testing.T) {
	s := newStub(t)
	s.reply(200, `{"run_id":"r1","completed":false}`)
	r, err := NewClient(s.srv.URL).Result(ctx, "r1", 0)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.Completed, false)
	eq(t, s.last(t).Query.Get("wait"), "0ms")
	eq(t, len(s.reqs), 1)
}

func TestErrorBodyMapsToError(t *testing.T) {
	s := newStub(t)
	s.reply(409, `{"error":{"code":"run_terminal","message":"run is completed","details":{"status":"completed"}}}`)
	_, err := NewClient(s.srv.URL).Cancel(ctx, "r1", "x")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("not an *ostro.Error: %v", err)
	}
	eq(t, e.Status, 409)
	eq(t, e.Code, "run_terminal")
	eq(t, e.Message, "run is completed")
	eq(t, e.Details, map[string]any{"status": "completed"})
	eq(t, jsonBody(t, s.last(t)), map[string]any{"reason": "x"})
	if !strings.Contains(err.Error(), "409 run_terminal") {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestNonJSONErrorIsHTTPStatus(t *testing.T) {
	s := newStub(t)
	s.reply(502, "bad gateway")
	_, err := NewClient(s.srv.URL).GetRun(ctx, "r1")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatal(err)
	}
	eq(t, [3]any{e.Status, e.Code, e.Message}, [3]any{502, "http_502", "bad gateway"})
}

func TestReason(t *testing.T) {
	s := newStub(t)
	s.reply(409, `{"error":{"code":"failed_precondition","message":"off","details":{"reason":"time_skipping_disabled"}}}`)
	_, err := NewClient(s.srv.URL).DevTime(ctx)
	var e *Error
	if !errors.As(err, &e) || e.Reason() != "time_skipping_disabled" {
		t.Fatalf("%v", err)
	}
}

func TestConnectionRefusedIsUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	_, err = NewClient("http://"+addr).GetRun(ctx, "r1")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatal(err)
	}
	eq(t, e.Code, "unreachable")
	eq(t, e.Status, 0)
}

func TestContextDeadlineIsHonoured(t *testing.T) {
	s := newStub(t)
	s.delay = 2 * time.Second
	c := NewClient(s.srv.URL)
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	_, err := c.GetRun(cctx, "r1")
	if el := time.Since(t0); el > time.Second {
		t.Fatalf("took %v", el)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatal(err)
	}
	eq(t, e.Code, "timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is DeadlineExceeded: %v", err)
	}
	// The client timeout alone also gives "timeout".
	c.Timeout = 100 * time.Millisecond
	_, err = c.GetRun(ctx, "r1")
	if !errors.As(err, &e) || e.Code != "timeout" {
		t.Fatalf("%v", err)
	}
}

func TestUploadModulePutsWasmUnderItsSHA(t *testing.T) {
	s := newStub(t)
	wasm := []byte("\x00asm\x01\x00\x00\x00")
	s.reply(201, `{"created":true,"module":{"sha256":"x","workflows":[{"name":"w","version":1}]}}`)
	out, err := NewClient(s.srv.URL).UploadModule(ctx, wasm, "m", "acme")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, out.Module.Workflows, []WorkflowVersion{{"w", 1}})
	sum := sha256.Sum256(wasm)
	r := s.last(t)
	eq(t, r.Method, "PUT")
	eq(t, r.Path, "/v1/modules/"+hex.EncodeToString(sum[:]))
	eq(t, r.Query, url.Values{"name": {"m"}, "tenant": {"acme"}})
	eq(t, r.Header.Get("Content-Type"), "application/wasm")
	eq(t, r.Body, wasm)
}

func TestListRunsAndHistoryQuery(t *testing.T) {
	s := newStub(t)
	c := NewClient(s.srv.URL)
	if _, err := c.ListRuns(ctx, RunFilter{Workflow: "w", Tags: []string{"a", "b"}, Limit: 5}); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last(t).Path, "/v1/runs")
	eq(t, s.last(t).Query, url.Values{"workflow": {"w"}, "tag": {"a", "b"}, "limit": {"5"}})
	s.reply(200, `{"events":[{"seq":1,"kind":"run_started","x":2}],"next_from_seq":null}`)
	h, err := c.History(ctx, "r1", 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, s.last(t).Query, url.Values{"from_seq": {"3"}, "limit": {"10"}})
	eq(t, [2]any{h.Events[0].Seq, h.Events[0].Kind}, [2]any{uint64(1), "run_started"})
	eq(t, h.Events[0].Raw["x"], 2.0)
}

func TestDevSkipAndReplayBodies(t *testing.T) {
	s := newStub(t)
	c := NewClient(s.srv.URL)
	if _, err := c.DevSkipBy(ctx, 72*time.Hour); err != nil {
		t.Fatal(err)
	}
	eq(t, jsonBody(t, s.last(t)), map[string]any{"by": "259200000ms"})
	if _, err := c.DevSkipTo(ctx, 5); err != nil {
		t.Fatal(err)
	}
	eq(t, jsonBody(t, s.last(t)), map[string]any{"to_ms": 5.0})
	if _, err := c.Replay(ctx, "abc", ReplayRequest{Runs: []string{"r1"}, AllowMissing: true}); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last(t).Path, "/v1/modules/abc/replay")
	eq(t, jsonBody(t, s.last(t)), map[string]any{"runs": []any{"r1"}, "allow_missing": true})
	if _, err := c.Replay(ctx, "abc", ReplayRequest{Sample: 5, Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	eq(t, jsonBody(t, s.last(t)), map[string]any{"sample": 5.0, "workflow": "w"})
	if _, err := c.Replay(ctx, "abc", ReplayRequest{}); err == nil {
		t.Fatal("replay without runs/sample accepted")
	}
}

func TestNewClientDefaults(t *testing.T) {
	t.Setenv("OSTRO_SERVER", "127.0.0.1:9")
	eq(t, NewClient("").BaseURL, "http://127.0.0.1:9")
	t.Setenv("OSTRO_SERVER", "")
	eq(t, NewClient("").BaseURL, DefaultServer)
	eq(t, NewClient("http://h:1/").BaseURL, "http://h:1")
}

// Every exported *Client method, called with sample arguments: its route must be in Routes.
func TestEveryMethodRouteIsDeclared(t *testing.T) {
	s := newStub(t)
	s.mu.Lock()
	for i := 0; i < 32; i++ {
		s.responses = append(s.responses, struct {
			status int
			body   string
		}{200, `{"completed":true,"workflows":[]}`})
	}
	s.mu.Unlock()
	c := NewClient(s.srv.URL)
	calls := map[string]func() error{
		"UploadModule":  func() error { _, e := c.UploadModule(ctx, []byte{1}, "", ""); return e },
		"Workflows":     func() error { _, e := c.Workflows(ctx); return e },
		"Activate":      func() error { _, e := c.Activate(ctx, "w", 1); return e },
		"StartRun":      func() error { _, e := c.StartRun(ctx, StartRunRequest{Workflow: "w"}); return e },
		"GetRun":        func() error { _, e := c.GetRun(ctx, "r"); return e },
		"ListRuns":      func() error { _, e := c.ListRuns(ctx, RunFilter{}); return e },
		"History":       func() error { _, e := c.History(ctx, "r", 0, 0); return e },
		"Result":        func() error { _, e := c.Result(ctx, "r", 0); return e },
		"Signal":        func() error { _, e := c.Signal(ctx, "r", "s", nil, ""); return e },
		"Cancel":        func() error { _, e := c.Cancel(ctx, "r", ""); return e },
		"Terminate":     func() error { _, e := c.Terminate(ctx, "r", ""); return e },
		"Retry":         func() error { _, e := c.Retry(ctx, "r"); return e },
		"CompleteToken": func() error { _, e := c.CompleteToken(ctx, "t", nil); return e },
		"DevTime":       func() error { _, e := c.DevTime(ctx); return e },
		"DevSkipBy":     func() error { _, e := c.DevSkipBy(ctx, time.Hour); return e },
		"DevSkipTo":     func() error { _, e := c.DevSkipTo(ctx, 1); return e },
		"Replay":        func() error { _, e := c.Replay(ctx, "sha", ReplayRequest{Runs: []string{"r"}}); return e },
	}
	ty := reflect.TypeOf(c)
	var exported, listed []string
	for i := 0; i < ty.NumMethod(); i++ {
		exported = append(exported, ty.Method(i).Name)
	}
	for k := range calls {
		listed = append(listed, k)
	}
	sort.Strings(listed)
	eq(t, exported, listed) // reflect lists methods sorted

	var used [][2]string
	routeHook = func(r [2]string) { used = append(used, r) }
	defer func() { routeHook = nil }()
	for name, call := range calls {
		n := len(used)
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(used) == n {
			t.Fatalf("%s sent no request", name)
		}
	}
	declared := map[[2]string]bool{}
	for _, r := range Routes {
		declared[r] = true
	}
	seen := map[[2]string]bool{}
	for _, r := range used {
		if !declared[r] {
			t.Errorf("route %v not in Routes", r)
		}
		seen[r] = true
	}
	eq(t, len(seen), len(declared))
}
