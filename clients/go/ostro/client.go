package ostro

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultServer is the address `ostro dev` listens on.
const DefaultServer = "http://127.0.0.1:7700"

// DefaultTimeout bounds every request (a result long-poll gets its wait on top).
const DefaultTimeout = 30 * time.Second

// maxPoll: the server caps one result long-poll at 60 s; longer waits loop.
const maxPoll = 60 * time.Second

// replayExtra: extra time a replay request may take on top of the client timeout.
const replayExtra = 300 * time.Second

// Client talks to one Ostro server over REST. The zero value is not usable; use NewClient.
type Client struct {
	// BaseURL is the server, e.g. "http://127.0.0.1:7700".
	BaseURL string
	// HTTP is the underlying client (no Timeout of its own; Timeout below applies).
	HTTP *http.Client
	// Timeout bounds every request; a result long-poll gets its wait on top. Zero means
	// DefaultTimeout.
	Timeout time.Duration
}

// NewClient returns a client for server; empty means $OSTRO_SERVER, else DefaultServer.
func NewClient(server string) *Client {
	if server == "" {
		server = os.Getenv("OSTRO_SERVER")
	}
	if server == "" {
		server = DefaultServer
	}
	if !strings.Contains(server, "://") {
		server = "http://" + server
	}
	return &Client{BaseURL: strings.TrimRight(server, "/"), HTTP: &http.Client{}, Timeout: DefaultTimeout}
}

// --- types --------------------------------------------------------------------------

// WorkflowVersion is one workflow a module defines.
type WorkflowVersion struct {
	Name    string `json:"name"`
	Version uint64 `json:"version"`
}

// ModuleInfo describes an uploaded module.
type ModuleInfo struct {
	SHA256    string            `json:"sha256"`
	SizeBytes uint64            `json:"size_bytes"`
	Workflows []WorkflowVersion `json:"workflows"`
}

// UploadOutcome is the answer to UploadModule (Created false: it already existed).
type UploadOutcome struct {
	Created bool       `json:"created"`
	Module  ModuleInfo `json:"module"`
}

// WorkflowInfo is one workflow with its versions.
type WorkflowInfo struct {
	Name          string            `json:"name"`
	ActiveVersion *uint64           `json:"active_version"`
	Versions      []json.RawMessage `json:"versions"`
	RecordVersion uint64            `json:"record_version"`
}

// StartRunRequest starts a run of Workflow. Extra holds further body fields of the API
// (snake_case: "retention", "priority", "delay_ms", ...).
type StartRunRequest struct {
	Workflow       string
	Input          any
	IdempotencyKey string
	ConflictPolicy string
	Tags           []string
	Version        uint64
	Queue          string
	Extra          map[string]any
}

// StartedRun is the answer to StartRun.
type StartedRun struct {
	RunID   string `json:"run_id"`
	Created bool   `json:"created"`
	DryRun  bool   `json:"dry_run"`
}

// RunInfo is a run's record.
type RunInfo struct {
	RunID         string          `json:"run_id"`
	Workflow      string          `json:"workflow"`
	Version       uint64          `json:"version"`
	ModuleSHA256  string          `json:"module_sha256"`
	Queue         string          `json:"queue"`
	Status        string          `json:"status"`
	Tags          []string        `json:"tags"`
	Result        json.RawMessage `json:"result"`
	Failure       json.RawMessage `json:"failure"`
	CreatedAtMs   *int64          `json:"created_at_ms"`
	CompletedAtMs *int64          `json:"completed_at_ms"`
}

// RunFilter selects runs for ListRuns (empty fields are not sent).
type RunFilter struct {
	Workflow  string
	Status    string
	Queue     string
	Tags      []string
	Limit     int
	PageToken string
}

// RunList is one page of runs.
type RunList struct {
	Runs          []RunInfo `json:"runs"`
	NextPageToken *string   `json:"next_page_token"`
}

// HistoryEvent is one journal event (Kind e.g. "timer_started"); Raw is the whole object.
type HistoryEvent struct {
	Seq  uint64
	Kind string
	Raw  map[string]any
}

// UnmarshalJSON keeps the whole event in Raw.
func (e *HistoryEvent) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &e.Raw); err != nil {
		return err
	}
	if k, ok := e.Raw["kind"].(string); ok {
		e.Kind = k
	}
	if s, ok := e.Raw["seq"].(float64); ok {
		e.Seq = uint64(s)
	}
	return nil
}

// HistoryPage is one page of a run's history.
type HistoryPage struct {
	Events      []HistoryEvent `json:"events"`
	NextFromSeq *uint64        `json:"next_from_seq"`
}

// RunResult is a run's outcome; Completed is false when the wait ended first.
type RunResult struct {
	RunID     string          `json:"run_id"`
	Completed bool            `json:"completed"`
	Status    string          `json:"status"`
	Result    json.RawMessage `json:"result"`
	Failure   json.RawMessage `json:"failure"`
}

// SignalOutcome is the answer to Signal.
type SignalOutcome struct {
	Accepted  bool `json:"accepted"`
	Duplicate bool `json:"duplicate"`
	DryRun    bool `json:"dry_run"`
}

// CancelOutcome is the answer to Cancel.
type CancelOutcome struct {
	AlreadyRequested bool `json:"already_requested"`
}

// TerminateOutcome is the answer to Terminate.
type TerminateOutcome struct {
	Status string `json:"status"`
}

// RetryOutcome is the answer to Retry.
type RetryOutcome struct {
	Mode  string `json:"mode"`
	RunID string `json:"run_id"`
}

// TokenOutcome is the answer to CompleteToken.
type TokenOutcome struct {
	RunID    string `json:"run_id"`
	Accepted bool   `json:"accepted"`
}

// DevTime is the dev clock (ostro dev --ephemeral --time-skipping).
type DevTime struct {
	NowMs     int64  `json:"now_ms"`
	OffsetMs  int64  `json:"offset_ms"`
	Mode      string `json:"mode"`
	Idle      bool   `json:"idle"`
	NextDueMs *int64 `json:"next_due_ms"`
}

// DevSkip is the answer to DevSkipBy / DevSkipTo.
type DevSkip struct {
	NowMs     int64 `json:"now_ms"`
	OffsetMs  int64 `json:"offset_ms"`
	SkippedMs int64 `json:"skipped_ms"`
}

// ReplayRequest selects runs to replay: Runs (ids), or Sample n optionally filtered by
// Workflow / Status.
type ReplayRequest struct {
	Runs         []string
	Sample       uint64
	Workflow     string
	Status       string
	AllowMissing bool
}

// ReplayVerdict is one run's replay verdict.
type ReplayVerdict struct {
	RunID    string  `json:"run_id"`
	Workflow string  `json:"workflow"`
	Verdict  string  `json:"verdict"`
	Seq      *uint64 `json:"seq"`
	Detail   *string `json:"detail"`
}

// ReplayReport is the answer to Replay.
type ReplayReport struct {
	ModuleSHA256 string          `json:"module_sha256"`
	Verdicts     []ReplayVerdict `json:"verdicts"`
	Summary      struct {
		OK         uint64 `json:"ok"`
		OKFrontier uint64 `json:"ok_frontier"`
		Diverged   uint64 `json:"diverged"`
		Skipped    uint64 `json:"skipped"`
		Error      uint64 `json:"error"`
	} `json:"summary"`
}

// --- transport ----------------------------------------------------------------------

type request struct {
	route       [2]string // {METHOD, template} from Routes (documentation; checked by tests)
	segments    []string
	query       url.Values
	body        any
	raw         []byte
	contentType string
	extra       time.Duration
}

// routeHook, when set (tests), sees every request's route.
var routeHook func(route [2]string)

func (c *Client) url(segments []string, q url.Values) string {
	parts := make([]string, len(segments))
	for i, s := range segments {
		// One escaped segment per value: a tenant-qualified name (acme/w) stays one segment.
		parts[i] = url.PathEscape(s)
	}
	u := c.BaseURL + "/" + strings.Join(parts, "/")
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

func (c *Client) do(ctx context.Context, r request, out any) error {
	if routeHook != nil {
		routeHook(r.route)
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+r.extra)
	defer cancel()
	var body io.Reader
	ct := ""
	switch {
	case r.raw != nil:
		body, ct = bytes.NewReader(r.raw), r.contentType
	case r.body != nil:
		b, err := json.Marshal(r.body)
		if err != nil {
			return &Error{Code: "usage", Message: "cannot encode the request body: " + err.Error(), Err: err}
		}
		body, ct = bytes.NewReader(b), "application/json"
	}
	req, err := http.NewRequestWithContext(ctx, r.route[0], c.url(r.segments, r.query), body)
	if err != nil {
		return &Error{Code: "usage", Message: err.Error(), Err: err}
	}
	req.Header.Set("Accept", "application/json")
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return c.transport(ctx, err)
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.transport(ctx, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return apiError(resp.StatusCode, text)
	}
	if out == nil || len(bytes.TrimSpace(text)) == 0 {
		return nil
	}
	if err := json.Unmarshal(text, out); err != nil {
		return &Error{
			Status:  resp.StatusCode,
			Code:    "http_" + strconv.Itoa(resp.StatusCode),
			Message: fmt.Sprintf("the server at %s answered HTTP %d with an unexpected body: %v", c.BaseURL, resp.StatusCode, err),
			Err:     err,
		}
	}
	return nil
}

func (c *Client) transport(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return &Error{Code: "timeout", Message: "request to the Ostro server at " + c.BaseURL + " timed out", Err: context.DeadlineExceeded}
	case errors.Is(err, context.Canceled):
		return &Error{Code: "cancelled", Message: "request to the Ostro server at " + c.BaseURL + " was cancelled", Err: context.Canceled}
	default:
		return &Error{Code: "unreachable", Message: "cannot reach the Ostro server at " + c.BaseURL + ": " + err.Error(), Err: err}
	}
}

func apiError(status int, text []byte) error {
	var body struct {
		Error *struct {
			Code    string         `json:"code"`
			Message string         `json:"message"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(text, &body) == nil && body.Error != nil {
		code := body.Error.Code
		if code == "" {
			code = "http_" + strconv.Itoa(status)
		}
		d := body.Error.Details
		if d == nil {
			d = map[string]any{}
		}
		return &Error{Status: status, Code: code, Message: body.Error.Message, Details: d}
	}
	return &Error{Status: status, Code: "http_" + strconv.Itoa(status), Message: strings.TrimSpace(string(text)), Details: map[string]any{}}
}

func ms(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) + "ms" }

// SHA256Hex is the lowercase hex SHA-256 of b (a module's id).
func SHA256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// --- modules and workflows ----------------------------------------------------------

// UploadModule uploads a Wasm component: PUT /v1/modules/{sha256}. name and tenant are
// optional ("" = not sent).
func (c *Client) UploadModule(ctx context.Context, wasm []byte, name, tenant string) (*UploadOutcome, error) {
	q := url.Values{}
	if name != "" {
		q.Set("name", name)
	}
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	var out UploadOutcome
	err := c.do(ctx, request{
		route:       [2]string{"PUT", "/v1/modules/{sha256}"},
		segments:    []string{"v1", "modules", SHA256Hex(wasm)},
		query:       q,
		raw:         wasm,
		contentType: "application/wasm",
	}, &out)
	return ret(&out, err)
}

// Workflows lists every workflow: GET /v1/workflows.
func (c *Client) Workflows(ctx context.Context) ([]WorkflowInfo, error) {
	var out struct {
		Workflows []WorkflowInfo `json:"workflows"`
	}
	err := c.do(ctx, request{route: [2]string{"GET", "/v1/workflows"}, segments: []string{"v1", "workflows"}}, &out)
	return out.Workflows, err
}

// Activate makes version the active one: PUT /v1/workflows/{name}/active.
func (c *Client) Activate(ctx context.Context, workflow string, version uint64) (*WorkflowInfo, error) {
	var out WorkflowInfo
	err := c.do(ctx, request{
		route:    [2]string{"PUT", "/v1/workflows/{name}/active"},
		segments: []string{"v1", "workflows", workflow, "active"},
		body:     map[string]any{"version": version},
	}, &out)
	return ret(&out, err)
}

// --- runs ---------------------------------------------------------------------------

// StartRun starts a run: POST /v1/workflows/{name}/runs.
func (c *Client) StartRun(ctx context.Context, r StartRunRequest) (*StartedRun, error) {
	body := map[string]any{}
	for k, v := range r.Extra {
		body[k] = v
	}
	body["input"] = r.Input
	if r.IdempotencyKey != "" {
		body["idempotency_key"] = r.IdempotencyKey
	}
	if r.ConflictPolicy != "" {
		body["conflict_policy"] = r.ConflictPolicy
	}
	if len(r.Tags) > 0 {
		body["tags"] = r.Tags
	}
	if r.Version != 0 {
		body["version"] = r.Version
	}
	if r.Queue != "" {
		body["queue"] = r.Queue
	}
	var out StartedRun
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/workflows/{name}/runs"},
		segments: []string{"v1", "workflows", r.Workflow, "runs"},
		body:     body,
	}, &out)
	return ret(&out, err)
}

// GetRun returns a run's record: GET /v1/runs/{run_id}.
func (c *Client) GetRun(ctx context.Context, runID string) (*RunInfo, error) {
	var out RunInfo
	err := c.do(ctx, request{route: [2]string{"GET", "/v1/runs/{run_id}"}, segments: []string{"v1", "runs", runID}}, &out)
	return ret(&out, err)
}

// ListRuns returns one page of runs: GET /v1/runs.
func (c *Client) ListRuns(ctx context.Context, f RunFilter) (*RunList, error) {
	q := url.Values{}
	for k, v := range map[string]string{"workflow": f.Workflow, "status": f.Status, "queue": f.Queue, "page_token": f.PageToken} {
		if v != "" {
			q.Set(k, v)
		}
	}
	for _, t := range f.Tags {
		q.Add("tag", t)
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	var out RunList
	err := c.do(ctx, request{route: [2]string{"GET", "/v1/runs"}, segments: []string{"v1", "runs"}, query: q}, &out)
	return ret(&out, err)
}

// History returns one page of a run's history: GET /v1/runs/{run_id}/history. fromSeq and
// limit 0 are not sent.
func (c *Client) History(ctx context.Context, runID string, fromSeq uint64, limit int) (*HistoryPage, error) {
	q := url.Values{}
	if fromSeq > 0 {
		q.Set("from_seq", strconv.FormatUint(fromSeq, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out HistoryPage
	err := c.do(ctx, request{
		route:    [2]string{"GET", "/v1/runs/{run_id}/history"},
		segments: []string{"v1", "runs", runID, "history"},
		query:    q,
	}, &out)
	return ret(&out, err)
}

// Result returns a run's outcome, long-polling up to wait (0 = no wait): GET
// /v1/runs/{run_id}/result. Completed is false when the wait ended first.
func (c *Client) Result(ctx context.Context, runID string, wait time.Duration) (*RunResult, error) {
	if wait < 0 {
		wait = 0
	}
	deadline := time.Now().Add(wait)
	left := wait
	for {
		poll := min(left, maxPoll)
		var out RunResult
		err := c.do(ctx, request{
			route:    [2]string{"GET", "/v1/runs/{run_id}/result"},
			segments: []string{"v1", "runs", runID, "result"},
			query:    url.Values{"wait": {ms(poll)}},
			extra:    poll,
		}, &out)
		if err != nil || out.Completed || left <= 0 {
			return ret(&out, err)
		}
		left = max(0, time.Until(deadline))
	}
}

// --- lifecycle ----------------------------------------------------------------------

// Signal sends signal name with payload: POST /v1/runs/{run_id}/signals/{name}. requestID
// "" is not sent.
func (c *Client) Signal(ctx context.Context, runID, name string, payload any, requestID string) (*SignalOutcome, error) {
	body := map[string]any{"payload": payload}
	if requestID != "" {
		body["request_id"] = requestID
	}
	var out SignalOutcome
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/runs/{run_id}/signals/{name}"},
		segments: []string{"v1", "runs", runID, "signals", name},
		body:     body,
	}, &out)
	return ret(&out, err)
}

func optReason(reason string) map[string]any {
	if reason == "" {
		return map[string]any{"reason": nil}
	}
	return map[string]any{"reason": reason}
}

// Cancel requests cooperative cancellation: POST /v1/runs/{run_id}/cancel.
func (c *Client) Cancel(ctx context.Context, runID, reason string) (*CancelOutcome, error) {
	var out CancelOutcome
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/runs/{run_id}/cancel"},
		segments: []string{"v1", "runs", runID, "cancel"},
		body:     optReason(reason),
	}, &out)
	return ret(&out, err)
}

// Terminate ends a run immediately: POST /v1/runs/{run_id}/terminate.
func (c *Client) Terminate(ctx context.Context, runID, reason string) (*TerminateOutcome, error) {
	var out TerminateOutcome
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/runs/{run_id}/terminate"},
		segments: []string{"v1", "runs", runID, "terminate"},
		body:     optReason(reason),
	}, &out)
	return ret(&out, err)
}

// Retry retries a failed run: POST /v1/runs/{run_id}/retry.
func (c *Client) Retry(ctx context.Context, runID string) (*RetryOutcome, error) {
	var out RetryOutcome
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/runs/{run_id}/retry"},
		segments: []string{"v1", "runs", runID, "retry"},
		body:     map[string]any{},
	}, &out)
	return ret(&out, err)
}

// CompleteToken completes an external-completion token: POST /v1/tokens/complete.
func (c *Client) CompleteToken(ctx context.Context, token string, payload any) (*TokenOutcome, error) {
	var out TokenOutcome
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/tokens/complete"},
		segments: []string{"v1", "tokens", "complete"},
		body:     map[string]any{"token": token, "payload": payload},
	}, &out)
	return ret(&out, err)
}

// --- dev clock and replay -----------------------------------------------------------

// DevTime returns the dev clock: GET /v1/dev/time (409 time_skipping_disabled unless the
// server runs `ostro dev --ephemeral --time-skipping`).
func (c *Client) DevTime(ctx context.Context) (*DevTime, error) {
	var out DevTime
	err := c.do(ctx, request{route: [2]string{"GET", "/v1/dev/time"}, segments: []string{"v1", "dev", "time"}}, &out)
	return ret(&out, err)
}

func (c *Client) devSkip(ctx context.Context, body map[string]any) (*DevSkip, error) {
	var out DevSkip
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/dev/time/skip"},
		segments: []string{"v1", "dev", "time", "skip"},
		body:     body,
	}, &out)
	return ret(&out, err)
}

// DevSkipBy moves the dev clock forward by d: POST /v1/dev/time/skip {"by"}.
func (c *Client) DevSkipBy(ctx context.Context, d time.Duration) (*DevSkip, error) {
	return c.devSkip(ctx, map[string]any{"by": ms(d)})
}

// DevSkipTo moves the dev clock to toMs (ms since the epoch; the past skips nothing): POST
// /v1/dev/time/skip {"to_ms"}.
func (c *Client) DevSkipTo(ctx context.Context, toMs int64) (*DevSkip, error) {
	return c.devSkip(ctx, map[string]any{"to_ms": toMs})
}

// Replay replays recorded runs against module sha: POST /v1/modules/{sha}/replay.
func (c *Client) Replay(ctx context.Context, sha string, r ReplayRequest) (*ReplayReport, error) {
	if (r.Runs == nil) == (r.Sample == 0) {
		return nil, &Error{Code: "usage", Message: "replay needs exactly one of Runs / Sample"}
	}
	body := map[string]any{}
	if r.Runs != nil {
		body["runs"] = r.Runs
	} else {
		body["sample"] = r.Sample
		if r.Workflow != "" {
			body["workflow"] = r.Workflow
		}
		if r.Status != "" {
			body["status"] = r.Status
		}
	}
	if r.AllowMissing {
		body["allow_missing"] = true
	}
	var out ReplayReport
	err := c.do(ctx, request{
		route:    [2]string{"POST", "/v1/modules/{sha}/replay"},
		segments: []string{"v1", "modules", sha, "replay"},
		body:     body,
		extra:    replayExtra,
	}, &out)
	return ret(&out, err)
}

// ret returns (nil, err) on failure, (out, nil) otherwise.
func ret[T any](out *T, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	return out, nil
}
