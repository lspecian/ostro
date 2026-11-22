package ostro

import "fmt"

// Error is the one error type of the client: the HTTP Status (0 when no response arrived),
// Code, Message and Details from the REST error body
// {"error": {"code", "message", "details"}}. Transport failures use Code "unreachable" (no
// connection), "timeout" (client timeout or context deadline) or "cancelled" (context
// cancelled); a non-JSON error response uses Code "http_<status>". Match it with errors.As.
type Error struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
	// Err is the underlying transport error, if any.
	Err error
}

func (e *Error) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("ostro: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("ostro: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// Unwrap returns the underlying transport error (context.DeadlineExceeded, ...).
func (e *Error) Unwrap() error { return e.Err }

// Reason is Details["reason"] when the server sent one (e.g. "time_skipping_disabled").
func (e *Error) Reason() string {
	if r, ok := e.Details["reason"].(string); ok {
		return r
	}
	return ""
}
