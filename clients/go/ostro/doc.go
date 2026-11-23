// Package ostro is the Go client for the Ostro REST API.
//
// NewClient returns a *Client with context-aware methods for modules, workflows, runs,
// signals, results (long-polled), the dev clock and replay; failures are *Error. Every
// route the client calls is listed in Routes (routes.go), which
// crates/ostro-client/tests/contract.rs checks against the server's OpenAPI document. The
// test kit (a spawned `ostro dev --ephemeral --time-skipping` per test) is package
// ostrotest. Standard library only: the module has no requirements.
package ostro

// Version is the client module version.
const Version = "0.1.0"
