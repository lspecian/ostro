// Every REST route the TS/JS client calls, as [METHOD, path template].
//
// Checked against the server's OpenAPI document (`ostro openapi`) by
// crates/ostro-client/tests/contract.rs (SDK-07): keep one literal pair per route, the same
// templates as the OpenAPI document. test/client.test.js checks that every client method's
// route is listed here.

export const ROUTES = [
  // Modules and workflows.
  ["PUT", "/v1/modules/{sha256}"],
  ["GET", "/v1/workflows"],
  ["PUT", "/v1/workflows/{name}/active"],
  // Runs.
  ["POST", "/v1/workflows/{name}/runs"],
  ["GET", "/v1/runs"],
  ["GET", "/v1/runs/{run_id}"],
  ["GET", "/v1/runs/{run_id}/history"],
  ["GET", "/v1/runs/{run_id}/result"],
  // Lifecycle.
  ["POST", "/v1/runs/{run_id}/signals/{name}"],
  ["POST", "/v1/runs/{run_id}/cancel"],
  ["POST", "/v1/runs/{run_id}/terminate"],
  ["POST", "/v1/runs/{run_id}/retry"],
  ["POST", "/v1/tokens/complete"],
  // Dev clock (ostro dev --ephemeral --time-skipping) and server-side replay.
  ["GET", "/v1/dev/time"],
  ["POST", "/v1/dev/time/skip"],
  ["POST", "/v1/modules/{sha}/replay"],
];
