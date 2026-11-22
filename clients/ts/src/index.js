// TypeScript/JavaScript client for the Ostro REST API (SDK-07).
//
// Node built-ins only (fetch, AbortController, node:crypto): no runtime dependencies. The
// route table (routes.js) is checked against the server's OpenAPI document by
// crates/ostro-client/tests/contract.rs.

/** Package version. */
export const VERSION = "0.1.0";

export { Client, OstroError, DEFAULT_SERVER, parseDuration, sha256Hex } from "./client.js";
export { ROUTES } from "./routes.js";
