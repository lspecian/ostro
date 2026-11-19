# order-saga

An example Ostro workflow written in Rust with the Ostro SDK
(`sdks/rust/ostro-sdk`). It walks an order through five durable steps and covers the
main building blocks:

- **Steps**: `reserve_inventory`, `charge_payment`, `create_shipment`,
  `notify_customer` and `complete_order`. Each runs at most once to a recorded result
  and is replayed from the journal after a crash.
- **Retries**: `charge_payment` retries up to 3 attempts with exponential backoff
  (200 ms, then 400 ms, capped at 2 s). The kind `card_declined` is never retried.
- **Timer**: a durable `ctx.sleep(settle_ms)` (default 1 s) before asking for approval.
- **Signal raced with cancellation**: `select!` over the `approve` signal (a JSON
  `true`/`false`, with a timeout of `approval_timeout_ms`, default 24 h) and
  `ctx.cancellation()`.
- **Compensations**: when a step fails, the order is rejected, the approval times out or
  the run is cancelled, `refund_payment` and then `release_inventory` run in reverse
  order inside `ctx.shielded`, so they finish even after a cancel.

The steps are simulations with no I/O, which makes the example deterministic. Replace
their bodies with real calls to your services. The module docs in `src/lib.rs` describe
the full behaviour.

## Quickstart

From a fresh clone of the Ostro repository. The only prerequisite is a Rust toolchain
installed with rustup; `rust-toolchain.toml` selects the pinned version.

```sh
# 1. One-time: add the WebAssembly component target.
rustup target add wasm32-wasip2

# 2. Build the ostro binary (from the repository root) and put it on PATH.
cargo build --release --locked -p ostro
export PATH="$PWD/target/release:$PATH"

# 3. Build the workflow component.
cd examples/order-saga
cargo build --release --target wasm32-wasip2

# 4. Start a local single-node server (http://127.0.0.1:7700, data in ./.ostro).
ostro dev &

# 5. Deploy the module and make it the active version.
ostro deploy target/wasm32-wasip2/release/order_saga.wasm --activate

# 6. Start an order. This prints `started run <run> (order-saga)`.
ostro run order-saga --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}'

# 7. Approve it. Send `false` to reject it and watch the compensations run.
ostro signal <run> approve --payload true

# 8. Wait for the result. Exit code 0 = completed, 3 = the run ended otherwise.
ostro result <run> --wait
```

`ostro inspect <run>`, `ostro history <run>` and `ostro ls` show the run, its journal
and every run. Add `--json` to any client command for the REST body. `ostro cancel
<run>` during the approval wait runs the compensations and ends the run `cancelled`.

[`quickstart.sh`](quickstart.sh) runs exactly these steps on a fresh clone and prints the
wall time. With a working docker it runs them in a clean `rust:1.100` container (the
"< 10 minutes on a clean machine" check, result line `QUICKSTART_RESULT:`). Without docker
it runs them locally with an empty target directory and labels the time as a local
equivalent, not the clean-machine figure.

```sh
examples/order-saga/quickstart.sh            # clones the repository it lives in
```

An approved order completes with:

```json
{"status":"completed","order_id":"o-1","shipment_id":"ship-o-1"}
```

A rejected order completes with:

```json
{"status":"compensated","order_id":"o-1","reason":"rejected","compensations":["refund_payment","release_inventory"]}
```

## Input

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `order_id` | string | required | Order id. Simulated ids derive from it. |
| `amount` | integer | required | Amount in cents. `0` is declined (`card_declined`, not retried). |
| `sku` | string | required | Product SKU. |
| `qty` | integer | required | Quantity. |
| `fail_at` | string | none | Forces a failure. `"charge_payment"` fails on every attempt, so the retry policy runs out after 3 attempts. Any other step name fails that step once, without retry. |
| `settle_ms` | integer | `1000` | Settlement timer before approval. |
| `approval_timeout_ms` | integer | `86400000` | How long to wait for `approve`. |

## Outcomes

- **Approved**: `{"status": "completed", ...}`.
- **Step failure, rejection or approval timeout**: the compensations run, then
  `{"status": "compensated", "reason": ...}`. The `compensations` field lists the ones
  that ran.
- **Cancelled** (graceful cancel): the compensations run, then the run ends with
  status `cancelled`.

## Notes

- This is a standalone cargo project with its own `[workspace]`. It depends on the SDK
  by path (`../../sdks/rust/ostro-sdk`). Cargo.lock is not committed, and versions are
  pinned exactly in `Cargo.toml`.
- No cargo-component is needed. Plain cargo with the `wasm32-wasip2` target builds the
  component.
- The Ostro repository's CI builds and lints this example (`scripts/ci-local.sh`, step
  "order-saga example").

## Troubleshooting

- **`ostro: error: cannot reach the Ostro server at http://127.0.0.1:7700`.** `ostro dev`
  is not running, is still starting, or listens elsewhere. Wait for its
  `listening on http://...` line. For another address, start it with
  `ostro dev --listen 127.0.0.1:7800` and point the client at it with
  `export OSTRO_SERVER=http://127.0.0.1:7800` (or `--server` on each command).
- **`ostro dev` exits at startup with a bind error.** Another process (often an earlier
  `ostro dev`) holds port 7700. Stop it, or use `--listen` as above.
- **`error[E0463]: can't find crate for core` / `wasm32-wasip2` target not installed.**
  Run `rustup target add wasm32-wasip2` (step 1) for the toolchain that
  `rust-toolchain.toml` selects.
- **`ostro run` answers 404 `not_found` for `order-saga`.** The module was not deployed
  or not activated. Re-run step 5 with `--activate`. A re-deploy of the same file reports
  "unchanged" and is harmless.
- **The run stays `scheduled`.** It is waiting for the `approve` signal (after the 1 s
  settle timer). `ostro history <run>` shows `signal_wait_started`. Send the signal
  (step 7). A signal sent earlier is buffered and matched when the wait starts.
- **`signal` answers 409 `run_terminal`.** The run has already ended (completed,
  compensated, cancelled or timed out); signals are not delivered to finished runs.
- **`result --wait` exits 3.** The run ended with a status other than `completed`
  (for example `cancelled` or `failed`). `ostro inspect <run>` shows why. A compensated
  order still exits 0: compensation is a normal completion with
  `"status": "compensated"` in the result.
- **Stale data from an older Ostro build.** `ostro dev` keeps its data in `./.ostro`.
  Data dirs created before Phase 5 do not open; delete `.ostro` (or use
  `ostro dev --ephemeral`, which keeps nothing).
- **Slow first build.** The first `cargo build --release -p ostro` compiles every
  dependency with fat LTO and takes most of the quickstart time. Later builds are
  incremental.
