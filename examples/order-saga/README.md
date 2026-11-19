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

> The `ostro deploy`, `run`, `signal` and `result` commands are added in plan 05-14.
> Until then, use the REST API that `ostro dev` serves.

```sh
# 1. One-time: add the WebAssembly component target.
rustup target add wasm32-wasip2

# 2. Build the workflow component (from this directory).
cargo build --release --target wasm32-wasip2

# 3. Start a local single-node server (in another terminal).
ostro dev

# 4. Deploy the module and make it the active version.
ostro deploy target/wasm32-wasip2/release/order_saga.wasm --activate

# 5. Start an order. This prints the run id.
ostro run order-saga --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}'

# 6. Approve it. Send `false` to reject it and watch the compensations run.
ostro signal <run> approve --payload true

# 7. Wait for the result.
ostro result <run> --wait
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
