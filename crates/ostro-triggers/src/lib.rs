//! ostro-triggers: cron schedules and webhook verification (Phase 8, TRIG-01..03).
//!
//! Every Phase 8 module is declared here once (plan 08-01, P8-D21) so later plans own disjoint
//! files and never edit this file:
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`cron`] | 08-05 | cron parsing and next/previous occurrence (croner behind the jiff-0.2 adapter, P8-D9) |
//! | [`tz`] | 08-05 | IANA zone loading: both jiff majors from the same TZif bytes |
//! | [`webhook`] | 08-06 | signature verifiers (Stripe, GitHub, Slack, Standard Webhooks, generic HMAC; P8-D12) |
//! | [`extract`] | 08-06 | idempotency-key extractors (`header:<name>`, `json:<pointer>`, `none`) |

pub mod cron;
pub mod extract;
pub mod tz;
pub mod webhook;

// Dependencies declared by plan 08-01 (P8-D21), anchored so cargo's unused-dependency lint
// stays quiet until 08-05/08-06 consume them.
mod dep_anchors {
    use croner as _;
    use hmac as _;
    use jiff as _;
    use jiff02 as _;
    use serde_json as _;
    use sha2 as _;
    use thiserror as _;
}

// Phase 14 (plan 14-01, D-P14-37): `base64` for the Kafka envelope input (14-32), anchored
// until it is used.
use base64 as _;
