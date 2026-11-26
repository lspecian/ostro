//! ostro-fuzz-targets: the fuzz target logic (Phase 14, SEC-05; D-P14-17).
//!
//! Every target is a plain function over arbitrary bytes. The libFuzzer binaries of the
//! separate `fuzz/` workspace (one per [`Target`], 14-01b) and the stable corpus replay test
//! of this crate (14-07) both call [`run`]. Invariant for every target: no panic, no abort,
//! bounded memory and time; apply rejects with `Invalid` instead of panicking; guest faults
//! fail one attempt and leave node budgets intact.
//!
//! This file (the [`Target`] list, [`Target::dir_name`] and the [`run`] dispatch) is final
//! after 14-01; the module files are filled by 14-07 (`apply`, `codec`, `meta`, `ext`,
//! `snapshot`, `wal`, `parsers`) and 14-08 (`evil_guest`, `smith`).

pub mod apply;
pub mod codec;
pub mod evil_guest;
pub mod ext;
pub mod meta;
pub mod parsers;
pub mod smith;
pub mod snapshot;
pub mod wal;

// Dependencies declared by plan 14-01 (D-P14-37), anchored so cargo's unused-dependency lint
// stays quiet until 14-07 / 14-08 consume them (owners delete the lines they use).
mod dep_anchors {
    use arbitrary as _;
    use ostro_api as _;
    use ostro_bridge as _;
    use ostro_engine as _;
    use ostro_log as _;
    use ostro_meta as _;
    use ostro_partition as _;
    use ostro_server as _;
    use ostro_triggers as _;
    use ostro_types as _;
    use serde_json as _;
    use tempfile as _;
    use tokio as _;
    use wasm_encoder as _;
    use wasm_smith as _;
    use wasmparser as _;
    use wit_component as _;
    use wit_parser as _;
}

/// A fuzz target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Target {
    /// Partition apply of decoded commands (14-07).
    ApplyCommand,
    /// Journal event encode/decode (14-07).
    JournalEventCodec,
    /// Meta record / command / snapshot decoding (14-07).
    MetaDecode,
    /// The meta ext TLV codec (14-07).
    ExtTlv,
    /// OSNP snapshot frames (14-07).
    SnapshotFrames,
    /// WAL recovery scan over arbitrary segment bytes (14-07).
    WalRecoveryScan,
    /// REST JSON request bodies (14-07).
    RestJson,
    /// Webhook signature verification (14-07).
    WebhookVerify,
    /// Cron expressions (14-07).
    CronExpr,
    /// Egress host patterns (14-07).
    EgressPattern,
    /// Secret templates (14-07).
    SecretTemplate,
    /// API bearer tokens (14-07, after 14-03).
    BearerToken,
    /// Module signatures and trusted keys (14-07, after 14-05).
    SignatureParse,
    /// Hostile guest components under limits (14-08).
    EvilGuest,
    /// wasm-smith generated components (14-08).
    SmithComponent,
}

impl Target {
    /// Every target, in declaration order.
    pub const ALL: [Target; 15] = [
        Target::ApplyCommand,
        Target::JournalEventCodec,
        Target::MetaDecode,
        Target::ExtTlv,
        Target::SnapshotFrames,
        Target::WalRecoveryScan,
        Target::RestJson,
        Target::WebhookVerify,
        Target::CronExpr,
        Target::EgressPattern,
        Target::SecretTemplate,
        Target::BearerToken,
        Target::SignatureParse,
        Target::EvilGuest,
        Target::SmithComponent,
    ];

    /// Every target, in declaration order.
    pub fn all() -> &'static [Target] {
        &Target::ALL
    }

    /// The target's name: the fuzz binary, `fuzz/corpus/<dir>/` and
    /// `fuzz/regressions/<dir>/`.
    pub const fn dir_name(self) -> &'static str {
        match self {
            Target::ApplyCommand => "apply_command",
            Target::JournalEventCodec => "journal_event_codec",
            Target::MetaDecode => "meta_decode",
            Target::ExtTlv => "ext_tlv",
            Target::SnapshotFrames => "snapshot_frames",
            Target::WalRecoveryScan => "wal_recovery_scan",
            Target::RestJson => "rest_json",
            Target::WebhookVerify => "webhook_verify",
            Target::CronExpr => "cron_expr",
            Target::EgressPattern => "egress_pattern",
            Target::SecretTemplate => "secret_template",
            Target::BearerToken => "bearer_token",
            Target::SignatureParse => "signature_parse",
            Target::EvilGuest => "evil_guest",
            Target::SmithComponent => "smith_component",
        }
    }

    /// Look a target up by [`Target::dir_name`].
    pub fn from_dir_name(name: &str) -> Option<Target> {
        Target::ALL.into_iter().find(|t| t.dir_name() == name)
    }
}

/// Run `t` over `data`.
pub fn run(t: Target, data: &[u8]) {
    match t {
        Target::ApplyCommand => apply::apply_command(data),
        Target::JournalEventCodec => codec::journal_event_codec(data),
        Target::MetaDecode => meta::meta_decode(data),
        Target::ExtTlv => ext::ext_tlv(data),
        Target::SnapshotFrames => snapshot::snapshot_frames(data),
        Target::WalRecoveryScan => wal::wal_recovery_scan(data),
        Target::RestJson => parsers::rest_json(data),
        Target::WebhookVerify => parsers::webhook_verify(data),
        Target::CronExpr => parsers::cron_expr(data),
        Target::EgressPattern => parsers::egress_pattern(data),
        Target::SecretTemplate => parsers::secret_template(data),
        Target::BearerToken => parsers::bearer_token(data),
        Target::SignatureParse => parsers::signature_parse(data),
        Target::EvilGuest => evil_guest::evil_guest(data),
        Target::SmithComponent => smith::smith_component(data),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn names_are_unique_and_round_trip() {
        let names: BTreeSet<_> = Target::all().iter().map(|t| t.dir_name()).collect();
        assert_eq!(names.len(), Target::ALL.len());
        for t in Target::ALL {
            assert_eq!(Target::from_dir_name(t.dir_name()), Some(t));
            assert!(
                t.dir_name()
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b == b'_')
            );
        }
        assert_eq!(Target::from_dir_name("nope"), None);
    }

    #[test]
    fn every_stub_accepts_any_input() {
        for t in Target::ALL {
            run(t, &[]);
            run(t, &[0xff; 64]);
        }
    }
}
