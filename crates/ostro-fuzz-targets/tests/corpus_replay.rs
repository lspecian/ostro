//! Stable corpus replay (14-07, SEC-05): every committed file under `fuzz/corpus/<target>/`
//! and `fuzz/regressions/<target>/` is run through `ostro_fuzz_targets::run` on the pinned
//! stable toolchain, so ci-local catches regressions without nightly or libFuzzer.
//!
//! One `#[test]` per target (filtered runs report per-target counts); a missing directory is
//! zero files, but the total over all targets must be > 0. A panicking input fails its test
//! with the list of every panicking file.
//!
//! `print_generated_seeds` (ignored) prints seeds that need a running store (OSNP snapshots
//! exported from a real partition) and REST bodies serialised from the DTO defaults, as
//! `SEED <target> <name> <hex>` lines; 14-07 wrote them into `fuzz/corpus/` with them.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

use ostro_fuzz_targets::{Target, run};

fn fuzz_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz")
}

/// Every replay file of `t`: `fuzz/corpus/<t>/*` then `fuzz/regressions/<t>/*` (sorted).
fn files(t: Target) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for kind in ["corpus", "regressions"] {
        let dir = fuzz_dir().join(kind).join(t.dir_name());
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut v: Vec<PathBuf> = rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        v.sort();
        out.extend(v);
    }
    out
}

/// Replays `t`; returns the file count, panics listing every panicking file.
fn replay(t: Target) -> usize {
    let files = files(t);
    let mut failed = Vec::new();
    for f in &files {
        let data = std::fs::read(f).unwrap_or_else(|e| panic!("read {}: {e}", f.display()));
        if catch_unwind(AssertUnwindSafe(|| run(t, &data))).is_err() {
            failed.push(f.display().to_string());
        }
    }
    println!("{}: {} files replayed", t.dir_name(), files.len());
    assert!(
        failed.is_empty(),
        "{}: {} panicking input(s):\n{}",
        t.dir_name(),
        failed.len(),
        failed.join("\n")
    );
    files.len()
}

macro_rules! replay_tests {
    ($($name:ident => $t:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                replay($t);
            }
        )*
    };
}

replay_tests!(
    apply_command => Target::ApplyCommand,
    journal_event_codec => Target::JournalEventCodec,
    meta_decode => Target::MetaDecode,
    ext_tlv => Target::ExtTlv,
    snapshot_frames => Target::SnapshotFrames,
    wal_recovery_scan => Target::WalRecoveryScan,
    rest_json => Target::RestJson,
    webhook_verify => Target::WebhookVerify,
    cron_expr => Target::CronExpr,
    egress_pattern => Target::EgressPattern,
    secret_template => Target::SecretTemplate,
    bearer_token => Target::BearerToken,
    signature_parse => Target::SignatureParse,
    evil_guest => Target::EvilGuest,
    smith_component => Target::SmithComponent,
);

#[test]
fn every_target_has_a_test_and_the_corpus_is_not_empty() {
    assert_eq!(
        Target::all().len(),
        15,
        "add a replay test for the new target"
    );
    let total: usize = Target::all().iter().map(|&t| files(t).len()).sum();
    println!("total: {total} files");
    assert!(total > 0, "no corpus or regression files found under fuzz/");
}

// ------------------------------------------------------------------------------ seed printer

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
#[ignore = "seed generator: prints SEED lines (run by hand when regenerating seeds)"]
fn print_generated_seeds() {
    use ostro_partition::store::snapshot::ExportOptions;
    use ostro_partition::{Cursor, PartitionStore, StoreConfig};
    use ostro_types::{Epoch, PartitionCount, PartitionId};

    // A store with the frozen golden commands applied (partition 5, as the goldens).
    let golden = include_str!("../../ostro-types/tests/golden/commands.txt");
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(5),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open");
    let mut cur = Cursor::from_meta(&meta);
    for line in golden.lines().filter(|l| !l.starts_with('#')) {
        let Some((_, h)) = line.split_once('\t') else {
            continue;
        };
        let bytes = unhex(h.trim());
        let env = if cur.epoch().0 == 0 { 7 } else { cur.epoch().0 };
        let mut b = store.batch();
        let mut actions = Vec::new();
        let off = cur.next;
        if ostro_partition::apply_command(&mut b, &mut cur, off, Epoch(env), &bytes, &mut actions)
            .is_ok()
        {
            let mu = cur.meta_update();
            b.commit(&mu).expect("commit");
        }
    }
    for (name, lz4) in [("golden-plain", false), ("golden-lz4", true)] {
        let mut out = Vec::new();
        store
            .export_snapshot_with(&mut out, 1_700_000_000_000, ExportOptions { lz4 })
            .expect("export");
        let mut seed = vec![0u8];
        seed.extend_from_slice(&out);
        println!("SEED snapshot_frames {name} {}", hex(&seed));
    }

    // REST bodies from the DTO defaults (every key spelled as the API spells it).
    macro_rules! dflt {
        ($($t:ty => $n:literal),* $(,)?) => {$(
            let j = serde_json::to_vec(&<$t>::default()).expect("serialise");
            println!("SEED rest_json {} {}", $n, hex(&j));
        )*};
    }
    use ostro_api::{phase7, phase8, phase11, phase14, rest};
    dflt!(
        rest::SignalBody => "signal",
        rest::StopRunBody => "stop",
        rest::RetryBody => "retry",
        rest::RunPolicyBody => "run-policy",
        phase7::PutQueueFlowBody => "queue-flow",
        phase7::PauseRunRequest => "pause",
        phase7::CreateWorkerTokenRequest => "worker-token",
        phase8::TriggerScheduleBody => "trigger-schedule",
        phase11::RequeueBody => "requeue",
        phase14::TenantSpec => "tenant",
        phase14::CreateTokenRequest => "create-token",
        phase14::RotateTokenRequest => "rotate-token",
        phase14::ModulePolicy => "module-policy",
        phase14::TrustedKeySpec => "trusted-key",
        phase14::AttachSignaturesRequest => "attach-signatures",
    );
}
