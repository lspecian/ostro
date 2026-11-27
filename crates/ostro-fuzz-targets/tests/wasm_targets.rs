//! The Wasm fuzz targets (Phase 14, 14-08; SEC-05): `evil_guest` generator quality (≥ 90% of
//! random inputs encode), its valid-call seed, five hand-written malformed cases (each fails
//! the attempt without a host panic, budgets intact), `smith_component` reaching
//! instantiation for ≥ 30% of random inputs. The committed corpora replay in
//! `corpus_replay.rs` (14-07).
//!
//! Every execution checks the node budget invariant itself
//! (`evil_guest::assert_budgets_intact` after each stage), so a leak fails the test.

use std::path::{Path, PathBuf};

use ostro_engine::sink::ActivationOutcome;
use ostro_fuzz_targets::evil_guest::{self, Depth, Program, Template};
use ostro_fuzz_targets::{Target, smith};

/// Deterministic pseudo-random bytes (xorshift64).
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn random_input(i: u64) -> Vec<u8> {
    bytes(i + 1, 64 + (i as usize * 97) % 2048)
}

fn fuzz_dir(kind: &str, target: Target) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz")
        .join(kind)
        .join(target.dir_name())
}

#[test]
fn evil_encodability_rate() {
    let encoded = (0..200)
        .filter(|i| evil_guest::build_component(&random_input(*i)).is_some())
        .count();
    eprintln!("evil_guest: {encoded}/200 random inputs encode");
    assert!(encoded >= 180, "only {encoded}/200 random inputs encode");
}

#[test]
fn evil_valid_call_seed_completes() {
    let t = Template::get();
    let p = evil_guest::valid_calls_program(t);
    let r = evil_guest::exec(&p.to_bytes(t));
    eprintln!("valid-call seed: {r:?}");
    assert!(r.encoded && r.prepared && r.instantiated, "{r:?}");
    assert_eq!(r.describe, Some(Ok(0)), "describe returns the empty list");
    let outcome = r.outcome.clone().expect("the activation ran");
    // Well-formed values pass canonical-ABI lifting (no trap) and reach the journal; zero
    // arguments may still break a host protocol rule (e.g. `wait` on an empty op list).
    assert!(r.appended > 0, "the calls reached the journal: {r:?}");
    assert!(
        matches!(
            outcome,
            ActivationOutcome::Completed { .. }
                | ActivationOutcome::Evicted { .. }
                | ActivationOutcome::AttemptFailed(
                    ostro_engine::error::AttemptFailure::ProtocolViolation(_)
                )
        ),
        "well-formed calls lift without a trap: {outcome:?}"
    );
}

fn malformed(name: &str) {
    let t = Template::get();
    let (_, p) = evil_guest::malformed_programs(t)
        .into_iter()
        .find(|(n, _)| *n == name)
        .expect("known case");
    let r = evil_guest::exec(&p.to_bytes(t));
    eprintln!("{name}: {r:?}");
    assert!(r.encoded && r.prepared && r.instantiated, "{name}: {r:?}");
    assert!(
        matches!(r.outcome, Some(ActivationOutcome::AttemptFailed(_))),
        "{name}: the attempt fails: {:?}",
        r.outcome
    );
}

#[test]
fn evil_oob_string_pointer() {
    malformed("oob_string_pointer");
}

#[test]
fn evil_invalid_discriminant() {
    malformed("invalid_discriminant");
}

#[test]
fn evil_huge_list_length() {
    malformed("huge_list_length");
}

#[test]
fn evil_invalid_utf8() {
    malformed("invalid_utf8");
}

#[test]
fn evil_realloc_out_of_bounds() {
    malformed("realloc_out_of_bounds");
}

/// Regression (14-08, SEC-05): a component whose `_initialize` loops forever (it runs at
/// instantiation) hung `Runtime::calibrate`, which instantiated with unlimited fuel and no
/// epoch deadline, so admission (deploy-time `describe`, every activation) never returned.
/// Now every instantiation is bounded: the run ends, the attempt fails, budgets are intact.
#[test]
fn evil_spinning_initialize_is_bounded() {
    let t = Template::get();
    let input = evil_guest::spinning_initialize_program(t).to_bytes(t);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(evil_guest::exec(&input));
    });
    let r = rx
        .recv_timeout(std::time::Duration::from_secs(120))
        .expect("a looping _initialize is bounded by fuel and the epoch deadline");
    eprintln!("spinning _initialize: {r:?}");
    assert!(r.encoded && r.prepared, "{r:?}");
    assert!(!r.instantiated, "instantiation runs out of fuel: {r:?}");
    assert!(
        matches!(r.outcome, Some(ActivationOutcome::AttemptFailed(_))),
        "{:?}",
        r.outcome
    );
}

#[test]
fn evil_random_inputs_run_clean() {
    let mut ran = 0;
    for i in 1000..1024 {
        let r = evil_guest::exec(&random_input(i));
        ran += usize::from(r.outcome.is_some());
    }
    eprintln!("evil_guest: {ran}/24 random inputs ran an activation");
    assert!(ran > 0);
}

#[test]
fn smith_instantiation_rate() {
    let mut encoded = 0;
    let mut instantiated = 0;
    for i in 0..200 {
        let r = smith::exec_to(&random_input(i), Depth::Instantiate);
        encoded += usize::from(r.encoded);
        instantiated += usize::from(r.instantiated);
    }
    eprintln!("smith_component: {encoded}/200 encode, {instantiated}/200 reach instantiation");
    assert!(
        instantiated >= 60,
        "only {instantiated}/200 random inputs reach instantiation"
    );
}

#[test]
fn smith_full_runs_keep_budgets() {
    let mut ran = 0;
    for i in 500..530 {
        let r = smith::exec(&random_input(i));
        ran += usize::from(r.outcome.is_some());
    }
    eprintln!("smith_component: {ran}/30 random inputs ran an activation");
    assert!(ran > 0);
}

/// Writes the seed corpora (20 inputs per target). Run with `--ignored` to regenerate.
#[test]
#[ignore = "regenerates fuzz/corpus/{evil_guest,smith_component}"]
fn write_seed_corpora() {
    let t = Template::get();
    let mut evil: Vec<(String, Vec<u8>)> = vec![(
        "valid_calls".into(),
        evil_guest::valid_calls_program(t).to_bytes(t),
    )];
    for (name, p) in evil_guest::malformed_programs(t) {
        evil.push((name.into(), p.to_bytes(t)));
    }
    let mut i = 0;
    while evil.len() < 20 {
        let data = random_input(7000 + i);
        i += 1;
        if evil_guest::build_component(&data).is_some() {
            let p = Program::from_bytes(t, &data);
            evil.push((format!("random_{i:02}"), p.to_bytes(t)));
        }
    }
    let mut smith_seeds = Vec::new();
    let mut i = 0;
    while smith_seeds.len() < 20 {
        let data = bytes(9000 + i, 512 + (i as usize * 211) % 3584);
        i += 1;
        if smith::exec_to(&data, Depth::Instantiate).instantiated {
            smith_seeds.push((format!("random_{i:02}"), data));
        }
    }
    let regressions = fuzz_dir("regressions", Target::EvilGuest);
    std::fs::create_dir_all(&regressions).expect("regressions dir");
    std::fs::write(
        regressions.join("spinning_initialize"),
        evil_guest::spinning_initialize_program(t).to_bytes(t),
    )
    .expect("write regression");
    for (target, seeds) in [
        (Target::EvilGuest, evil),
        (Target::SmithComponent, smith_seeds),
    ] {
        let dir = fuzz_dir("corpus", target);
        std::fs::create_dir_all(&dir).expect("corpus dir");
        for (name, data) in seeds {
            std::fs::write(dir.join(format!("seed_{name}")), data).expect("write seed");
        }
    }
}
