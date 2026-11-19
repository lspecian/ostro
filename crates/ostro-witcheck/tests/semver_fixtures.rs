//! Positive/negative fixtures for the WIT-07 additive checker (plan 05-05).
//!
//! `tests/wit-fixtures/base/` is `wit/ostro-runtime-0.1.0/runtime.wit` verbatim. Every other
//! fixture is an `ostro:runtime@0.1.1` package that differs from base by exactly one named
//! change (plus the version bump). The fixtures are generated from base by the edit table
//! in [`fixtures`]. `fixtures_are_generated_from_the_real_wit` fails if a committed fixture
//! drifts from the generator or base drifts from the real WIT. To regenerate after a
//! doc-comment edit of runtime.wit (allowed only before the 05-20 freeze):
//!
//! ```text
//! OSTRO_BLESS=1 cargo test -p ostro-witcheck --test semver_fixtures -- fixtures_are_generated
//! ```

use std::path::{Path, PathBuf};

use ostro_witcheck::{Checker, SemverOutcome, check_additive, check_additive_report};

const REAL_WIT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../wit/ostro-runtime-0.1.0/runtime.wit"
);
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wit-fixtures");

const VERSION_FROM: &str = "package ostro:runtime@0.1.0;";
const VERSION_TO: &str = "package ostro:runtime@0.1.1;";

/// One fixture: name, expected verdict, and its `(anchor, replacement)` edits. Each anchor
/// must occur exactly once in base.
struct Fixture {
    name: &'static str,
    additive: bool,
    edits: &'static [(&'static str, &'static str)],
}

fn fixtures() -> Vec<Fixture> {
    vec![
        // ---- positive: additive ----
        Fixture {
            name: "add-func-context",
            additive: true,
            edits: &[(
                "    continue-as-new-suggested: func() -> bool;\n",
                "    continue-as-new-suggested: func() -> bool;\n    \
                 /// Fixture: a new function in an imported interface.\n    \
                 queue-name: func() -> option<string>;\n",
            )],
        },
        Fixture {
            name: "add-interface-imported",
            additive: true,
            edits: &[
                (
                    "/// The world every Ostro workflow component targets.\n",
                    "/// Fixture: a new interface imported by the world.\n\
                     interface metrics {\n    \
                     counter: func(name: string, delta: u64);\n}\n\n\
                     /// The world every Ostro workflow component targets.\n",
                ),
                (
                    "    import log;\n",
                    "    import log;\n    import metrics;\n",
                ),
            ],
        },
        // ---- negative: not additive ----
        Fixture {
            name: "add-record-field",
            additive: false,
            edits: &[(
                "        continued-from: option<string>,\n",
                "        continued-from: option<string>,\n        queue: option<string>,\n",
            )],
        },
        Fixture {
            name: "add-variant-case",
            additive: false,
            edits: &[(
                "        cancelled,\n    }\n",
                "        cancelled,\n        terminated,\n    }\n",
            )],
        },
        Fixture {
            name: "change-param-type",
            additive: false,
            edits: &[(
                "    sleep: func(duration-ms: u64) -> op-id;\n",
                "    sleep: func(duration-ms: u32) -> op-id;\n",
            )],
        },
        Fixture {
            name: "remove-func",
            additive: false,
            edits: &[("    is-replaying: func() -> bool;\n", "")],
        },
        Fixture {
            name: "rename-interface",
            additive: false,
            edits: &[
                ("interface log {\n", "interface logging {\n"),
                ("    import log;\n", "    import logging;\n"),
            ],
        },
        Fixture {
            name: "add-workflows-export",
            additive: false,
            edits: &[(
                "    run: func(name: string, input: payload) -> result<run-result, failure>;\n",
                "    run: func(name: string, input: payload) -> result<run-result, failure>;\n    \
                 /// Fixture: a new export function (old guests do not provide it).\n    \
                 query: func(name: string, input: payload) -> result<payload, failure>;\n",
            )],
        },
    ]
}

fn render(base: &str, f: &Fixture) -> String {
    let mut out = base.to_string();
    for (anchor, repl) in [(VERSION_FROM, VERSION_TO)].iter().chain(f.edits) {
        assert_eq!(
            out.matches(anchor).count(),
            1,
            "fixture {}: anchor {anchor:?} must occur exactly once",
            f.name
        );
        out = out.replacen(anchor, repl, 1);
    }
    out
}

fn dir(name: &str) -> PathBuf {
    Path::new(FIXTURES).join(name)
}

fn wit_file(name: &str) -> PathBuf {
    dir(name).join("runtime.wit")
}

#[test]
fn fixtures_are_generated_from_the_real_wit() {
    let base = std::fs::read_to_string(REAL_WIT).expect("read runtime.wit");
    let bless = std::env::var("OSTRO_BLESS").as_deref() == Ok("1");
    let mut expected = vec![("base", base.clone())];
    for f in fixtures() {
        expected.push((f.name, render(&base, &f)));
    }
    for (name, content) in expected {
        if bless {
            std::fs::create_dir_all(dir(name)).expect("mkdir fixture");
            std::fs::write(wit_file(name), &content).expect("write fixture");
        }
        let committed = std::fs::read_to_string(wit_file(name))
            .unwrap_or_else(|e| panic!("fixture {name}: {e} (run with OSTRO_BLESS=1)"));
        assert_eq!(
            committed, content,
            "fixture {name} drifted from runtime.wit + its edit; re-bless (OSTRO_BLESS=1)"
        );
    }
}

#[test]
fn same_version_is_rejected_without_panicking() {
    // One Resolve cannot hold the same package version twice (wit-parser panics), so the
    // semver leg is skipped and the structural leg rejects the non-increasing version;
    // everything else is identical.
    let report = check_additive_report(dir("base"), dir("base"));
    assert!(
        matches!(report.semver, SemverOutcome::LoadFailed(_)),
        "{report:?}"
    );
    assert_eq!(report.load_error, None);
    assert_eq!(
        report.structural.len(),
        1,
        "only the version rule fires: {:?}",
        report.structural
    );
    assert!(report.structural[0].contains("not a semver-compatible successor"));
    assert!(check_additive(dir("base"), dir("base")).is_err());
}

#[test]
fn fixture_table_verdicts() {
    let mut table = Vec::new();
    let mut failures = Vec::new();
    for f in fixtures() {
        let report = check_additive_report(dir("base"), dir(f.name));
        let semver = match &report.semver {
            SemverOutcome::Compatible => "accept".to_string(),
            SemverOutcome::Incompatible(_) => "REJECT".to_string(),
            SemverOutcome::LoadFailed(e) => format!("load-failed ({e})"),
        };
        let structural = if report.structural.is_empty() {
            "accept".to_string()
        } else {
            format!("REJECT: {}", report.structural.join("; "))
        };
        table.push(format!(
            "{:<24} expect={:<8} semver_check={:<8} structural={}",
            f.name,
            if f.additive { "additive" } else { "breaking" },
            semver,
            structural
        ));
        let verdict = check_additive(dir("base"), dir(f.name));
        match (f.additive, &verdict) {
            (true, Err(e)) => failures.push(format!("{}: expected Ok, got {e:?}", f.name)),
            (false, Ok(())) => failures.push(format!("{}: expected Err, got Ok", f.name)),
            (false, Err(e)) => {
                assert!(
                    e.iter().all(|i| i.checker != Checker::Load),
                    "{}: rejection must be a real incompatibility, not a load error: {e:?}",
                    f.name
                );
                // Human-readable: the structural leg names the changed item.
                assert!(
                    e.iter().any(|i| i.checker == Checker::Structural),
                    "{}: structural diff must report a readable reason: {e:?}",
                    f.name
                );
            }
            (true, Ok(())) => {}
        }
        // semver_check must load both versions into one Resolve for every fixture.
        assert!(
            !matches!(report.semver, SemverOutcome::LoadFailed(_)),
            "{}: semver_check failed to load: {:?}",
            f.name,
            report.semver
        );
    }
    eprintln!("WIT-07 fixture table:\n{}", table.join("\n"));
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn semver_check_classes() {
    // Pins which checker catches which class (recorded in 05-05-SUMMARY). If a future
    // wit-component bump changes semver_check's behaviour, this fails and the table is
    // re-examined; check_additive stays sound because the structural diff runs anyway.
    let expect_semver_reject = [
        ("add-func-context", false),
        ("add-interface-imported", false),
        ("add-record-field", true),
        ("add-variant-case", true),
        ("change-param-type", true),
        ("remove-func", true),
        ("rename-interface", true),
        ("add-workflows-export", true),
    ];
    for (name, reject) in expect_semver_reject {
        let report = check_additive_report(dir("base"), dir(name));
        assert_eq!(
            matches!(report.semver, SemverOutcome::Incompatible(_)),
            reject,
            "{name}: {:?}",
            report.semver
        );
    }
}
