//! ENG-10 (13-07, D-P13-17): side-by-side WIT linking by major line.
//!
//! - The 0.1 line defines only the newest `0.1.N`; 0.1.0 and 0.1.N guests both instantiate
//!   on it (imports and the `workflows` export resolve semver-compatibly).
//! - `select` reads a component's imports: one line per module, min level = max over the
//!   imported functions' `introduced_in` (not the package version).
//! - With `wit-next-fixture`: a real host built at `ostro:runtime@0.1.1` runs an unmodified
//!   0.1.0 guest (the fuel-aware journal overrides reach it) and a 0.1.1 guest; a test-only
//!   `ostro:runtime@0.2.0` line links side by side with its own linker.
//! - The AOT cache keys artefacts by line.
//!
//! Fixture components are real guests (`tests/common/wit_fixtures.rs`).

mod common;
#[path = "common/wit_fixtures.rs"]
mod wit_fixtures;

use ostro_engine::cache::{
    AotCache, cache_key, cache_key_for, compat_digest, load_or_create_key, module_sha,
};
use ostro_engine::config::EngineConfig;
use ostro_engine::host::linker_set::{
    LinkerSet, WIT_0_1_LATEST, WitLine, WitSelection, line_from_bytes, ostro_iface, select,
    select_lines,
};
use ostro_engine::host::state::journal_iface;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::validate::{ValidateError, validate_component};
use wasmtime::component::Component;

fn engine() -> wasmtime::Engine {
    common::test_engine()
}

fn component(engine: &wasmtime::Engine, wasm: &[u8]) -> Component {
    Component::from_binary(engine, wasm).expect("component")
}

fn ostro_imports(engine: &wasmtime::Engine, c: &Component) -> Vec<String> {
    c.component_type()
        .imports(engine)
        .map(|(n, _)| n.to_string())
        .filter(|n| n.starts_with("ostro:runtime/"))
        .collect()
}

fn exports(engine: &wasmtime::Engine, c: &Component) -> Vec<String> {
    c.component_type()
        .exports(engine)
        .map(|(n, _)| n.to_string())
        .collect()
}

const V0_1_L1: WitSelection = WitSelection {
    line: WitLine::V0_1,
    min_level: 1,
};

#[test]
fn instantiate_0_1_0_on_latest() {
    let engine = engine();
    let set = LinkerSet::new(&engine).expect("linkers");
    let linker = set.linker(WitLine::V0_1).expect("0.1 linker");
    let wasm = wit_fixtures::guest_0_1_0();
    let v = validate_component(&engine, linker, &wasm).expect("0.1.0 guest validates");
    assert_eq!(v.selection, V0_1_L1);
    assert!(v.imports.iter().any(|n| n == "ostro:runtime/journal@0.1.0"));
    // The workflows export is found by the 0.1 bindings through InstancePre.
    let pre = linker
        .instantiate_pre(&v.component)
        .expect("instantiate_pre");
    ostro_engine::host::bindings::WorkflowPre::new(pre).expect("workflows export");
    let (sel, _) = set
        .for_component(&v.component, &engine)
        .expect("for_component");
    assert_eq!(sel, V0_1_L1);
}

/// A guest built against a newer `0.1.N` package that calls only 0.1.0 functions imports
/// `@0.1.1` names, needs level 1 and instantiates on the 0.1.0 production host: its imports
/// and its `workflows@0.1.1` export resolve through Wasmtime's semver alternate key.
#[test]
fn instantiate_0_1_n_on_latest() {
    let engine = engine();
    let set = LinkerSet::new(&engine).expect("linkers");
    let linker = set.linker(WitLine::V0_1).expect("0.1 linker");
    let wasm = wit_fixtures::guest_0_1_1_lite();
    let c = component(&engine, &wasm);
    let imports = ostro_imports(&engine, &c);
    assert!(
        imports.iter().all(|n| n.ends_with("@0.1.1")),
        "built against 0.1.1: {imports:?}"
    );
    assert!(
        exports(&engine, &c).contains(&"ostro:runtime/workflows@0.1.1".to_string()),
        "{:?}",
        exports(&engine, &c)
    );
    let v = validate_component(&engine, linker, &wasm).expect("lite guest validates");
    assert_eq!(v.selection, V0_1_L1);
    let pre = linker
        .instantiate_pre(&v.component)
        .expect("instantiate_pre");
    ostro_engine::host::bindings::WorkflowPre::new(pre).expect("workflows@0.1 export lookup");
}

/// The lite guest also runs end to end on the production runtime (0.1.0 host).
#[tokio::test]
async fn lite_0_1_1_guest_runs_on_0_1_0_host() {
    use ostro_engine::sink::{ActivationOutcome, RunRef};
    use ostro_engine::testkit::{RunSpec, Script, drive_to_completion};
    let rt = Runtime::new(EngineConfig::default(), RuntimeOptions::default()).expect("runtime");
    let p = rt
        .prepare_cached(&wit_fixtures::guest_0_1_1_lite())
        .await
        .expect("prepare");
    assert_eq!(p.wit, V0_1_L1);
    let run = RunRef {
        partition: 0,
        run_id: "lite".into(),
    };
    let d = drive_to_completion(
        &rt,
        &p,
        RunSpec::new(run, "w011-lite", b"in"),
        Script::new(),
    )
    .await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        panic!("{:?}", d.outcome);
    };
    assert_eq!(output.data, b"w011-lite:in");
}

#[test]
fn select_min_level_from_functions() {
    let engine = engine();
    // Built against 0.1.1, only 0.1.0 functions: level 1 (functions, not the package).
    let c = component(&engine, &wit_fixtures::guest_0_1_1_lite());
    assert_eq!(select(&c, &engine).expect("select"), V0_1_L1);
    // A plain 0.1.0 guest: level 1.
    let c = component(&engine, &wit_fixtures::guest_0_1_0());
    assert_eq!(select(&c, &engine).expect("select"), V0_1_L1);
    // Calls journal.note-hint: level 2 in the test-only introduced_in entry; unknown to a
    // production build (an upload error, not a silent level 1).
    let c = component(&engine, &wit_fixtures::guest_0_1_1());
    assert!(
        ostro_imports(&engine, &c).contains(&"ostro:runtime/journal@0.1.1".to_string()),
        "{:?}",
        ostro_imports(&engine, &c)
    );
    let got = select_lines(&c, &engine);
    if cfg!(feature = "wit-next-fixture") {
        assert_eq!(
            got,
            Ok(WitSelection {
                line: WitLine::V0_1,
                min_level: 2
            })
        );
    } else {
        assert!(
            matches!(&got, Err(ValidateError::UnknownWitFunction { func, .. }) if func == "note-hint"),
            "{got:?}"
        );
    }
}

#[test]
fn mixed_lines_rejected() {
    let engine = engine();
    let wasm = wit_fixtures::guest_mixed();
    let c = component(&engine, &wasm);
    let imports = ostro_imports(&engine, &c);
    assert!(
        imports.contains(&"ostro:runtime/log@0.1.0".to_string())
            && imports.contains(&"ostro:runtime/log@0.2.0".to_string()),
        "{imports:?}"
    );
    let mixed = ValidateError::MixedWitLines(vec!["0.1".into(), "0.2".into()]);
    assert_eq!(select_lines(&c, &engine), Err(mixed.clone()));
    assert_eq!(line_from_bytes(&wasm), Err(mixed.clone()));
    // Upload error: an EngineError::Validation carrying the message.
    let err = select(&c, &engine).expect_err("mixed");
    assert!(
        err.to_string().contains("mixes WIT lines 0.1, 0.2"),
        "{err}"
    );
    let set = LinkerSet::new(&engine).expect("linkers");
    let err = validate_component(&engine, set.linker(WitLine::V0_1).unwrap(), &wasm)
        .expect_err("mixed guest must not validate");
    assert!(err.to_string().contains(&mixed.to_string()), "{err}");
    let rt = Runtime::new(EngineConfig::default(), RuntimeOptions::default()).expect("runtime");
    let err = rt.prepare(&wasm).expect_err("runtime rejects mixed lines");
    assert!(err.to_string().contains("mixes WIT lines"), "{err}");
}

/// Non-`ostro:runtime` imports (the WASI shim) never take part in line selection.
#[test]
fn wasi_imports_ignored_for_selection() {
    let engine = engine();
    let wasm = wit_fixtures::guest_0_1_0();
    let c = component(&engine, &wasm);
    let all: Vec<String> = c
        .component_type()
        .imports(&engine)
        .map(|(n, _)| n.to_string())
        .collect();
    assert!(all.iter().any(|n| n.starts_with("wasi:")), "{all:?}");
    assert_eq!(select(&c, &engine).expect("select"), V0_1_L1);
    assert_eq!(line_from_bytes(&wasm), Ok(WitLine::V0_1));
}

/// Every hand-written `ostro:runtime` interface name in `host/` derives from
/// `WIT_0_1_LATEST` (only the constant table in `linker_set.rs` may spell a version).
#[test]
fn no_versioned_iface_literal_in_host() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/host");
    let mut offenders = Vec::new();
    for e in std::fs::read_dir(dir).expect("src/host") {
        let path = e.expect("entry").path();
        if path.extension().is_none_or(|x| x != "rs")
            || path.file_name().is_some_and(|n| n == "linker_set.rs")
        {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read");
        for (i, line) in text.lines().enumerate() {
            if line.contains("@0.1.0\"") {
                offenders.push(format!("{}:{}: {line}", path.display(), i + 1));
            }
        }
    }
    assert!(offenders.is_empty(), "{offenders:#?}");
    assert_eq!(journal_iface(), ostro_iface("journal", WIT_0_1_LATEST));
    assert_eq!(
        journal_iface(),
        format!("ostro:runtime/journal@{WIT_0_1_LATEST}")
    );
}

/// `fixtures/wit-0.1.1` is the frozen 0.1.0 package, version bumped, plus exactly one
/// function (so it stays additive in the WIT-07 sense); `wit/` itself is untouched.
#[test]
fn fixture_0_1_1_is_frozen_plus_one_function() {
    let root = common::repo_root();
    let frozen = std::fs::read_to_string(root.join("wit/ostro-runtime-0.1.0/runtime.wit"))
        .expect("frozen wit");
    let fixture =
        std::fs::read_to_string(root.join("crates/ostro-engine/fixtures/wit-0.1.1/runtime.wit"))
            .expect("fixture wit");
    let anchor = "    begin-step: func(name: string, opts: step-options) -> step-start;\n";
    let added = "\n    /// Test fixture (13-07, never shipped): the one function 0.1.1 adds over 0.1.0.\n    \
                 /// Level 2 in the engine's test-only `introduced_in` entry. The host ignores the \
                 hint.\n    note-hint: func(hint: string);\n";
    let want = frozen
        .replacen(
            "package ostro:runtime@0.1.0;",
            "package ostro:runtime@0.1.1;",
            1,
        )
        .replacen(anchor, &format!("{anchor}{added}"), 1);
    assert_eq!(fixture, want);
    let entries: Vec<_> = std::fs::read_dir(root.join("wit"))
        .expect("wit/")
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .collect();
    assert!(
        !entries
            .iter()
            .any(|e| e.contains("0.1.1") || e.contains("0.2")),
        "fixtures never live under wit/: {entries:?}"
    );
}

#[cfg(not(feature = "wit-next-fixture"))]
#[test]
fn unsupported_0_2_without_feature() {
    let engine = engine();
    let wasm = wit_fixtures::guest_0_2_0();
    let c = component(&engine, &wasm);
    let unsupported = ValidateError::UnsupportedWitLine("0.2".into());
    assert_eq!(select_lines(&c, &engine), Err(unsupported.clone()));
    assert_eq!(line_from_bytes(&wasm), Err(unsupported.clone()));
    assert_eq!(LinkerSet::default().lines(), [WitLine::V0_1]);
    let rt = Runtime::new(EngineConfig::default(), RuntimeOptions::default()).expect("runtime");
    let err = rt.prepare(&wasm).expect_err("0.2 is not linked");
    assert!(
        err.to_string().contains("WIT line 0.2 is not supported"),
        "{err}"
    );
}

/// L1 AOT key and L2 by line: the same bytes compiled for two lines are two artefacts.
#[tokio::test]
async fn cache_key_includes_line() {
    let engine = engine();
    let wasm = wit_fixtures::guest_0_1_0();
    let sha = module_sha(&wasm);
    let k01 = cache_key_for(&engine, &sha, WitLine::V0_1);
    let k02 = cache_key_for(&engine, &sha, WitLine::V0_2);
    assert_ne!(k01, k02);
    // The 0.1 key is byte-identical to the pre-13-07 key (hex sha, '-', 16 hex chars of
    // the compat digest); another line salts the digest part with the line.
    assert_eq!(cache_key(&engine, &sha), k01);
    let compat: String = compat_digest(&engine)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(k01, format!("{}-{}", &k01[..64], &compat[..16]));
    assert_eq!(k01[..65], k02[..65]);
    assert_eq!(k02.len(), 64 + 1 + 16);

    let dir = tempfile::tempdir().expect("tempdir");
    let key = load_or_create_key(dir.path()).expect("key");
    let cache = AotCache::new(dir.path(), key, engine.clone(), 2).expect("cache");
    cache
        .component_for(sha, &wasm, WitLine::V0_1)
        .await
        .expect("0.1");
    cache
        .component_for(sha, &wasm, WitLine::V0_2)
        .await
        .expect("0.2");
    cache
        .component_for(sha, &wasm, WitLine::V0_1)
        .await
        .expect("0.1 again");
    let s = cache.stats();
    assert_eq!((s.compiles, s.hits), (2, 1), "{s:?}");
    assert!(cache.disk().artefact_path(&k01).exists());
    assert!(cache.disk().artefact_path(&k02).exists());
}

#[cfg(feature = "wit-next-fixture")]
mod fixture {
    use super::*;
    use std::sync::Arc;

    use ostro_engine::engine::{OstroEngine, configure_store};
    use ostro_engine::host::fixture_0_1_1;
    use ostro_engine::host::next_fixture::{self, FIXTURE_REGION};
    use ostro_engine::host::state::{HostInit, HostState};
    use ostro_engine::sink::{ActivationOutcome, RunRef};
    use ostro_engine::testkit::{RunSpec, Script, drive_to_completion};
    use ostro_types::journal::EventKind;
    use std::time::{Duration, Instant};

    fn fixture_runtime() -> Runtime {
        Runtime::with_linkers(
            EngineConfig::default(),
            RuntimeOptions::default(),
            LinkerSet::with_fixture_0_1_1,
        )
        .expect("fixture runtime")
    }

    fn run(id: &str) -> RunRef {
        RunRef {
            partition: 0,
            run_id: id.into(),
        }
    }

    /// The real-host proof of the semver path (plan check W12): a host built at 0.1.1 runs
    /// an unmodified 0.1.0 SDK guest whose steps go through `begin-step` and
    /// `[method]step.complete`. The fixture's bindgen versions of those two fail loudly, so
    /// completing three steps proves the fuel-aware overrides reached the 0.1.0 guest (no
    /// separate exact-name `@0.1.0` journal exists).
    #[tokio::test]
    async fn guest_0_1_0_on_host_0_1_1() {
        let rt = fixture_runtime();
        let p = rt
            .prepare_cached(&wit_fixtures::guest_0_1_0())
            .await
            .expect("0.1.0 guest prepares on the 0.1.1 host");
        assert_eq!(p.wit, V0_1_L1);
        let d = drive_to_completion(
            &rt,
            &p,
            RunSpec::new(run("old"), "trivial-steps", br#"{"n":3}"#),
            Script::new(),
        )
        .await;
        let ActivationOutcome::Completed { output } = &d.outcome else {
            panic!("{:?}", d.outcome);
        };
        assert_eq!(output.data, b"3");
        let steps = d
            .journal
            .iter()
            .filter(|e| e.body.kind() == EventKind::StepCompleted)
            .count();
        assert_eq!(steps, 3);

        // And a 0.1.1 guest (calls note-hint; exports workflows@0.1.1, found by the
        // runtime's workflows@0.1.0 lookup) runs on the same host at level 2.
        let p = rt
            .prepare_cached(&wit_fixtures::guest_0_1_1())
            .await
            .expect("0.1.1 guest prepares");
        assert_eq!(
            p.wit,
            WitSelection {
                line: WitLine::V0_1,
                min_level: 2
            }
        );
        let d = drive_to_completion(
            &rt,
            &p,
            RunSpec::new(run("new"), "w011", b"x"),
            Script::new(),
        )
        .await;
        let ActivationOutcome::Completed { output } = &d.outcome else {
            panic!("{:?}", d.outcome);
        };
        assert_eq!(output.data, b"w011:x");
    }

    /// The trap the 13-07 fix closes: with the overrides at a stale exact `journal@0.1.0`
    /// name on a 0.1.1 host, that name holds only the two overrides and wins the lookup, so
    /// a 0.1.0 guest no longer instantiates.
    #[test]
    fn journal_overrides_cover_0_1_0_guest() {
        let engine = engine();
        let c = component(&engine, &wit_fixtures::guest_0_1_0());
        let good = fixture_0_1_1::build_linker(&engine).expect("fixture linker");
        good.instantiate_pre(&c)
            .expect("overrides at journal@0.1.1 cover the 0.1.0 guest");
        let stale = fixture_0_1_1::build_linker_with_journal_iface(
            &engine,
            &ostro_iface("journal", "0.1.0"),
        )
        .expect("stale linker builds");
        let err = stale
            .instantiate_pre(&c)
            .err()
            .expect("a stale exact-name journal must break the 0.1.0 guest");
        assert!(format!("{err:#}").contains("journal"), "{err:#}");
    }

    #[test]
    fn select_0_2_fixture() {
        let engine = engine();
        let wasm = wit_fixtures::guest_0_2_0();
        let c = component(&engine, &wasm);
        assert!(
            ostro_imports(&engine, &c)
                .iter()
                .all(|n| n.ends_with("@0.2.0")),
            "{:?}",
            ostro_imports(&engine, &c)
        );
        assert_eq!(
            select(&c, &engine).expect("select"),
            WitSelection {
                line: WitLine::V0_2,
                min_level: 2
            }
        );
        assert_eq!(line_from_bytes(&wasm), Ok(WitLine::V0_2));
        let set = LinkerSet::new(&engine).expect("linkers");
        assert_eq!(set.lines(), [WitLine::V0_1, WitLine::V0_2]);
        let (sel, _) = set.for_component(&c, &engine).expect("for_component");
        assert_eq!(sel.line, WitLine::V0_2);
        let v = validate_component(&engine, set.linker(WitLine::V0_2).unwrap(), &wasm)
            .expect("0.2 guest validates on the 0.2 linker");
        assert_eq!(v.selection.line, WitLine::V0_2);
        // Lines never cross: the 0.2 guest does not link on 0.1, nor the 0.1.0 guest on 0.2.
        assert!(
            set.linker(WitLine::V0_1)
                .unwrap()
                .instantiate_pre(&c)
                .is_err()
        );
        let c01 = component(&engine, &wit_fixtures::guest_0_1_0());
        assert!(
            set.linker(WitLine::V0_2)
                .unwrap()
                .instantiate_pre(&c01)
                .is_err()
        );
        // The runtime validates it but has no 0.2 activation driver.
        let rt = Runtime::new(EngineConfig::default(), RuntimeOptions::default()).expect("rt");
        let err = rt.prepare(&wasm).expect_err("0.2 is not executable");
        assert!(err.to_string().contains("no activation driver"), "{err}");
    }

    /// The 0.2 guest instantiates on its own linker and runs through the fixture bindings:
    /// the changed `run-info` (region) reaches it and its step goes through the overrides.
    #[tokio::test]
    async fn instantiate_0_2_fixture() {
        let engine = OstroEngine::new(EngineConfig::default()).expect("engine");
        let set = LinkerSet::new(engine.engine()).expect("linkers");
        let linker = set.linker(WitLine::V0_2).expect("0.2 linker");
        let c = component(engine.engine(), &wit_fixtures::guest_0_2_0());
        let pre = next_fixture::bindings::WorkflowPre::new(
            linker.instantiate_pre(&c).expect("instantiate_pre"),
        )
        .expect("workflows@0.2.0 export");
        let spec = RunSpec::new(run("v2"), "w020", b"in");
        let state = HostState::new(HostInit {
            run: run("v2"),
            attempt: 1,
            module_hash_hex: "ab".repeat(32),
            journal: vec![spec.run_started_event(&"ab".repeat(32))],
            clock: Arc::new(ostro_engine::testkit::FakeClock::with_step(
                ostro_engine::testkit::DEFAULT_ACCEPTED_AT,
                1_000_000,
            )),
            limiter: engine.new_limiter(),
            wall_deadline: Instant::now() + Duration::from_secs(60),
            token_key: [7; 32],
            continue_as_new_events: engine.config().continue_as_new_events,
            sink: None,
            hot: None,
        })
        .expect("host state");
        let mut store = wasmtime::Store::new(engine.engine(), state);
        configure_store(&mut store, engine.config().activation_fuel).expect("store");
        let wf = pre
            .instantiate_async(&mut store)
            .await
            .expect("instantiate");
        let metas = wf
            .ostro_runtime_workflows()
            .call_describe(&mut store)
            .await
            .expect("describe");
        assert_eq!(metas[0].name, "w020");
        let input = next_fixture::bindings::ostro::runtime::types::Payload {
            content_type: "application/octet-stream".into(),
            data: b"in".to_vec(),
        };
        let out = wf
            .ostro_runtime_workflows()
            .call_run(&mut store, "w020", &input)
            .await
            .expect("run must not trap")
            .expect("run ok");
        use next_fixture::bindings::exports::ostro::runtime::workflows::RunResult;
        let RunResult::Completed(p) = out else {
            panic!("expected completed");
        };
        assert_eq!(p.data, format!("{FIXTURE_REGION}:in").into_bytes());
    }
}
