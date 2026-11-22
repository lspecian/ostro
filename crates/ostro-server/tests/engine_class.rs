//! `[engine] class` and `[engine] pool_total_core_instances` (plan 09-07, ENG-2): TOML,
//! env, `--print-config` round trip, defaults unchanged, unknown class rejected.

use std::{collections::HashMap, path::PathBuf};

use ostro_engine::config::{EngineConfig, LanguageClass, describe_wasmtime_config};
use ostro_server::config::{EngineClass, EnvSource, Overrides, ServerConfig, load_layered};

fn env(pairs: &[(&str, &str)]) -> EnvSource {
    EnvSource::Map(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect::<HashMap<_, _>>(),
    )
}

fn write_toml(dir: &tempfile::TempDir, body: &str) -> PathBuf {
    let p = dir.path().join("ostro.toml");
    std::fs::write(&p, body).expect("write toml");
    p
}

fn load(file: Option<&PathBuf>, env: &EnvSource) -> Result<ServerConfig, String> {
    load_layered(
        ServerConfig::dev_defaults(),
        file.map(PathBuf::as_path),
        file.is_some(),
        env,
        &Overrides::default(),
    )
    .map_err(|e| e.to_string())
}

#[test]
fn defaults_unchanged() {
    let c = load(None, &env(&[])).expect("load");
    assert_eq!(c.engine.class, EngineClass::Rust);
    assert_eq!(c.engine.pool_total_core_instances, None);
    let e = c.engine.to_engine_config();
    assert_eq!(e.class, LanguageClass::Rust);
    assert_eq!(e.pool.total_core_instances, None);
    assert_eq!(e.effective_total_core_instances(), 3_000);
    // Same Wasmtime configuration as the engine default (snapshot-pinned there).
    assert_eq!(
        describe_wasmtime_config(&e),
        describe_wasmtime_config(&EngineConfig::default())
    );
    // An unset override is not printed.
    let s = c.to_toml_string();
    assert!(s.contains("class = \"rust\""), "{s}");
    assert!(!s.contains("pool_total_core_instances"), "{s}");
}

#[test]
fn from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(
        &dir,
        "[engine]\nclass = \"python\"\npool_total_core_instances = 50000\n",
    );
    let c = load(Some(&p), &env(&[])).expect("load");
    assert_eq!(c.engine.class, EngineClass::Python);
    assert_eq!(c.engine.pool_total_core_instances, Some(50_000));
    let e = c.engine.to_engine_config();
    assert_eq!(e.class, LanguageClass::Python);
    assert_eq!(e.effective_total_core_instances(), 50_000);
}

#[test]
fn from_env() {
    let c = load(
        None,
        &env(&[
            ("OSTRO_ENGINE__CLASS", "mixed"),
            ("OSTRO_ENGINE__POOL_TOTAL_CORE_INSTANCES", "42000"),
        ]),
    )
    .expect("load");
    assert_eq!(c.engine.class, EngineClass::Mixed);
    assert_eq!(c.engine.pool_total_core_instances, Some(42_000));
    let e = c.engine.to_engine_config();
    assert_eq!(e.class, LanguageClass::Mixed);
    assert_eq!(e.effective_total_core_instances(), 42_000);

    let c = load(None, &env(&[("OSTRO_ENGINE__CLASS", "typescript")])).expect("load");
    assert_eq!(c.engine.to_engine_config().class, LanguageClass::TypeScript);
}

#[test]
fn print_config_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(
        &dir,
        "[engine]\nclass = \"typescript\"\npool_total_core_instances = 13000\n",
    );
    let c = load(Some(&p), &env(&[])).expect("load");
    let s = c.to_toml_string();
    assert!(s.contains("class = \"typescript\""), "{s}");
    assert!(s.contains("pool_total_core_instances = 13000"), "{s}");
    let dir2 = tempfile::tempdir().unwrap();
    let p2 = write_toml(&dir2, &s);
    let back = load(Some(&p2), &env(&[])).expect("reload printed config");
    assert_eq!(back, c);
}

#[test]
fn unknown_class_names_allowed_values() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, "[engine]\nclass = \"go\"\n");
    let err = load(Some(&p), &env(&[])).unwrap_err();
    for want in ["go", "rust", "python", "typescript", "mixed"] {
        assert!(err.contains(want), "{want} missing from: {err}");
    }
    let err = load(None, &env(&[("OSTRO_ENGINE__CLASS", "java")])).unwrap_err();
    assert!(err.contains("java") && err.contains("mixed"), "{err}");
}
