//! 10-08: `ostro.toml` schema (strict), project detection and name validation.

use std::path::Path;

use ostro_build::project::{
    self, BuildSection, DeploySection, Lang, Manifest, ProjectError, ProjectSection, ToolchainMode,
};

fn write(dir: &Path, name: &str, text: &str) {
    std::fs::write(dir.join(name), text).unwrap();
}

#[test]
fn ostro_toml_defaults() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "ostro.toml",
        "[project]\nname = \"a\"\nlang = \"python\"\n",
    );
    let p = project::load(d.path()).unwrap();
    assert!(!p.detected);
    assert_eq!(p.root, d.path());
    assert_eq!(
        p.manifest,
        Manifest {
            project: ProjectSection {
                name: "a".into(),
                lang: Lang::Python,
                entry: Some("app".into()),
            },
            build: BuildSection {
                toolchain: ToolchainMode::Auto
            },
            deploy: DeploySection {
                activate: true,
                tenant: None
            },
        }
    );
    assert_eq!(p.manifest.project.entry(), "app");
    assert_eq!(project::output_dir(&p), d.path().join("target/ostro"));
}

#[test]
fn ostro_toml_full_and_entry_defaults() {
    let m = Manifest::parse(
        "[project]\nname = \"w\"\nlang = \"ts\"\nentry = \"src/main.ts\"\n\
         [build]\ntoolchain = \"docker\"\n[deploy]\nactivate = false\ntenant = \"acme\"\n",
    )
    .unwrap();
    assert_eq!(m.project.entry(), "src/main.ts");
    assert_eq!(m.build.toolchain, ToolchainMode::Docker);
    assert!(!m.deploy.activate);
    assert_eq!(m.deploy.tenant.as_deref(), Some("acme"));
    for (lang, entry) in [
        ("rust", "Cargo.toml"),
        ("python", "app"),
        ("ts", "src/index.ts"),
    ] {
        let m = Manifest::parse(&format!("[project]\nname = \"x\"\nlang = \"{lang}\"\n")).unwrap();
        assert_eq!(m.project.entry.as_deref(), Some(entry), "{lang}");
    }
    for mode in ["auto", "hermetic", "system", "docker"] {
        Manifest::parse(&format!(
            "[project]\nname = \"x\"\nlang = \"rust\"\n[build]\ntoolchain = \"{mode}\"\n"
        ))
        .unwrap();
    }
}

#[test]
fn unknown_keys_are_rejected() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "ostro.toml",
        "[project]\nname = \"a\"\nlang = \"python\"\n[build]\nfoo = 1\n",
    );
    let e = project::load(d.path()).unwrap_err();
    assert!(matches!(e, ProjectError::Parse { .. }), "{e:?}");
    assert!(e.to_string().contains("foo"), "{e}");
    assert!(e.to_string().contains("ostro.toml"), "{e}");
    assert!(e.is_usage());

    for text in [
        "[project]\nname = \"a\"\nlang = \"rust\"\nextra = 1\n",
        "[project]\nname = \"a\"\nlang = \"rust\"\n[deploy]\nbar = true\n",
        "[project]\nname = \"a\"\nlang = \"rust\"\n[other]\n",
        "[project]\nname = \"a\"\nlang = \"go\"\n",
        "[project]\nname = \"a\"\nlang = \"rust\"\n[build]\ntoolchain = \"nix\"\n",
        "[project]\nlang = \"rust\"\n",
    ] {
        assert!(Manifest::parse(text).is_err(), "{text}");
    }
    let e = Manifest::parse("[project]\nname = \"A\"\nlang = \"rust\"\n").unwrap_err();
    assert!(matches!(e, ProjectError::InvalidName { .. }), "{e:?}");
}

#[test]
fn detection() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "Cargo.toml",
        "[package]\nname = \"x\"\nversion = \"0.1.0\"\n",
    );
    let p = project::load(d.path()).unwrap();
    assert!(p.detected);
    assert_eq!(p.manifest.project.lang, Lang::Rust);
    assert_eq!(p.manifest.project.name, "x");
    assert_eq!(p.manifest.build.toolchain, ToolchainMode::Auto);
    assert!(p.manifest.deploy.activate);

    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "package.json",
        r#"{"name": "web-wf", "type": "module"}"#,
    );
    let p = project::load(d.path()).unwrap();
    assert_eq!(
        (p.manifest.project.lang, p.manifest.project.name.as_str()),
        (Lang::Ts, "web-wf")
    );
    assert_eq!(p.manifest.project.entry(), "src/index.ts");

    let d = tempfile::tempdir().unwrap();
    write(d.path(), "pyproject.toml", "[project]\nname = \"py_wf\"\n");
    let p = project::load(d.path()).unwrap();
    assert_eq!(
        (p.manifest.project.lang, p.manifest.project.name.as_str()),
        (Lang::Python, "py_wf")
    );

    // ostro.toml wins over a language manifest
    write(
        d.path(),
        "ostro.toml",
        "[project]\nname = \"other\"\nlang = \"ts\"\n",
    );
    let p = project::load(d.path()).unwrap();
    assert!(!p.detected);
    assert_eq!(p.manifest.project.name, "other");

    let d = tempfile::tempdir().unwrap();
    let e = project::load(d.path()).unwrap_err();
    assert!(matches!(e, ProjectError::NoProject(_)), "{e:?}");
    assert!(e.is_usage());

    // a workspace-only Cargo.toml has no package name
    let d = tempfile::tempdir().unwrap();
    write(d.path(), "Cargo.toml", "[workspace]\nmembers = []\n");
    let e = project::load(d.path()).unwrap_err();
    assert!(matches!(e, ProjectError::Detect { .. }), "{e:?}");
}

#[test]
fn name_validation() {
    for ok in ["hello", "my-wf_2", "0x", "a", &"a".repeat(64)] {
        project::validate_name(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    for bad in [
        "",
        "a/b",
        "a:b",
        "__x",
        "UPPER",
        "-lead",
        "_lead",
        "sp ace",
        "dot.ted",
        "é",
        &"a".repeat(65),
    ] {
        let e = project::validate_name(bad).unwrap_err();
        assert!(
            matches!(e, ProjectError::InvalidName { .. }),
            "{bad}: {e:?}"
        );
    }
}

#[test]
fn lang_strings() {
    for l in Lang::ALL {
        assert_eq!(l.as_str().parse::<Lang>().unwrap(), l);
        assert_eq!(l.to_string(), l.as_str());
    }
    assert!("go".parse::<Lang>().is_err());
}
