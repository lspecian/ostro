//! Catalogue integrity (09-02). The catalogue is written once; owner plans only add spec
//! files under `conformance/spec/<leg>/`, and an entry is pending exactly while its file
//! is absent.

use std::collections::BTreeSet;

use ostro_conformance::spec::{
    self, CatalogueEntry, Kind, Leg, catalogue, load_catalogue, load_spec, pending_by_owner,
    spec_root,
};

/// `^C-[A-Z-]+-[0-9]{2}$` without a regex dependency.
fn id_ok(id: &str) -> bool {
    let Some(rest) = id.strip_prefix("C-") else {
        return false;
    };
    let b = rest.as_bytes();
    if b.len() < 4 {
        return false;
    }
    let (head, digits) = b.split_at(b.len() - 2);
    digits.iter().all(u8::is_ascii_digit)
        && head.len() >= 2
        && head[head.len() - 1] == b'-'
        && head[..head.len() - 1]
            .iter()
            .all(|c| c.is_ascii_uppercase() || *c == b'-')
        && head[0].is_ascii_uppercase()
}

/// `09-0[1-9] | 09-1[0-9] | 09-20`.
fn owner_ok(owner: &str) -> bool {
    let Some(n) = owner.strip_prefix("09-") else {
        return false;
    };
    n.len() == 2 && n.bytes().all(|c| c.is_ascii_digit()) && matches!(n.parse::<u32>(), Ok(1..=20))
}

#[test]
fn id_and_owner_patterns() {
    assert!(id_ok("C-STEP-01"));
    assert!(id_ok("C-SIG-API-01"));
    assert!(!id_ok("C-STEP-1"));
    assert!(!id_ok("C-step-01"));
    assert!(!id_ok("X-STEP-01"));
    assert!(!id_ok("C--01"));
    assert!(owner_ok("09-01") && owner_ok("09-19") && owner_ok("09-20"));
    assert!(!owner_ok("09-00") && !owner_ok("09-21") && !owner_ok("08-11") && !owner_ok("09-1"));
}

#[test]
fn catalogue_is_well_formed() {
    let cat = load_catalogue().expect("catalogue.json parses");
    assert!(!cat.entries.is_empty());
    let mut seen = BTreeSet::new();
    for e in &cat.entries {
        assert!(seen.insert(e.id.clone()), "duplicate id {}", e.id);
        assert!(id_ok(&e.id), "bad id {}", e.id);
        assert!(owner_ok(&e.owner), "{}: bad owner {}", e.id, e.owner);
        assert!(!e.title.trim().is_empty(), "{}: empty title", e.id);
        assert!(!e.languages.is_empty(), "{}: no languages", e.id);
    }
    // W6: exactly one rebuild entry, C-REBUILD-01, owned by 09-17.
    let rebuild: Vec<&CatalogueEntry> = cat
        .entries
        .iter()
        .filter(|e| e.kind == Kind::Rebuild)
        .collect();
    assert_eq!(rebuild.len(), 1);
    assert_eq!(rebuild[0].id, "C-REBUILD-01");
    assert_eq!(rebuild[0].owner, "09-17");
    // Leg runners never see the rebuild entry.
    assert!(spec::scenarios(Leg::Engine).all(|e| e.id != "C-REBUILD-01"));
    // Ids the later plans rely on.
    for id in [
        "C-STEP-04",
        "C-NATIVE-01",
        "C-VAL-01",
        "C-CHILD-05",
        "C-CHILD-08",
        "C-CHILD-10",
        "C-CHILD-11",
        "C-SAGA-03",
        "C-CRASH-01",
        "C-CRASH-02",
    ] {
        assert!(cat.get(id).is_some(), "missing {id}");
    }
}

#[test]
fn every_spec_file_belongs_to_the_catalogue_and_parses() {
    let cat = catalogue();
    let mut checked = 0;
    for leg in [Leg::Engine, Leg::Server, Leg::Crash] {
        let dir = spec_root().join(leg.dir());
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for f in rd {
            let path = f.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let id = path.file_stem().unwrap().to_string_lossy().into_owned();
            let entry = cat
                .get(&id)
                .unwrap_or_else(|| panic!("{} is not in the catalogue", path.display()));
            assert_eq!(entry.leg, leg, "{}: catalogue leg differs", path.display());
            let spec = load_spec(&id).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(spec.id, id);
            assert_eq!(spec.leg, leg);
            checked += 1;
        }
    }
    eprintln!("{checked} spec file(s) present and valid");
}

#[test]
fn report_pending() {
    let pending = pending_by_owner();
    let total: usize = pending.values().map(Vec::len).sum();
    eprintln!(
        "{total} pending of {} catalogue entries",
        catalogue().entries.len()
    );
    for (owner, ids) in &pending {
        eprintln!("  pending ({owner}): {}", ids.join(", "));
    }
    assert_eq!(total, spec::pending_ids().len());
}

/// Gate check (09-20): every catalogue entry has its spec file.
#[test]
#[ignore = "gate (09-20): run with --ignored all_specs_present"]
fn all_specs_present() {
    let pending = spec::pending_ids();
    assert!(pending.is_empty(), "pending spec files: {pending:?}");
}
