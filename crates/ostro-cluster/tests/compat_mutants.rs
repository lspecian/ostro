//! Mutant bite for the 13-04 gate (D-P13-25): `upgrade::mutant::emit_ungated` makes
//! `compat::require` allow a gated feature before finalization. The gate property holds with
//! the mutant off, is violated with it on, and holds again after `fail::remove`.
#![cfg(feature = "failpoints")]

use ostro_cluster::compat::{self, MUTANT_EMIT_UNGATED};
use ostro_meta::MetaHandle;
use ostro_types::compat::{Feature, LEVEL, MUTANTS};

#[tokio::test(flavor = "multi_thread")]
async fn emit_ungated_lets_a_gated_feature_through_before_finalization() {
    assert!(
        MUTANTS.contains(&MUTANT_EMIT_UNGATED),
        "registered mutant name"
    );
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    meta.init_cluster("c-13-04-m", 1, 4, 1, [3; 32])
        .await
        .expect("init");
    let refused = || compat::require(meta.view(), Feature::RepinRun, LEVEL).is_err();

    assert!(
        refused(),
        "property holds without the mutant (finalized = LEVEL_MIN)"
    );
    fail::cfg(MUTANT_EMIT_UNGATED, "return").unwrap();
    let bitten = !refused();
    fail::remove(MUTANT_EMIT_UNGATED);
    assert!(
        bitten,
        "the mutant bites: a gated feature passed the gate unfinalized"
    );
    assert!(refused(), "holds again after fail::remove");
    meta.stop().await;
}
