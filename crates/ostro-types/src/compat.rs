//! Phase 13 feature levels and the mixed-version registry (13-01; D-P13-3, D-P13-7, D-P13-25).
//!
//! - A binary supports the feature levels `[LEVEL_MIN, LEVEL]` (one minor of downgrade
//!   window). `LEVEL_MIN` = the frozen v0.1 formats plus the Phase 12 commands and ext tags;
//!   `LEVEL` = Phase 13's emitted novelties.
//! - [`Feature`] is the one registry of gated emitters. Every Phase 13 feature is introduced at
//!   level 2. Per-WIT-function levels live in the engine's `introduced_in` table (13-07), not
//!   here.
//! - The machinery ext tags 96–99 (NodeVersion, MemberState, ClusterVersion,
//!   ReplicationPolicy) are written at **every** level: they are the gate's own state, unknown
//!   tags are preserved and ignored, and apply never validates ext values (05-20 rule 5).
//! - [`level_max`] is the highest level this process may emit. With the `level-emulation`
//!   feature (enabled only through the failpoints chains), `OSTRO_TEST_LEVEL_MAX=k` makes the
//!   process behave as a level-k binary (read once, clamped to `[LEVEL_MIN, LEVEL]`).
//! - [`MUTANTS`] lists the eight permanent Phase 13 failpoint mutants; each owning plan places
//!   its site with a "mutant bites" unit test, 13-18 runs the matrix.

/// Lowest feature level this binary can read and run (the frozen baseline).
pub const LEVEL_MIN: u32 = 1;

/// Highest feature level this binary can emit (Phase 13).
pub const LEVEL: u32 = 2;

/// Environment variable read by [`level_max`] when the `level-emulation` feature is on.
pub const TEST_LEVEL_MAX_ENV: &str = "OSTRO_TEST_LEVEL_MAX";

/// A gated Phase 13 emitter. Proposers check `finalized >= f.introduced_at()` before they
/// emit (13-04 `compat::require`); apply never checks (rule 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Feature {
    /// `MetaCommand::AlterReplicas` (variant 8).
    AlterReplicas,
    /// Partition `Cmd::RepinRun` (tag 43).
    RepinRun,
    /// `PartitionAssignment` ext tag 100 (Reassignment).
    ReassignmentExt,
    /// `ModuleRecord` ext tag 101 (WitRequirement).
    WitRequirementExt,
    /// `ModuleRecord` / `WorkflowVersion` ext tag 102 (Retired).
    RetiredExt,
    /// Phase 14 (D-P14-28): `ApiToken` ext tag 112 (ApiTokenMeta).
    ApiTokenMetaExt,
    /// Phase 14: `ModuleRecord` ext tag 113 (ModuleSignatures).
    ModuleSignaturesExt,
    /// Phase 14: `ModuleRecord` ext tag 114 (ModuleUploaders).
    ModuleUploadersExt,
    /// Phase 14: `ClusterInfo` ext tag 115 (SecurityPolicy).
    SecurityPolicyExt,
    /// Phase 14: `Tenant` ext tag 116 (TenantSecurity).
    TenantSecurityExt,
    /// Phase 14: `SealedSecret` ext tag 117 (SecretTimes).
    SecretTimesExt,
}

impl Feature {
    /// Every feature, in declaration order.
    pub const ALL: [Feature; 11] = [
        Feature::AlterReplicas,
        Feature::RepinRun,
        Feature::ReassignmentExt,
        Feature::WitRequirementExt,
        Feature::RetiredExt,
        Feature::ApiTokenMetaExt,
        Feature::ModuleSignaturesExt,
        Feature::ModuleUploadersExt,
        Feature::SecurityPolicyExt,
        Feature::TenantSecurityExt,
        Feature::SecretTimesExt,
    ];

    /// The Phase 14 ext emitters (D-P14-28): introduced at [`LEVEL`], no new level.
    pub const P14: [Feature; 6] = [
        Feature::ApiTokenMetaExt,
        Feature::ModuleSignaturesExt,
        Feature::ModuleUploadersExt,
        Feature::SecurityPolicyExt,
        Feature::TenantSecurityExt,
        Feature::SecretTimesExt,
    ];

    /// The feature level at which this feature may first be emitted.
    pub const fn introduced_at(self) -> u32 {
        match self {
            Feature::AlterReplicas
            | Feature::RepinRun
            | Feature::ReassignmentExt
            | Feature::WitRequirementExt
            | Feature::RetiredExt => 2,
            // Phase 14 registers its ext emissions at the Phase 13 level (D-P14-28).
            Feature::ApiTokenMetaExt
            | Feature::ModuleSignaturesExt
            | Feature::ModuleUploadersExt
            | Feature::SecurityPolicyExt
            | Feature::TenantSecurityExt
            | Feature::SecretTimesExt => LEVEL,
        }
    }

    /// Stable name (logs, API error details).
    pub const fn name(self) -> &'static str {
        match self {
            Feature::AlterReplicas => "alter_replicas",
            Feature::RepinRun => "repin_run",
            Feature::ReassignmentExt => "reassignment_ext",
            Feature::WitRequirementExt => "wit_requirement_ext",
            Feature::RetiredExt => "retired_ext",
            Feature::ApiTokenMetaExt => "api_token_meta_ext",
            Feature::ModuleSignaturesExt => "module_signatures_ext",
            Feature::ModuleUploadersExt => "module_uploaders_ext",
            Feature::SecurityPolicyExt => "security_policy_ext",
            Feature::TenantSecurityExt => "tenant_security_ext",
            Feature::SecretTimesExt => "secret_times_ext",
        }
    }
}

/// The gated feature of a partition `Cmd` oneof tag, if any (43 → [`Feature::RepinRun`]).
pub const fn feature_for_cmd_tag(tag: u32) -> Option<Feature> {
    match tag {
        43 => Some(Feature::RepinRun),
        _ => None,
    }
}

/// The gated feature of a `MetaCommand` postcard variant index, if any
/// (8 → [`Feature::AlterReplicas`]).
pub const fn feature_for_meta_variant(index: u32) -> Option<Feature> {
    match index {
        8 => Some(Feature::AlterReplicas),
        _ => None,
    }
}

/// The eight permanent Phase 13 failpoint mutants (D-P13-25; RESERVATIONS §11). Each owning
/// plan places its site and a unit "mutant bites" test; `tests/mutants_p13.rs` (13-18) runs
/// one scenario per name.
pub const MUTANTS: [&str; 8] = [
    "upgrade::mutant::emit_ungated",
    "upgrade::mutant::reject_and_continue",
    "member::mutant::skip_quorum_check",
    "member::mutant::no_leave_joint",
    "reassign::mutant::raise_min_insync_early",
    "route::mutant::serve_without_lease",
    "gc::mutant::single_scan",
    "migrate::mutant::skip_seq_check",
];

/// Failpoint prefixes owned by Phase 13 (RESERVATIONS §11).
pub const FAILPOINT_PREFIXES: [&str; 7] = [
    "member::",
    "reassign::",
    "route::",
    "upgrade::",
    "gc::",
    "handoff::",
    "migrate::",
];

/// Parse an `OSTRO_TEST_LEVEL_MAX` value: `k` clamped to `[LEVEL_MIN, LEVEL]`; `None` when
/// absent or not a number.
pub fn parse_level_override(v: Option<&str>) -> Option<u32> {
    let k: u32 = v?.trim().parse().ok()?;
    Some(k.clamp(LEVEL_MIN, LEVEL))
}

/// The highest feature level this process may emit: [`LEVEL`], or with the `level-emulation`
/// feature, `OSTRO_TEST_LEVEL_MAX` clamped to `[LEVEL_MIN, LEVEL]` (read once per process).
pub fn level_max() -> u32 {
    #[cfg(feature = "level-emulation")]
    {
        static EMULATED: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
        *EMULATED.get_or_init(|| {
            parse_level_override(std::env::var(TEST_LEVEL_MAX_ENV).ok().as_deref()).unwrap_or(LEVEL)
        })
    }
    #[cfg(not(feature = "level-emulation"))]
    {
        LEVEL
    }
}

/// Whether a binary with `level_max` can run data finalized at `finalized`.
pub const fn supports(finalized: u32, level_max: u32) -> bool {
    finalized >= LEVEL_MIN && finalized <= level_max
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn levels_are_ordered() {
        const { assert!(LEVEL_MIN >= 1) };
        const { assert!(LEVEL_MIN <= LEVEL) };
        assert_eq!((LEVEL_MIN, LEVEL), (1, 2));
    }

    #[test]
    fn every_feature_is_within_the_supported_range() {
        for f in Feature::ALL {
            assert!(
                f.introduced_at() > LEVEL_MIN,
                "{f:?} must be gated above the baseline"
            );
            assert!(f.introduced_at() <= LEVEL, "{f:?} introduced above LEVEL");
        }
        let names: BTreeSet<_> = Feature::ALL.iter().map(|f| f.name()).collect();
        assert_eq!(names.len(), Feature::ALL.len(), "feature names unique");
    }

    #[test]
    fn phase14_features_are_at_level_and_in_all() {
        for f in Feature::P14 {
            assert_eq!(f.introduced_at(), LEVEL, "{f:?}: no new level in Phase 14");
            assert!(f.introduced_at() <= LEVEL);
            assert!(Feature::ALL.contains(&f), "{f:?} missing from ALL");
        }
        assert_eq!(
            Feature::P14.map(|f| f.name()),
            [
                "api_token_meta_ext",
                "module_signatures_ext",
                "module_uploaders_ext",
                "security_policy_ext",
                "tenant_security_ext",
                "secret_times_ext",
            ]
        );
    }

    #[test]
    fn ordinal_maps_point_at_the_p13_block() {
        assert_eq!(feature_for_cmd_tag(43), Some(Feature::RepinRun));
        assert_eq!(feature_for_meta_variant(8), Some(Feature::AlterReplicas));
        for t in [1, 13, 27, 44, 45] {
            assert_eq!(feature_for_cmd_tag(t), None, "cmd tag {t}");
        }
        for v in 0..=7 {
            assert_eq!(feature_for_meta_variant(v), None, "meta variant {v}");
        }
    }

    #[test]
    fn mutant_names_are_unique_with_known_prefixes() {
        let set: BTreeSet<_> = MUTANTS.iter().collect();
        assert_eq!(set.len(), MUTANTS.len(), "mutant names unique");
        for m in MUTANTS {
            assert!(
                FAILPOINT_PREFIXES.iter().any(|p| m.starts_with(p)),
                "{m}: unknown prefix"
            );
            assert!(m.contains("::mutant::"), "{m}: not a mutant name");
        }
        assert!(MUTANTS.contains(&"member::mutant::skip_quorum_check"));
    }

    #[test]
    fn level_override_parses_and_clamps() {
        assert_eq!(parse_level_override(None), None);
        assert_eq!(parse_level_override(Some("x")), None);
        assert_eq!(parse_level_override(Some("1")), Some(1));
        assert_eq!(parse_level_override(Some(" 2 ")), Some(2));
        assert_eq!(parse_level_override(Some("0")), Some(LEVEL_MIN));
        assert_eq!(parse_level_override(Some("99")), Some(LEVEL));
    }

    #[test]
    fn supports_is_the_closed_range() {
        assert!(!supports(0, LEVEL));
        assert!(supports(LEVEL_MIN, LEVEL));
        assert!(supports(LEVEL, LEVEL));
        assert!(!supports(LEVEL + 1, LEVEL));
        assert!(!supports(2, 1));
    }

    /// With `level-emulation` the emulated level always stays inside the supported range.
    #[cfg(feature = "level-emulation")]
    #[test]
    fn emulated_level_is_within_range() {
        let l = level_max();
        assert!((LEVEL_MIN..=LEVEL).contains(&l), "{l}");
        assert_eq!(level_max(), l, "read once");
    }

    /// Without `level-emulation` the environment variable is never read.
    #[cfg(not(feature = "level-emulation"))]
    #[test]
    fn emulation_env_is_ignored_without_the_feature() {
        // SAFETY: test-only; no other test in this crate reads this variable.
        unsafe { std::env::set_var(TEST_LEVEL_MAX_ENV, "1") };
        assert_eq!(level_max(), LEVEL);
        // SAFETY: as above.
        unsafe { std::env::remove_var(TEST_LEVEL_MAX_ENV) };
    }
}
