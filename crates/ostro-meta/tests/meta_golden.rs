//! 05-07 Task 3: the meta encoding golden file and the pre-freeze format
//! rejection (D20).
//!
//! `tests/golden/meta_v2.txt` pins `postcard(MetaRecord)` for one record of
//! every kind (each with a non-empty ext under the test-only tag 15), the
//! `[META_FORMAT] ‖ postcard(MetaCommand)` Raft entry of every command, and
//! the snapshot of an empty and of a fully populated `MetaState`.
//!
//! The file is **append-only**: new lines may be added at the end, existing
//! lines never change. Bless new lines with
//! `OSTRO_BLESS=1 cargo test -p ostro-meta --test meta_golden`; blessing
//! refuses to rewrite an existing line. A drifted line means the frozen meta
//! encoding changed: follow D20 (new variant + META_FORMAT bump with decoders,
//! or a registered ext tag) instead.

use std::collections::BTreeMap;

use ostro_meta::{
    ActiveVersion, ApiToken, ClusterInfo, Connection, DecodeError, EXT_TAG_TEST, EgressPolicy,
    Grant, META_FORMAT, Member, MetaCommand, MetaKey, MetaKind, MetaRecord, MetaResult, MetaSm,
    MetaState, ModuleRecord, PRE_FREEZE_META_FORMAT, PartitionAssignment, QueueConfig, QueuePolicy,
    RateSpec, Schedule, SealedSecret, SpillPolicy, StateMachine, Tenant, Webhook, WorkflowPolicy,
    WorkflowVersion, decode_command, decode_snapshot, encode_command, encode_snapshot, ext_entries,
    ext_put,
};
use serde::{Deserialize, Serialize};

const GOLDEN: &str = include_str!("golden/meta_v2.txt");
const HEADER: &str = "# ostro meta golden vectors (META_FORMAT 2) — APPEND-ONLY: existing lines never change (D20; frozen at 05-20)";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn ext(v: &[u8]) -> Vec<u8> {
    ext_put(&[], EXT_TAG_TEST, v).unwrap()
}

fn rate(limit: u32, period_ms: u64, burst: u32) -> RateSpec {
    RateSpec {
        limit,
        period_ms,
        burst,
    }
}

/// One fixed record per kind, in `MetaKind::ALL` order. Every field is set
/// explicitly so a new field cannot slip in through `Default`.
fn golden_records() -> Vec<(&'static str, MetaRecord)> {
    vec![
        (
            "cluster",
            MetaRecord::Cluster(ClusterInfo {
                cluster_id: "0193c8e0-golden".into(),
                partitions: 2,
                format: 1,
                created_at_ms: 1_700_000_000_000,
                ext: ext(&[0]),
            }),
        ),
        (
            "member",
            MetaRecord::Member(Member {
                node_id: 2,
                addr: "10.0.0.2:7000".into(),
                joined_at_ms: 1_700_000_000_001,
                cluster_addr: Some("10.0.0.2:7001".into()),
                repl_addr: Some("10.0.0.2:7002".into()),
                ext: ext(&[1]),
            }),
        ),
        (
            "partition",
            MetaRecord::Partition(PartitionAssignment {
                partition: 5,
                leader: 2,
                epoch: 3,
                isr: vec![2, 1],
                replicas: vec![2, 1, 3],
                isr_version: 4,
                min_insync: 2,
                ext: ext(&[2]),
            }),
        ),
        (
            "module",
            MetaRecord::Module(ModuleRecord {
                sha256: [0xab; 32],
                size: 123_456,
                language: Some("rust".into()),
                workflows: vec![("order".into(), 1), ("order".into(), 2)],
                uploaded_at_ms: 1_700_000_000_002,
                ext: ext(&[3]),
            }),
        ),
        (
            "workflow",
            MetaRecord::Workflow(WorkflowVersion {
                name: "acme/order".into(),
                version: 2,
                module_sha256: [0xab; 32],
                default_queue: Some("acme/default".into()),
                run_timeout_ms: Some(86_400_000),
                created_at_ms: 1_700_000_000_003,
                module: "acme/order-svc".into(),
                ext: ext(&[4]),
            }),
        ),
        (
            "active",
            MetaRecord::Active(ActiveVersion {
                name: "acme/order".into(),
                version: 2,
                ext: ext(&[5]),
            }),
        ),
        (
            "queue",
            MetaRecord::Queue(QueueConfig {
                name: "acme/default".into(),
                max_concurrency: Some(64),
                created_at_ms: 1_700_000_000_004,
                ext: ext(&[6]),
            }),
        ),
        (
            "schedule",
            MetaRecord::Schedule(Schedule {
                name: "acme/nightly".into(),
                workflow: "acme/order".into(),
                version: Some(2),
                cron: "0 3 * * *".into(),
                input: b"{\"full\":true}".to_vec(),
                paused: false,
                timezone: "Europe/Lisbon".into(),
                overlap: 1,
                catchup: 2,
                catchup_window_ms: 3_600_000,
                start_at_ms: Some(1_700_000_000_000),
                end_at_ms: Some(1_800_000_000_000),
                created_at_ms: 1_700_000_000_005,
                ext: ext(&[7]),
            }),
        ),
        (
            "token",
            MetaRecord::Token(ApiToken {
                id: "otk_golden".into(),
                hash: [0x11; 32],
                scopes: vec!["runs:write".into(), "runs:read".into()],
                tenant: Some("acme".into()),
                created_at_ms: 1_700_000_000_006,
                expires_at_ms: Some(1_700_086_400_006),
                ext: ext(&[8]),
            }),
        ),
        (
            "tenant",
            MetaRecord::Tenant(Tenant {
                id: "acme".into(),
                created_at_ms: 1_700_000_000_007,
                weight: Some(3),
                max_concurrency: Some(500),
                fuel_per_s: Some(10_000_000),
                fuel_burst: Some(50_000_000),
                memory_bytes: Some(1 << 30),
                event_rate: Some(rate(1000, 1000, 100)),
                suspended: false,
                ext: ext(&[9]),
            }),
        ),
        (
            "secret",
            MetaRecord::Secret(SealedSecret {
                name: "acme/stripe".into(),
                ciphertext: vec![0xc0, 0xff, 0xee],
                key_id: "k1".into(),
                ext: ext(&[10]),
            }),
        ),
        (
            "grant",
            MetaRecord::Grant(Grant {
                id: "secret:acme/stripe".into(),
                subject: "acme/order".into(),
                scopes: vec!["read".into()],
                ext: ext(&[11]),
            }),
        ),
        (
            "connection",
            MetaRecord::Connection(Connection {
                name: "acme/pg".into(),
                kind: "postgres".into(),
                config: b"{\"host\":\"db\"}".to_vec(),
                ext: ext(&[12]),
            }),
        ),
        (
            "egress",
            MetaRecord::Egress(EgressPolicy {
                name: "acme/default".into(),
                allow: vec!["api.stripe.com".into(), "*.example.com".into()],
                ext: ext(&[13]),
            }),
        ),
        (
            "workflow_policy",
            MetaRecord::WorkflowPolicy(WorkflowPolicy {
                workflow: "acme/order".into(),
                retention_ms: Some(604_800_000),
                schedule_to_start_ms: Some(60_000),
                paused: true,
                on_retries_exhausted: Some(1),
                ext: ext(&[14]),
            }),
        ),
        (
            "queue_policy",
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: "acme/default".into(),
                retention_ms: Some(86_400_000),
                schedule_to_start_ms: Some(30_000),
                placement: Some(2),
                selector: vec!["zone=a".into(), "gpu=true".into()],
                spill: Some(SpillPolicy {
                    cpu_pct: 85,
                    slots_pct: 90,
                    mem_pct: 80,
                    lag_ms: 2000,
                    sustain_ms: 10_000,
                    cooldown_ms: 60_000,
                    hysteresis_pct: 10,
                }),
                concurrency: Some(100),
                weight: Some(2),
                key_concurrency: Some(1),
                throttle: Some(rate(50, 1000, 10)),
                key_throttle: Some(rate(5, 1000, 0)),
                rate_limit: Some(rate(1000, 60_000, 0)),
                key_rate_limit: Some(rate(10, 60_000, 2)),
                on_retries_exhausted: Some(0),
                paused: false,
                ext: ext(&[15]),
            }),
        ),
        (
            "webhook",
            MetaRecord::Webhook(Webhook {
                name: "acme/stripe".into(),
                action: 1,
                workflow: "acme/order".into(),
                version: None,
                signal_name: "paid".into(),
                run_ref: "/data/object/metadata/run_id".into(),
                input_ref: String::new(),
                verifier: 1,
                verifier_config: vec![],
                secret_ref: "acme/stripe".into(),
                idempotency: "header:Stripe-Idempotency-Key".into(),
                tolerance_s: 300,
                ext: ext(&[16]),
            }),
        ),
    ]
}

fn golden_commands() -> Vec<(&'static str, MetaCommand)> {
    let records = golden_records();
    let schedule = records[7].1.clone();
    vec![
        (
            "init_cluster",
            MetaCommand::InitCluster {
                cluster_id: "0193c8e0-golden".into(),
                node_id: 1,
                partitions: 2,
                created_at_ms: 1_700_000_000_000,
                token_key: [0x42; 32],
            },
        ),
        (
            "put_schedule",
            MetaCommand::Put {
                key: schedule.key(),
                expected_version: Some(3),
                record: schedule,
            },
        ),
        (
            "delete_webhook",
            MetaCommand::Delete {
                key: MetaKey::Webhook("acme/stripe".into()),
                expected_version: 1,
            },
        ),
        (
            "set_partition_leader",
            MetaCommand::SetPartitionLeader {
                partition: 1,
                epoch: 7,
                leader: 2,
                isr: vec![2],
            },
        ),
    ]
}

/// InitCluster (P=2) then every golden record, applied at fixed indices.
fn full_state() -> MetaState {
    let mut s = MetaState::default();
    let (_, init) = golden_commands().swap_remove(0);
    assert_eq!(s.apply(1, init), MetaResult::Ok { version: 1 });
    for (i, (name, rec)) in golden_records().into_iter().enumerate() {
        let expected = (rec.kind() == MetaKind::Cluster).then_some(1);
        let r = s.apply(
            i as u64 + 2,
            MetaCommand::Put {
                key: rec.key(),
                expected_version: expected,
                record: rec,
            },
        );
        assert!(matches!(r, MetaResult::Ok { .. }), "{name}: {r:?}");
    }
    s
}

fn render() -> String {
    let mut out = format!("{HEADER}\n");
    for (name, rec) in golden_records() {
        out.push_str(&format!(
            "record {name} {}\n",
            hex(&postcard::to_stdvec(&rec).unwrap())
        ));
    }
    for (name, cmd) in golden_commands() {
        out.push_str(&format!("command {name} {}\n", hex(&encode_command(&cmd))));
    }
    out.push_str(&format!(
        "snapshot empty {}\n",
        hex(&encode_snapshot(&MetaState::default()))
    ));
    out.push_str(&format!(
        "snapshot full {}\n",
        hex(&encode_snapshot(&full_state()))
    ));
    out
}

/// With `OSTRO_BLESS=1`, rewrite the golden file (atomically) and return true.
/// Refuses unless the existing file is a line prefix of the new rendering.
fn bless() -> bool {
    if std::env::var("OSTRO_BLESS").as_deref() != Ok("1") {
        return false;
    }
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");
    let path = format!("{dir}/meta_v2.txt");
    let content = render();
    if let Ok(old) = std::fs::read_to_string(&path) {
        let new: Vec<&str> = content.lines().collect();
        for (i, line) in old.lines().enumerate() {
            assert_eq!(
                new.get(i).copied(),
                Some(line),
                "bless would change existing golden line {}; the meta golden is append-only (D20)",
                i + 1
            );
        }
    }
    let tmp = format!("{dir}/meta_v2.txt.{}.tmp", std::process::id());
    std::fs::write(&tmp, &content).expect("write tmp");
    std::fs::rename(&tmp, &path).expect("rename");
    true
}

fn golden_line(prefix: &str, name: &str) -> Vec<u8> {
    let mut lines = GOLDEN.lines();
    assert_eq!(lines.next(), Some(HEADER), "golden header changed");
    let line = lines
        .find(|l| {
            let mut parts = l.split(' ');
            parts.next() == Some(prefix) && parts.next() == Some(name)
        })
        .unwrap_or_else(|| panic!("no golden line `{prefix} {name}`"));
    unhex(line.rsplit(' ').next().unwrap())
}

#[test]
fn golden_file_is_append_only_and_matches() {
    if bless() {
        return;
    }
    let rendered = render();
    let got: Vec<&str> = rendered.lines().collect();
    let want: Vec<&str> = GOLDEN.lines().collect();
    for (i, line) in want.iter().enumerate() {
        assert_eq!(
            got.get(i),
            Some(line),
            "meta golden line {} drifted: the frozen meta encoding changed; follow D20 instead of re-blessing",
            i + 1
        );
    }
    assert_eq!(
        got.len(),
        want.len(),
        "new golden lines: bless them with OSTRO_BLESS=1 (append-only)"
    );
}

#[test]
fn golden_records_cover_every_kind_and_round_trip() {
    let records = golden_records();
    let kinds: Vec<MetaKind> = records.iter().map(|(_, r)| r.kind()).collect();
    assert_eq!(
        kinds,
        MetaKind::ALL.to_vec(),
        "one record per kind, in order"
    );
    for (name, rec) in &records {
        // Non-empty ext using only the test-only tag 15 (RESERVATIONS §10b).
        let entries = ext_entries(rec.ext()).unwrap();
        assert_eq!(entries.len(), 1, "{name}");
        assert_eq!(entries[0].tag, EXT_TAG_TEST, "{name}");
        let bytes = golden_line("record", name);
        let back: MetaRecord = postcard::from_bytes(&bytes).expect(name);
        assert_eq!(&back, rec, "{name}");
    }
    match &records[9].1 {
        MetaRecord::Tenant(t) => assert!(t.weight.is_some() && t.event_rate.is_some()),
        other => panic!("tenant fixture is {other:?}"),
    }
    match &records[4].1 {
        MetaRecord::Workflow(w) => assert_eq!(w.module, "acme/order-svc"),
        other => panic!("workflow fixture is {other:?}"),
    }
    for (name, cmd) in golden_commands() {
        let bytes = golden_line("command", name);
        assert_eq!(bytes[0], META_FORMAT);
        assert_eq!(decode_command(&bytes).expect(name), cmd);
    }
    let empty = golden_line("snapshot", "empty");
    assert_eq!(decode_snapshot(&empty).unwrap(), MetaState::default());
    let full = golden_line("snapshot", "full");
    let state = decode_snapshot(&full).unwrap();
    assert_eq!(state, full_state());
    for kind in MetaKind::ALL {
        let n = state_count(&state, kind);
        assert!(n > 0, "full snapshot populates {kind:?}");
    }
    // And a state machine restores it byte-identically.
    let (mut sm, _view) = MetaSm::new();
    sm.restore(9, &full).unwrap();
    assert_eq!(sm.snapshot(), full);
}

fn state_count(state: &MetaState, kind: MetaKind) -> usize {
    golden_records()
        .iter()
        .filter(|(_, r)| r.kind() == kind)
        .filter(|(_, r)| state.get(&r.key()).is_some())
        .count()
}

// ---- pre-freeze (Phase 4 dev) format ----------------------------------------
//
// Local copies of the META_FORMAT 1 shapes, as of commit 5396b51. Only the
// variants up to `Schedule` (index 7) are spelled out; `Unit` placeholders keep
// the positional variant indices.

#[derive(Serialize, Deserialize, Default)]
struct V1ClusterInfo {
    cluster_id: String,
    partitions: u16,
    format: u32,
    created_at_ms: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct V1Schedule {
    name: String,
    workflow: String,
    version: Option<u32>,
    cron: String,
    input: Vec<u8>,
    paused: bool,
}

#[derive(Serialize, Deserialize)]
struct V1Versioned<T> {
    version: u64,
    updated_index: u64,
    value: T,
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
enum V1MetaKey {
    Cluster,
    Member(u64),
    Partition(u16),
    Module([u8; 32]),
    Workflow(String, u32),
    Active(String),
    Queue(String),
    Schedule(String),
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
enum V1MetaRecord {
    Cluster(V1ClusterInfo),
    Member(()),
    Partition(()),
    Module(()),
    Workflow(()),
    Active(()),
    Queue(()),
    Schedule(V1Schedule),
}

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
enum V1MetaCommand {
    InitCluster(()),
    Put {
        key: V1MetaKey,
        expected_version: Option<u64>,
        record: V1MetaRecord,
    },
}

/// The 14-map Phase 4 `MetaState`; maps other than `schedules` are empty, so
/// their value type does not affect the encoding.
#[derive(Serialize, Deserialize, Default)]
struct V1MetaState {
    cluster: Option<V1Versioned<V1ClusterInfo>>,
    members: BTreeMap<u64, ()>,
    partitions: BTreeMap<u16, ()>,
    modules: BTreeMap<[u8; 32], ()>,
    workflows: BTreeMap<(String, u32), ()>,
    actives: BTreeMap<String, ()>,
    queues: BTreeMap<String, ()>,
    schedules: BTreeMap<String, V1Versioned<V1Schedule>>,
    tokens: BTreeMap<String, ()>,
    tenants: BTreeMap<String, ()>,
    secrets: BTreeMap<String, ()>,
    grants: BTreeMap<String, ()>,
    connections: BTreeMap<String, ()>,
    egress: BTreeMap<String, ()>,
}

fn v1_schedule() -> V1Schedule {
    V1Schedule {
        name: "nightly".into(),
        workflow: "report".into(),
        version: Some(1),
        cron: "0 3 * * *".into(),
        input: vec![1, 2],
        paused: false,
    }
}

fn with_format(format: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![format];
    out.extend_from_slice(body);
    out
}

fn v1_snapshot_body() -> Vec<u8> {
    let mut st = V1MetaState {
        cluster: Some(V1Versioned {
            version: 1,
            updated_index: 2,
            value: V1ClusterInfo {
                cluster_id: "old".into(),
                partitions: 16,
                format: 1,
                created_at_ms: 5,
            },
        }),
        ..Default::default()
    };
    st.schedules.insert(
        "nightly".into(),
        V1Versioned {
            version: 1,
            updated_index: 3,
            value: v1_schedule(),
        },
    );
    postcard::to_stdvec(&st).unwrap()
}

fn v1_put_schedule_body() -> Vec<u8> {
    postcard::to_stdvec(&V1MetaCommand::Put {
        key: V1MetaKey::Schedule("nightly".into()),
        expected_version: None,
        record: V1MetaRecord::Schedule(v1_schedule()),
    })
    .unwrap()
}

#[test]
fn format_was_bumped_once() {
    assert_eq!(PRE_FREEZE_META_FORMAT, 1);
    assert_eq!(META_FORMAT, 2);
    assert_eq!(encode_snapshot(&MetaState::default())[0], 2);
}

#[test]
fn pre_freeze_snapshot_fails_with_a_clear_error() {
    let old = with_format(PRE_FREEZE_META_FORMAT, &v1_snapshot_body());
    let err = decode_snapshot(&old).unwrap_err();
    assert_eq!(err, DecodeError::PreFreezeFormat { found: 1 });
    let msg = err.to_string();
    assert!(
        msg.contains("pre-Phase-5 dev meta format; delete the dev data dir"),
        "{msg}"
    );
    // Restore (and so node start) fails with the same message.
    let (mut sm, _view) = MetaSm::new();
    let e = sm.restore(1, &old).unwrap_err();
    assert!(e.contains("delete the dev data dir"), "{e}");
    // Even mislabelled as the new format, the old shape is never misread.
    let relabelled = with_format(META_FORMAT, &v1_snapshot_body());
    assert!(matches!(
        decode_snapshot(&relabelled),
        Err(DecodeError::Malformed(_))
    ));
    let empty_v1 = with_format(
        META_FORMAT,
        &postcard::to_stdvec(&V1MetaState::default()).unwrap(),
    );
    assert!(decode_snapshot(&empty_v1).is_err(), "14 maps != 17 maps");
}

#[test]
fn pre_freeze_raft_entry_fails_to_decode() {
    let old = with_format(PRE_FREEZE_META_FORMAT, &v1_put_schedule_body());
    assert_eq!(
        decode_command(&old),
        Err(DecodeError::PreFreezeFormat { found: 1 })
    );
    // Mislabelled as format 2 it still errors instead of yielding a record.
    let relabelled = with_format(META_FORMAT, &v1_put_schedule_body());
    assert!(matches!(
        decode_command(&relabelled),
        Err(DecodeError::Malformed(_))
    ));
    // Applying it is Invalid, changes nothing, and flags the data dir.
    let (mut sm, view) = MetaSm::new();
    assert_eq!(view.pre_freeze_format(), None);
    let r = ostro_meta::decode_result(&sm.apply(1, 1, &old).unwrap()).unwrap();
    assert!(
        matches!(&r, MetaResult::Invalid(m) if m.contains("delete the dev data dir")),
        "{r:?}"
    );
    assert_eq!(view.read(|s| s.clone()), MetaState::default());
    assert_eq!(view.pre_freeze_format(), Some(1));
}
