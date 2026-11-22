//! 12-05 Task 1: append-only golden encodings of every `MetaCommand` variant 0..=6.
//!
//! `tests/golden/meta_commands.txt` holds one `[META_FORMAT] ‖ postcard(MetaCommand)` hex
//! vector per variant (`command <ordinal> <name> <hex>`). Every line must decode and
//! re-encode byte-identically, and the fixed commands below must encode to their line.
//! The file is **append-only**: a new variant appends a line (bless with
//! `OSTRO_BLESS=1 cargo test -p ostro-meta --test meta_commands_golden`, which refuses to
//! rewrite an existing line). A drifted line means a frozen command encoding changed.

use ostro_meta::ext::{ELECTION_REASON_FAILOVER, ELECTION_REASON_RESIGN, ElectionInfo};
use ostro_meta::{
    META_FORMAT, Member, MetaCommand, MetaKey, MetaRecord, decode_command, encode_command,
};

const GOLDEN: &str = include_str!("golden/meta_commands.txt");
const HEADER: &str = "# ostro MetaCommand golden vectors (META_FORMAT 2), one per variant — APPEND-ONLY: existing lines never change";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// One fixed command per variant, in ordinal order. Every field is explicit.
fn golden_commands() -> Vec<(u8, &'static str, MetaCommand)> {
    vec![
        (
            0,
            "InitCluster",
            MetaCommand::InitCluster {
                cluster_id: "0193c8e0-golden".into(),
                node_id: 1,
                partitions: 4,
                created_at_ms: 1_700_000_000_000,
                token_key: [0x42; 32],
            },
        ),
        (
            1,
            "Put",
            MetaCommand::Put {
                key: MetaKey::Member(2),
                expected_version: Some(3),
                record: MetaRecord::Member(Member {
                    node_id: 2,
                    addr: "10.0.0.2:7000".into(),
                    joined_at_ms: 1_700_000_000_001,
                    cluster_addr: Some("10.0.0.2:7001".into()),
                    repl_addr: None,
                    ext: Vec::new(),
                }),
            },
        ),
        (
            2,
            "Delete",
            MetaCommand::Delete {
                key: MetaKey::Queue("acme/default".into()),
                expected_version: 5,
            },
        ),
        (
            3,
            "SetPartitionLeader",
            MetaCommand::SetPartitionLeader {
                partition: 1,
                epoch: 7,
                leader: 2,
                isr: vec![2, 3],
            },
        ),
        (
            4,
            "ElectLeader",
            MetaCommand::ElectLeader {
                partition: 3,
                epoch: 8,
                leader: 2,
                isr: vec![2, 3],
                expected_isr_version: 9,
                info: ElectionInfo {
                    reason: ELECTION_REASON_FAILOVER,
                    elected_at_ms: 1_700_000_000_123,
                    prev_leader: 1,
                },
            },
        ),
        (
            5,
            "AlterIsr",
            MetaCommand::AlterIsr {
                partition: 3,
                epoch: 8,
                expected_isr_version: 10,
                isr: vec![1, 2, 3],
            },
        ),
        (
            6,
            "ResignLeader",
            MetaCommand::ResignLeader {
                partition: 3,
                epoch: 8,
                node: 2,
                info: ElectionInfo {
                    reason: ELECTION_REASON_RESIGN,
                    elected_at_ms: 1_700_000_000_456,
                    prev_leader: 2,
                },
            },
        ),
    ]
}

fn line_of(ordinal: u8, name: &str, cmd: &MetaCommand) -> String {
    format!("command {ordinal} {name} {}", hex(&encode_command(cmd)))
}

/// `(ordinal, name, bytes)` for every vector line of the file.
fn file_vectors() -> Vec<(u8, String, Vec<u8>)> {
    GOLDEN
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| {
            let parts: Vec<&str> = l.split(' ').collect();
            assert_eq!(parts.len(), 4, "malformed golden line {l:?}");
            assert_eq!(parts[0], "command", "{l:?}");
            (
                parts[1].parse().expect("ordinal"),
                parts[2].to_owned(),
                unhex(parts[3]),
            )
        })
        .collect()
}

#[test]
fn golden_header_is_append_only() {
    assert_eq!(GOLDEN.lines().next(), Some(HEADER));
}

#[test]
fn every_line_decodes_and_reencodes_byte_identically() {
    let vectors = file_vectors();
    assert!(!vectors.is_empty());
    for (ordinal, name, bytes) in vectors {
        assert_eq!(bytes[0], META_FORMAT, "{name}");
        assert_eq!(bytes[1], ordinal, "{name}: postcard variant index");
        let cmd = decode_command(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(encode_command(&cmd), bytes, "{name}: re-encoding drifted");
    }
}

#[test]
fn every_variant_has_its_pinned_line() {
    let lines: Vec<&str> = GOLDEN.lines().collect();
    let bless = std::env::var_os("OSTRO_BLESS").is_some();
    let mut missing = Vec::new();
    for (ordinal, name, cmd) in golden_commands() {
        assert_eq!(encode_command(&cmd)[1], ordinal, "{name} ordinal");
        let want = line_of(ordinal, name, &cmd);
        let prefix = format!("command {ordinal} {name} ");
        match lines.iter().find(|l| l.starts_with(&prefix)) {
            Some(have) => assert_eq!(
                *have, want,
                "{name}: the golden encoding changed; a frozen command encoding must never change"
            ),
            None => missing.push(want),
        }
    }
    if missing.is_empty() {
        return;
    }
    assert!(
        bless,
        "missing golden lines (bless with OSTRO_BLESS=1):\n{}",
        missing.join("\n")
    );
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/meta_commands.txt"
    );
    let mut text = std::fs::read_to_string(path).unwrap_or_else(|_| format!("{HEADER}\n"));
    for l in missing {
        text.push_str(&l);
        text.push('\n');
    }
    std::fs::write(path, text).expect("bless golden");
}

#[test]
fn phase12_variants_are_4_5_6() {
    let vectors = file_vectors();
    for (ordinal, name) in [(4, "ElectLeader"), (5, "AlterIsr"), (6, "ResignLeader")] {
        assert!(
            vectors.iter().any(|(o, n, _)| *o == ordinal && n == name),
            "{name} ({ordinal}) missing"
        );
    }
    let ordinals: Vec<u8> = vectors.iter().map(|v| v.0).collect();
    assert_eq!(ordinals, (0..=6).collect::<Vec<u8>>());
}
