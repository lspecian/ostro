//! Fuzz target `meta_decode` (14-07, SEC-05): every `META_FORMAT` decoder of `ostro-meta`
//! (`decode_command`, `decode_snapshot`, `decode_result`) and the pure meta apply.
//!
//! Input: `selector ‖ bytes`, `selector % 4`:
//! - 0: `bytes` is one encoded `MetaCommand`; decoded commands are applied to an empty state
//!   and to a state holding an initialised cluster;
//! - 1: `bytes` is an encoded Raft snapshot (`MetaState`);
//! - 2: `bytes` is an encoded `MetaResult`;
//! - 3: `bytes` is a sequence of `len: u16 LE ‖ encoded MetaCommand` applied in order to an
//!   initialised state (at most [`MAX_CMDS`]).
//!
//! Invariants: decoding never panics (newer / pre-freeze / malformed payloads are errors);
//! a decoded value re-encodes to bytes that decode to an equal value and re-encode
//! byte-identically; `MetaState::apply` never panics and the state's snapshot round-trips
//! after every apply.

use ostro_meta::{
    MetaCommand, MetaState, decode_command, decode_result, decode_snapshot, encode_command,
    encode_result, encode_snapshot,
};

/// Commands applied per input in sequence mode.
pub const MAX_CMDS: usize = 32;

/// Fuzz entry point.
pub fn meta_decode(data: &[u8]) {
    let Some((&sel, bytes)) = data.split_first() else {
        return;
    };
    match sel % 4 {
        0 => {
            if let Some(cmd) = command(bytes) {
                let mut empty = MetaState::default();
                apply(&mut empty, 1, cmd.clone());
                let mut st = initialised();
                apply(&mut st, 2, cmd);
            }
        }
        1 => {
            if let Ok(state) = decode_snapshot(bytes) {
                snapshot_round_trip(&state);
                touch(&state);
            }
        }
        2 => {
            if let Ok(r) = decode_result(bytes) {
                let b1 = encode_result(&r);
                let r2 = decode_result(&b1).expect("re-encoded result decodes");
                assert_eq!(r2, r, "result round trip");
                assert_eq!(encode_result(&r2), b1, "result re-encoding is not stable");
            }
        }
        _ => {
            let mut st = initialised();
            let mut rest = bytes;
            let mut index = 2;
            for _ in 0..MAX_CMDS {
                let Some((frame, tail)) = split_frame(rest) else {
                    break;
                };
                rest = tail;
                if let Some(cmd) = command(frame) {
                    apply(&mut st, index, cmd);
                    index += 1;
                }
            }
            touch(&st);
        }
    }
}

fn split_frame(b: &[u8]) -> Option<(&[u8], &[u8])> {
    if b.len() < 2 {
        return None;
    }
    let len = usize::from(u16::from_le_bytes([b[0], b[1]])).min(b.len() - 2);
    Some(b[2..].split_at(len))
}

/// Decode a command and check its round trip.
fn command(bytes: &[u8]) -> Option<MetaCommand> {
    let cmd = decode_command(bytes).ok()?;
    let b1 = encode_command(&cmd);
    let cmd2 = decode_command(&b1).expect("re-encoded command decodes");
    assert_eq!(cmd2, cmd, "command round trip");
    assert_eq!(
        encode_command(&cmd2),
        b1,
        "command re-encoding is not stable"
    );
    Some(cmd)
}

fn initialised() -> MetaState {
    let mut st = MetaState::default();
    let _ = st.apply(
        1,
        MetaCommand::InitCluster {
            cluster_id: "fuzz".into(),
            node_id: 1,
            partitions: 4,
            created_at_ms: 1_700_000_000_000,
            token_key: [7; 32],
        },
    );
    st
}

fn apply(st: &mut MetaState, index: u64, cmd: MetaCommand) {
    let r = st.apply(index, cmd);
    let b = encode_result(&r);
    assert_eq!(decode_result(&b).expect("apply result decodes"), r);
    snapshot_round_trip(st);
}

fn snapshot_round_trip(state: &MetaState) {
    let b1 = encode_snapshot(state);
    let s2 = decode_snapshot(&b1).expect("re-encoded snapshot decodes");
    assert_eq!(&s2, state, "snapshot round trip");
    assert_eq!(
        encode_snapshot(&s2),
        b1,
        "snapshot re-encoding is not stable"
    );
}

/// Exercise the read accessors of a decoded state.
fn touch(st: &MetaState) {
    let _ = st.cluster();
    let _ = st.members().count();
    let _ = st.partitions().count();
    let _ = st.modules().count();
    let _ = st.workflows().count();
    let _ = st.queues().count();
    let _ = st.tokens().count();
    let _ = st.schedules().count();
    let _ = st.tenants().count();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        for sel in 0..4u8 {
            meta_decode(&[sel]);
            meta_decode(&[sel, 2]);
            meta_decode(&[sel, 2, 0]);
            meta_decode(&[sel, 1, 0]);
            meta_decode(&[sel, 9, 0]);
        }
        let mut seq = vec![3];
        let cmd = encode_command(&MetaCommand::Delete {
            key: ostro_meta::MetaKey::Cluster,
            expected_version: 1,
        });
        seq.extend_from_slice(&(cmd.len() as u16).to_le_bytes());
        seq.extend_from_slice(&cmd);
        meta_decode(&seq);
    }
}
