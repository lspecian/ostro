//! Big-endian composite key codecs.
//!
//! All integers are big-endian so byte order equals numeric order. Strings are
//! encoded as `u16 BE length ‖ utf8`. Run ids are their 18 raw bytes
//! (`u16 BE partition ‖ uuid`).

use ostro_types::RunId;

const RUN_LEN: usize = 18;

fn put_str(out: &mut Vec<u8>, s: &str) {
    let len = u16::try_from(s.len()).expect("key string longer than 65535 bytes");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_u64(b: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(b.try_into().ok()?))
}

/// Maps an `i32` priority to a `u32` that sorts DESCENDING by priority
/// (higher priority first).
fn priority_desc(p: i32) -> u32 {
    !((p as u32) ^ 0x8000_0000)
}

fn priority_from_desc(v: u32) -> i32 {
    ((!v) ^ 0x8000_0000) as i32
}

/// `runs`: run (18 B).
pub fn run_key(run: &RunId) -> Vec<u8> {
    run.as_bytes().to_vec()
}

/// `running`: run (18 B), same as [`run_key`]. Value: postcard `(epoch, n)`.
pub fn running_key(run: &RunId) -> Vec<u8> {
    run_key(run)
}

/// `events`: run ‖ seq.
pub fn event_key(run: &RunId, seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + 8);
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// `payloads`: run ‖ kind (0 = input, 1 = result).
pub fn payload_key(run: &RunId, kind: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + 1);
    k.extend_from_slice(run.as_bytes());
    k.push(kind);
    k
}

/// Prefix of all `ready` rows for one queue.
pub fn ready_prefix(queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + queue.len());
    put_str(&mut k, queue);
    k
}

/// `ready`: queue ‖ priority (higher first) ‖ ready_at ‖ run.
pub fn ready_key(queue: &str, priority: i32, ready_at_ms: u64, run: &RunId) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + queue.len() + 4 + 8 + RUN_LEN);
    put_str(&mut k, queue);
    k.extend_from_slice(&priority_desc(priority).to_be_bytes());
    k.extend_from_slice(&ready_at_ms.to_be_bytes());
    k.extend_from_slice(run.as_bytes());
    k
}

/// Inverse of [`ready_key`]: `(queue, priority, ready_at_ms, run)`.
pub fn decode_ready_key(k: &[u8]) -> Option<(String, i32, u64, RunId)> {
    let len = usize::from(u16::from_be_bytes(k.get(..2)?.try_into().ok()?));
    let rest = k.get(2..)?;
    if rest.len() != len + 4 + 8 + RUN_LEN {
        return None;
    }
    let queue = std::str::from_utf8(&rest[..len]).ok()?.to_string();
    let rest = &rest[len..];
    let p = priority_from_desc(u32::from_be_bytes(rest[..4].try_into().ok()?));
    let at = read_u64(&rest[4..12])?;
    let run = RunId::from_bytes(&rest[12..]).ok()?;
    Some((queue, p, at, run))
}

/// `timers`: due ‖ run ‖ op; a range scan yields timers in due order.
pub fn timer_key(due_at_ms: u64, run: &RunId, op: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + RUN_LEN + 8);
    k.extend_from_slice(&due_at_ms.to_be_bytes());
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(&op.to_be_bytes());
    k
}

/// Inverse of [`timer_key`]: `(due_at_ms, run, op)`.
pub fn decode_timer_key(k: &[u8]) -> Option<(u64, RunId, u64)> {
    if k.len() != 8 + RUN_LEN + 8 {
        return None;
    }
    let due = read_u64(&k[..8])?;
    let run = RunId::from_bytes(&k[8..8 + RUN_LEN]).ok()?;
    let op = read_u64(&k[8 + RUN_LEN..])?;
    Some((due, run, op))
}

/// Name-length sentinel of legacy `inbox` rows. Signal names are at most
/// `MAX_SIGNAL_NAME_LEN` (256) bytes, so no signal row ever has this length field.
pub const INBOX_LEGACY_TAG: u16 = 0xFFFF;

/// Legacy `inbox` row (value [`InboxMsg`](super::values::InboxMsg)): run ‖ `0xFFFF` ‖ n.
///
/// Two key shapes share the `inbox` keyspace (plan 05-02): these legacy rows and the signal
/// rows of [`signal_inbox_key`]. The sentinel keeps every [`signal_inbox_prefix`] scan free
/// of legacy rows. `RunRecord.inbox_next` supplies `n` for both shapes.
pub fn inbox_key(run: &RunId, n: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + 2 + 8);
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(&INBOX_LEGACY_TAG.to_be_bytes());
    k.extend_from_slice(&n.to_be_bytes());
    k
}

/// Prefix of every `inbox` row of one run (both shapes); also the `signal_seen` and
/// `events` run prefix.
pub fn run_prefix(run: &RunId) -> Vec<u8> {
    run.as_bytes().to_vec()
}

fn put_name(out: &mut Vec<u8>, name: &str) {
    assert!(
        name.len() < usize::from(INBOX_LEGACY_TAG),
        "signal name too long for a key (apply validates MAX_SIGNAL_NAME_LEN first)"
    );
    put_str(out, name);
}

/// Prefix of the signal `inbox` rows of `(run, name)`: run ‖ name_len u16 ‖ name.
pub fn signal_inbox_prefix(run: &RunId, name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + 2 + name.len() + 8);
    k.extend_from_slice(run.as_bytes());
    put_name(&mut k, name);
    k
}

/// Signal `inbox` row (value [`SignalSlot`](super::values::SignalSlot)):
/// run ‖ name_len u16 ‖ name ‖ n BE. A prefix scan over [`signal_inbox_prefix`] yields one
/// name's buffered signals in arrival (FIFO) order.
pub fn signal_inbox_key(run: &RunId, name: &str, n: u64) -> Vec<u8> {
    let mut k = signal_inbox_prefix(run, name);
    k.extend_from_slice(&n.to_be_bytes());
    k
}

/// Inverse of [`signal_inbox_key`]: `(run, name, n)`; `None` for legacy rows.
pub fn decode_signal_inbox_key(k: &[u8]) -> Option<(RunId, String, u64)> {
    let run = RunId::from_bytes(k.get(..RUN_LEN)?).ok()?;
    let rest = &k[RUN_LEN..];
    let len = u16::from_be_bytes(rest.get(..2)?.try_into().ok()?);
    if len == INBOX_LEGACY_TAG || rest.len() != 2 + usize::from(len) + 8 {
        return None;
    }
    let name = std::str::from_utf8(&rest[2..2 + usize::from(len)])
        .ok()?
        .to_string();
    let n = read_u64(&rest[2 + usize::from(len)..])?;
    Some((run, name, n))
}

/// Shape byte of a `waits` wait row ([`wait_key`]).
pub const WAIT_ROW: u8 = 0x01;
/// Shape byte of a `waits` FIFO index row ([`wait_name_key`]).
pub const WAIT_NAME: u8 = 0x02;

/// `waits` wait row (value [`WaitRow`](super::values::WaitRow)): `0x01` ‖ run ‖ op BE.
pub fn wait_key(run: &RunId, op: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + RUN_LEN + 8);
    k.push(WAIT_ROW);
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(&op.to_be_bytes());
    k
}

/// Prefix of the `waits` FIFO index rows of `(run, name)`: `0x02` ‖ run ‖ name_len ‖ name.
pub fn wait_name_prefix(run: &RunId, name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + RUN_LEN + 2 + name.len() + 8);
    k.push(WAIT_NAME);
    k.extend_from_slice(run.as_bytes());
    put_name(&mut k, name);
    k
}

/// `waits` FIFO index row (empty value): `0x02` ‖ run ‖ name_len u16 ‖ name ‖ op BE. A
/// prefix scan over [`wait_name_prefix`] yields one name's waits oldest op first.
pub fn wait_name_key(run: &RunId, name: &str, op: u64) -> Vec<u8> {
    let mut k = wait_name_prefix(run, name);
    k.extend_from_slice(&op.to_be_bytes());
    k
}

/// The two `waits` prefixes of one run: `[0x01 ‖ run, 0x02 ‖ run]`.
pub fn wait_prefixes(run: &RunId) -> [Vec<u8>; 2] {
    let mut a = Vec::with_capacity(1 + RUN_LEN);
    a.push(WAIT_ROW);
    a.extend_from_slice(run.as_bytes());
    let mut b = Vec::with_capacity(1 + RUN_LEN);
    b.push(WAIT_NAME);
    b.extend_from_slice(run.as_bytes());
    [a, b]
}

/// Inverse of [`wait_key`]: `(run, op)`.
pub fn decode_wait_key(k: &[u8]) -> Option<(RunId, u64)> {
    if k.len() != 1 + RUN_LEN + 8 || k[0] != WAIT_ROW {
        return None;
    }
    let run = RunId::from_bytes(&k[1..1 + RUN_LEN]).ok()?;
    Some((run, read_u64(&k[1 + RUN_LEN..])?))
}

/// Inverse of [`wait_name_key`]: `(run, name, op)`.
pub fn decode_wait_name_key(k: &[u8]) -> Option<(RunId, String, u64)> {
    if k.first() != Some(&WAIT_NAME) {
        return None;
    }
    let run = RunId::from_bytes(k.get(1..1 + RUN_LEN)?).ok()?;
    let rest = &k[1 + RUN_LEN..];
    let len = usize::from(u16::from_be_bytes(rest.get(..2)?.try_into().ok()?));
    if rest.len() != 2 + len + 8 {
        return None;
    }
    let name = std::str::from_utf8(&rest[2..2 + len]).ok()?.to_string();
    Some((run, name, read_u64(&rest[2 + len..])?))
}

/// `signal_seen` (D13 signal dedupe; empty value): run ‖ request_id (raw, rest of key).
/// Deleted with the run (prefix [`run_prefix`]).
pub fn signal_seen_key(run: &RunId, request_id: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + request_id.len());
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(request_id);
    k
}

/// `schedule_state` key-shape tag of a schedule's watermark row ([`schedule_state_key`]).
pub const SCHEDULE_STATE: u8 = 0x01;
/// `schedule_state` key-shape tag of the run → schedule reverse index ([`schedule_run_key`]).
pub const SCHEDULE_RUN: u8 = 0x02;

/// `schedule_state` watermark row (Phase 8, P8-D10): `0x01 ‖ name` (raw, rest of key).
/// Value: `ScheduleStateRow`.
pub fn schedule_state_key(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + name.len());
    k.push(SCHEDULE_STATE);
    k.extend_from_slice(name.as_bytes());
    k
}

/// `schedule_state` reverse index (Phase 8; read by 08-07's terminal hook): `0x02 ‖ run`.
/// Value: `ScheduleRunRow`.
pub fn schedule_run_key(run: &RunId) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + RUN_LEN);
    k.push(SCHEDULE_RUN);
    k.extend_from_slice(run.as_bytes());
    k
}

/// Inverse of [`schedule_state_key`]: the schedule name.
pub fn decode_schedule_state_key(k: &[u8]) -> Option<&str> {
    match k.split_first() {
        Some((&SCHEDULE_STATE, name)) => std::str::from_utf8(name).ok(),
        _ => None,
    }
}

/// Inverse of [`schedule_run_key`]: the run id.
pub fn decode_schedule_run_key(k: &[u8]) -> Option<RunId> {
    match k.split_first() {
        Some((&SCHEDULE_RUN, run)) => RunId::from_bytes(run).ok(),
        _ => None,
    }
}

/// `dedupe`: workflow ‖ idempotency key (raw, rest of key).
pub fn dedupe_key(workflow: &str, idem_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + workflow.len() + idem_key.len());
    put_str(&mut k, workflow);
    k.extend_from_slice(idem_key);
    k
}

/// `outbox`: target partition ‖ seq.
pub fn outbox_key(target: u16, seq: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(&target.to_be_bytes());
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

/// Prefix of all `outbox` rows for one target partition.
pub fn outbox_prefix(target: u16) -> Vec<u8> {
    target.to_be_bytes().to_vec()
}

/// Inverse of [`outbox_key`]: `(target, seq)`.
pub fn decode_outbox_key(k: &[u8]) -> Option<(u16, u64)> {
    if k.len() != 10 {
        return None;
    }
    let target = u16::from_be_bytes(k[..2].try_into().ok()?);
    Some((target, read_u64(&k[2..])?))
}

/// `inbound_seen`: source partition.
pub fn inbound_key(src: u16) -> Vec<u8> {
    src.to_be_bytes().to_vec()
}

/// `children`: parent ‖ child.
pub fn children_key(parent: &RunId, child: &RunId) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 * RUN_LEN);
    k.extend_from_slice(parent.as_bytes());
    k.extend_from_slice(child.as_bytes());
    k
}

/// Prefix of all `children` rows for one parent.
pub fn children_prefix(parent: &RunId) -> Vec<u8> {
    parent.as_bytes().to_vec()
}

/// `effects` (keyspace #12, RESERVATIONS §6; rows first written in Phase 6, plan 06-03):
/// run (18 B) ‖ op BE u64. Value: [`EffectRow`](super::values::EffectRow). Derived state (D18,
/// additive): every row follows from the log's `RegisterEffect` / `CancelEffect` intents,
/// `CompleteEffect` and terminal/expiry cleanup.
pub fn effect_key(run: &RunId, op: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(RUN_LEN + 8);
    k.extend_from_slice(run.as_bytes());
    k.extend_from_slice(&op.to_be_bytes());
    k
}

/// Prefix of every `effects` row of one run (its 18 id bytes).
pub fn effect_run_prefix(run: &RunId) -> Vec<u8> {
    run.as_bytes().to_vec()
}

/// Inverse of [`effect_key`]: `(run, op)`.
pub fn decode_effect_key(k: &[u8]) -> Option<(RunId, u64)> {
    if k.len() != RUN_LEN + 8 {
        return None;
    }
    let run = RunId::from_bytes(&k[..RUN_LEN]).ok()?;
    let op = read_u64(&k[RUN_LEN..])?;
    Some((run, op))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effect_key_roundtrip_and_order() {
        let mut b = [0u8; 18];
        b[1] = 3;
        let r = RunId::from_bytes(&b).unwrap();
        let k = effect_key(&r, 7);
        assert_eq!(k.len(), 26);
        assert!(k.starts_with(&effect_run_prefix(&r)));
        assert_eq!(decode_effect_key(&k), Some((r, 7)));
        assert!(effect_key(&r, 7) < effect_key(&r, 256));
        assert_eq!(decode_effect_key(&k[..25]), None);
    }

    #[test]
    fn priority_mapping_round_trips_and_reverses() {
        for p in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert_eq!(priority_from_desc(priority_desc(p)), p);
        }
        assert!(priority_desc(10) < priority_desc(-10));
        assert!(priority_desc(i32::MAX) < priority_desc(i32::MIN));
    }
}
