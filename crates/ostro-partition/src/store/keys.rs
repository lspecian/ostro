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

/// `inbox`: run ‖ n.
pub fn inbox_key(run: &RunId, n: u64) -> Vec<u8> {
    event_key(run, n)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_mapping_round_trips_and_reverses() {
        for p in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert_eq!(priority_from_desc(priority_desc(p)), p);
        }
        assert!(priority_desc(10) < priority_desc(-10));
        assert!(priority_desc(i32::MAX) < priority_desc(i32::MIN));
    }
}
