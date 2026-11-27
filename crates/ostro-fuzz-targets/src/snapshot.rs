//! Fuzz target `snapshot_frames` (14-07, SEC-05): the OSNP v1 snapshot reader
//! (`ostro_partition::store::snapshot`).
//!
//! Input: `selector ‖ bytes`.
//! - `selector & 1 == 0` (raw): `bytes` is a whole snapshot stream, read frame by frame with
//!   [`OsnpReader`] and fully checked with [`verify_snapshot`].
//! - `selector & 1 == 1` (framed): a valid header (lz4 flag = `selector & 2`) followed by
//!   frames built from `bytes` as `kind: u8 ‖ len: u16 LE ‖ payload`, each encoded with its
//!   correct CRC ([`RawFrame::encode`]), so the frame decoder, lz4 and the structural checks
//!   behind the CRCs are reached.
//!
//! Invariants: never panics, never allocates beyond the format's caps (frame length and lz4
//! size are checked before allocation; libFuzzer's `-rss_limit_mb` enforces it); the frame
//! reader and `verify_snapshot` agree that a stream with a frame error does not verify.

use std::io::Cursor;

use ostro_partition::store::snapshot::{
    OsnpReader, RawFrame, STATE_FORMAT, SnapshotHeader, verify_snapshot,
};

/// Frames built per input in framed mode.
pub const MAX_FRAMES: usize = 64;

/// Fuzz entry point.
pub fn snapshot_frames(data: &[u8]) {
    let Some((&sel, bytes)) = data.split_first() else {
        return;
    };
    if sel & 1 == 0 {
        read(bytes);
    } else {
        read(&framed(sel & 2 != 0, bytes));
    }
}

/// The framed-mode stream: header + CRC-correct frames from `bytes`.
pub fn framed(lz4: bool, mut bytes: &[u8]) -> Vec<u8> {
    let header = SnapshotHeader {
        flags: u16::from(lz4),
        partition: 0,
        partition_count: 4,
        state_format: STATE_FORMAT,
        applied_offset: 0,
        latest_epoch: 0,
        created_at_ms: 0,
    };
    let mut out = header.encode().to_vec();
    for _ in 0..MAX_FRAMES {
        if bytes.len() < 3 {
            break;
        }
        let kind = bytes[0];
        let len = usize::from(u16::from_le_bytes([bytes[1], bytes[2]])).min(bytes.len() - 3);
        let payload = bytes[3..3 + len].to_vec();
        bytes = &bytes[3 + len..];
        out.extend_from_slice(&RawFrame { kind, payload }.encode());
    }
    out
}

fn read(stream: &[u8]) {
    // Frame by frame, as the transport forwards them.
    let mut frame_error = false;
    match OsnpReader::new(Cursor::new(stream)) {
        Ok(mut r) => {
            let _ = r.header().lz4();
            loop {
                match r.next_frame() {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => {
                        frame_error = true;
                        break;
                    }
                }
            }
            assert!(r.bytes() <= stream.len() as u64, "read past the end");
        }
        Err(_) => frame_error = true,
    }
    let verified = verify_snapshot(Cursor::new(stream));
    if frame_error {
        assert!(
            verified.is_err(),
            "verify accepted a stream with a frame error"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        snapshot_frames(&[]);
        snapshot_frames(&[0]);
        snapshot_frames(&[0; 80]);
        snapshot_frames(&[1]);
        snapshot_frames(&[1, 1, 3, 0, 1, 0, b'x']);
        snapshot_frames(&[3, 2, 6, 0, 0xff, 0xff, 0xff, 0x7f, 0, 0]); // huge lz4 claim
        snapshot_frames(&[1, 5, 0, 0]); // short trailer
    }
}
