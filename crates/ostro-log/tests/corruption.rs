//! STOR-02: a corrupt record outside a torn tail fails the open, loudly, with
//! its location and reason. Nothing is ever skipped.
//!
//! Segments are written through the real `GroupCommitWal` (acks = 1), then
//! corrupted offline with `ostro_testkit::corrupt::*`.

mod common;

use common::{CANONICAL_OFF, TOPIC, make_record, overwrite, splice_out, write_batches, write_log};
use ostro_log::ostro_wal::WalRecord;
use ostro_log::recovery::recover_partition;
use ostro_log::verify::verify_record;
use ostro_log::{Corruption, LogError};
use ostro_testkit::corrupt;
use ostro_testkit::fs::tempdir;
use ostro_types::envelope::{self, EnvelopeError};
use ostro_types::{Epoch, Offset, PartitionId};

const P: PartitionId = PartitionId(0);

/// Three segments, epochs 1, 1, 2.
const LOG: &[&[(u32, u64)]] = &[&[(1, 3), (1, 2)], &[(1, 4), (1, 1)], &[(2, 5), (2, 2)]];

fn expect_corrupt(
    r: Result<ostro_log::RecoveryReport, LogError>,
) -> (std::path::PathBuf, u64, i64, i64, Corruption) {
    match r {
        Err(LogError::Corrupt {
            partition,
            segment,
            byte_pos,
            first,
            last,
            reason,
        }) => {
            assert_eq!(partition, P.0);
            (segment, byte_pos, first, last, reason)
        }
        Err(e) => panic!("expected Corrupt, got {e:?}"),
        Ok(r) => panic!(
            "expected Corrupt, recovery succeeded: leo {:?} torn {:?}",
            r.leo,
            r.torn.map(|t| t.reason)
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_multi_segment_log_recovers() {
    let d = tempdir();
    let w = write_log(d.path(), P, LOG).await;
    let total: u64 = w.iter().map(|b| b.count).sum();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert_eq!(r.leo, Offset(total));
    assert_eq!(r.log_start, Offset(0));
    // epoch 2 starts at the first batch of the third segment
    let k = w[4].base;
    assert_eq!(r.epochs, vec![(Epoch(1), Offset(0)), (Epoch(2), k)]);
    assert!(r.torn.is_none());
    assert_eq!(r.records, w.len() as u64);
    assert_eq!(r.segments.len(), 3);
    assert_eq!(r.bytes, w.iter().map(|b| b.len).sum::<u64>());
    // also with the floor at the end of the log
    let r2 = recover_partition(d.path(), P, Offset(total)).unwrap();
    assert_eq!(r2.leo, r.leo);
}

#[test]
fn missing_partition_dir_is_an_empty_log() {
    let d = tempdir();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert_eq!(r.leo, Offset(0));
    assert_eq!(r.log_start, Offset(0));
    assert!(r.epochs.is_empty());
    assert!(r.segments.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn bit_flip_in_sealed_segment_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, LOG).await;
    let victim = &w[2]; // first record of the second (sealed) segment
    corrupt::flip_bit(&victim.segment, victim.byte_pos + CANONICAL_OFF + 40, 3).unwrap();
    let (seg, pos, first, last, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, victim.segment);
    assert_eq!(pos, victim.byte_pos);
    assert_eq!(first, victim.base.as_i64());
    assert_eq!(last, (victim.end().0 - 1) as i64);
    assert!(
        matches!(
            reason,
            Corruption::FrameCrc { .. } | Corruption::Envelope(_)
        ),
        "{reason:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bit_flip_mid_last_segment_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)], &[(1, 2), (1, 4), (1, 3)]]).await;
    let victim = &w[2]; // middle record of the last segment, not the final one
    corrupt::flip_bit(&victim.segment, victim.byte_pos + CANONICAL_OFF + 10, 0).unwrap();
    let (seg, pos, _, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, victim.segment);
    assert_eq!(pos, victim.byte_pos);
    assert!(matches!(reason, Corruption::FrameCrc { .. }), "{reason:?}");
    // the file was not touched
    assert_eq!(
        std::fs::metadata(&victim.segment).unwrap().len(),
        w.last().unwrap().end_pos()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bit_flip_in_final_record_below_floor_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2), (1, 4)]]).await;
    let fin = w.last().unwrap();
    corrupt::flip_bit(&fin.segment, fin.byte_pos + CANONICAL_OFF + 5, 6).unwrap();
    let leo = fin.end();
    let (_, pos, _, _, _) = expect_corrupt(recover_partition(d.path(), P, leo));
    assert_eq!(pos, fin.byte_pos);
}

#[tokio::test(flavor = "multi_thread")]
async fn envelope_crc_mismatch_under_valid_frame_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)], &[(1, 2), (1, 4)]]).await;
    let victim = &w[1];
    let data = std::fs::read(&victim.segment).unwrap();
    let raw = &data[victim.byte_pos as usize..victim.end_pos() as usize];
    let rec = WalRecord::from_bytes(raw).unwrap();
    let WalRecord::V2 {
        topic,
        partition,
        mut canonical_data,
        base_offset,
        last_offset,
        record_count,
        ..
    } = rec
    else {
        panic!("v2");
    };
    // Damage a command byte (past the 32-byte header); re-encode the frame so
    // its CRC matches the damaged bytes.
    canonical_data[envelope::HEADER_LEN + 6] ^= 0x20;
    let forged = WalRecord::new_v2(
        topic,
        partition,
        canonical_data,
        base_offset,
        last_offset,
        record_count,
    );
    let bytes = forged.to_bytes().unwrap();
    assert_eq!(bytes.len() as u64, victim.len);
    overwrite(&victim.segment, victim.byte_pos, &bytes);

    let (seg, pos, _, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, victim.segment);
    assert_eq!(pos, victim.byte_pos);
    assert!(
        matches!(
            reason,
            Corruption::Envelope(EnvelopeError::CrcMismatch { .. })
        ),
        "{reason:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn record_from_another_partition_fails_open() {
    let d = tempdir();
    // partition 1's log, then move its segment into partition 0's directory
    let other = PartitionId(1);
    let w1 = write_log(d.path(), other, &[&[(1, 3), (1, 2)]]).await;
    let w0 = write_log(d.path(), P, &[&[(1, 3)]]).await;
    let dst = w0[0]
        .segment
        .parent()
        .unwrap()
        .join(format!("wal_{}_{}.log", P.0, 7));
    std::fs::copy(&w1[0].segment, &dst).unwrap();
    let (seg, _, _, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, dst);
    assert!(
        matches!(
            reason,
            Corruption::Misplaced { .. } | Corruption::Envelope(_)
        ),
        "{reason:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn offset_gap_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2), (1, 4)], &[(1, 1)]]).await;
    let gone = &w[1];
    splice_out(&gone.segment, gone.byte_pos, gone.len);
    let (seg, pos, first, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, gone.segment);
    assert_eq!(pos, gone.byte_pos);
    assert_eq!(first, w[2].base.as_i64());
    assert_eq!(
        reason,
        Corruption::Gap {
            expected: gone.base.0,
            found: w[2].base.0
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn epoch_regression_fails_open() {
    let d = tempdir();
    let w1 = write_batches(d.path(), P, Offset(0), &[(2, 3)]).await;
    let w2 = write_batches(d.path(), P, w1[0].end(), &[(1, 2)]).await;
    let (seg, pos, _, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, w2[0].segment);
    assert_eq!(pos, 0);
    assert_eq!(reason, Corruption::EpochRegression { prev: 2, found: 1 });
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_in_sealed_segment_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)], &[(1, 2)]]).await;
    // an incomplete record at the end of a SEALED segment is corruption
    corrupt::append_garbage(&w[0].segment, &[0x4f, 0x57, 0x02]).unwrap();
    let (seg, pos, _, _, reason) = expect_corrupt(recover_partition(d.path(), P, Offset(0)));
    assert_eq!(seg, w[0].segment);
    assert_eq!(pos, w[0].end_pos());
    assert_eq!(reason, Corruption::Incomplete);
}

#[test]
fn error_message_names_location() {
    let e = LogError::Corrupt {
        partition: 3,
        segment: "wal_3_1.log".into(),
        byte_pos: 42,
        first: 10,
        last: 12,
        reason: Corruption::Gap {
            expected: 9,
            found: 10,
        },
    };
    let s = e.to_string();
    assert!(s.contains("partition 3"), "{s}");
    assert!(s.contains("wal_3_1.log"), "{s}");
    assert!(s.contains("byte 42"), "{s}");
    assert!(s.contains("10..=12"), "{s}");
}

// ---- verify_record unit cases ------------------------------------------------

#[test]
fn verify_record_accepts_good_record() {
    let rec = make_record(P, 4, Offset(10), 3);
    let b = verify_record(&rec, Offset(10), P).unwrap();
    assert_eq!(b.epoch, Epoch(4));
    assert_eq!(b.base, Offset(10));
    assert_eq!(b.last(), Offset(12));
    assert_eq!(b.end(), Offset(13));
    let offs: Vec<Offset> = b.commands_with_offsets().map(|(o, _)| o).collect();
    assert_eq!(offs, vec![Offset(10), Offset(11), Offset(12)]);
    assert_eq!(b.commands[1], common::cmd(11));
}

#[test]
fn verify_record_frame_crc() {
    let rec = make_record(P, 1, Offset(0), 2);
    let mut bytes = rec.to_bytes().unwrap();
    bytes[CANONICAL_OFF as usize + 3] ^= 1;
    // from_bytes does not check V2 CRCs (W1): it parses fine
    let bad = WalRecord::from_bytes(&bytes).unwrap();
    assert!(matches!(
        verify_record(&bad, Offset(0), P),
        Err(Corruption::FrameCrc { .. })
    ));
}

#[test]
fn verify_record_not_v2() {
    let rec = WalRecord::new(0, None, b"v1".to_vec(), 0);
    assert_eq!(
        verify_record(&rec, Offset(0), P).unwrap_err(),
        Corruption::NotV2
    );
}

#[test]
fn verify_record_trailer_mismatch() {
    let cmds: Vec<bytes::Bytes> = (0..3).map(common::cmd).collect();
    let env = envelope::encode(P, Epoch(1), Offset(0), &cmds);
    // trailer says 2 commands, envelope carries 3
    let rec = WalRecord::new_v2(TOPIC.into(), P.as_i32(), env, 0, 1, 2);
    assert_eq!(
        verify_record(&rec, Offset(0), P).unwrap_err(),
        Corruption::TrailerMismatch
    );
}

#[test]
fn verify_record_wrong_partition_and_gap() {
    let rec = make_record(PartitionId(2), 1, Offset(0), 1);
    assert!(matches!(
        verify_record(&rec, Offset(0), P).unwrap_err(),
        Corruption::Misplaced {
            partition: 2,
            base: 0
        }
    ));
    let rec = make_record(P, 1, Offset(5), 1);
    assert_eq!(
        verify_record(&rec, Offset(4), P).unwrap_err(),
        Corruption::Gap {
            expected: 4,
            found: 5
        }
    );
}
