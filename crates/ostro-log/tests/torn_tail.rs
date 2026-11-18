//! STOR-03: the log end is recovered by scanning, and a torn tail is cut.
//!
//! Torn tails come from power loss or kernel crashes, never from `kill -9` (the page cache
//! survives the process), so they are produced here by offline injection: truncation at
//! every byte of the final records and zero-filled 512-byte sectors (research Pitfall 2).

mod common;

use std::path::Path;

use common::{Written, write_batches, write_log};
use ostro_log::recovery::recover_partition;
use ostro_log::{Corruption, LogError, RecoveryReport};
use ostro_testkit::corrupt::{self, SECTOR};
use ostro_testkit::fs::{file_len, tempdir};
use ostro_types::{Offset, PartitionId};

const P: PartitionId = PartitionId(0);

/// A 512-aligned sector lying entirely inside `w`'s record.
fn sector_inside(w: &Written) -> u64 {
    let s = w.byte_pos.div_ceil(SECTOR);
    assert!(
        (s + 1) * SECTOR <= w.end_pos(),
        "record {}..{} holds no full sector",
        w.byte_pos,
        w.end_pos()
    );
    s
}

fn assert_corrupt(r: Result<RecoveryReport, LogError>, seg: &Path, pos: u64) {
    match r {
        Err(LogError::Corrupt {
            segment, byte_pos, ..
        }) => {
            assert_eq!(segment, seg);
            assert_eq!(byte_pos, pos);
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn truncation_at_every_byte_of_the_final_two_records_is_cut() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2)], &[(1, 4), (2, 3), (2, 5)]]).await;
    assert_eq!(w.len(), 5);
    let floor = w[2].end(); // end of batch 3
    let seg = w[4].segment.clone();
    let original = std::fs::read(&seg).unwrap();

    for pos in (w[3].byte_pos + 1)..w[4].end_pos() {
        std::fs::write(&seg, &original).unwrap();
        corrupt::truncate_at(&seg, pos).unwrap();
        let r = recover_partition(d.path(), P, floor)
            .unwrap_or_else(|e| panic!("truncated at {pos}: {e}"));
        let cut_rec = if pos < w[4].byte_pos { &w[3] } else { &w[4] };
        if pos == w[4].byte_pos {
            // exactly on a record boundary: nothing is torn
            assert!(r.torn.is_none(), "pos {pos}");
            assert_eq!(r.leo, w[4].base);
            assert_eq!(file_len(&seg), pos);
            continue;
        }
        let torn = r
            .torn
            .as_ref()
            .unwrap_or_else(|| panic!("pos {pos}: no cut"));
        assert_eq!(r.leo, cut_rec.base, "pos {pos}");
        assert_eq!(torn.byte_pos, cut_rec.byte_pos, "pos {pos}");
        assert_eq!(torn.segment, seg);
        assert_eq!(torn.discarded_records, 0);
        assert_eq!(torn.reason, Corruption::Incomplete, "pos {pos}");
        assert_eq!(file_len(&seg), cut_rec.byte_pos, "pos {pos}");
        assert_eq!(r.log_start, Offset(0));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_sector_in_final_record_above_floor_is_cut() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)], &[(1, 2), (1, 40)]]).await;
    let fin = &w[2];
    corrupt::zero_sector(&fin.segment, sector_inside(fin)).unwrap();
    let r = recover_partition(d.path(), P, w[1].end()).unwrap();
    let torn = r.torn.expect("torn");
    assert_eq!(torn.byte_pos, fin.byte_pos);
    assert_eq!(torn.discarded_records, 0);
    assert_eq!(r.leo, fin.base);
    assert_eq!(file_len(&fin.segment), fin.byte_pos);
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_sector_in_second_to_last_discards_the_valid_last() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 2), (1, 40), (1, 3)]]).await;
    let victim = &w[1];
    corrupt::zero_sector(&victim.segment, sector_inside(victim)).unwrap();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    let torn = r.torn.expect("torn");
    assert_eq!(torn.byte_pos, victim.byte_pos);
    assert_eq!(torn.discarded_records, 1);
    assert_eq!(r.leo, victim.base);
    assert_eq!(r.records, 1);
    assert_eq!(file_len(&victim.segment), victim.byte_pos);
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_sector_below_floor_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 2), (1, 40)]]).await;
    let fin = &w[1];
    corrupt::zero_sector(&fin.segment, sector_inside(fin)).unwrap();
    // floor = leo: the final record was applied, so it was fsynced; damage is real
    assert_corrupt(
        recover_partition(d.path(), P, fin.end()),
        &fin.segment,
        fin.byte_pos,
    );
    // floor inside the final batch: still protected
    assert_corrupt(
        recover_partition(d.path(), P, fin.base + 1),
        &fin.segment,
        fin.byte_pos,
    );
    // untouched
    assert_eq!(file_len(&fin.segment), fin.end_pos());
}

#[tokio::test(flavor = "multi_thread")]
async fn torn_condition_in_sealed_segment_fails_open() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 2), (1, 40)], &[(1, 3)]]).await;
    let victim = &w[1];
    corrupt::zero_sector(&victim.segment, sector_inside(victim)).unwrap();
    assert_corrupt(
        recover_partition(d.path(), P, Offset(0)),
        &victim.segment,
        victim.byte_pos,
    );
    // a short tail in a sealed segment is corruption too
    let d2 = tempdir();
    let w2 = write_log(d2.path(), P, &[&[(1, 2), (1, 3)], &[(1, 3)]]).await;
    corrupt::truncate_at(&w2[1].segment, w2[1].end_pos() - 1).unwrap();
    assert_corrupt(
        recover_partition(d2.path(), P, Offset(0)),
        &w2[1].segment,
        w2[1].byte_pos,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_is_idempotent_after_a_cut() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)], &[(1, 2), (2, 4)]]).await;
    corrupt::truncate_at(&w[2].segment, w[2].end_pos() - 7).unwrap();
    let first = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert!(first.torn.is_some());
    let second = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert!(second.torn.is_none());
    assert_eq!(
        second,
        RecoveryReport {
            torn: None,
            ..first.clone()
        }
    );
    assert_eq!(second.leo, w[2].base);
}

#[tokio::test(flavor = "multi_thread")]
async fn appends_continue_contiguously_after_a_cut() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2), (1, 4)]]).await;
    corrupt::truncate_at(&w[2].segment, w[2].byte_pos + 20).unwrap();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert_eq!(r.leo, w[2].base);
    let more = write_batches(d.path(), P, r.leo, &[(2, 6)]).await;
    let r2 = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert!(r2.torn.is_none());
    assert_eq!(r2.leo, more[0].end());
    assert_eq!(r2.records, 3);
    assert_eq!(r2.epochs.last().unwrap().1, w[2].base);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_last_segment_is_ignored() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2)], &[(1, 4)]]).await;
    corrupt::truncate_at(&w[2].segment, 0).unwrap();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert!(r.torn.is_none());
    assert_eq!(r.leo, w[1].end());
    assert_eq!(r.segments.len(), 2, "empty segment stays listed");
    assert!(w[2].segment.exists(), "empty segment is not deleted");
}

#[tokio::test(flavor = "multi_thread")]
async fn torn_first_record_leaves_an_empty_log() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)]]).await;
    corrupt::truncate_at(&w[0].segment, 30).unwrap();
    let r = recover_partition(d.path(), P, Offset(0)).unwrap();
    assert!(r.torn.is_some());
    assert_eq!(r.leo, Offset(0));
    assert_eq!(r.records, 0);
    assert_eq!(file_len(&w[0].segment), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn short_or_zero_tail_garbage_is_cut_but_nonzero_garbage_is_not() {
    // fewer bytes than a frame header: incomplete at EOF
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)]]).await;
    corrupt::append_garbage(&w[0].segment, &[0x4f, 0x57, 0x02, 0x00, 0x10]).unwrap();
    let r = recover_partition(d.path(), P, w[0].end()).unwrap();
    assert_eq!(r.torn.expect("torn").reason, Corruption::Incomplete);
    assert_eq!(file_len(&w[0].segment), w[0].end_pos());

    // an all-zero remainder (unwritten extent) shorter than a sector
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)]]).await;
    corrupt::append_garbage(&w[0].segment, &[0u8; 100]).unwrap();
    let r = recover_partition(d.path(), P, w[0].end()).unwrap();
    assert!(r.torn.is_some());
    assert_eq!(file_len(&w[0].segment), w[0].end_pos());

    // non-zero bytes that do not frame a record: corruption
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3)]]).await;
    corrupt::append_garbage(&w[0].segment, &[0xab; 40]).unwrap();
    assert_corrupt(
        recover_partition(d.path(), P, Offset(0)),
        &w[0].segment,
        w[0].end_pos(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn nonzero_bit_flip_in_final_record_above_floor_is_corruption() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 4)]]).await;
    corrupt::flip_bit(&w[1].segment, w[1].byte_pos + 40, 2).unwrap();
    assert_corrupt(
        recover_partition(d.path(), P, Offset(0)),
        &w[1].segment,
        w[1].byte_pos,
    );
}
