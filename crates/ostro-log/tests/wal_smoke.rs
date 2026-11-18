//! Smoke test: the vendored WAL is reachable through ostro-log and round-trips
//! a record through group commit.

use ostro_log::ostro_wal::{GroupCommitConfig, GroupCommitWal, WalRecord};

#[tokio::test(flavor = "multi_thread")]
async fn wal_append_then_read_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let wal = GroupCommitWal::new(tmp.path().to_path_buf(), GroupCommitConfig::default());

    let payload = b"ostro-log smoke payload".to_vec();
    let record = WalRecord::new_v2("p".to_string(), 0, payload.clone(), 0, 0, 1);

    // acks=1: append returns only once the batch is fsynced.
    wal.append("p".to_string(), 0, record, 1).await.unwrap();

    let got = wal
        .read_tail("p", 0, 0, 10, 1 << 20)
        .await
        .expect("tail cache answers for a just-written partition");
    assert_eq!(got.len(), 1);

    let rec = &got[0];
    assert_eq!(rec.get_base_offset(), 0);
    assert_eq!(rec.get_last_offset(), 0);
    assert_eq!(rec.get_record_count(), 1);
    assert_eq!(rec.get_topic_partition(), Some(("p", 0)));
    match rec {
        WalRecord::V2 { canonical_data, .. } => assert_eq!(canonical_data, &payload),
        WalRecord::V1 { .. } => panic!("expected a V2 record"),
    }
}
