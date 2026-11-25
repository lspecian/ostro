//! 13-09 Task 1: the dynamic address book (D-P13-8/9). Member records in meta feed the book
//! live; seeds are replaced by Member addresses; configured (`[[cluster.peers]]`) entries
//! stay pinned (drift logged once, never applied); the TCP transport dials the new address
//! after a Member's `cluster_addr` changes.

mod support_meta;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::address_book::{AddressBook, PeerEntry, Seed};
use ostro_cluster::transport::ClusterTransport;
use ostro_cluster::transport::tcp::TcpTransport;
use ostro_meta::{Member, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use tokio_util::sync::CancellationToken;

async fn put_member(meta: &MetaHandle, id: u64, addr: &str) {
    let cur = meta.view().read(|s| s.member(id).map(|m| m.version));
    let res = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Member(id),
            expected_version: cur,
            record: MetaRecord::Member(Member {
                node_id: id,
                addr: format!("api-{id}"),
                joined_at_ms: 1,
                cluster_addr: Some(addr.to_owned()),
                repl_addr: None,
                ext: Vec::new(),
            }),
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn members_feed_the_book_seeds_yield_and_pinned_peers_stay() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    meta.init_cluster("c-13-09", 1, 2, 1, [1; 32])
        .await
        .unwrap();
    // Node 2 configured (pinned), node 3 a seed, node 4 unknown.
    let book = Arc::new(AddressBook::from_peers([PeerEntry {
        id: 2,
        addr: "proxy-2:1".into(),
        repl_addr: None,
    }]));
    book.add_seeds([Seed {
        id: 3,
        addr: "seed-3:1".into(),
        repl_addr: None,
    }]);
    assert!(book.is_pinned(2) && !book.is_pinned(3));
    let stop = CancellationToken::new();
    let task = book.follow(meta.view().clone(), Some(1), stop.clone());
    put_member(&meta, 1, "self:1").await;
    put_member(&meta, 2, "real-2:1").await;
    put_member(&meta, 3, "real-3:1").await;
    put_member(&meta, 4, "real-4:1").await;
    until("members applied to the book", || {
        book.cluster_addr(3).as_deref() == Some("real-3:1")
            && book.cluster_addr(4).as_deref() == Some("real-4:1")
    })
    .await;
    assert_eq!(book.cluster_addr(2).as_deref(), Some("proxy-2:1"), "pinned");
    assert_eq!(
        book.cluster_addr(1),
        None,
        "own record never feeds its book"
    );
    // A live change.
    put_member(&meta, 4, "moved-4:1").await;
    until("live update", || {
        book.cluster_addr(4).as_deref() == Some("moved-4:1")
    })
    .await;
    // from_meta takes the existing records at once.
    let fresh = AddressBook::from_meta(
        meta.view().clone(),
        vec![Seed {
            id: 4,
            addr: "stale:1".into(),
            repl_addr: None,
        }],
    );
    assert_eq!(fresh.cluster_addr(4).as_deref(), Some("moved-4:1"));
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_redials_a_changed_member_address() {
    let ca = support_meta::TestCa::new("c-13-09");
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    meta.init_cluster("c-13-09", 1, 2, 1, [1; 32])
        .await
        .unwrap();
    let a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let book = Arc::new(AddressBook::new());
    let stop = CancellationToken::new();
    let task = book.follow(meta.view().clone(), Some(1), stop.clone());
    let transport = TcpTransport::new("c-13-09", 1, ca.node(1), Arc::clone(&book)).unwrap();
    // Dials land on the listener the Member record names, before and after a move.
    for l in [&a, &b] {
        let addr = l.local_addr().unwrap().to_string();
        put_member(&meta, 2, &addr).await;
        until("book follows", || {
            book.cluster_addr(2).as_deref() == Some(addr.as_str())
        })
        .await;
        let ch = transport.channel(2).expect("channel");
        let call = tokio::spawn(async move {
            use tower::ServiceExt as _;
            let req = tonic::codegen::http::Request::builder()
                .uri("https://node/ostro.cluster.v1.MetaPeer/Probe")
                .body(tonic::body::Body::empty())
                .unwrap();
            let _ = ch.oneshot(req).await;
        });
        let accepted = tokio::time::timeout(Duration::from_secs(5), l.accept()).await;
        assert!(accepted.is_ok(), "no dial reached {addr}");
        call.abort();
    }
    stop.cancel();
    task.await.unwrap();
}
