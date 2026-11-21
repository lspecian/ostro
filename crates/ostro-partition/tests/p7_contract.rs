//! Phase 7 partition contracts (plan 07-02): `Placement` / `SemaphorePlacement`,
//! `FlowControl` / `NoFlow` / `FlowPermit`, and `QueueGauges`. The dispatcher does not use
//! them yet (07-05 wires them); these tests pin the contract later waves build on.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ostro_partition::query::ReadyEntryRow;
use ostro_partition::{
    Admit, EntryView, FlowControl, FlowPermit, NoFlow, Placement, QueueGauge, QueueGauges,
    ReservationKind, Reserve, SemaphorePlacement,
};
use ostro_types::PartitionId;
use tokio::sync::Semaphore;

fn entry() -> ReadyEntryRow {
    ReadyEntryRow {
        queue: "default".into(),
        priority: 2,
        concurrency_key: "k".into(),
        workflow: "order".into(),
        tenant: Some("acme".into()),
        first_activation: true,
        ..ReadyEntryRow::default()
    }
}

#[test]
fn entry_view_borrows_the_ready_row_including_f7_first_activation() {
    let row = entry();
    let e = EntryView::from_row(&row);
    assert_eq!(e.queue, "default");
    assert_eq!(e.workflow, "order");
    assert_eq!(e.tenant, Some("acme"));
    assert_eq!(e.concurrency_key, "k");
    assert_eq!(e.priority, 2);
    assert!(e.first_activation);
}

#[tokio::test]
async fn semaphore_placement_grants_blocks_and_releases() {
    let slots = Arc::new(Semaphore::new(2));
    let p = SemaphorePlacement::new(Arc::clone(&slots));
    let mut changed = p.changed();
    let start = *changed.borrow_and_update();
    let row = entry();
    let e = EntryView::from_row(&row);

    let Reserve::Granted(a) = p.reserve(&e) else {
        panic!("first reserve must be granted");
    };
    assert_eq!(a.kind(), ReservationKind::Embedded);
    let Reserve::Granted(b) = p.reserve(&e) else {
        panic!("second reserve must be granted");
    };
    assert_eq!(slots.available_permits(), 0);
    assert!(matches!(p.reserve(&e), Reserve::Blocked), "no slot left");
    assert!(!changed.has_changed().expect("sender alive"));

    drop(a);
    assert_eq!(slots.available_permits(), 1, "the permit went back");
    assert!(
        changed.has_changed().expect("sender alive"),
        "release bumps changed()"
    );
    assert!(*changed.borrow_and_update() > start);
    let Reserve::Granted(c) = p.reserve(&e) else {
        panic!("a released slot is reusable");
    };
    drop((b, c));
    assert_eq!(slots.available_permits(), 2);
}

#[test]
fn reservation_downcasts_to_its_inner_value() {
    use ostro_partition::Reservation;
    let r = Reservation::new(ReservationKind::Worker, 42u64);
    assert_eq!(r.kind(), ReservationKind::Worker);
    assert_eq!(r.downcast_ref::<u64>(), Some(&42));
    assert!(r.downcast_ref::<u32>().is_none());
    let r = r
        .into_inner::<u32>()
        .expect_err("wrong type gives the reservation back");
    assert_eq!(*r.into_inner::<u64>().expect("right type"), 42);
}

#[test]
fn no_flow_admits_everything_with_weight_one() {
    let f = NoFlow::new();
    let row = entry();
    let e = EntryView::from_row(&row);
    for now in [0, 1, u64::MAX] {
        assert!(matches!(f.admit(&e, now), Admit::Yes(_)));
    }
    assert_eq!(f.queue_weight("default"), 1);
    assert_eq!(f.queue_weight("anything"), 1);
    let rx = f.changed();
    assert!(!rx.has_changed().expect("sender alive"));
}

#[test]
fn flow_permit_rolls_back_unless_committed() {
    let charged = Arc::new(AtomicU32::new(0));
    let permit = |c: &Arc<AtomicU32>| {
        c.fetch_add(1, Ordering::SeqCst);
        let c = Arc::clone(c);
        FlowPermit::new(move || {
            c.fetch_sub(1, Ordering::SeqCst);
        })
    };
    // Uncommitted: the tentative charge is undone on drop (W2).
    drop(permit(&charged));
    assert_eq!(charged.load(Ordering::SeqCst), 0);
    // Committed: kept.
    let mut p = permit(&charged);
    assert!(!p.is_committed());
    p.commit();
    assert!(p.is_committed());
    drop(p);
    assert_eq!(charged.load(Ordering::SeqCst), 1);
    // The empty permit is a no-op either way.
    drop(FlowPermit::none());
    let mut n = FlowPermit::none();
    n.commit();
    drop(n);
    assert_eq!(charged.load(Ordering::SeqCst), 1);
}

#[test]
fn queue_gauges_sum_partitions_and_drop_a_removed_one() {
    let g = QueueGauges::new();
    g.set(
        PartitionId(0),
        "default",
        QueueGauge {
            ready: 3,
            ready_unblocked: 2,
            oldest_ready_at_ms: Some(500),
            running_embedded: 1,
            running_on_workers: 0,
        },
    );
    g.set(
        PartitionId(1),
        "default",
        QueueGauge {
            ready: 4,
            ready_unblocked: 4,
            oldest_ready_at_ms: Some(200),
            running_embedded: 0,
            running_on_workers: 5,
        },
    );
    g.set(
        PartitionId(1),
        "batch",
        QueueGauge {
            ready: 1,
            ..QueueGauge::default()
        },
    );
    let snap = g.snapshot();
    assert_eq!(
        snap.keys().map(String::as_str).collect::<Vec<_>>(),
        ["batch", "default"]
    );
    assert_eq!(
        snap["default"],
        QueueGauge {
            ready: 7,
            ready_unblocked: 6,
            oldest_ready_at_ms: Some(200),
            running_embedded: 1,
            running_on_workers: 5,
        }
    );
    assert_eq!(snap["batch"].oldest_ready_at_ms, None);

    // Overwrite, not accumulate.
    g.set(
        PartitionId(0),
        "default",
        QueueGauge {
            ready: 1,
            ..QueueGauge::default()
        },
    );
    assert_eq!(g.snapshot()["default"].ready, 5);

    g.remove_partition(PartitionId(1));
    let snap = g.snapshot();
    assert!(
        !snap.contains_key("batch"),
        "partition 1 was its only source"
    );
    assert_eq!(
        snap["default"],
        QueueGauge {
            ready: 1,
            ..QueueGauge::default()
        }
    );
}
