//! Oracle `status` (12-03): what `GET /v1/admin/replication` reports is honest.
//!
//! For partition `p`, the reported ISR must equal the ISR published in meta, and every
//! member must really hold the high watermark: its **measured** durable LEO (offline
//! verified WAL scan, [`super::disk::scan_partition`]) is at least HW ("ISR is fiction" is
//! exactly a status that lists a replica with nothing on disk).
//!
//! Expected JSON shape (12-14 implements the route; adjust [`reported_isr`] if it lands
//! differently): `{"partitions": [{"partition": <u16>, "isr": [<node id>, ...], ...}, ...]}`.

use std::collections::BTreeMap;

use ostro_types::Offset;
use serde_json::Value;

use super::Report;

/// The ISR the status JSON reports for partition `p` (`None` when absent or malformed).
pub fn reported_isr(admin: &Value, p: u16) -> Option<Vec<u64>> {
    admin["partitions"]
        .as_array()?
        .iter()
        .find(|e| e["partition"].as_u64() == Some(u64::from(p)))?["isr"]
        .as_array()?
        .iter()
        .map(Value::as_u64)
        .collect()
}

/// Check partition `p`'s reported ISR against the published `meta_isr` and the measured
/// durable LEOs (`node -> LEO`; a node without an entry counts as LEO 0).
pub fn honest(
    admin: &Value,
    p: u16,
    meta_isr: &[u64],
    measured_durable_leos: &BTreeMap<u64, Offset>,
    hw: Offset,
) -> Report {
    let mut report = Report::new("status");
    let Some(mut isr) = reported_isr(admin, p) else {
        report.violation(format!("partition {p}: the status reports no ISR"));
        return report;
    };
    isr.sort_unstable();
    let mut published = meta_isr.to_vec();
    published.sort_unstable();
    if isr != published {
        report.violation(format!(
            "partition {p}: reported ISR {isr:?} != published ISR {published:?}"
        ));
    }
    for r in &isr {
        let leo = measured_durable_leos
            .get(r)
            .copied()
            .unwrap_or(Offset::ZERO);
        if leo < hw {
            report.violation(format!(
                "partition {p}: node {r} is reported in the ISR but its measured durable LEO {} < HW {}",
                leo.0, hw.0
            ));
        }
    }
    report
}
