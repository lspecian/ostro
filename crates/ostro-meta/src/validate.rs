//! Record validation at meta apply.
//!
//! Only shape rules live here: enum ranges, length caps and "Some must be
//! positive". Semantic checks (IANA zone names, cron syntax, JSON pointers,
//! verifier configs) belong to the phase that uses the field. Validation
//! runs at apply, so every node reaches the same verdict, and a rejected Put
//! leaves the state unchanged.
//!
//! After the 05-20 freeze these rules may be relaxed but never tightened: a
//! stricter rule would reject replay of a command that was accepted before.

use crate::command::MetaRecord;
use crate::records::{
    CATCHUP_ALL, ON_EXHAUSTED_PAUSE, OVERLAP_TERMINATE_OTHER, PLACEMENT_AUTO, QueuePolicy,
    RateSpec, SCHEDULE_TIMEZONE_MAX, SELECTOR_ENTRY_MAX, SELECTOR_MAX_ENTRIES, Schedule,
    SpillPolicy, Webhook, WorkflowPolicy,
};

type Check = Result<(), String>;

fn max_u8(field: &str, v: u8, max: u8) -> Check {
    if v > max {
        return Err(format!("{field} = {v} is out of range (max {max})"));
    }
    Ok(())
}

fn opt_max_u8(field: &str, v: Option<u8>, max: u8) -> Check {
    v.map_or(Ok(()), |v| max_u8(field, v, max))
}

fn rate(field: &str, r: &Option<RateSpec>) -> Check {
    match r {
        Some(r) if r.period_ms == 0 => Err(format!("{field}.period_ms must be > 0")),
        _ => Ok(()),
    }
}

fn schedule(s: &Schedule) -> Check {
    max_u8("overlap", s.overlap, OVERLAP_TERMINATE_OTHER)?;
    max_u8("catchup", s.catchup, CATCHUP_ALL)?;
    if s.timezone.len() > SCHEDULE_TIMEZONE_MAX {
        return Err(format!(
            "timezone is {} bytes (max {SCHEDULE_TIMEZONE_MAX})",
            s.timezone.len()
        ));
    }
    Ok(())
}

fn workflow_policy(p: &WorkflowPolicy) -> Check {
    opt_max_u8(
        "on_retries_exhausted",
        p.on_retries_exhausted,
        ON_EXHAUSTED_PAUSE,
    )
}

fn spill(s: &Option<SpillPolicy>) -> Check {
    let Some(s) = s else { return Ok(()) };
    for (field, v) in [
        ("spill.cpu_pct", s.cpu_pct),
        ("spill.slots_pct", s.slots_pct),
        ("spill.mem_pct", s.mem_pct),
        ("spill.hysteresis_pct", s.hysteresis_pct),
    ] {
        max_u8(field, v, 100)?;
    }
    Ok(())
}

fn queue_policy(p: &QueuePolicy) -> Check {
    opt_max_u8("placement", p.placement, PLACEMENT_AUTO)?;
    opt_max_u8(
        "on_retries_exhausted",
        p.on_retries_exhausted,
        ON_EXHAUSTED_PAUSE,
    )?;
    if p.selector.len() > SELECTOR_MAX_ENTRIES {
        return Err(format!(
            "selector has {} entries (max {SELECTOR_MAX_ENTRIES})",
            p.selector.len()
        ));
    }
    if let Some(e) = p.selector.iter().find(|e| e.len() > SELECTOR_ENTRY_MAX) {
        return Err(format!(
            "selector entry is {} bytes (max {SELECTOR_ENTRY_MAX})",
            e.len()
        ));
    }
    spill(&p.spill)?;
    rate("throttle", &p.throttle)?;
    rate("key_throttle", &p.key_throttle)?;
    rate("rate_limit", &p.rate_limit)?;
    rate("key_rate_limit", &p.key_rate_limit)
}

fn webhook(w: &Webhook) -> Check {
    max_u8(
        "action",
        w.action,
        crate::records::WEBHOOK_ACTION_COMPLETE_TOKEN,
    )?;
    max_u8(
        "verifier",
        w.verifier,
        crate::records::VERIFIER_STANDARD_WEBHOOKS,
    )
}

/// Shape-check `record`; `Err` carries the reason reported as `Invalid`.
pub(crate) fn validate_record(record: &MetaRecord) -> Check {
    match record {
        MetaRecord::Schedule(s) => schedule(s),
        MetaRecord::WorkflowPolicy(p) => workflow_policy(p),
        MetaRecord::QueuePolicy(p) => queue_policy(p),
        MetaRecord::Webhook(w) => webhook(w),
        _ => Ok(()),
    }
}
