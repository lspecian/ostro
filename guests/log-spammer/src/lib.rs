//! Guest-log test guest (plan 08-17, OBS-03): emits a configurable volume of structured
//! log lines, optionally in chunks separated by a step and/or a durable sleep (so each
//! chunk can land in its own activation, and a restart can happen mid-run).
//!
//! Input: `{"lines": N, "burst": B?, "step_between": bool?, "sleep_ms": ms?,
//! "fields": {k: v}?, "tag": "..."?, "level": "trace|debug|info|warn|error"?}`.
//! Line `i` has message `"<tag> line <i>"` and fields `seq = i`, `chunk = <chunk index>`
//! followed by every input field. Chunks are `B` lines (default: all of them).

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_sdk::prelude::*;

/// `log-spammer` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub lines: u32,
    #[serde(default)]
    pub burst: Option<u32>,
    #[serde(default)]
    pub step_between: bool,
    #[serde(default)]
    pub sleep_ms: u64,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub level: Option<String>,
}

/// `log-spammer` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub requested: u32,
    pub chunks: u32,
}

#[step]
fn mark(chunk: u32) -> Result<u32, StepError> {
    Ok(chunk)
}

fn level(name: Option<&str>) -> Level {
    match name {
        Some("trace") => Level::Trace,
        Some("debug") => Level::Debug,
        Some("warn") => Level::Warn,
        Some("error") => Level::Error,
        _ => Level::Info,
    }
}

#[workflow(name = "log-spammer", version = 1)]
pub fn log_spammer(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let burst = input.burst.unwrap_or(input.lines).max(1);
    let lvl = level(input.level.as_deref());
    let mut next = 0u32;
    let mut chunk = 0u32;
    while next < input.lines {
        let end = next.saturating_add(burst).min(input.lines);
        let chunk_s = chunk.to_string();
        for seq in next..end {
            let seq_s = seq.to_string();
            let mut fields: Vec<(&str, &str)> = vec![("seq", &seq_s), ("chunk", &chunk_s)];
            fields.extend(input.fields.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            ctx.log_with(lvl, &format!("{} line {seq}", input.tag), &fields);
        }
        next = end;
        chunk += 1;
        if next < input.lines {
            if input.step_between {
                mark(chunk)?;
            }
            if input.sleep_ms > 0 {
                ctx.sleep(Duration::from_millis(input.sleep_ms)).wait()?;
            }
        }
    }
    Ok(Output {
        requested: input.lines,
        chunks: chunk,
    })
}

ostro_sdk::export!(LogSpammerWorkflow);
