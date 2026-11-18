//! Host impl of `ostro:runtime/log`. Log lines are never journaled and are dropped
//! while replaying; live lines are bounded per activation (1000 lines / 1 MiB, excess
//! counted) and emitted as `tracing` events with run id / attempt fields.

use crate::host::bindings::ostro::runtime::log::{Host, Level};
use crate::host::state::{HostState, LogLine};

fn level_name(l: Level) -> &'static str {
    match l {
        Level::Trace => "trace",
        Level::Debug => "debug",
        Level::Info => "info",
        Level::Warn => "warn",
        Level::Error => "error",
    }
}

impl Host for HostState {
    async fn emit(
        &mut self,
        level: Level,
        message: String,
        fields: Vec<(String, String)>,
    ) -> wasmtime::Result<()> {
        if self.replay.is_replaying() {
            return Ok(());
        }
        let run = self.run.run_id.clone();
        let attempt = self.attempt;
        let fields_text = fields
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        let kept = self.logs.push(LogLine {
            level: level_name(level),
            message: message.clone(),
            fields,
        });
        if !kept {
            return Ok(());
        }
        macro_rules! ev {
            ($lvl:ident) => {
                tracing::$lvl!(
                    target: "ostro::guest",
                    run = %run,
                    attempt,
                    fields = %fields_text,
                    "{message}"
                )
            };
        }
        match level {
            Level::Trace => ev!(trace),
            Level::Debug => ev!(debug),
            Level::Info => ev!(info),
            Level::Warn => ev!(warn),
            Level::Error => ev!(error),
        }
        Ok(())
    }
}
