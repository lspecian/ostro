//! Run-logs controller (08-17, OBS-03): `ostro_api::phase8::RunLogsApi` over the guest
//! log sink's per-run ring buffers ([`super::ControlCtx::guest_logs`], P8-D16).
//!
//! `GET /v1/runs/{id}/logs?limit=N&level=L` returns the run's last `N` (≤ 1000) lines at
//! or above level `L`, oldest first. Logs are best effort and non-durable (never
//! journaled): an unknown run, a run the node forgot (idle, or the node restarted) or a
//! run whose ring was evicted under the node byte budget answers **200 with an empty
//! list**, never 404. `dropped` counts this run's lines lost to rate limits, the lifetime
//! cap and ring eviction since the node started, so a non-zero `dropped` next to fewer
//! lines than asked for marks a truncated/evicted ring.

use ostro_api::ApiError;
use ostro_api::phase8::{LogLevelDto, RunLogLine, RunLogsApi, RunLogsRequest, RunLogsResponse};

use super::ControlCtx;
use crate::services::guest_logs::RingLine;

/// Most lines one request returns (the ring keeps at most `[guest_logs] ring_lines`).
pub const MAX_LIMIT: u32 = 1000;

/// `RunLogsApi` for the server.
#[derive(Debug)]
pub struct RunLogsController {
    ctx: ControlCtx,
}

impl RunLogsController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }
}

fn level_dto(level: &str) -> LogLevelDto {
    match level {
        "trace" => LogLevelDto::Trace,
        "debug" => LogLevelDto::Debug,
        "warn" => LogLevelDto::Warn,
        "error" => LogLevelDto::Error,
        _ => LogLevelDto::Info,
    }
}

fn min_rank(level: Option<LogLevelDto>) -> u8 {
    match level {
        None | Some(LogLevelDto::Trace) => 0,
        Some(LogLevelDto::Debug) => 1,
        Some(LogLevelDto::Info) => 2,
        Some(LogLevelDto::Warn) => 3,
        Some(LogLevelDto::Error) => 4,
    }
}

fn line_dto(l: RingLine) -> RunLogLine {
    let fields: serde_json::Map<String, serde_json::Value> = l
        .fields
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();
    RunLogLine {
        ts_ms: l.ts_ms,
        level: level_dto(l.level),
        message: l.message,
        fields: serde_json::Value::Object(fields),
        attempt: l.attempt,
    }
}

#[async_trait::async_trait]
impl RunLogsApi for RunLogsController {
    async fn run_logs(&self, req: RunLogsRequest) -> Result<RunLogsResponse, ApiError> {
        let limit = req.limit.clamp(1, MAX_LIMIT) as usize;
        let logs = self
            .ctx
            .guest_logs
            .run_logs(&req.run_id, limit, min_rank(req.level));
        Ok(RunLogsResponse {
            run_id: req.run_id,
            lines: logs.lines.into_iter().map(line_dto).collect(),
            dropped: logs.dropped,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::guest_logs::level_rank;

    #[test]
    fn run_logs_level_mapping_matches_the_sink() {
        for (s, dto) in [
            ("trace", LogLevelDto::Trace),
            ("debug", LogLevelDto::Debug),
            ("info", LogLevelDto::Info),
            ("warn", LogLevelDto::Warn),
            ("error", LogLevelDto::Error),
        ] {
            assert_eq!(level_dto(s), dto);
            assert_eq!(min_rank(Some(dto)), level_rank(s));
        }
        assert_eq!(min_rank(None), 0);
    }

    #[test]
    fn run_logs_line_fields_become_a_json_object() {
        let l = line_dto(RingLine {
            ts_ms: 7,
            level: "warn",
            message: "m".into(),
            fields: vec![("a".into(), "1".into()), ("b".into(), "x".into())],
            attempt: 2,
        });
        assert_eq!(l.fields, serde_json::json!({"a": "1", "b": "x"}));
        assert_eq!(l.level, LogLevelDto::Warn);
        assert_eq!((l.ts_ms, l.attempt), (7, 2));
    }
}
