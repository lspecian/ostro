//! Host impl of `ostro:runtime/context` (WIT-04): run info, logical time, deterministic
//! randomness, patch markers and status. Only `patched` and `set-status` journal.

use ostro_types::journal::{
    CommandArgs, CommandKind, CommandMeta, EventBody, HASH_VER, StatusPublished,
};

use crate::env::RandomStream;
use crate::host::bindings::ostro::runtime::context::Host;
use crate::host::bindings::ostro::runtime::types::{Datetime, RunInfo};
use crate::host::state::{HostState, trap, violation};
use crate::replay::CommandResult;

/// Max `random-bytes` length and `set-status` JSON length.
pub const MAX_CONTEXT_BYTES: usize = 65_536;

pub(crate) fn datetime(nanos: u64) -> Datetime {
    Datetime {
        seconds: nanos / 1_000_000_000,
        nanoseconds: (nanos % 1_000_000_000) as u32,
    }
}

impl crate::host::bindings::ostro::runtime::types::Host for HostState {}

impl Host for HostState {
    async fn info(&mut self) -> wasmtime::Result<RunInfo> {
        let rs = self.replay.run();
        Ok(RunInfo {
            run_id: self.run.run_id.clone(),
            workflow: rs.workflow.clone(),
            workflow_version: rs.workflow_version,
            attempt: self.attempt,
            module_hash: self.module_hash_hex.clone(),
            tenant: rs.tenant.clone(),
            parent_run_id: rs.parent_run_id.clone(),
            deadline: rs.deadline_nanos.map(datetime),
            continued_from: rs.continued_from.clone(),
        })
    }

    async fn now(&mut self) -> wasmtime::Result<Datetime> {
        Ok(datetime(self.replay.logical_now_nanos()))
    }

    async fn random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(self.replay.rng(RandomStream::CtxRandom).next_u64())
    }

    async fn random_bytes(&mut self, len: u32) -> wasmtime::Result<Vec<u8>> {
        let len = len as usize;
        if len > MAX_CONTEXT_BYTES {
            return Err(violation("random-bytes len exceeds 65536"));
        }
        let mut buf = vec![0u8; len];
        self.replay.rng(RandomStream::CtxRandom).fill(&mut buf);
        Ok(buf)
    }

    async fn uuid_v7(&mut self) -> wasmtime::Result<String> {
        Ok(self.replay.new_uuid_v7())
    }

    async fn is_replaying(&mut self) -> wasmtime::Result<bool> {
        Ok(self.replay.is_replaying())
    }

    async fn patched(&mut self, change_id: String) -> wasmtime::Result<bool> {
        let hash = self.hash(&CommandArgs::Patch {
            change_id: &change_id,
        })?;
        self.replay.patched(hash, &change_id).map_err(trap)
    }

    async fn set_status(&mut self, json: String) -> wasmtime::Result<()> {
        if json.len() > MAX_CONTEXT_BYTES {
            return Err(violation("set-status json exceeds 65536 bytes"));
        }
        let hash = self.hash(&CommandArgs::SetStatus)?;
        let live_json = json.clone();
        let res = self
            .replay
            .command(
                CommandKind::SetStatus,
                hash,
                &|| "set-status".to_owned(),
                |_| {
                    EventBody::StatusPublished(StatusPublished {
                        meta: CommandMeta {
                            hash_ver: HASH_VER,
                            hash,
                        },
                        json: live_json,
                    })
                },
            )
            .map_err(trap)?;
        // The published status is always the journaled one (recorded json on replay).
        let journaled = match res {
            CommandResult::Replayed(ev) => match &ev.body {
                EventBody::StatusPublished(s) => s.json.clone(),
                other => {
                    return Err(violation(format!(
                        "bad journal: set-status matched {}",
                        other.summary()
                    )));
                }
            },
            CommandResult::Live { .. } => json,
        };
        self.latest_status = Some(journaled);
        Ok(())
    }

    async fn continue_as_new_suggested(&mut self) -> wasmtime::Result<bool> {
        Ok(self.replay.continue_as_new_suggested(self.can_events))
    }
}
