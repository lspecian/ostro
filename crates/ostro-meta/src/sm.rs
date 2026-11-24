//! [`MetaSm`]: the Raft [`StateMachine`] over [`MetaState`], and [`MetaView`],
//! the read side shared with the rest of the node.
//!
//! # Encoding evolution (D20)
//!
//! Raft entries are `[META_FORMAT] ‖ postcard(MetaCommand)` and snapshots are
//! `[META_FORMAT] ‖ postcard(MetaState)`, both positional. `META_FORMAT` was
//! bumped once before the 05-20 freeze (1 -> 2, 05-07); format 1 is rejected
//! with [`DecodeError::PreFreezeFormat`]: a snapshot fails `restore` (and so
//! node start), and a replayed format-1 log entry makes
//! [`crate::MetaHandle::start`] fail. That pre-freeze bump is the only one
//! without a decoder.
//!
//! After the freeze, the encoding changes only by:
//! - appending a new `MetaRecord`/`MetaKind`/`MetaKey` variant (and its
//!   `MetaState` map, last) in `.planning/RESERVATIONS.md` §10 order, plus a
//!   `META_FORMAT` bump with a decoder for **every** older format; or
//! - registering an `ext` tag (RESERVATIONS §10b) on an existing record: no
//!   variant, no bump; or
//! - appending a `MetaCommand` variant (RESERVATIONS §10a): no bump, because no
//!   existing encoding changes.
//!
//! Fields are never added to an existing meta struct after the freeze.
//! `tests/golden/meta_v2.txt` pins the encoding; lines are only appended.
//!
//! # Fail-stop on newer input (Phase 13, 13-05, D-P13-6 / plan check B6)
//!
//! A committed entry this binary cannot decode (a newer `META_FORMAT`, an unknown
//! `MetaCommand` variant, any other undecodable non-empty bytes) or a command above the
//! binary's level ([`required_level`], level emulation only) **halts the voter**: `apply`
//! returns [`FatalApply`] and the vendored driver stops before `applied` advances. It is
//! never answered with `MetaResult::Invalid`, which would let this voter's state diverge
//! from a newer voter that applied it. The pre-freeze format byte 1 keeps its D20 path.

use std::sync::atomic::{AtomicU8, Ordering};

use ostro_raft_driver::FatalApply;
use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::watch;

use crate::StateMachine;
use crate::command::{
    self, DecodeError, META_FORMAT, MetaCommand, MetaResult, decode_command, encode_result,
};
use crate::state::MetaState;

/// Applies encoded [`crate::MetaCommand`]s to a shared [`MetaState`]. Runs on
/// the Raft driver thread.
#[derive(Debug)]
pub struct MetaSm {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Sender<u64>,
    pre_freeze: Arc<AtomicU8>,
    /// Non-zero once a newer-format entry or snapshot was refused (its format byte).
    newer_format: Arc<AtomicU8>,
    /// The highest feature level this binary may apply ([`required_level`]); `u32::MAX`
    /// unless level emulation is on (13-05).
    level_max: u32,
}

/// The phase-13 feature level a metadata command needs. Mirrors
/// `ostro_types::compat::feature_for_meta_variant` (ostro-meta does not depend on
/// ostro-types; ostro-cluster's `meta_peer` tests pin the two together): variant 8
/// `AlterReplicas` is `Feature::AlterReplicas`, introduced at level 2; everything else is
/// level 1.
pub fn required_level(cmd: &MetaCommand) -> u32 {
    match cmd {
        MetaCommand::AlterReplicas { .. } => 2,
        _ => 1,
    }
}

/// The `FatalApply` message of an entry written by a newer Ostro.
pub const NEWER_ENTRY: &str = "metadata entry written by a newer Ostro; upgrade this binary";

/// Read-only view of the metadata state. Cheap to clone.
#[derive(Debug, Clone)]
pub struct MetaView {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Receiver<u64>,
    /// Non-zero once a pre-freeze-format entry was applied (its format byte).
    pre_freeze: Arc<AtomicU8>,
    /// Non-zero once a newer-format entry or snapshot was refused (its format byte).
    newer_format: Arc<AtomicU8>,
}

impl MetaSm {
    /// A state machine over an empty state, plus its view.
    pub fn new() -> (MetaSm, MetaView) {
        Self::with_level_max(u32::MAX)
    }

    /// [`MetaSm::new`] applying only commands up to feature level `level_max` (level
    /// emulation, 13-05): a committed command above it halts this voter.
    pub fn with_level_max(level_max: u32) -> (MetaSm, MetaView) {
        let shared = Arc::new(RwLock::new(MetaState::default()));
        let (tx, rx) = watch::channel(0);
        let pre_freeze = Arc::new(AtomicU8::new(0));
        let newer_format = Arc::new(AtomicU8::new(0));
        (
            MetaSm {
                shared: shared.clone(),
                applied: tx,
                pre_freeze: pre_freeze.clone(),
                newer_format: newer_format.clone(),
                level_max,
            },
            MetaView {
                shared,
                applied: rx,
                pre_freeze,
                newer_format,
            },
        )
    }
}

impl StateMachine for MetaSm {
    fn apply(&mut self, index: u64, _term: u64, command: &[u8]) -> Result<Vec<u8>, FatalApply> {
        let result = match decode_command(command) {
            Ok(cmd) => {
                let need = required_level(&cmd);
                if need > self.level_max {
                    return Err(FatalApply(format!(
                        "{NEWER_ENTRY} (entry {index} needs feature level {need}, this binary runs at level {})",
                        self.level_max
                    )));
                }
                let mut state = self.shared.write().unwrap_or_else(PoisonError::into_inner);
                state.apply(index, cmd)
            }
            // Genuinely empty bytes (the driver never hands those over: an empty entry is
            // a leader no-op) stay Invalid.
            Err(DecodeError::Empty) => MetaResult::Invalid(format!(
                "undecodable metadata command: {}",
                DecodeError::Empty
            )),
            Err(DecodeError::PreFreezeFormat { found }) => {
                // Never a silently wrong record: remember it so that
                // MetaHandle::start refuses to serve this data dir.
                self.pre_freeze.store(found.max(1), Ordering::Release);
                MetaResult::Invalid(format!(
                    "undecodable metadata command: {}",
                    DecodeError::PreFreezeFormat { found }
                ))
            }
            Err(e) => {
                // A newer format byte, an unknown variant or other undecodable bytes:
                // halt this voter before `applied` advances (13-05).
                if let DecodeError::NewerFormat { found, .. } = e {
                    self.newer_format.store(found, Ordering::Release);
                }
                return Err(FatalApply(format!("{NEWER_ENTRY} (entry {index}: {e})")));
            }
        };
        self.applied.send_replace(index);
        Ok(encode_result(&result))
    }

    /// `[META_FORMAT] ‖ postcard(MetaState)`; byte-identical for equal states.
    fn snapshot(&self) -> Vec<u8> {
        let state = self.shared.read().unwrap_or_else(PoisonError::into_inner);
        command::encode_snapshot(&state)
    }

    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), String> {
        let state = command::decode_snapshot(data).map_err(|e| {
            if let DecodeError::NewerFormat { found, .. } = e {
                self.newer_format.store(found, Ordering::Release);
            }
            format!("metadata snapshot: {e}")
        })?;
        *self.shared.write().unwrap_or_else(PoisonError::into_inner) = state;
        self.applied.send_replace(index);
        Ok(())
    }
}

impl MetaView {
    /// Run `f` against the current state under the read lock. Clone out what
    /// you need; never hold anything borrowed across an `.await`.
    pub fn read<R>(&self, f: impl FnOnce(&MetaState) -> R) -> R {
        let state = self.shared.read().unwrap_or_else(PoisonError::into_inner);
        f(&state)
    }

    /// The highest Raft index applied (or restored) so far.
    pub fn applied(&self) -> u64 {
        *self.applied.borrow()
    }

    /// `Some(format byte)` if a pre-freeze-format Raft entry was applied: the
    /// data dir predates the Phase 5 meta format and must be deleted.
    pub fn pre_freeze_format(&self) -> Option<u8> {
        match self.pre_freeze.load(Ordering::Acquire) {
            0 => None,
            f => Some(f),
        }
    }

    /// `Some(format byte)` once an entry or snapshot in a meta format newer than
    /// [`META_FORMAT`] was refused: a newer Ostro finalized this cluster (13-05).
    pub fn newer_format(&self) -> Option<u8> {
        match self.newer_format.load(Ordering::Acquire) {
            0 => None,
            f if f > META_FORMAT => Some(f),
            _ => None,
        }
    }

    /// Notified after every apply/restore with the applied index.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.applied.clone()
    }
}
