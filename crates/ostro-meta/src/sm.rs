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

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::watch;

use crate::StateMachine;
use crate::command::{self, DecodeError, MetaResult, decode_command, encode_result};
use crate::state::MetaState;

/// Applies encoded [`crate::MetaCommand`]s to a shared [`MetaState`]. Runs on
/// the Raft driver thread.
#[derive(Debug)]
pub struct MetaSm {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Sender<u64>,
    pre_freeze: Arc<AtomicU8>,
}

/// Read-only view of the metadata state. Cheap to clone.
#[derive(Debug, Clone)]
pub struct MetaView {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Receiver<u64>,
    /// Non-zero once a pre-freeze-format entry was applied (its format byte).
    pre_freeze: Arc<AtomicU8>,
}

impl MetaSm {
    /// A state machine over an empty state, plus its view.
    pub fn new() -> (MetaSm, MetaView) {
        let shared = Arc::new(RwLock::new(MetaState::default()));
        let (tx, rx) = watch::channel(0);
        let pre_freeze = Arc::new(AtomicU8::new(0));
        (
            MetaSm {
                shared: shared.clone(),
                applied: tx,
                pre_freeze: pre_freeze.clone(),
            },
            MetaView {
                shared,
                applied: rx,
                pre_freeze,
            },
        )
    }
}

impl StateMachine for MetaSm {
    fn apply(&mut self, index: u64, _term: u64, command: &[u8]) -> Vec<u8> {
        let result = match decode_command(command) {
            Ok(cmd) => {
                let mut state = self.shared.write().unwrap_or_else(PoisonError::into_inner);
                state.apply(index, cmd)
            }
            Err(e) => {
                if let DecodeError::PreFreezeFormat { found } = e {
                    // Never a silently wrong record: remember it so that
                    // MetaHandle::start refuses to serve this data dir.
                    self.pre_freeze.store(found.max(1), Ordering::Release);
                }
                MetaResult::Invalid(format!("undecodable metadata command: {e}"))
            }
        };
        self.applied.send_replace(index);
        encode_result(&result)
    }

    /// `[META_FORMAT] ‖ postcard(MetaState)`; byte-identical for equal states.
    fn snapshot(&self) -> Vec<u8> {
        let state = self.shared.read().unwrap_or_else(PoisonError::into_inner);
        command::encode_snapshot(&state)
    }

    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), String> {
        let state =
            command::decode_snapshot(data).map_err(|e| format!("metadata snapshot: {e}"))?;
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

    /// Notified after every apply/restore with the applied index.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.applied.clone()
    }
}
