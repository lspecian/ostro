//! [`MetaSm`]: the Raft [`StateMachine`] over [`MetaState`], and [`MetaView`],
//! the read side shared with the rest of the node.

use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::watch;

use crate::StateMachine;
use crate::command::{self, MetaResult, decode_command, encode_result};
use crate::state::MetaState;

/// Applies encoded [`crate::MetaCommand`]s to a shared [`MetaState`]. Runs on
/// the Raft driver thread.
#[derive(Debug)]
pub struct MetaSm {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Sender<u64>,
}

/// Read-only view of the metadata state. Cheap to clone.
#[derive(Debug, Clone)]
pub struct MetaView {
    shared: Arc<RwLock<MetaState>>,
    applied: watch::Receiver<u64>,
}

impl MetaSm {
    /// A state machine over an empty state, plus its view.
    pub fn new() -> (MetaSm, MetaView) {
        let shared = Arc::new(RwLock::new(MetaState::default()));
        let (tx, rx) = watch::channel(0);
        (
            MetaSm {
                shared: shared.clone(),
                applied: tx,
            },
            MetaView {
                shared,
                applied: rx,
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
            Err(e) => MetaResult::Invalid(format!("undecodable metadata command: {e}")),
        };
        self.applied.send_replace(index);
        encode_result(&result)
    }

    /// `[META_FORMAT] ‖ postcard(MetaState)`; byte-identical for equal states.
    fn snapshot(&self) -> Vec<u8> {
        let state = self.shared.read().unwrap_or_else(PoisonError::into_inner);
        command::encode(&*state)
    }

    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), String> {
        let state: MetaState =
            command::decode(data).map_err(|e| format!("metadata snapshot: {e}"))?;
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

    /// Notified after every apply/restore with the applied index.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.applied.clone()
    }
}
