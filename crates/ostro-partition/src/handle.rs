//! `PartitionHandle`: the submit path and read helpers of one partition (STOR-11, STOR-14).
//!
//! `submit` encodes the commands into one envelope, appends it (the position is fixed by
//! the receipt), registers the offsets with [`PendingAcks`] and waits for the apply thread
//! to resolve them, which happens only after the chunk containing them was committed to the
//! state store. A failed machine makes every in-flight and later submit return an error,
//! never success.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use ostro_blob::{BlobError, BlobStore};
use ostro_log::{LogError, ReplicatedLog};
use ostro_types::command::{AttemptGranted, AttemptTokenMsg, encode_command};
use ostro_types::payload::payload_ref::Kind;
use ostro_types::{
    AttemptToken, Command, Epoch, INLINE_PAYLOAD_LIMIT, Offset, PartitionId, PayloadRef, RunId,
};

use crate::acks::PendingAcks;
use crate::apply::{ApplyOutcome, RejectReason};
use crate::machine::{MachineHandle, wait_failed};
use crate::store::keys::run_key;
use crate::store::values::{self, RunRecord};
use crate::store::{PartitionStore, StoreError, StoreReader, Table};

/// Errors of the partition submit/read path.
#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("log: {0}")]
    Log(#[from] LogError),
    #[error("state store: {0}")]
    Store(#[from] StoreError),
    #[error("blob store: {0}")]
    Blob(#[from] BlobError),
    /// The partition failed (fail-stop) or stopped; the outcome of in-flight work is unknown.
    #[error("partition failed: {0}")]
    Failed(String),
    /// The outcome of this offset is no longer retained (unknown, never `Ok`).
    #[error("outcome of offset {0} unknown (ack lost)")]
    AckLost(Offset),
    #[error("rejected: {0:?}")]
    Rejected(RejectReason),
    #[error("not leader")]
    NotLeader,
}

struct Inner<L> {
    partition: PartitionId,
    log: Arc<L>,
    store: Arc<PartitionStore>,
    machine: MachineHandle,
    acks: Arc<PendingAcks>,
    blobs: Arc<BlobStore>,
    epoch: AtomicU32,
}

/// One partition's submit/read handle. Cheap to clone; clones share the epoch.
pub struct PartitionHandle<L: ReplicatedLog> {
    inner: Arc<Inner<L>>,
}

impl<L: ReplicatedLog> Clone for PartitionHandle<L> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<L: ReplicatedLog> std::fmt::Debug for PartitionHandle<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionHandle")
            .field("partition", &self.inner.partition)
            .field("epoch", &self.epoch())
            .finish_non_exhaustive()
    }
}

impl<L: ReplicatedLog> PartitionHandle<L> {
    pub fn new(
        partition: PartitionId,
        log: Arc<L>,
        store: Arc<PartitionStore>,
        machine: MachineHandle,
        acks: Arc<PendingAcks>,
        blobs: Arc<BlobStore>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                partition,
                log,
                store,
                machine,
                acks,
                blobs,
                epoch: AtomicU32::new(0),
            }),
        }
    }

    pub fn partition(&self) -> PartitionId {
        self.inner.partition
    }

    /// Epoch stamped on appends (shared by every clone).
    pub fn set_epoch(&self, e: Epoch) {
        self.inner.epoch.store(e.0, Ordering::Release);
    }

    pub fn epoch(&self) -> Epoch {
        Epoch(self.inner.epoch.load(Ordering::Acquire))
    }

    /// Append `cmds` as one envelope and return their outcomes in order, after apply.
    pub async fn submit(&self, cmds: Vec<Command>) -> Result<Vec<ApplyOutcome>, PartitionError> {
        if cmds.is_empty() {
            return Ok(Vec::new());
        }
        let m = &self.inner.machine;
        m.check_healthy()?;
        let bytes = cmds.iter().map(encode_command).collect();
        let receipt = self.inner.log.append(self.epoch(), bytes).await?;
        let rxs = self.inner.acks.register(receipt.base, receipt.last);
        let mut health = m.health();
        let mut out = Vec::with_capacity(rxs.len());
        for (i, rx) in rxs.into_iter().enumerate() {
            let off = receipt.base + i as u64;
            // biased: an outcome resolved after commit wins over a failure noticed later.
            let r = tokio::select! {
                biased;
                r = rx => r,
                reason = wait_failed(&mut health) => return Err(PartitionError::Failed(reason)),
            };
            match r {
                Ok(Ok(o)) => out.push(o),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(PartitionError::AckLost(off)),
            }
        }
        Ok(out)
    }

    pub async fn submit_one(&self, cmd: Command) -> Result<ApplyOutcome, PartitionError> {
        let mut v = self.submit(vec![cmd]).await?;
        v.pop()
            .ok_or_else(|| PartitionError::Failed("submit returned no outcome".into()))
    }

    /// Grant the next attempt of `run` under the current epoch (logged `AttemptGranted`).
    pub async fn grant_attempt(&self, run: RunId) -> Result<AttemptToken, PartitionError> {
        let epoch = self.epoch();
        let current = self.read_run(&run)?.and_then(|r| r.current_attempt);
        let n = match current {
            Some((e, n)) if e == epoch.0 => n.saturating_add(1),
            _ => 1,
        };
        let cmd = Command::attempt_granted(AttemptGranted {
            run_id: run.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: epoch.0, n }),
        });
        match self.submit_one(cmd).await? {
            ApplyOutcome::Accepted => Ok(AttemptToken { epoch, n }),
            ApplyOutcome::Rejected(r) => Err(PartitionError::Rejected(r)),
            ApplyOutcome::Duplicate { .. } => Err(PartitionError::Rejected(RejectReason::Fenced {
                current: None,
            })),
        }
    }

    /// Inline up to 256 KiB; larger payloads are written to the blob store (durably: fsync +
    /// rename + dir fsync) **before** the caller appends the referencing command.
    pub async fn prepare_payload(&self, bytes: Bytes) -> Result<PayloadRef, PartitionError> {
        if bytes.len() <= INLINE_PAYLOAD_LIMIT {
            return Ok(PayloadRef::inline(bytes));
        }
        let blobs = Arc::clone(&self.inner.blobs);
        let r = tokio::task::spawn_blocking(move || blobs.put(&bytes))
            .await
            .map_err(|e| PartitionError::Failed(format!("blob put task: {e}")))??;
        Ok(PayloadRef::blob(r.sha256, r.len))
    }

    /// Payload bytes; blob reads verify length and sha256 (never wrong bytes).
    pub async fn fetch_payload(&self, p: &PayloadRef) -> Result<Bytes, PartitionError> {
        match &p.kind {
            Some(Kind::Inline(b)) => Ok(b.clone()),
            Some(Kind::Blob(b)) => {
                let sha: [u8; 32] = b.sha256.as_slice().try_into().map_err(|_| {
                    BlobError::NotFound(format!("malformed blob ref ({} byte sha)", b.sha256.len()))
                })?;
                let r = ostro_blob::BlobRef {
                    sha256: sha,
                    len: b.len,
                };
                let blobs = Arc::clone(&self.inner.blobs);
                let v = tokio::task::spawn_blocking(move || blobs.get(&r))
                    .await
                    .map_err(|e| PartitionError::Failed(format!("blob get task: {e}")))??;
                Ok(Bytes::from(v))
            }
            None => Err(BlobError::NotFound("empty payload ref".into()).into()),
        }
    }

    pub fn read_run(&self, run: &RunId) -> Result<Option<RunRecord>, PartitionError> {
        match self.reader().get(Table::Runs, &run_key(run))? {
            Some(v) => Ok(Some(values::decode(&v)?)),
            None => Ok(None),
        }
    }

    pub fn reader(&self) -> StoreReader {
        self.inner.store.reader()
    }

    pub fn machine(&self) -> &MachineHandle {
        &self.inner.machine
    }

    pub fn log(&self) -> &Arc<L> {
        &self.inner.log
    }

    pub fn store(&self) -> &Arc<PartitionStore> {
        &self.inner.store
    }

    pub fn blobs(&self) -> &Arc<BlobStore> {
        &self.inner.blobs
    }
}
