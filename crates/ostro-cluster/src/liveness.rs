//! Phase 12: implemented in 12-10 (liveness accumulator over `RaftHandle::sample_live`,
//! voters ∪ learners, plus suspect reports; D-P12-9).

/// Receives `MetaPeer.ReportSuspect` reports (and local fetch failures). The controller
/// confirms a suspect with its next liveness sample and never acts on a report alone.
pub trait SuspectSink: Send + Sync {
    /// `reporter` saw `suspect` fail (connection refused/reset), for `partition` if known.
    fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>);
}
