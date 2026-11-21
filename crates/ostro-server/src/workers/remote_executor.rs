//! Phase 7: implemented in 07-12.
//!
//! The remote runner: a dispatcher `Job` placed on a worker stays a server-side job; the
//! runner appends `RunStarted` on the server, sends `Activate`, serves `FetchJournal` /
//! `Append` / `StartEffect` through the run's `PartitionSink` (built by
//! `EngineExecutor::sink_for`, bridge included, 06-14 B1) and maps `Done` through the
//! shared outcome mapper. Stream loss is not an attempt failure (Pitfall 3).
