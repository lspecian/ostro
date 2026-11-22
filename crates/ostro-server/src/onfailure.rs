//! `on_failure` hook and DLQ policy resolution (Phase 11, D-P11-12, D-P11-29): reading
//! ext tags 144 (`OnFailure`, WorkflowPolicy and QueuePolicy; the workflow wins) and 145
//! (`DlqPolicy`, QueuePolicy) from the meta policy records and building the hook run's
//! `StartRun`. Declared once by 11-04 (D-P11-23); implemented by **11-12**. Every emission
//! of the Phase 11 encodings goes through [`crate::p11::p11_feature_enabled`].
//!
//! Stub: no items yet.
