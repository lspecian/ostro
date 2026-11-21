//! Phase 7: implemented in 07-06.
//!
//! The cluster CA (`<data_dir>/tls/ca.{crt,key}`, keys 0600, bound to `cluster_id`,
//! D-W5), the node certificate (`ostro://<cluster_id>/node/<node_id>`, D-W20) and CSR
//! signing for worker client certificates (D-W4).
