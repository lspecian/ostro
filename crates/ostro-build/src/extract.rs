//! Safe archive extraction (.tar.gz, .tar.xz, single binary, .whl verify-only): rejects
//! absolute paths, `..` entries, escaping symlinks (checked against the canonicalised on-disk
//! parent) and escaping hardlinks (resolved against the archive root).
//!
//! Owner: 10-07 (stub from 10-02).
