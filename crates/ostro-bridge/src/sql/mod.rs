//! The SQL connector (06-11, FX-04): PostgreSQL and MySQL pools per named connection, the
//! exactly-once statement allow-list, dynamic value mapping and the token-first exactly-once
//! protocol in `ostro_effects`.

pub mod classify;
pub mod config;
pub mod value;
