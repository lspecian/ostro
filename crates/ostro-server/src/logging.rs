//! Tracing subscriber initialisation.

use tracing_subscriber::{EnvFilter, fmt};

use crate::config::{LogFormat, LogSection};

/// Install the global subscriber: `RUST_LOG` wins over `cfg.level`; pretty or compact
/// output per `cfg.format`. Idempotent: a second call (or one after another subscriber
/// was installed, e.g. in tests) is a no-op and returns `false`.
pub fn init_tracing(cfg: &LogSection) -> bool {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt().with_env_filter(filter);
    let res = match cfg.format {
        LogFormat::Pretty => builder.pretty().try_init(),
        LogFormat::Compact => builder.compact().try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
    res.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent() {
        let cfg = LogSection::default();
        let _first = init_tracing(&cfg);
        assert!(!init_tracing(&cfg), "second init must be a no-op");
    }
}
