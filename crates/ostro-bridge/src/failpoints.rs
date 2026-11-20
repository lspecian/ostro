//! Bridge failpoints (06-02, P-13).
//!
//! Every site uses [`bridge_fail!`](crate::bridge_fail), which evaluates the `fail` crate
//! failpoint `name` when the crate is built with the `failpoints` feature and is a no-op
//! (`None`) otherwise. Supported actions:
//!
//! | FAILPOINTS action     | [`FailAction`] | Effect of [`apply`]                                    |
//! |-----------------------|----------------|--------------------------------------------------------|
//! | `return(hang)`        | `Hang`         | pends forever (never blocks a tokio worker)            |
//! | `return(wait:<ms>)`   | `Wait(ms)`     | async sleep                                            |
//! | `return(error)`       | `Error`        | `Err(())`: the site returns its transport-style error  |
//!
//! `fail`'s own `pause`/`sleep` block the OS thread and would deadlock tokio workers, so they
//! are never used. Crash tests configure `return(hang)` and then `kill -9` the process.
//!
//! Sites (names reserved under the `bridge::` prefix, RESERVATIONS §11):
//! `bridge::http::before_send`, `bridge::http::after_response`,
//! `bridge::sql::before_user_stmt`, `bridge::sql::after_commit`, `bridge::object::before_put`,
//! `bridge::effect::before_complete_submit`.

/// What a configured failpoint asks the site to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailAction {
    /// `return(hang)`: never resume.
    Hang,
    /// `return(wait:<ms>)`: sleep `ms` milliseconds, then continue.
    Wait(u64),
    /// `return(error)` (and any other or empty return value): fail the site.
    Error,
}

impl FailAction {
    /// Parses the `return(<arg>)` argument of a failpoint.
    pub fn from_arg(arg: Option<&str>) -> Self {
        match arg.map(str::trim) {
            Some("hang") => Self::Hang,
            Some(a) => match a
                .strip_prefix("wait:")
                .and_then(|ms| ms.trim().parse().ok())
            {
                Some(ms) => Self::Wait(ms),
                None => Self::Error,
            },
            None => Self::Error,
        }
    }
}

/// Evaluates failpoint `name`. Use [`bridge_fail!`](crate::bridge_fail) instead of calling
/// this directly.
#[cfg(feature = "failpoints")]
pub fn eval(name: &str) -> Option<FailAction> {
    fail::eval(name, |arg| arg).map(|arg| FailAction::from_arg(arg.as_deref()))
}

/// Evaluates failpoint `name`: always `None` without the `failpoints` feature.
#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub fn eval(_name: &str) -> Option<FailAction> {
    None
}

/// Carries out `action`: `Hang` pends forever, `Wait` sleeps, `Error` returns `Err(())` so the
/// site can produce its own transport-style error.
// `()` is the intended error: each site maps it to its own failure.
#[allow(clippy::result_unit_err)]
pub async fn apply(action: FailAction) -> Result<(), ()> {
    match action {
        FailAction::Hang => std::future::pending().await,
        FailAction::Wait(ms) => {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            Ok(())
        }
        FailAction::Error => Err(()),
    }
}

/// `bridge_fail!(name)` -> `Option<FailAction>`: the configured action of failpoint `name`, or
/// `None` (always `None` unless `ostro-bridge` is built with `failpoints`).
#[macro_export]
macro_rules! bridge_fail {
    ($name:expr) => {
        $crate::failpoints::eval($name)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_actions() {
        assert_eq!(FailAction::from_arg(Some("hang")), FailAction::Hang);
        assert_eq!(
            FailAction::from_arg(Some("wait:250")),
            FailAction::Wait(250)
        );
        assert_eq!(FailAction::from_arg(Some("error")), FailAction::Error);
        assert_eq!(FailAction::from_arg(Some("wait:x")), FailAction::Error);
        assert_eq!(FailAction::from_arg(None), FailAction::Error);
    }

    #[tokio::test]
    async fn apply_wait_and_error() {
        assert_eq!(apply(FailAction::Wait(1)).await, Ok(()));
        assert_eq!(apply(FailAction::Error).await, Err(()));
        let hang = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            apply(FailAction::Hang),
        )
        .await;
        assert!(hang.is_err(), "hang never resumes");
    }

    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn unconfigured_without_feature() {
        assert_eq!(crate::bridge_fail!("bridge::http::before_send"), None);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn configured_with_feature() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("bridge::test::site", "return(wait:5)").unwrap();
        assert_eq!(
            crate::bridge_fail!("bridge::test::site"),
            Some(FailAction::Wait(5))
        );
        fail::cfg("bridge::test::site", "return(hang)").unwrap();
        assert_eq!(
            crate::bridge_fail!("bridge::test::site"),
            Some(FailAction::Hang)
        );
        fail::remove("bridge::test::site");
        assert_eq!(crate::bridge_fail!("bridge::test::site"), None);
        scenario.teardown();
    }
}
