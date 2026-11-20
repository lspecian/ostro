//! Span-id override contract shared by the engine and the server (P8-D14, plan 08-01).
//!
//! Deterministic span ids: the server's custom OpenTelemetry `IdGenerator` (08-04) calls
//! [`take_next_ids`] and, when an override is present, uses it instead of random ids. The code
//! that starts a span calls [`set_next_ids`] immediately before `start_with_context`, on the same
//! thread, so the override is consumed by exactly that span. The derivation of the ids
//! (`blake3("ostro/trace" ‖ run_id)` etc.) is 08-11's.

use std::cell::Cell;

thread_local! {
    static NEXT_IDS: Cell<Option<([u8; 16], [u8; 8])>> = const { Cell::new(None) };
}

/// Sets the `(trace id, span id)` the next span started on this thread must use.
pub fn set_next_ids(trace: [u8; 16], span: [u8; 8]) {
    NEXT_IDS.with(|c| c.set(Some((trace, span))));
}

/// Takes (and clears) this thread's pending override, if any.
pub fn take_next_ids() -> Option<([u8; 16], [u8; 8])> {
    NEXT_IDS.with(Cell::take)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_is_taken_once_and_thread_local() {
        assert_eq!(take_next_ids(), None);
        set_next_ids([1; 16], [2; 8]);
        std::thread::spawn(|| assert_eq!(take_next_ids(), None))
            .join()
            .expect("thread");
        assert_eq!(take_next_ids(), Some(([1; 16], [2; 8])));
        assert_eq!(take_next_ids(), None);
        set_next_ids([3; 16], [4; 8]);
        set_next_ids([5; 16], [6; 8]);
        assert_eq!(take_next_ids(), Some(([5; 16], [6; 8])), "last set wins");
    }
}
