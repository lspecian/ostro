//! Fuzz target `journal_event_codec` (14-07, SEC-05): the frozen journal event and recorded
//! result codecs (`ostro_types::journal`).
//!
//! Input: `selector ‖ bytes`.
//! - selector `0xfe` / `0xff`: `bytes = codec_ver ‖ postcard` decoded as a recorded step
//!   result (`Result<Payload, Failure>`) / op result (`Result<OpOutput, Failure>`);
//! - any other selector: the event kind code (`decode_event(seq, selector, bytes)`; unknown
//!   kinds must be rejected).
//!
//! Invariants: decoding never panics; a decoded value re-encodes (same kind) to bytes that
//! decode again, and re-encoding those is byte-identical (decode → encode → decode is
//! stable; compared on bytes so NaN payloads compare equal); the summary helpers never panic.

use ostro_types::journal::{
    EventKind, Failure, OpOutput, Payload, RecordedResult, decode_event, decode_result,
    encode_event, encode_result,
};

/// Fuzz entry point.
pub fn journal_event_codec(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel {
        0xfe => step_result(rest),
        0xff => op_result(rest),
        kind => event(u32::from(kind), rest),
    }
}

fn event(kind: u32, bytes: &[u8]) {
    let seq = 7;
    let Ok(ev) = decode_event(seq, kind, bytes) else {
        return;
    };
    assert!(
        EventKind::from_code(kind).is_some(),
        "unknown kind {kind} decoded"
    );
    assert_eq!(ev.seq, seq);
    assert_eq!(ev.body.kind().code(), kind, "decoded body of another kind");
    let _ = ev.body.summary();
    let _ = ev.body.command_meta();
    let _ = ev.body.completes_op();
    let _ = ev.body.is_guest_command();

    let (k1, b1) = encode_event(&ev);
    assert_eq!(k1, kind, "re-encoded under another kind");
    let ev2 = decode_event(seq, k1, &b1).expect("re-encoded event decodes");
    let (k2, b2) = encode_event(&ev2);
    assert_eq!((k2, &b2), (k1, &b1), "event re-encoding is not stable");
}

/// Recorded result of type `$t`: decode → encode → decode, compared on bytes.
macro_rules! result_rt {
    ($name:ident, $t:ty) => {
        fn $name(bytes: &[u8]) {
            let Some((&codec_ver, body)) = bytes.split_first() else {
                return;
            };
            let r = RecordedResult {
                codec_ver,
                bytes: body.to_vec(),
            };
            let Ok(v) = decode_result::<$t>(&r) else {
                return;
            };
            let r1 = encode_result(&v);
            let v2: $t = decode_result(&r1).expect("re-encoded result decodes");
            assert_eq!(encode_result(&v2), r1, "result re-encoding is not stable");
        }
    };
}
result_rt!(step_result, Result<Payload, Failure>);
result_rt!(op_result, Result<OpOutput, Failure>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        journal_event_codec(&[]);
        journal_event_codec(&[1]);
        journal_event_codec(&[1, 1]);
        journal_event_codec(&[200, 1, 0, 0]);
        journal_event_codec(&[0xfe, 1, 0, 0]);
        journal_event_codec(&[0xff, 1, 0, 3]);
    }
}
