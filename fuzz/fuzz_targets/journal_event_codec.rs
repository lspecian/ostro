//! Fuzz target `journal_event_codec` (Phase 14, D-P14-17): libFuzzer bytes into
//! `ostro_fuzz_targets::run(Target::JournalEventCodec, ..)`; the logic lives in `crates/ostro-fuzz-targets`.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    ostro_fuzz_targets::run(ostro_fuzz_targets::Target::JournalEventCodec, data)
});
