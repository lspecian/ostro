//! `wasi:random/{random,insecure,insecure-seed}` from the per-run deterministic streams.

use super::WasiImpl;
use super::generated::wasi::random::{insecure, insecure_seed, random};
use crate::env::{GuestEnv, RandomStream};

/// Largest single random request served (guards host memory against a hostile `len`).
const MAX_RANDOM_LEN: u64 = 1 << 20;

fn bytes<T: GuestEnv>(env: &mut T, stream: RandomStream, len: u64) -> wasmtime::Result<Vec<u8>> {
    if len > MAX_RANDOM_LEN {
        return Err(wasmtime::Error::msg(format!(
            "wasi:random request of {len} bytes exceeds the {MAX_RANDOM_LEN}-byte limit"
        )));
    }
    let mut buf = vec![0u8; usize::try_from(len)?];
    env.fill_random(stream, &mut buf);
    Ok(buf)
}

fn u64_from<T: GuestEnv>(env: &mut T, stream: RandomStream) -> u64 {
    let mut buf = [0u8; 8];
    env.fill_random(stream, &mut buf);
    u64::from_le_bytes(buf)
}

impl<T: GuestEnv> random::Host for WasiImpl<'_, T> {
    fn get_random_bytes(&mut self, len: u64) -> wasmtime::Result<Vec<u8>> {
        bytes(self.0, RandomStream::WasiRandom, len)
    }

    fn get_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(u64_from(self.0, RandomStream::WasiRandom))
    }
}

impl<T: GuestEnv> insecure::Host for WasiImpl<'_, T> {
    fn get_insecure_random_bytes(&mut self, len: u64) -> wasmtime::Result<Vec<u8>> {
        bytes(self.0, RandomStream::WasiInsecure, len)
    }

    fn get_insecure_random_u64(&mut self) -> wasmtime::Result<u64> {
        Ok(u64_from(self.0, RandomStream::WasiInsecure))
    }
}

impl<T: GuestEnv> insecure_seed::Host for WasiImpl<'_, T> {
    fn insecure_seed(&mut self) -> wasmtime::Result<(u64, u64)> {
        Ok(self.0.insecure_seed())
    }
}
