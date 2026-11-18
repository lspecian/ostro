//! The `GuestEnv` seam: everything the deterministic WASI shim (plan 03-04) needs from
//! the host. `HostState` (plan 03-07) implements it from the replay core.

/// Source of real wall-clock time (used only to stamp new journal events).
pub trait Clock: Send + Sync {
    fn now_unix_nanos(&self) -> u64;
}

/// [`Clock`] backed by the system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_nanos(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// Independent deterministic random streams derived from the run seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RandomStream {
    CtxRandom = 1,
    Uuid = 2,
    WasiRandom = 3,
    WasiInsecure = 4,
}

/// Guest stdio stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdStream {
    Stdout,
    Stderr,
}

/// Deterministic environment exposed to the guest through the WASI shim.
pub trait GuestEnv: Send + 'static {
    /// Resource table of the store.
    fn table(&mut self) -> &mut wasmtime::component::ResourceTable;
    /// Deterministic wall clock `(seconds, nanoseconds)`.
    fn wall_now(&self) -> (u64, u32);
    /// Deterministic monotonic clock in nanoseconds.
    fn monotonic_now_ns(&self) -> u64;
    /// Fill `buf` from the given deterministic random stream.
    fn fill_random(&mut self, stream: RandomStream, buf: &mut [u8]);
    /// Seed for `wasi:random/insecure-seed`.
    fn insecure_seed(&self) -> (u64, u64);
    /// Write guest stdio (suppressed on replay by the implementor).
    fn write_stdio(&mut self, stream: StdStream, bytes: &[u8]);
    /// Remaining stdio byte budget for `stream`.
    fn stdio_budget(&self, stream: StdStream) -> u64;
}
