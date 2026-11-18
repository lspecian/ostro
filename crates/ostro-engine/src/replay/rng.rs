//! Per-run deterministic randomness (stub; implemented in the GREEN step).

use ostro_types::journal::RngSeed;

use crate::env::RandomStream;

/// One ChaCha20 keystream.
pub struct ChaChaStream {
    _p: (),
}

#[allow(unused_variables)]
impl ChaChaStream {
    pub fn new(seed: &RngSeed, stream: RandomStream) -> Self {
        todo!()
    }
    pub fn fill(&mut self, buf: &mut [u8]) {
        todo!()
    }
    pub fn next_u64(&mut self) -> u64 {
        todo!()
    }
}

/// The four independent per-run streams.
pub struct RngStreams {
    _p: (),
}

#[allow(unused_variables)]
impl RngStreams {
    pub fn new(seed: &RngSeed) -> Self {
        todo!()
    }
    pub fn stream(&mut self, stream: RandomStream) -> &mut ChaChaStream {
        todo!()
    }
}

#[allow(unused_variables)]
pub fn insecure_seed(seed: &RngSeed) -> (u64, u64) {
    todo!()
}

#[allow(unused_variables)]
pub fn uuid_v7(now_ms: u64, stream: &mut ChaChaStream) -> String {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: RngSeed = RngSeed([7; 32]);

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn first32(stream: RandomStream) -> String {
        let mut s = ChaChaStream::new(&SEED, stream);
        let mut buf = [0u8; 32];
        s.fill(&mut buf);
        hex(&buf)
    }

    /// Golden vectors computed independently (Python `cryptography` ChaCha20, RFC 8439,
    /// counter 0, nonce = stream u32 LE || 8 zero bytes, key = seed).
    #[test]
    fn golden_rng_streams() {
        let golden = [
            (
                RandomStream::CtxRandom,
                "bb159a6778b48f51a65b4e00b861f924800670f84587e74016fd243a28198228",
            ),
            (
                RandomStream::Uuid,
                "819fca27a63803aa2ec0a274aa2386eacc710073183b94e71e48b9a3d49f963d",
            ),
            (
                RandomStream::WasiRandom,
                "b3c7ed0cbafb6cbb9c13bd78769f48f2f48405ea8678b307e544e3a9a78cb7f7",
            ),
            (
                RandomStream::WasiInsecure,
                "aa767aef297b6314f4517ef424bd3c49d90269b8f9f769c196d5a62de8d417c6",
            ),
        ];
        for (stream, want) in golden {
            assert_eq!(first32(stream), want, "{stream:?}");
        }
        let all: std::collections::HashSet<String> =
            golden.iter().map(|(s, _)| first32(*s)).collect();
        assert_eq!(all.len(), 4, "streams must be independent");
    }

    #[test]
    fn golden_stream_position_is_kept() {
        let mut a = ChaChaStream::new(&SEED, RandomStream::CtxRandom);
        let mut x = [0u8; 13];
        let mut y = [0u8; 19];
        a.fill(&mut x);
        a.fill(&mut y);
        let joined = [x.as_slice(), y.as_slice()].concat();
        assert_eq!(hex(&joined), first32(RandomStream::CtxRandom));

        let mut b = ChaChaStream::new(&SEED, RandomStream::CtxRandom);
        assert_eq!(
            b.next_u64(),
            u64::from_le_bytes([0xbb, 0x15, 0x9a, 0x67, 0x78, 0xb4, 0x8f, 0x51])
        );
        assert_eq!(
            b.next_u64(),
            u64::from_le_bytes([0xa6, 0x5b, 0x4e, 0x00, 0xb8, 0x61, 0xf9, 0x24])
        );
    }

    #[test]
    fn golden_streams_via_rng_streams() {
        let mut r = RngStreams::new(&SEED);
        let mut buf = [0u8; 32];
        r.stream(RandomStream::WasiRandom).fill(&mut buf);
        assert_eq!(hex(&buf), first32(RandomStream::WasiRandom));
        // Drawing from one stream does not move another.
        r.stream(RandomStream::Uuid).fill(&mut buf);
        assert_eq!(hex(&buf), first32(RandomStream::Uuid));
    }

    /// sha256(b"ostro/insecure-seed/v1" || seed)[..16] as two LE u64 (Python hashlib).
    #[test]
    fn golden_insecure_seed() {
        assert_eq!(
            insecure_seed(&SEED),
            (845_902_734_363_064_856, 2_093_221_784_760_027_366)
        );
    }

    #[test]
    fn golden_uuid_v7() {
        let mut s = ChaChaStream::new(&SEED, RandomStream::Uuid);
        let u = uuid_v7(1_700_000_000_000, &mut s);
        assert_eq!(u, "018bcfe5-6800-719f-8a27-a63803aa2ec0");
        // 48-bit ms prefix, version 7, RFC 9562 variant.
        assert_eq!(&u[..8], "018bcfe5");
        assert_eq!(&u[14..15], "7");
        assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"));
        // The next uuid draws fresh bits.
        let v = uuid_v7(1_700_000_000_000, &mut s);
        assert_ne!(u, v);
        assert_eq!(&v[..13], &u[..13]);
    }
}
