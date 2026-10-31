//! Canonical one-shot foldhash 0.2.0 quality, with a caller-supplied fixed seed.
//!
//! Altered from Orson Peters' foldhash: safe loads, always little endian, always full u128
//! products and 64-bit rotations. Upstream deliberately allows platform/version differences;
//! corgi's content ids do not. Keep this algorithm and its constants pinned.
//! See foldhash-LICENSE for the original license.

const SEEDS: [u64; 6] = [
    0xc0ac29b7c97c50dd, 0x3f84d5b5b5470917, 0x9216d5d98979fb1b,
    0xd1310ba698dfb5ac, 0x2ffd72dbd01adfb7, 0xb8e1afed6a267e96,
];

fn folded(x: u64, y: u64) -> u64 {
    let p = u128::from(x) * u128::from(y);
    p as u64 ^ (p >> 64) as u64
}

fn load(v: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(v[at..at + 8].try_into().unwrap())
}

pub(super) fn hash(mut v: &[u8], seed: u64) -> u64 {
    let mut a = seed.rotate_right(v.len() as u32);
    let mut b = SEEDS[1];
    if v.len() <= 16 {
        let n = v.len();
        if n >= 8 {
            a ^= load(v, 0);
            b ^= load(v, n - 8);
        } else if n >= 4 {
            a ^= u32::from_le_bytes(v[..4].try_into().unwrap()) as u64;
            b ^= u32::from_le_bytes(v[n - 4..].try_into().unwrap()) as u64;
        } else if n > 0 {
            a ^= v[0] as u64;
            b ^= ((v[n - 1] as u64) << 8) | v[n / 2] as u64;
        }
        a = folded(a, b);
    } else {
        b = a.wrapping_add(SEEDS[1]);
        if v.len() > 128 {
            let mut c = a.wrapping_add(SEEDS[2]);
            let mut d = a.wrapping_add(SEEDS[3]);
            if v.len() > 256 {
                let mut e = a.wrapping_add(SEEDS[4]);
                let mut f = a.wrapping_add(SEEDS[5]);
                loop {
                    a = folded(load(v, 0) ^ a, load(v, 48) ^ SEEDS[0]);
                    b = folded(load(v, 8) ^ b, load(v, 56) ^ SEEDS[0]);
                    c = folded(load(v, 16) ^ c, load(v, 64) ^ SEEDS[0]);
                    d = folded(load(v, 24) ^ d, load(v, 72) ^ SEEDS[0]);
                    e = folded(load(v, 32) ^ e, load(v, 80) ^ SEEDS[0]);
                    f = folded(load(v, 40) ^ f, load(v, 88) ^ SEEDS[0]);
                    v = &v[96..];
                    if v.len() <= 256 { break; }
                }
                a ^= e;
                b ^= f;
            }
            loop {
                a = folded(load(v, 0) ^ a, load(v, 32) ^ SEEDS[0]);
                b = folded(load(v, 8) ^ b, load(v, 40) ^ SEEDS[0]);
                c = folded(load(v, 16) ^ c, load(v, 48) ^ SEEDS[0]);
                d = folded(load(v, 24) ^ d, load(v, 56) ^ SEEDS[0]);
                v = &v[64..];
                if v.len() <= 128 { break; }
            }
            a ^= c;
            b ^= d;
        }
        let n = v.len();
        a = folded(load(v, 0) ^ a, load(v, n - 16) ^ SEEDS[0]);
        b = folded(load(v, 8) ^ b, load(v, n - 8) ^ SEEDS[0]);
        for (threshold, offset) in [(32, 16), (64, 32), (96, 48)] {
            if n >= threshold {
                a = folded(load(v, offset) ^ a, load(v, n - offset - 16) ^ SEEDS[0]);
                b = folded(load(v, offset + 8) ^ b, load(v, n - offset - 8) ^ SEEDS[0]);
            }
        }
        a ^= b;
    }
    // The extra multiply distinguishes the quality variant from the fast variant.
    folded(a, 0x243f6a8885a308d3)
}

// Upstream uses different arithmetic on some targets; the golden test remains universal.
#[cfg(all(test, target_endian = "little", target_pointer_width = "64",
          not(any(target_arch = "sparc64", target_arch = "wasm64"))))]
mod tests {
    use std::hash::Hasher;

    #[test]
    fn matches_upstream_quality_at_every_length_boundary() {
        for seed in [0, super::super::LIST, 0x452821e638d01377, u64::MAX] {
            for n in 0..=2048 {
                let bytes: Vec<u8> = (0..n).map(|i| (i * 137 + n * 31) as u8).collect();
                let mut h = foldhash::quality::FoldHasher::with_seed(seed, foldhash::SharedSeed::global_fixed());
                h.write(&bytes);
                assert_eq!(super::hash(&bytes, seed), h.finish(), "seed {seed}, len {n}");
            }
        }
    }
}
