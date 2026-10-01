//! Word-backed storage: a buffer of `u64` words read and written as a slice of narrower unsigned
//! integers. The one place corgi reinterprets memory.
//!
//! Why words: a column stored in `u64` words can be viewed at any of the four widths, so one
//! allocation can hold a column at whatever width its values need, can be narrowed in place, and
//! can be a window of a larger word buffer (a decoded message) without copying out of it. The
//! reverse is not true: a `Vec<u8>` cannot be viewed as `u64`s, since its allocation is only
//! 1-aligned. (`dev/integers.md` measures what this buys and what reading words without a cast
//! would cost instead.)
//!
//! The views are `bytemuck::cast_slice`/`cast_slice_mut` from `u64` to a narrower unsigned lane:
//! a `u64` buffer is 8-aligned, `n` words are exactly `8n` bytes, and every bit pattern is a valid
//! lane, so the cast always succeeds. corgi itself has no `unsafe`.
//!
//! Which `T` lands in which word depends on the target's byte order. Within one process that is
//! invisible (a column is always read at the width it was written); the codec's wire format is
//! little-endian, so viewing a received buffer in place is little-endian only (see `bytes.rs`).

mod sealed {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
}

/// An unsigned lane that word-backed storage can be viewed at: `u8`, `u16`, `u32` or `u64`.
pub(crate) trait Lane: sealed::Sealed + bytemuck::Pod + Ord + Default + std::fmt::Debug + Into<u64> + 'static {
    /// the lane's width in bits.
    const BITS: u32;
    /// `x` truncated to the lane (callers only pass values that fit).
    fn from_u64(x: u64) -> Self;
}

macro_rules! lane {
    ($($t:ty),*) => {$(
        impl Lane for $t {
            const BITS: u32 = <$t>::BITS;
            #[inline(always)]
            fn from_u64(x: u64) -> Self { x as $t }
        }
    )*};
}
lane!(u8, u16, u32, u64);

/// `words` read as `T`s: `8 * words.len() / size_of::<T>()` of them.
#[inline]
pub(crate) fn view<T: Lane>(words: &[u64]) -> &[T] {
    bytemuck::cast_slice(words)
}

/// `words` written as `T`s.
#[inline]
pub(crate) fn view_mut<T: Lane>(words: &mut [u64]) -> &mut [T] {
    bytemuck::cast_slice_mut(words)
}

/// how many words hold `n` lanes of `bits` bits (0 for `bits == 0`: nothing is stored).
#[inline]
pub(crate) fn words_for(n: usize, bits: u32) -> usize {
    if bits == 0 { 0 } else { (n * bits as usize).div_ceil(64) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A view reads back what the narrow writes put down, at every width, including a trailing
    /// partial word; and a write through one view is a write to the words.
    #[test]
    fn views_round_trip() {
        for n in [0usize, 1, 7, 8, 9, 33] {
            let mut w = vec![0u64; words_for(n, 8)];
            view_mut::<u8>(&mut w)[..n].iter_mut().enumerate().for_each(|(i, x)| *x = i as u8);
            assert_eq!(&view::<u8>(&w)[..n], &(0..n).map(|i| i as u8).collect::<Vec<_>>()[..]);

            let mut w = vec![0u64; words_for(n, 16)];
            view_mut::<u16>(&mut w)[..n].iter_mut().enumerate().for_each(|(i, x)| *x = 1000 + i as u16);
            assert_eq!(&view::<u16>(&w)[..n], &(0..n).map(|i| 1000 + i as u16).collect::<Vec<_>>()[..]);

            let mut w = vec![0u64; words_for(n, 32)];
            view_mut::<u32>(&mut w)[..n].iter_mut().enumerate().for_each(|(i, x)| *x = u32::MAX - i as u32);
            assert_eq!(&view::<u32>(&w)[..n], &(0..n).map(|i| u32::MAX - i as u32).collect::<Vec<_>>()[..]);

            let w: Vec<u64> = (0..n as u64).collect();
            assert_eq!(view::<u64>(&w), &w[..]);
        }
    }

    /// The view length is the whole buffer at the lane width, never more.
    #[test]
    fn view_lengths() {
        let w = vec![0u64; 3];
        assert_eq!(view::<u8>(&w).len(), 24);
        assert_eq!(view::<u16>(&w).len(), 12);
        assert_eq!(view::<u32>(&w).len(), 6);
        assert_eq!(view::<u64>(&w).len(), 3);
        assert_eq!(words_for(9, 8), 2);
        assert_eq!(words_for(9, 0), 0);
    }
}
