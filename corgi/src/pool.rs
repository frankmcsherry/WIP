//! Reused leaf buffers. Nearly every op of a run reads its operand's buffer and writes a new one, so
//! a run allocates and frees a leaf buffer per op; from the system allocator a large one arrives as
//! fresh pages (a page fault and a zero-fill per page) and is handed back to the system when freed.
//! Here a leaf buffer freed during a run goes onto a free list (one list per storage) instead, and an
//! op takes its output buffer from the list, so a run in steady state allocates no leaf memory and
//! touches no fresh pages.
//!
//! The pool belongs to a [`crate::Program`], which installs it on the running thread for the length
//! of a run ([`run_with`]); it persists from run to run, so from block to block for a host that runs
//! a program a block at a time. Outside a run nothing is installed, and taking is a plain allocation
//! and giving a plain free: `eval_graph` called directly behaves as it always has.
//!
//! The hook is [`Buf`], the buffer a leaf holds: when the last reference to a leaf is dropped,
//! wherever in an op that happens, its `Drop` gives the `Vec` to the installed pool.

use std::cell::RefCell;
use std::sync::Arc;

/// a leaf's element type: each has its own free list.
pub trait Pooled: Copy + Default + 'static {
    /// this storage's free list in `pool`, and the count of buffers taken from it this run.
    #[doc(hidden)]
    fn parts(pool: &mut Pool) -> (&mut Vec<Vec<Self>>, &mut usize);
}

macro_rules! storages {
    ($($t:ty => $f:ident),+) => {
        /// the free lists, one per storage, and how many buffers of each the current run has taken.
        #[derive(Default)]
        pub struct Pool {
            $( $f: (Vec<Vec<$t>>, usize), )+
        }
        $( impl Pooled for $t {
            fn parts(pool: &mut Pool) -> (&mut Vec<Vec<$t>>, &mut usize) {
                let (list, takes) = &mut pool.$f;
                (list, takes)
            }
        } )+
        impl Pool {
            /// end of a run: keep at most as many buffers of each storage as the run took (the next
            /// run of the same program will take about as many), the largest first, and free the
            /// rest. Without this a pool would keep every buffer a host hands in, as each run's
            /// input, and that no op takes back out.
            fn trim(&mut self) {
                $( trim_list(&mut self.$f.0, std::mem::take(&mut self.$f.1)); )+
            }
        }
    };
}
storages!(u8 => bytes, i64 => ints, u64 => floats);

fn trim_list<T>(list: &mut Vec<Vec<T>>, keep: usize) {
    if list.len() > keep {
        list.sort_unstable_by_key(|v| std::cmp::Reverse((v.capacity(), v.len())));
        list.truncate(keep);
    }
}

/// the most buffers of one storage a pool holds, whatever a run gives back.
const MAX_HELD: usize = 64;

thread_local! {
    static ACTIVE: RefCell<Option<Pool>> = const { RefCell::new(None) };
}

/// run `f` with `pool` installed on this thread, so that leaf buffers freed during `f` go into it
/// and ops take their outputs from it; then trim it and put it back. A run nested in another (a host
/// kernel running a second program) installs its own pool and restores the outer one after.
pub(crate) fn run_with<R>(pool: &mut Pool, f: impl FnOnce() -> R) -> R {
    struct Restore<'a> {
        home: &'a mut Pool,
        outer: Option<Pool>,
    }
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            let outer = self.outer.take();
            if let Ok(Some(mut p)) = ACTIVE.try_with(|a| a.replace(outer)) {
                p.trim();
                *self.home = p;
            }
        }
    }
    let mine = std::mem::take(pool);
    let outer = ACTIVE.with(|a| a.replace(Some(mine)));
    let _restore = Restore { home: pool, outer };
    f()
}

/// `f` on the installed pool's list for `T` and its take count, if a pool is installed (and not
/// already in use further up this thread's stack).
fn with_list<T: Pooled, R>(f: impl FnOnce(&mut Vec<Vec<T>>, &mut usize) -> R) -> Option<R> {
    ACTIVE
        .try_with(|a| match a.try_borrow_mut() {
            Ok(mut a) => a.as_mut().map(|p| {
                let (list, takes) = T::parts(p);
                f(list, takes)
            }),
            Err(_) => None,
        })
        .ok()
        .flatten()
}

/// the pooled buffer with room for `n` that `rank` likes least, removed from the list. "Room" is a
/// capacity from `n` to about `2n`, so a small request never takes, and pins, a large buffer that the
/// next large request would want.
fn find<T: Pooled, K: Ord>(n: usize, rank: impl Fn(&Vec<T>) -> K) -> Option<Vec<T>> {
    with_list::<T, _>(|list, takes| {
        *takes += 1;
        let max = 2 * n + 64;
        let best = (0..list.len()).filter(|&i| (n..=max).contains(&list[i].capacity())).min_by_key(|&i| rank(&list[i]))?;
        Some(list.swap_remove(best))
    })
    .flatten()
}

/// an empty buffer with room for `n` elements: from the pool when one fits (the smallest, and of
/// those the one holding the fewest elements, leaving full ones for [`take_init`]), else a fresh
/// allocation.
pub(crate) fn take<T: Pooled>(n: usize) -> Vec<T> {
    if n == 0 {
        return Vec::new();
    }
    match find::<T, _>(n, |v| (v.capacity(), v.len())) {
        Some(mut v) => {
            v.clear();
            v
        }
        None => Vec::with_capacity(n),
    }
}

/// a buffer of exactly `n` initialized elements, for a kernel that writes slots in place rather than
/// appending (compress): the pool's buffer that already holds the most initialized elements, so
/// that only a tail it never had is zeroed; with no pool, a zeroed allocation (what `vec![0; n]`
/// gives). The elements' values are whatever the buffer last held.
pub(crate) fn take_init<T: Pooled>(n: usize) -> Vec<T> {
    if n == 0 {
        return Vec::new();
    }
    match find::<T, _>(n, |v| (std::cmp::Reverse(v.len().min(n)), v.capacity())) {
        Some(mut v) => {
            v.truncate(n);
            v.resize(n, T::default());
            v
        }
        None => vec![T::default(); n],
    }
}

/// a buffer holding exactly the elements `it` yields, written into a buffer from [`take`]. For an
/// iterator over a slice this is the same loop `collect` would run.
pub(crate) fn collect<T: Pooled>(it: impl ExactSizeIterator<Item = T>) -> Vec<T> {
    let mut v = take(it.len());
    v.extend(it);
    v
}

/// return a buffer to the installed pool (no pool, or a full one: free it).
fn give<T: Pooled>(v: Vec<T>) {
    if v.capacity() == 0 {
        return;
    }
    let mut v = Some(v);
    with_list::<T, _>(|list, _| {
        if list.len() < MAX_HELD {
            list.push(v.take().unwrap());
        }
    });
}

/// a leaf's buffer, from a `Vec`.
pub(crate) fn leaf<T: Pooled>(v: Vec<T>) -> Arc<Buf<T>> {
    Arc::new(Buf(v))
}

/// a leaf's buffer: a `Vec` that goes back to the running program's pool when the last reference to
/// its leaf is dropped. It reads and writes as the `Vec` it holds.
pub struct Buf<T: Pooled>(Vec<T>);

impl<T: Pooled> Buf<T> {
    /// the `Vec`, moved out (the leaf no longer owns a buffer to give back).
    pub(crate) fn into_vec(mut self) -> Vec<T> {
        std::mem::take(&mut self.0)
    }
}

impl<T: Pooled> Drop for Buf<T> {
    fn drop(&mut self) {
        give(std::mem::take(&mut self.0));
    }
}

impl<T: Pooled> std::ops::Deref for Buf<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Vec<T> {
        &self.0
    }
}

impl<T: Pooled> std::ops::DerefMut for Buf<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.0
    }
}

impl<T: Pooled> Clone for Buf<T> {
    fn clone(&self) -> Self {
        let mut v = take(self.0.len());
        v.extend_from_slice(&self.0);
        Buf(v)
    }
}

impl<T: Pooled + PartialEq> PartialEq for Buf<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<T: Pooled + Eq> Eq for Buf<T> {}

impl<T: Pooled + std::fmt::Debug> std::fmt::Debug for Buf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
