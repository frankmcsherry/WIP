# The survey, rank at a time

`corgi/src/ops/cmp/survey.rs`. The merge kernel walks two structurally sorted columns by the
same descent the sort makes, one level of the shape at a time over every class that level
leaves equal, instead of one structural comparison per step.

## Contract

    survey_groups(a, b) -> Vec<GroupRun>      // GroupRun::{A(lo, hi), B(lo, hi), Both(alo, ahi, blo, bhi)}
    survey(a, b) -> Vec<Run>                  // Run::{A(lo, hi), B(lo, hi), Both(ia, ib)}, the pairwise form

Requires `a` and `b` of one shape, each in structural order. Ensures, for `survey_groups`: the
`A` ranges and the `a` halves of the `Both` classes partition `0..a.len()` in order without gap
or overlap, likewise `b`; expanding the reports to their rows gives a non-decreasing structural
sequence; every row of a `Both` class on either side is structurally equal to every other, and
the class is maximal on both sides; adjacent reports never share a side. `survey` is the same
interleaving reported pair by pair: a class of `k` rows against `l` is `min(k, l)` matched pairs
and the excess joins the run that follows, which is what a two-pointer walk reports.

## Design

- **A run of leaf lanes is one level.** Consecutive fields that are leaves, units, or products
  of those, up to four lanes with nested products flattened, are merged as one level keyed by
  their tuple, which is what a structural comparison of those fields reads; `u64` lanes are read
  directly. The `(key, val)` column of an arrangement, `(u64, u64)` or `((u64, u64), u64)`, is
  one such level, and the pairwise `survey` walks it directly as it walks a leaf. This is PR #22's
  lane-wise gallop, subsumed.
- **The leading level gallops.** A leaf level merges each open class with the doubling-and-
  bisecting gallop of the old survey, reading keys through the level's index lists, one width
  dispatch per level. An equal class gallops past its duplicates on both sides. A bare leaf pair
  in `survey` takes the old pairwise walk directly and costs what it did.
- **Every level below refines all its classes at once.** A product's next field re-merges the
  classes the field before left equal, at the same rows. A sum merges on the tag as a virtual
  leaf, then each lane merges its classes at the carried within-lane offsets. A list merges on
  the length, then element by element over the classes still that long, each position as one
  level over index lists into the elements; a class at its length is equal throughout.
- **Reports are a tree.** An equal class holds its refinement as children; an empty refinement
  means equal throughout. A level's work is proportional to the classes it refines and the
  reports it makes, not to the reports already there, and the tree is flattened once at the end
  with adjacent runs of one side joined. A class keeps its offsets in the index lists of the
  level that made it, and a level that descends rewrites them to the lists it builds, so a
  sub-class's rows are always a sub-range of an ancestor's segment.

## Measurements (2026-09-06, M4, ns per row of the two inputs together, 1M rows each side)

A probe over sorted pairs of columns, master → here (PR #22, which handled only products of
`u64` leaves, in the middle column where it applies):

| shape | master | #22 | here |
|---|---|---|---|
| u64, 2M distinct values | 3.9 | 4.0 | 3.9 |
| (u64 % 200k, u64 % 1000) | 108.8 | 5.7 | 7.7 |
| (u64 % 4M, u64 % 1000) | 48.2 | 4.9 | 7.5 |
| (u64 % 100k, Sum of two u64 lanes % 1000) | 216.3 | | 26.4 |
| (u64 % 100k, List<u64> of 0..3 % 1000) | 142.5 | | 34.8 |
| Sum of two u64 lanes % 100k | 73.0 | | 5.6 |
| List<u64> of 0..3 % 1000 | 68.5 | | 16.1 |

Gap rows, `survey` against a typed Rust two-pointer over tuples, at 1M and 8M rows a side:
R4, the leaf over 256-row runs, 0.061 → 0.065 and 0.219 → 0.215; R11 `(u64, u64)` 38.5 → 1.84
and 38.6 → 1.78 (Rust 1.54 / 1.36), at the ceiling; R12 `(u64, Sum of two u64 lanes)` 77.5 → 24.8
and 78.2 → 15.7 (Rust 1.4 / 1.3). What #22 still has over this on a product of leaves is that it
reads the second lane only when the first ties, where the tuple key reads every lane per step;
a comparator form of `merge` would close that. What the structured shapes still pay, an order
of magnitude over the typed walk, is per report: the tree node, the flatten, and the pairwise
expansion `survey` adds on top of the groups. DDIR built against this branch passes its tests.

## Follow-ups

1. DDIR's chunk merge on `survey_groups`: exclusive runs copied by range, the `Both` classes
   consolidated once each, retiring the per-row `compare_at` that is 7–13% of an arrangement's
   initial epoch and the merge's 22–24% around it.
2. Range copies for that merge, corgi's `extend_from_self`: the values of the merged column
   assembled from ranges of the inputs.
3. `find` for sorted needles as the one-sided reading of this kernel, retiring `batched_bound`'s
   allocation per round on structured needles.
4. The tie vectors of `compare_pairs` through a scratch, and its `List` arm position by position.
5. A comparator form of `merge`, reading a run's later lanes only on ties, for the last 30% on
   products of leaves.
