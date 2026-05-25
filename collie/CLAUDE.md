# collie — session orientation

If you're starting a session here, **read `dev/BACKLOG.md` first.**
It's the truth about what's pending, what's prioritized, and what was
just done. Anything you're about to start should already be there (or
get added before you start).

## File map

Consumer-facing (repo root):

| File | Role |
|---|---|
| `README.md` | Top-level overview, layout, install/run. |
| `PRINCIPLES.md` | The three design principles + the trades they make. |
| `BAKEOFF.md` | Cross-language comparison + perf numbers. |

Workshop notes (`dev/`):

| File | Role |
|---|---|
| `dev/BACKLOG.md` | **Start here.** Pending, prioritized, status. |
| `dev/FOLLOWUPS.md` | Long-form architectural sketches. Linked from BACKLOG items as `FOLLOWUPS §X`. |
| `dev/SURFACE.md` | Design proposal for the eventual surface IR. |
| `dev/ONBOARDING.md` | Older orientation tour. Use BACKLOG for current state; tour for the "reading order" still applies. |

## Project layout

```
src/
  ir/         language definition (value, stack, op, shape, typecheck, profile)
  ops/        operators (each = PrimOp + Typed)
  syntax/     parser + registry
  tools/      binary-only utilities (REPL, bench, pretty, serialize, demos)
  lib.rs      library entry
  main.rs    binary entry (CLI)
examples/     19 .col files — curated tour from basics to WCO triangle
dev/          workshop notes (BACKLOG, FOLLOWUPS, SURFACE, ONBOARDING)
```

Library is published via `lib.rs` and importable as `use collie::...`.
The `tools/` subdir is binary-only (not part of the public API).

## Working conventions

**Read first, then decide.** When opening a session, read `BACKLOG.md`'s
Next list. The top item is usually what to start with unless the user
redirects.

**Update BACKLOG as you go.** Items move Next → Active → Done in the
file. The task tracker (system-level, persistent) is the mirror; both
should agree at session end.

**Commit messages, not BACKLOG, are the history.** BACKLOG's "Done
recently" section only keeps the last ~10. Older context lives in
commit messages and FOLLOWUPS.

**Verify before recommending.** When `BACKLOG` or `FOLLOWUPS` names a
specific file, function, or task #, check it still exists / makes sense
before acting. These files do drift over time even when the discipline
is good.

**Run the full suite when you change anything in `src/`:**

```
PATH="$HOME/.cargo/bin:$PATH" cargo test --release
PATH="$HOME/.cargo/bin:$PATH" ./target/release/collie examples
```

83 unit tests + 19 examples should pass. If something breaks, that's
a real regression — find and fix.

**Bench when perf is the point.** `./target/release/collie bench` for
the microbench suite. For workload-level perf (WCO triangle, Q1
aggregation), `time ./target/release/collie examples/17_wco_list_intersect.col`
and the `time` ops inside each example bracket their phases.

## Quick references

- **The three principles** (flat-data + positions, whole-collection
  dispatch, layered typing) are in `PRINCIPLES.md`.
- **The four-layer architecture** (Layer 1: core IR, Layer 2: ops,
  Layer 3: types, Layer 4: syntax) is in `README.md` and visible in
  the `src/` directory structure.
- **`>name` and `def` syntax** — see `dev/SURFACE.md` and the
  later WCO examples (`18`, `19` use both heavily).
- **Positions idiom** — surveying ops return positions; `gather`
  materializes. See `PRINCIPLES.md` (Idioms section) and
  `dev/FOLLOWUPS.md` §0a for the original audit.
- **Sequences-by-default convention** — `dev/FOLLOWUPS.md` §−1;
  every op signature is `SEQ<T0> → SEQ<T1>`.

## When in doubt

Ask. The task tracker is the system memory; `dev/BACKLOG.md` is the
human-readable view of it; `dev/FOLLOWUPS.md` is the design rationale.
If those three disagree, the user is the tiebreaker.
