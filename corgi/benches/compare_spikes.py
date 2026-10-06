#!/usr/bin/env python3
"""Compare this spike with a checkout of origin/corgi-int-spike, without edits.

Usage: python3 corgi/benches/compare_spikes.py OTHER_WORKTREE [--rows 1048576]
Builds separate temporary harnesses because both path dependencies are named
corgi 0.1.0. Prints CSV to stdout; build logs go to stderr. No new dependencies.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("other_worktree", type=Path)
    parser.add_argument("--rows", type=int, default=1048576)
    parser.add_argument("--cargo", default=shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo"))
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be positive")
    here = Path(__file__).resolve().parent
    other = args.other_worktree.resolve() / "corgi"
    if not (other / "Cargo.toml").is_file():
        parser.error("other_worktree must contain corgi/Cargo.toml")
    print("spike,case,rows,status,input_payload_bytes,output_payload_bytes,output_width,median_ns_per_row", flush=True)
    with tempfile.TemporaryDirectory(prefix="corgi-compare-") as temporary:
        for adapter, dependency in [("preparation", here.parent), ("frame", other)]:
            harness = Path(temporary) / adapter
            (harness / "src").mkdir(parents=True)
            # JSON string escaping is valid TOML basic string escaping here.
            (harness / "Cargo.toml").write_text(
                '[package]\nname = "integer-comparison"\nversion = "0.0.0"\nedition = "2021"\n'
                f'[dependencies]\ncorgi = {{ path = {json.dumps(str(dependency))} }}\n'
            )
            shutil.copyfile(here / "comparison/main.rs.in", harness / "src/main.rs")
            shutil.copyfile(here / f"comparison/{adapter}.rs.in", harness / "src/adapter.rs")
            # Do not allow an inherited shared target directory to mix adapters.
            env = dict(os.environ, CARGO_TARGET_DIR=str(harness / "target"))
            subprocess.run([args.cargo, "run", "--release", "--manifest-path", str(harness / "Cargo.toml"), "--", str(args.rows)], env=env, check=True)


if __name__ == "__main__":
    main()
