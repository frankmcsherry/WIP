#!/usr/bin/env bash
# A/B-compare the graph eval path vs legacy eval on a set of examples.
# Three runs each. Outputs the in-program `time` phase markers per run.
#
# Usage: ./scripts/bench_ab.sh [example1.col example2.col ...]
# Default set: 14_q1, 17, 18, 19.

set -u
BIN=./target/release/collie

if [ "$#" -eq 0 ]; then
    examples=(
        examples/14_q1_aggregation.col
        examples/17_wco_list_intersect.col
        examples/18_wco_lftj_idiomatic.col
        examples/19_wco_lftj_def.col
    )
else
    examples=("$@")
fi

for ex in "${examples[@]}"; do
    name=$(basename "$ex" .col)
    echo "================ $name ================"
    for path in legacy graph; do
        flag=""
        [ "$path" = "legacy" ] && flag="--legacy"
        echo "  --- $path ---"
        for i in 1 2 3; do
            t=$( { time "$BIN" $flag "$ex" >/dev/null; } 2>&1 | grep real | awk '{print $2}')
            phases=$("$BIN" $flag "$ex" 2>&1 | grep "^time:" | awk '{print $2 $3}' | tr '\n' ' ')
            printf "    run %d: wall=%-8s phases=%s\n" "$i" "$t" "$phases"
        done
    done
done
