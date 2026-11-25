#!/bin/bash
# run clickbench under each list form, twice, and print best-of per query with ratios to offsets
# usage: dev/lists-spike/run.sh TAG [queries...]
set -e
cd "$(dirname "$0")/../.."
export PATH="$HOME/.cargo/bin:$PATH" CORGI_CLICKBENCH=$HOME/Projects/datasets/clickbench/data
tag=$1; shift
cargo bench -q --bench clickbench --no-run 2>/dev/null
for f in offsets lens unary; do
  for rep in 1 2; do CORGI_LISTS=$f cargo bench -q --bench clickbench -- "$@" > dev/lists-spike/$tag-$f-$rep.txt 2>&1; done
done
cd dev/lists-spike
grep -h DIFF $tag-*.txt || true
python3 - "$tag" <<'PY'
import sys
tag=sys.argv[1]
def load(f):
    d={}
    for r in (1,2):
        for line in open(f"{tag}-{f}-{r}.txt").read().splitlines()[1:]:
            w=line.split()
            if len(w)>=2:
                try: d[w[0]]=min(d.get(w[0],1e18), float(w[1]))
                except: pass
    return d
o,l,u=load('offsets'),load('lens'),load('unary')
print(f"{'query':6} {'offsets':>9} {'lens':>9} {'unary':>9}  {'lens/off':>8} {'unary/off':>9}")
for q in o:
    if o[q] < 0.05: continue
    print(f"{q:6} {o[q]:9.2f} {l[q]:9.2f} {u[q]:9.2f}  {l[q]/o[q]:8.2f} {u[q]/o[q]:9.2f}")
PY
