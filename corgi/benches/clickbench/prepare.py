#!/usr/bin/env python3
"""Prepare ClickBench data for `cargo bench --bench clickbench`, and DuckDB's answers and times.

    python3 benches/clickbench/prepare.py OUT_DIR hits_0.parquet [hits_1.parquet ...]

The partitions are ClickBench's own (https://datasets.clickhouse.com/hits_compatible/athena_partitioned/
hits_N.parquet, a million rows each). Needs the `duckdb` and `numpy` Python packages.

Writes to OUT_DIR:
- one file per column the queries read: `NAME.u64` (little-endian u64 values) for numbers, or
  `NAME.ends` (u64 row ends) and `NAME.bytes` for strings. Columns that can be negative are stored in
  corgi's signed encoding (the sign bit flipped), so corgi's order is the signed order.
- `expected/QUERY.txt`: DuckDB's answer to each query in `algorithms/clickbench/`, in the canonical
  form the bench prints corgi's in (one row per line, tab-separated, strings in hex).
- `duckdb.tsv`: DuckDB's best time per query, on one thread and on all of them, the table in memory.
"""
import os, re, sys, time
import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
QUERIES = os.path.join(HERE, '..', '..', 'algorithms', 'clickbench')

# the columns the queries read, and how each is stored: u (unsigned), i (signed), s (string)
COLUMNS = {
    'WatchID': 'i', 'Title': 's', 'EventTime': 'u', 'EventDate': 'u', 'CounterID': 'u', 'ClientIP': 'i',
    'RegionID': 'u', 'UserID': 'i', 'URL': 's', 'Referer': 's', 'IsRefresh': 'u', 'ResolutionWidth': 'u',
    'MobilePhone': 'u', 'MobilePhoneModel': 's', 'TraficSourceID': 'i', 'SearchEngineID': 'u',
    'SearchPhrase': 's', 'AdvEngineID': 'u', 'IsLink': 'u', 'IsDownload': 'u', 'DontCountHits': 'u',
    'URLHash': 'i', 'RefererHash': 'i', 'WindowClientWidth': 'u', 'WindowClientHeight': 'u',
}


def queries():
    """(name, sql, output kinds) for every query file: the `# sql:` and `# output:` header lines."""
    out = []
    for f in sorted(os.listdir(QUERIES)):
        if not f.endswith('.col'):
            continue
        head = open(os.path.join(QUERIES, f)).read()
        sql = ' '.join(m.strip() for m in re.findall(r'^# sql:(.*)$', head, re.M))
        kinds = re.search(r'^# output: *(\S+)', head, re.M).group(1)
        out.append((f[:-4], sql, kinds))
    return out


def field(kind, v):
    if kind in 'ui':
        return str(int(v))
    if kind == 'f':
        return f'{float(v):.4f}'
    if kind == 's':
        return bytes(v).hex()
    raise ValueError(kind)


def canonical(kinds, rows):
    ks = kinds.strip('[]').split(',')
    return ''.join('\t'.join(field(k, v) for k, v in zip(ks, row)) + '\n' for row in rows)


def main():
    out, files = sys.argv[1], sys.argv[2:]
    os.makedirs(os.path.join(out, 'expected'), exist_ok=True)
    db = duckdb.connect()
    db.sql(f"CREATE TABLE hits AS SELECT {', '.join(COLUMNS)} FROM read_parquet({files!r})")
    n = db.sql('SELECT count(*) FROM hits').fetchone()[0]
    print(f'{n} rows')
    cols = db.sql(f"SELECT {', '.join(COLUMNS)} FROM hits").fetchnumpy()
    for name, kind in COLUMNS.items():
        v = cols[name]
        if kind == 's':
            vals = [bytes(x) for x in v]
            np.cumsum([len(x) for x in vals], dtype=np.uint64).astype('<u8').tofile(os.path.join(out, name + '.ends'))
            open(os.path.join(out, name + '.bytes'), 'wb').write(b''.join(vals))
        elif kind == 'i':
            (v.astype(np.int64).view(np.uint64) ^ np.uint64(1 << 63)).astype('<u8').tofile(os.path.join(out, name + '.u64'))
        else:
            v.astype(np.int64).astype(np.uint64).astype('<u8').tofile(os.path.join(out, name + '.u64'))
    times = []
    for name, sql, kinds in queries():
        open(os.path.join(out, 'expected', name + '.txt'), 'w').write(canonical(kinds, db.sql(sql).fetchall()))
        row = [name]
        for threads in (1, os.cpu_count()):
            db.sql(f'SET threads = {threads}')
            best = float('inf')
            for _ in range(5):
                t = time.perf_counter()
                db.sql(sql).fetchall()
                best = min(best, time.perf_counter() - t)
            row.append(f'{best * 1e3:.2f}')
        times.append('\t'.join(row))
        print(times[-1])
    open(os.path.join(out, 'duckdb.tsv'), 'w').write('query\tone_thread_ms\tall_threads_ms\n' + '\n'.join(times) + '\n')


if __name__ == '__main__':
    main()
