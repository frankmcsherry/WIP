#!/usr/bin/env python3
"""Prepare TPC-H data for `cargo bench --bench tpch`, and DuckDB's answers and times.

    python3 benches/tpch/prepare.py OUT_DIR [SCALE_FACTOR]     # default 0.2

Generates the tables with DuckDB's `tpch` extension and keeps only the columns the queries read,
as integers: decimals in hundredths (prices in cents, discounts and taxes in percent), quantities as
whole units, dates as days since 1970-01-01. The queries' SQL reads these integer tables, so DuckDB
and corgi compute the same integer expressions and their answers match exactly. Needs the `duckdb`
and `numpy` Python packages.

Writes to OUT_DIR:
- one file per column: `NAME.u64` (little-endian u64) for numbers, or `NAME.ends` (u64 row ends) and
  `NAME.bytes` for strings;
- `expected/QUERY.txt`: DuckDB's answer to each query in `algorithms/tpch/`, in the canonical form the
  bench prints corgi's in (one row per line, tab-separated, strings in hex);
- `duckdb.tsv`: DuckDB's best time per query, on one thread and on all of them, tables in memory.
"""
import os, re, sys, time
import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
QUERIES = os.path.join(HERE, '..', '..', 'algorithms', 'tpch')

DAYS = "({} - DATE '1970-01-01')"
CENTS = "CAST(round({} * 100) AS BIGINT)"

# per table, the columns kept: name -> (SQL over the generated table, kind: u number, s string)
TABLES = {
    'lineitem': {
        'l_orderkey': ('l_orderkey', 'u'), 'l_partkey': ('l_partkey', 'u'), 'l_suppkey': ('l_suppkey', 'u'),
        'l_quantity': ('CAST(l_quantity AS BIGINT)', 'u'), 'l_extendedprice': (CENTS.format('l_extendedprice'), 'u'),
        'l_discount': (CENTS.format('l_discount'), 'u'), 'l_tax': (CENTS.format('l_tax'), 'u'),
        'l_returnflag': ('l_returnflag', 's'), 'l_linestatus': ('l_linestatus', 's'),
        'l_shipdate': (DAYS.format('l_shipdate'), 'u'), 'l_commitdate': (DAYS.format('l_commitdate'), 'u'),
        'l_receiptdate': (DAYS.format('l_receiptdate'), 'u'), 'l_shipinstruct': ('l_shipinstruct', 's'),
        'l_shipmode': ('l_shipmode', 's'),
    },
    'orders': {
        'o_orderkey': ('o_orderkey', 'u'), 'o_custkey': ('o_custkey', 'u'), 'o_orderdate': (DAYS.format('o_orderdate'), 'u'),
        'o_orderpriority': ('o_orderpriority', 's'), 'o_shippriority': ('o_shippriority', 'u'),
    },
    'customer': {'c_custkey': ('c_custkey', 'u'), 'c_nationkey': ('c_nationkey', 'u'), 'c_mktsegment': ('c_mktsegment', 's')},
    'part': {
        'p_partkey': ('p_partkey', 'u'), 'p_brand': ('p_brand', 's'), 'p_type': ('p_type', 's'), 'p_size': ('p_size', 'u'),
        'p_container': ('p_container', 's'),
    },
    'supplier': {'s_suppkey': ('s_suppkey', 'u'), 's_nationkey': ('s_nationkey', 'u')},
    'nation': {'n_nationkey': ('n_nationkey', 'u'), 'n_name': ('n_name', 's'), 'n_regionkey': ('n_regionkey', 'u')},
    'region': {'r_regionkey': ('r_regionkey', 'u'), 'r_name': ('r_name', 's')},
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
    if kind == 'u':
        return str(int(v))
    if kind == 'f':
        return f'{float(v):.4f}'
    if kind == 's':
        return (v.encode() if isinstance(v, str) else bytes(v)).hex()
    raise ValueError(kind)


def canonical(kinds, rows):
    ks = kinds.strip('[]').split(',')
    return ''.join('\t'.join(field(k, v) for k, v in zip(ks, row)) + '\n' for row in rows)


def main():
    out = sys.argv[1]
    sf = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
    os.makedirs(os.path.join(out, 'expected'), exist_ok=True)
    db = duckdb.connect()
    db.sql('INSTALL tpch; LOAD tpch')
    db.sql('CREATE SCHEMA gen')
    db.sql(f"CALL dbgen(sf={sf}, schema='gen')")
    for table, cols in TABLES.items():
        db.sql(f"CREATE TABLE {table} AS SELECT {', '.join(f'{e} AS {c}' for c, (e, _) in cols.items())} FROM gen.{table}")
        print(table, db.sql(f'SELECT count(*) FROM {table}').fetchone()[0])
        data = db.sql(f'SELECT * FROM {table}').fetchnumpy()
        for c, (_, kind) in cols.items():
            v = data[c]
            if kind == 's':
                vals = [x.encode() if isinstance(x, str) else bytes(x) for x in v]
                np.cumsum([len(x) for x in vals], dtype=np.uint64).astype('<u8').tofile(os.path.join(out, c + '.ends'))
                open(os.path.join(out, c + '.bytes'), 'wb').write(b''.join(vals))
            else:
                v.astype(np.int64).astype(np.uint64).astype('<u8').tofile(os.path.join(out, c + '.u64'))
    db.sql('DROP SCHEMA gen CASCADE')
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
