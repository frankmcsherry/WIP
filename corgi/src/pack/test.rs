use super::*;
use crate::engine::{clone_ref, gather};

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

/// Row ends of `rows` rows, each of up to `most` elements.
fn ends(next: &mut dyn FnMut() -> u64, rows: usize, most: u64) -> Bounds {
    let mut end = 0;
    Bounds::offsets((0..rows).map(|_| { end += (next() % (most + 1)) as usize; end }).collect())
}

/// Lists of letters, held as bytes, over `bounds`.
fn text(next: &mut dyn FnMut() -> u64, bounds: Bounds) -> Value {
    let total = bounds.total();
    Value::List(bounds, Box::new(Value::u8((0..total).map(|_| b'a' + (next() % 26) as u8).collect())))
}

/// A column of `n` rows with a field of every shape and leaf storage.
///
/// The fields: edge integers; a constant; integers whose high halves vary; random integers; bytes; text, some empty; edge floats; lists of lists, some empty; lists of one width; lists of no elements; a sum with a variant no row carries; a sum of one variant throughout; a sum of a sum; lists of sums; units; a fieldless product; references to rows of text, repeated and out of order; references to rows of pairs; and an identifier and a compound key, as an arrangement's hashed keys are.
fn column(n: usize, seed: &mut u64) -> Value {
    let mut next = || xorshift(seed);
    let edges = [i64::MIN, i64::MAX, 0, -1, 1, i64::MIN + 1, i64::MAX - 1];
    let ints = |next: &mut dyn FnMut() -> u64, f: &dyn Fn(u64) -> i64| Value::i64((0..n).map(|_| f(next())).collect());
    let floats = [
        0.0,
        -0.0,
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0xFFF8_0000_0000_0001),
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        1.5,
        -2.25,
    ];

    let edge = ints(&mut next, &|x| edges[x as usize % edges.len()]);
    let constant = ints(&mut next, &|_| 7);
    let halves = ints(&mut next, &|x| (((x % 3) as i64 - 1) << 32) | (x >> 40) as i64);
    let random = ints(&mut next, &|x| x as i64);
    let bytes = Value::u8((0..n).map(|_| next() as u8).collect());
    let word_ends = ends(&mut next, n, 12);
    let words = text(&mut next, word_ends);
    let float = Value::f64((0..n).map(|_| floats[next() as usize % floats.len()]).collect());
    let outer = ends(&mut next, n, 3);
    let inner = ends(&mut next, outer.total(), 2);
    let inner_total = inner.total();
    let nested = Value::List(outer, Box::new(Value::List(inner, Box::new(Value::i64((0..inner_total).map(|_| next() as i64 >> 50).collect())))));
    let stride = Value::List(Bounds::Stride(3, n), Box::new(Value::i64((0..3 * n).map(|_| (next() % 9) as i64).collect())));
    let hollow = Value::List(Bounds::Stride(0, n), Box::new(Value::f64(Vec::new())));
    let tags: Vec<usize> = (0..n).map(|_| (next() % 2) as usize).collect();
    let carried = tags.iter().filter(|&&t| t == 1).count();
    let carried_ends = ends(&mut next, carried, 4);
    let sum = Value::sum(tags, vec![
        Value::i64((0..n - carried).map(|_| next() as i64).collect()),
        text(&mut next, carried_ends),
        Value::Unit(0),
    ]);
    let one_variant = Value::sum(vec![1; n], vec![Value::Unit(0), Value::i64((0..n).map(|_| (next() % 3) as i64).collect())]);
    let outer_tags: Vec<usize> = (0..n).map(|_| (next() % 3).min(1) as usize).collect();
    let in_first = outer_tags.iter().filter(|&&t| t == 0).count();
    let inner_tags: Vec<usize> = (0..in_first).map(|_| (next() % 2) as usize).collect();
    let in_ints = inner_tags.iter().filter(|&&t| t == 0).count();
    let in_bytes_ends = ends(&mut next, n - in_first, 3);
    let sum_of_sums = Value::sum(outer_tags, vec![
        Value::sum(inner_tags, vec![Value::u8((0..in_ints).map(|_| next() as u8).collect()), Value::Unit(in_first - in_ints)]),
        text(&mut next, in_bytes_ends),
    ]);
    let listed = ends(&mut next, n, 3);
    let listed_total = listed.total();
    let listed_tags: Vec<usize> = (0..listed_total).map(|_| (next() % 2) as usize).collect();
    let listed_ints = listed_tags.iter().filter(|&&t| t == 0).count();
    let sums = Value::List(listed, Box::new(Value::sum(listed_tags, vec![
        Value::i64((0..listed_ints).map(|_| next() as i64 % 100).collect()),
        Value::Unit(listed_total - listed_ints),
    ])));
    let arena_rows = 1 + n / 2;
    let arena_ends = ends(&mut next, arena_rows, 5);
    let arena = text(&mut next, arena_ends);
    let refs = Value::Ref(Arc::new(arena), Arc::new((0..n).map(|_| next() as usize % arena_rows).collect()));
    let pair_ends = ends(&mut next, arena_rows, 3);
    let pair_total = pair_ends.total();
    let pairs = Value::List(pair_ends, Box::new(Value::Prod(vec![
        Value::u8((0..pair_total).map(|_| next() as u8).collect()),
        Value::f64((0..pair_total).map(|_| next() as f64).collect()),
    ])));
    let pair_refs = Value::Ref(Arc::new(pairs), Arc::new((0..n).map(|i| (i * 7) % arena_rows).collect()));
    let ids: Vec<i64> = {
        let mut id = -(n as i64);
        (0..n).map(|i| { if i % 3 == 0 || next() % 2 == 0 { id += 1 + (next() % 4) as i64; } id }).collect()
    };
    let key_ends = ends(&mut next, n, 2);
    let keys = Value::Prod(vec![Value::i64(ids), Value::Prod(vec![Value::u8((0..n).map(|_| next() as u8 % 4).collect()), text(&mut next, key_ends)])]);

    Value::Prod(vec![
        edge, constant, halves, random, bytes, words, float, nested, stride, hollow, sum, one_variant, sum_of_sums, sums,
        Value::Unit(n), Value::Prod(Vec::new()), refs, pair_refs, keys,
    ])
}

/// The column's fields, by name, for demands that pick them.
const FIELDS: usize = 19;
const TEXT: usize = 5;
const NESTED: usize = 7;
const SUM: usize = 10;
const SUM_OF_SUMS: usize = 12;
const SUMS: usize = 13;
const UNITS: usize = 14;
const REFS: usize = 16;
const PAIR_REFS: usize = 17;
const KEYS: usize = 18;

/// The two columns hold the same rows, with references compared by the rows they reference, at the same leaf storages.
fn assert_same(actual: &Value, expected: &Value) {
    assert_eq!(Layout::of(actual), Layout::of(expected), "storages differ");
    assert_eq!(clone_ref(actual.clone()), clone_ref(expected.clone()));
}

/// Rows `rows` of `value`, as corgi gathers them.
fn rows_of(value: &Value, rows: Range<usize>) -> Value {
    gather(value, &rows.collect::<Vec<_>>())
}

fn encoded(value: &Value, rows: Range<usize>) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, rows, &mut out);
    out
}

/// The codec of each part of an encoded range, `None` for a part of no values.
fn codecs(bytes: &[u8]) -> Vec<Option<u8>> {
    let (_, layout, mut parts) = header(bytes);
    (0..layout.parts()).map(|_| parts.next().first().copied()).collect()
}

#[test]
fn every_shape_round_trips() {
    let mut seed = 11;
    for n in [0, 1, 2, 500] {
        let value = column(n, &mut seed);
        let mut ranges = vec![0..n, 0..0, n..n, n / 3..2 * n / 3];
        if n > 0 { ranges.extend([0..1, n - 1..n]); }
        for rows in ranges {
            let decoded = decode(&[&encoded(&value, rows.clone())], &Demand::All).unwrap();
            assert_same(&decoded, &rows_of(&value, rows.clone()));
            // Each field on its own, so that every shape is also a range's top.
            let Value::Prod(fields) = &value else { unreachable!() };
            for field in fields.iter().filter(|f| !matches!(f, Value::Prod(fs) if fs.is_empty())) {
                let decoded = decode(&[&encoded(field, rows.clone())], &Demand::All).unwrap();
                assert_same(&decoded, &rows_of(field, rows.clone()));
            }
        }
    }
}

/// Values at the edges of each storage come back bit for bit: a float leaf compares by its keys, so NaN payloads and the sign of zero count.
#[test]
fn edge_values_round_trip() {
    let nans = [f64::NAN, -f64::NAN, f64::from_bits(0x7FF0_0000_0000_0001), f64::from_bits(0xFFFF_FFFF_FFFF_FFFF), f64::from_bits(0x7FF8_DEAD_BEEF_0000)];
    let cases = vec![
        Value::i64(vec![i64::MIN, i64::MAX]),
        Value::i64(vec![i64::MIN; 5]),
        Value::i64(vec![i64::MAX; 5]),
        Value::i64(vec![-1, 0]),
        Value::u8(vec![0, 255, 0, 255]),
        Value::u8(vec![255; 9]),
        Value::f64(nans.to_vec()),
        Value::f64(vec![0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY]),
        Value::f64(vec![-0.0; 4]),
        Value::List(Bounds::offsets(vec![0, 0, 0]), Box::new(Value::i64(Vec::new()))),
        Value::List(Bounds::offsets(vec![0, 2, 2]), Box::new(Value::List(Bounds::offsets(vec![0, 0]), Box::new(Value::u8(Vec::new()))))),
        Value::sum(vec![2, 2], vec![Value::f64(Vec::new()), Value::List(Bounds::offsets(Vec::new()), Box::new(Value::u8(Vec::new()))), Value::Unit(2)]),
        Value::Ref(Arc::new(Value::List(Bounds::offsets(vec![0, 3]), Box::new(Value::f64(nans[..3].to_vec())))), Arc::new(vec![1, 0, 1, 1])),
    ];
    for value in cases {
        let n = value.len();
        for rows in [0..n, 1..n, 0..0] {
            assert_same(&decode(&[&encoded(&value, rows.clone())], &Demand::All).unwrap(), &rows_of(&value, rows));
        }
    }
}

/// Each codec, on a run it is the smallest for, and on runs at the extremes of each width.
#[test]
fn codecs_round_trip() {
    let mut seed = 5;
    let mut next = || xorshift(&mut seed);
    let three = [next(), next(), next()];
    let key = |x: i64| x.key();
    let cases: Vec<(Vec<u64>, usize, Option<u8>)> = vec![
        ((0..1000).map(|_| next() % 256).collect(), 1, Some(RAW)),
        ((0..1000).map(|_| (1 << 40) | (next() % 1000)).collect(), 8, Some(FOR)),
        (vec![key(i64::MIN); 1000], 8, Some(FOR)),
        ((0..1000).map(|_| three[next() as usize % 3]).collect(), 8, Some(DICT)),
        ((0..1000).map(|_| ([1, 7, 1000][next() as usize % 3] << 32) | (next() % (1 << 20))).collect(), 8, Some(SPLIT)),
        ((0..1000).map(|_| key([i64::MIN, i64::MAX, 0][next() as usize % 3])).collect(), 8, None),
        ((0..1000).map(|_| next()).collect(), 8, None),
        ((0..999).map(|_| next() >> 32).collect(), 4, None),
        (vec![0, u64::MAX], 8, None),
        (vec![u64::MAX], 8, None),
        (vec![0, 255], 1, None),
    ];
    for (run, width, codec) in cases {
        let mut bytes = vec![0xAA];
        encode_run(&run, width, &mut bytes);
        if let Some(codec) = codec { assert_eq!(bytes[1], codec); }
        let mut out = Vec::new();
        assert_eq!(decode_run(&bytes[1..], run.len(), width, &mut out, |w| w), bytes.len() - 1);
        assert_eq!(out, run);
    }
}

/// Each codec on a leaf, a list's lengths or a sum's tags it is the smallest for, through `encode`.
#[test]
fn codecs_on_parts() {
    let mut seed = 7;
    let mut next = || xorshift(&mut seed);
    let n = 1000;
    let far = [-(1 << 50), 3, 1 << 61];
    let lengths = ends(&mut next, n, 6);
    let tags: Vec<usize> = (0..n).map(|_| (next() % 3) as usize).collect();
    let lanes = (0..3).map(|t| Value::Unit(tags.iter().filter(|&&s| s == t).count())).collect();
    let cases = vec![
        (Value::u8((0..n).map(|_| next() as u8).collect()), vec![RAW]),
        (Value::u8((0..n).map(|_| b'a' + (next() % 4) as u8).collect()), vec![FOR]),
        (Value::u8((0..n).map(|_| [0, 255][next() as usize % 2]).collect()), vec![DICT]),
        (Value::i64((0..n).map(|_| next() as i64).collect()), vec![RAW]),
        (Value::i64((0..n).map(|_| -500 + (next() % 1000) as i64).collect()), vec![FOR]),
        (Value::i64(vec![-3; n]), vec![FOR]),
        (Value::i64((0..n).map(|_| far[next() as usize % 3]).collect()), vec![DICT]),
        (Value::i64((0..n).map(|_| (far[next() as usize % 3] << 2) | (next() % (1 << 20)) as i64).collect()), vec![SPLIT]),
        (Value::f64((0..n).map(|_| f64::from_bits(next())).collect()), vec![RAW]),
        (Value::f64((0..n).map(|_| [1.5, -0.0, f64::NAN][next() as usize % 3]).collect()), vec![DICT]),
        (Value::f64((0..n).map(|_| (next() % 1000) as f64).collect()), vec![SPLIT]),
        (Value::List(Bounds::Stride(2, n), Box::new(Value::u8(vec![9; 2 * n]))), vec![FOR, FOR]),
        (Value::List(lengths.clone(), Box::new(Value::Unit(lengths.total()))), vec![FOR]),
        (Value::sum(tags, lanes), vec![FOR]),
    ];
    for (value, expected) in cases {
        let bytes = encoded(&value, 0..value.len());
        let used: Vec<u8> = codecs(&bytes).into_iter().flatten().collect();
        assert_eq!(used, expected, "{}", crate::shape_of_value(&value));
        assert_same(&decode(&[&bytes], &Demand::All).unwrap(), &value);
    }
}

/// Demands on the column's fields.
fn field(picks: &[(usize, Demand)]) -> Demand {
    Demand::Prod((0..FIELDS).map(|i| picks.iter().find(|(j, _)| *j == i).map_or(Demand::None, |(_, d)| d.clone())).collect())
}

fn demands() -> Vec<Demand> {
    let list = |d: Demand| Demand::List(Box::new(d));
    vec![
        Demand::None,
        Demand::All,
        field(&[]),
        Demand::Prod((0..FIELDS).map(|i| if i % 2 == 0 { Demand::All } else { Demand::None }).collect()),
        field(&[(TEXT, list(Demand::None)), (NESTED, list(list(Demand::All)))]),
        field(&[(NESTED, list(list(Demand::None)))]),
        field(&[(SUM, Demand::Sum(vec![Demand::None, Demand::All, Demand::None])), (SUMS, list(Demand::Sum(vec![Demand::All, Demand::None])))]),
        field(&[(SUM_OF_SUMS, Demand::Sum(vec![Demand::Sum(vec![Demand::None, Demand::All]), list(Demand::None)]))]),
        field(&[(REFS, Demand::Ref(Box::new(list(Demand::None)))), (UNITS, Demand::All)]),
        field(&[(REFS, Demand::Ref(Box::new(Demand::All))), (6, Demand::All), (3, Demand::All)]),
        field(&[(PAIR_REFS, Demand::Ref(Box::new(list(Demand::Prod(vec![Demand::None, Demand::All])))))]),
        field(&[(KEYS, Demand::Prod(vec![Demand::All, Demand::None]))]),
        field(&[(KEYS, Demand::Prod(vec![Demand::None, Demand::Prod(vec![Demand::None, list(Demand::None)])]))]),
        field(&[(15, Demand::All), (14, Demand::None)]),
    ]
}

#[test]
fn demands_decode_as_project() {
    let mut seed = 31;
    let value = column(800, &mut seed);
    let rows = 100..700;
    let bytes = encoded(&value, rows.clone());
    let some = rows_of(&value, rows);
    for demand in demands() {
        let decoded = decode(&[&bytes], &demand).unwrap();
        assert_same(&decoded, &demand.project(some.clone()).unwrap());
    }
    // Demands that do not fit the shape are errors, as they are for `Demand::project`.
    let wrong = [
        field(&[(0, Demand::List(Box::new(Demand::All)))]),
        field(&[(REFS, Demand::Ref(Box::new(Demand::None)))]),
        field(&[(REFS, Demand::List(Box::new(Demand::All)))]),
        field(&[(TEXT, Demand::Ref(Box::new(Demand::All)))]),
        field(&[(SUM, Demand::Sum(vec![Demand::All]))]),
        field(&[(UNITS, Demand::Prod(vec![Demand::All]))]),
        Demand::Prod(vec![Demand::All; FIELDS - 1]),
        Demand::List(Box::new(Demand::All)),
    ];
    for demand in wrong {
        assert!(demand.project(some.clone()).is_err(), "{demand:?}");
        assert!(decode(&[&bytes], &demand).is_err(), "{demand:?}");
    }
}

/// A decode reads none of the parts its demand skips: they can hold anything.
#[test]
fn skipped_parts_are_not_read() {
    let mut seed = 41;
    let value = column(300, &mut seed);
    let mut bytes = encoded(&value, 0..300);
    // The parts of the fields before the text, and of the text, located through the directory.
    let (_, layout, parts) = header(&bytes);
    let Layout::Prod(fields) = &layout else { unreachable!() };
    let before: usize = fields[..TEXT].iter().map(Layout::parts).sum();
    let start = bytes.len() - parts.data.len();
    let mut cursor = Parts { directory: parts.directory, data: parts.data };
    let skipped: usize = (0..before + fields[TEXT].parts()).map(|_| cursor.next().len()).sum();
    for byte in &mut bytes[start..start + skipped] { *byte = 0xFF; }
    let demand = field(&[(TEXT + 1, Demand::All), (REFS, Demand::All), (KEYS, Demand::All)]);
    let decoded = decode(&[&bytes], &demand).unwrap();
    assert_same(&decoded, &demand.project(value.clone()).unwrap());
    // Reading the overwritten parts would meet an unknown codec.
    assert!(std::panic::catch_unwind(|| decode(&[&bytes], &Demand::All)).is_err());
}

/// Consecutive ranges, decoded together, are the rows they cover, for every demand.
#[test]
fn several_ranges_decode_as_their_rows() {
    let mut seed = 23;
    let n = 1500;
    let value = column(n, &mut seed);
    let mut next = || xorshift(&mut seed);
    let mut cuts = vec![0, 0];
    while *cuts.last().unwrap() < n { cuts.push((cuts.last().unwrap() + (next() % 200) as usize).min(n)); }
    let ranges: Vec<Vec<u8>> = cuts.windows(2).map(|w| encoded(&value, w[0]..w[1])).collect();
    for start in 0..ranges.len() {
        for end in (start + 1..=ranges.len()).filter(|&end| end - start <= 3 || end == ranges.len()) {
            let run: Vec<&[u8]> = ranges[start..end].iter().map(Vec::as_slice).collect();
            let rows = rows_of(&value, cuts[start]..cuts[end]);
            assert_same(&decode(&run, &Demand::All).unwrap(), &rows);
            if end - start == 3 {
                for demand in demands() { assert_same(&decode(&run, &demand).unwrap(), &demand.project(rows.clone()).unwrap()); }
            }
        }
    }
}

/// Ranges whose integer leaves are held at different storages decode as `i64`s; ranges of different shapes, or no ranges, are errors.
#[test]
fn ranges_meet_at_one_storage() {
    let bytes = Value::u8(vec![1, 2, 3]);
    let wide = Value::i64(vec![300, -1]);
    let (b, w) = (encoded(&bytes, 0..3), encoded(&wide, 0..2));
    assert_same(&decode(&[&b, &w], &Demand::All).unwrap(), &Value::i64(vec![1, 2, 3, 300, -1]));
    assert_same(&decode(&[&w, &b], &Demand::All).unwrap(), &Value::i64(vec![300, -1, 1, 2, 3]));
    assert_same(&decode(&[&b, &b], &Demand::All).unwrap(), &Value::u8(vec![1, 2, 3, 1, 2, 3]));
    let listed = |v: Value| Value::List(Bounds::offsets(vec![1, v.len()]), Box::new(v));
    let (lb, lw) = (encoded(&listed(bytes.clone()), 0..2), encoded(&listed(wide.clone()), 0..2));
    let both = Value::List(Bounds::offsets(vec![1, 3, 4, 5]), Box::new(Value::i64(vec![1, 2, 3, 300, -1])));
    assert_same(&decode(&[&lb, &lw], &Demand::All).unwrap(), &both);
    let float = encoded(&Value::f64(vec![1.0]), 0..1);
    assert!(decode(&[&b, &float], &Demand::All).is_err());
    assert!(decode(&[&b, &lb], &Demand::All).is_err());
    assert!(decode(&[], &Demand::All).is_err());
}

#[test]
fn integer_slices_round_trip() {
    let mut seed = 3;
    let mut next = || xorshift(&mut seed);
    let i64s: Vec<Vec<i64>> = vec![
        vec![],
        vec![i64::MIN],
        vec![i64::MIN, i64::MAX, 0, -1],
        vec![1; 1000],
        (0..1000).map(|_| [1, -1, 3][next() as usize % 3]).collect(),
        (0..1000).map(|_| next() as i64).collect(),
        (0..1000).map(|i| i / 7).collect(),
    ];
    let mut all = Vec::new();
    let mut bytes = Vec::new();
    for xs in &i64s {
        let mut one = Vec::new();
        encode_i64s(xs, &mut one);
        let mut back = vec![42];
        decode_i64s(&one, &mut back);
        assert_eq!(back[1..], xs[..]);
        bytes.push(one);
        all.extend_from_slice(xs);
    }
    // Decoding appends, so slices decode into one vector.
    let mut back = Vec::new();
    for one in &bytes { decode_i64s(one, &mut back); }
    assert_eq!(back, all);

    let u64s: Vec<Vec<u64>> = vec![
        vec![],
        vec![u64::MAX],
        vec![0, u64::MAX],
        (0..1000).map(|i| i / 7).collect(),
        (0..1000).map(|_| [0, 1, 5, u64::MAX][next() as usize % 4]).collect(),
        (0..1000).map(|_| next()).collect(),
    ];
    for xs in &u64s {
        let mut one = Vec::new();
        encode_u64s(xs, &mut one);
        let mut back = Vec::new();
        decode_u64s(&one, &mut back);
        assert_eq!(&back, xs);
    }
    // A constant takes a few bytes whatever its length.
    let mut one = Vec::new();
    encode_i64s(&[5; 100_000], &mut one);
    assert!(one.len() < 16, "{} bytes", one.len());
}

/// Each row's bytes, by its shape: storage widths, eight per list row with offsets, nine per sum row with tags, and a reference with the row it references.
#[test]
fn row_bytes_follow_storage() {
    let arena = Value::List(Bounds::offsets(vec![2, 5]), Box::new(Value::u8(vec![1, 2, 3, 4, 5])));
    let value = Value::Prod(vec![
        Value::u8(vec![1, 2]),
        Value::f64(vec![1.0, 2.0]),
        Value::List(Bounds::offsets(vec![0, 3]), Box::new(Value::i64(vec![1, 2, 3]))),
        Value::List(Bounds::Stride(2, 2), Box::new(Value::u8(vec![1, 2, 3, 4]))),
        Value::sum(vec![1, 0], vec![Value::i64(vec![7]), Value::Unit(1)]),
        Value::sum(vec![0, 0], vec![Value::u8(vec![7, 8]), Value::Unit(0)]),
        Value::Unit(2),
        Value::Ref(Arc::new(arena), Arc::new(vec![1, 1])),
    ]);
    // 1 + 8 + (8 + 0 | 8 + 24) + 2 + (9 + 0 | 9 + 8) + 1 + 0 + (8 + 8 + 3)
    assert_eq!(row_bytes(&value), vec![1 + 8 + 8 + 2 + 9 + 1 + 19, 1 + 8 + 32 + 2 + 17 + 1 + 19]);
    let mut seed = 13;
    let column = column(200, &mut seed);
    assert_eq!(row_bytes(&column).len(), 200);
}
