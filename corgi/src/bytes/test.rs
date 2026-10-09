use super::*;

/// Encode, decode, and check both the value and the promised length.
fn round_trip(v: &Value) {
    let mut buf = Vec::new();
    write_to(v, &mut buf).unwrap();
    assert_eq!(buf.len(), length_in_bytes(v), "length_in_bytes disagrees with write_to for {v:?}");
    assert_eq!(buf.len() % 8, 0, "encoding is not word-aligned for {v:?}");
    let (back, read) = read_from(&buf).unwrap();
    assert_eq!(read, buf.len(), "read_from consumed {read} of {} bytes", buf.len());
    assert_eq!(&back, v, "round trip changed the value");
}

/// One of each constructor, at each leaf width, including the empty cases.
fn corpus() -> Vec<Value> {
    vec![
        Value::Unit(0),
        Value::Unit(7),
        Value::u8(vec![]),
        Value::u8(vec![1, 2, 3]),                       // an odd payload length, to exercise padding
        Value::f64(vec![1.5, -0.0, f64::NAN, 4.0, 5.0]),
        Value::i64(vec![-1, 0, 12345, i64::MIN]),
        Value::Prod(vec![]),
        Value::Prod(vec![Value::i64(vec![1, 2]), Value::u8(vec![3, 4])]),
        Value::List(Bounds::offsets(vec![1, 1, 4]), Box::new(Value::f64(vec![9.0, 8.0, 7.0, 6.0]))),
        Value::List(Bounds::Stride(2, 3), Box::new(Value::i64(vec![1, 2, 3, 4, 5, 6]))),
        Value::sum(vec![0, 1, 0], vec![Value::i64(vec![10, 20]), Value::u8(vec![30])]),
        // a lane no row uses: an empty column of its shape, which must survive as such
        Value::sum(vec![0, 0], vec![Value::i64(vec![1, 2]), Value::f64(vec![])]),
        // the `Const` assignment: every row one tag, so neither witness column is on the wire
        Value::sum_tagged(Tags::Const(1, 3), vec![Value::i64(vec![]), Value::u8(vec![4, 5, 6])]),
        // nesting: the recursion has to keep alignment across every level
        Value::Prod(vec![
            Value::List(Bounds::offsets(vec![2, 3]), Box::new(Value::Prod(vec![
                Value::u8(vec![1, 2, 3]),
                Value::i64(vec![4, 5, 6]),
            ]))),
            Value::Unit(2),
        ]),
    ]
}

#[test]
fn round_trips() {
    for v in corpus() { round_trip(&v); }
}

#[test]
fn round_trip_is_shape_preserving() {
    for v in corpus() {
        let mut buf = Vec::new();
        write_to(&v, &mut buf).unwrap();
        let (back, _) = read_from(&buf).unwrap();
        assert_eq!(crate::shape_of_value(&back), crate::shape_of_value(&v));
        assert_eq!(back.len(), v.len());
    }
}

/// Rows survive individually, not just in bulk: the decoded column hashes row-for-row like
/// the original, which is the property the distribution boundary actually depends on.
#[test]
fn round_trip_preserves_row_hashes() {
    for v in corpus() {
        let mut buf = Vec::new();
        write_to(&v, &mut buf).unwrap();
        let (back, _) = read_from(&buf).unwrap();
        assert_eq!(crate::hash::hash(&back), crate::hash::hash(&v));
    }
}

/// A truncated message is an error, not a panic or an out-of-bounds read.
#[test]
fn truncation_is_an_error() {
    for v in corpus() {
        let mut buf = Vec::new();
        write_to(&v, &mut buf).unwrap();
        for cut in (0..buf.len()).step_by(8) {
            assert!(read_from(&buf[..cut]).is_err(), "decoding {cut} of {} bytes should fail", buf.len());
        }
    }
}

/// A leaf column goes out as its stored bytes plus a fixed header — the per-column,
/// not per-row, cost the codec exists to deliver.
#[test]
fn wide_leaves_cost_their_payload() {
    let v = Value::i64((0..10_000i64).collect());
    assert_eq!(length_in_bytes(&v), 24 + 8 * 10_000);
}

/// A splitmix64 stream — deterministic, seedable, no dependency. Enough randomness to shake
/// out shapes a hand-written corpus does not think of.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        crate::hash::mix64(self.0)
    }
    /// A value in `[0, n)`.
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A well-formed `Value` of exactly `rows` rows, nested up to `depth` levels.
///
/// Well-formed matters more than random here: a `Prod`'s fields agree on length, a `Sum`'s
/// offsets are the running per-lane counts its tags imply, and a `List`'s bounds total its
/// values' length. Generating malformed columns would test the codec against inputs the rest
/// of corgi cannot produce.
fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
    // At depth 0 only leaves, so recursion always terminates.
    let arms = if depth == 0 { 2 } else { 5 };
    match rng.below(arms) {
        0 => match rng.below(3) {
            0 => Value::u8((0..rows).map(|_| rng.next() as u8).collect()),
            1 => Value::f64((0..rows).map(|_| f64::from_bits(rng.next())).collect()),
            _ => Value::i64((0..rows).map(|_| rng.next() as i64).collect()),
        },
        1 => Value::Unit(rows),
        2 => {
            let fields = 1 + rng.below(3);
            Value::Prod((0..fields).map(|_| random_value(rng, rows, depth - 1)).collect())
        }
        3 => {
            // Lists: sometimes uniform (so `Stride` is exercised), sometimes ragged.
            let (bounds, total) = if rng.below(2) == 0 {
                let stride = rng.below(3);
                (Bounds::Stride(stride, rows), stride * rows)
            } else {
                let mut ends = Vec::with_capacity(rows);
                let mut acc = 0;
                for _ in 0..rows {
                    acc += rng.below(3);
                    ends.push(acc);
                }
                (Bounds::offsets(ends), acc)
            };
            Value::List(bounds, Box::new(random_value(rng, total, depth - 1)))
        }
        _ => {
            // Sums: pick a tag per row, count per lane; a lane no row picks is an empty column.
            let lanes = 1 + rng.below(3);
            let tags: Vec<usize> = (0..rows).map(|_| rng.below(lanes)).collect();
            let mut counts = vec![0usize; lanes];
            let offsets: Vec<usize> = tags
                .iter()
                .map(|&t| {
                    counts[t] += 1;
                    counts[t] - 1
                })
                .collect();
            let variants = counts
                .iter()
                .map(|&n| random_value(rng, n, depth - 1))
                .collect();
            Value::sum_tagged(
                Tags::Column(
                    Prim::U8(std::sync::Arc::new(tags.iter().map(|&t| t as u8).collect())),
                    std::sync::Arc::new(offsets),
                ),
                variants,
            )
        }
    }
}

/// The general property, over shapes nobody wrote down: whatever the encoder was handed comes
/// back, its promised length is its actual length, and the encoding stays word-aligned.
#[test]
fn round_trips_random_shapes() {
    let mut rng = Rng(0x5EED);
    for i in 0..400 {
        let rows = i % 7; // includes 0 — empty columns at every nesting depth
        let v = random_value(&mut rng, rows, 3);
        round_trip(&v);
    }
}

/// Row identity survives too, for the same random shapes: the decoded column hashes
/// row-for-row like the original, which is what a distribution boundary depends on.
#[test]
fn random_shapes_preserve_row_hashes() {
    let mut rng = Rng(0xC0FFEE);
    for i in 0..400 {
        let v = random_value(&mut rng, 1 + i % 9, 3);
        let mut buf = Vec::new();
        write_to(&v, &mut buf).unwrap();
        let (back, _) = read_from(&buf).unwrap();
        assert_eq!(crate::hash::hash(&back), crate::hash::hash(&v), "{v:?}");
    }
}

// --- adversarial input ----------------------------------------------------------------------
//
// Truncation is the easy malformed-input family: it shortens the buffer, so the bounds checks
// catch it. The interesting families corrupt a length or a tag IN PLACE, leaving the buffer
// exactly as long as the decoder expects — which is where wrapped arithmetic, wire-sized
// reservations, unbounded recursion, and structurally impossible columns live.
//
// Several of these depend on the build profile (a debug overflow panic is a release wrap), so
// run them both ways: `cargo test` and `cargo test --release`.

/// Word values chosen to break size arithmetic: zero and small tags, the wrap-to-large
/// maximum, the wrap-to-small `2^63` (doubling it is 0), and a value big enough to make any
/// reservation fatal if it were believed.
const NASTY: [u64; 8] = [0, 1, 2, 5, u64::MAX, 1 << 63, 1 << 61, 1 << 32];

/// Use a decoded value the way a consumer would. This is the real assertion of the adversarial
/// tests: not just that `read_from` returned, but that what it returned can be indexed,
/// hashed and shaped without panicking — which is what "the decode validates structure" means.
fn exercise(v: &Value) {
    let _ = crate::shape_of_value(v);
    let _ = v.len();
    // The payload-free constructors declare rows without spending bytes, so a mutated header
    // can legitimately name an enormous column — documented, and `declared_rows` is exactly
    // the guard a consumer is told to apply. Using it here is the test asserting that the
    // advice works: `Value::len` alone would not see a `Unit` nested in a `Sum` lane.
    if declared_rows(v) <= 10_000 {
        let _ = crate::hash::hash(v);
    }
}

/// Corrupt one word of a valid encoding, leaving the length alone, and the decoder must still
/// return — `Err`, or an `Ok` whose value is safe to use. This is the family the truncation
/// test cannot reach.
#[test]
fn mutated_headers_never_panic() {
    let mut rng = Rng(0xD15EA5E);
    let mut values = corpus();
    values.extend((0..40).map(|i| random_value(&mut rng, 1 + i % 5, 3)));
    for v in &values {
        let mut buf = Vec::new();
        write_to(v, &mut buf).unwrap();
        for word in 0..buf.len() / 8 {
            let original: [u8; 8] = buf[word * 8..word * 8 + 8].try_into().unwrap();
            for nasty in NASTY {
                buf[word * 8..word * 8 + 8].copy_from_slice(&nasty.to_le_bytes());
                if let Ok((decoded, read)) = read_from(&buf) {
                    assert!(read <= buf.len(), "reported {read} bytes read of {}", buf.len());
                    exercise(&decoded);
                }
            }
            buf[word * 8..word * 8 + 8].copy_from_slice(&original);
        }
    }
}

/// A length near `u64::MAX` must be rejected on its face, not multiplied by a leaf width
/// first. In debug that multiply panics; in release it wraps — to something small, which
/// would decode "successfully" to an empty leaf and desync the frame for a framing consumer,
/// or to something huge, which would walk off the buffer.
#[test]
fn leaf_lengths_are_bounded_before_they_are_scaled() {
    for bits in [8u64, 16, 32, 64] {
        for len in [u64::MAX, 1 << 63, 1 << 61, 1 << 32, 1000] {
            // tag = Prim, then the width and the claimed element count, and nothing after.
            let mut buf = Vec::new();
            for w in [0, bits, len] {
                buf.extend_from_slice(&w.to_le_bytes());
            }
            assert!(
                read_from(&buf).is_err(),
                "a {bits}-bit leaf claiming {len} elements with no payload must be rejected"
            );
        }
    }
}

/// Field, lane and bound counts are reservations, so they must be bounded by what the
/// remaining bytes could encode before they reach `Vec::with_capacity` — otherwise a
/// sixteen-byte message asks for a multi-gigabyte allocation.
#[test]
fn wire_counts_do_not_become_reservations() {
    // (value tag, the words that precede the count, description)
    let cases: [(u64, &[u64], &str); 3] = [
        (1, &[], "product fields"),
        (3, &[0], "list bounds"),   // List, Offsets form, then the bound count
        (4, &[], "unit rows"),      // Unit's count is not a reservation; it must still not panic
    ];
    for (tag, prefix, what) in cases {
        for n in [u64::MAX, 1 << 61, 1 << 40, 1 << 30] {
            let mut buf = Vec::new();
            buf.extend_from_slice(&tag.to_le_bytes());
            for w in prefix {
                buf.extend_from_slice(&w.to_le_bytes());
            }
            buf.extend_from_slice(&n.to_le_bytes());
            // A `Unit` legitimately declares rows in no bytes; everything else must be refused.
            let result = read_from(&buf);
            if tag == 4 {
                assert!(result.is_ok(), "{what}: a unit row count is not a reservation");
            } else {
                assert!(result.is_err(), "{what}: {n} must be refused, not reserved");
            }
        }
    }
}

/// A chain of nested headers costs sixteen bytes a level, so without a cap a small message
/// walks the decoder off the stack — an abort the caller cannot catch, not an `Err`.
#[test]
fn nesting_is_capped() {
    // One-field products all the way down: `Prod, 1, Prod, 1, ...`. Deep enough that without
    // the cap this is the reported failure — a stack overflow and a process abort, not an
    // `Err` — so reverting the cap fails this test the way the bug actually behaves.
    let levels = 100_000;
    let mut buf = Vec::with_capacity(16 * levels + 16);
    for _ in 0..levels {
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes());
    }
    buf.extend_from_slice(&4u64.to_le_bytes()); // a Unit at the bottom
    buf.extend_from_slice(&0u64.to_le_bytes());
    let err = read_from(&buf).expect_err("nesting past the cap must be an error");
    assert!(err.contains("nesting"), "unexpected error: {err}");

    // And the cap is generous rather than tight: a shape well inside it still decodes.
    let mut deep = Value::Unit(3);
    for _ in 0..MAX_DEPTH / 2 {
        deep = Value::Prod(vec![deep]);
    }
    round_trip(&deep);
}

/// The structural invariants the rest of corgi indexes by. Each of these is a byte string the
/// framing accepts and the structure must not: without the checks, the first two panic inside
/// `hash` on a column `read_from` handed back as valid.
#[test]
fn structurally_impossible_columns_are_refused() {
    /// Encode `v`, overwrite word `word` with `to`, and return the bytes — same length as a
    /// valid message, so only the structure is wrong.
    fn patched(v: &Value, word: usize, to: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        write_to(v, &mut buf).unwrap();
        buf[word * 8..word * 8 + 8].copy_from_slice(&to.to_le_bytes());
        buf
    }

    // A sum whose tag names a lane that is not there. Words: [Sum][bits][len][tags payload]…
    // and the payload word carries the single u8 discriminant in its low byte.
    let bad_tag = patched(
        &Value::sum_tagged(
            Tags::Column(Prim::U8(std::sync::Arc::new(vec![0])), std::sync::Arc::new(vec![0])),
            vec![Value::i64(vec![7])],
        ),
        4,
        5,
    );
    assert!(read_from(&bad_tag).is_err(), "a tag naming a missing lane must be refused");

    // A sum whose carried offset points past the end of the lane it names.
    let bad_offset = patched(
        &Value::sum_tagged(
            Tags::Column(Prim::U8(std::sync::Arc::new(vec![0])), std::sync::Arc::new(vec![0])),
            vec![Value::i64(vec![7])],
        ),
        6, // [Sum][form][bits][len][tags][n_offsets][offsets[0]]
        9,
    );
    assert!(read_from(&bad_offset).is_err(), "an offset outside its lane must be refused");

    // A list whose partition reaches past its values. Words: [List][form][n][ends[0]]…
    let over_reach = patched(&Value::List(Bounds::offsets(vec![2]), Box::new(Value::i64(vec![1, 2]))), 3, 10);
    assert!(read_from(&over_reach).is_err(), "bounds reaching past the values must be refused");

    // The `Stride` form of the same thing: three rows of two over a two-element leaf.
    let over_stride = patched(&Value::List(Bounds::Stride(2, 1), Box::new(Value::i64(vec![1, 2]))), 3, 3);
    assert!(read_from(&over_stride).is_err(), "a stride reaching past the values must be refused");

    // A product whose fields disagree on length — `Value::len` reads field 0, so the column
    // would silently lie about how many rows it holds. Words:
    // [Prod][2] [Prim][64][2][payload×2] [Prim][64][2][payload×2], so field 1's count is word 9.
    let ragged = patched(&Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]), 9, 1);
    assert!(read_from(&ragged).is_err(), "a product with ragged fields must be refused");

    // A sum discriminant at a width corgi cannot construct (`sum_opt` stores u8 and asserts
    // the arity fits it), which would otherwise let a tag column carry more than 256 lanes.
    let wide_tags = patched(
        &Value::sum_tagged(
            Tags::Column(Prim::U8(std::sync::Arc::new(vec![0])), std::sync::Arc::new(vec![0])),
            vec![Value::i64(vec![7])],
        ),
        2,
        64,
    );
    assert!(read_from(&wide_tags).is_err(), "a non-int sum discriminant must be refused");
}

/// `declared_rows` has to see what `Value::len` cannot, or the advice attached to it is
/// useless: the expensive column is the nested one.
#[test]
fn declared_rows_sees_through_nesting() {
    // as large as `usize` allows on the target: 2^40 natively, 2^28 where it is 32 bits.
    #[cfg(target_pointer_width = "64")]
    let huge = 1usize << 40;
    #[cfg(not(target_pointer_width = "64"))]
    let huge = 1usize << 28;

    // A one-row sum whose lane names a trillion rows.
    let hidden_in_a_lane = Value::sum_tagged(
        Tags::Column(Prim::U8(std::sync::Arc::new(vec![0])), std::sync::Arc::new(vec![0])),
        vec![Value::Unit(huge)],
    );
    assert_eq!(hidden_in_a_lane.len(), 1);
    assert_eq!(declared_rows(&hidden_in_a_lane), huge as u64);

    // A one-row list whose values do.
    let hidden_under_a_list = Value::List(Bounds::offsets(vec![huge]), Box::new(Value::Unit(huge)));
    assert_eq!(hidden_under_a_list.len(), 1);
    assert_eq!(declared_rows(&hidden_under_a_list), huge as u64);

    // A stride multiplies rather than storing, so its total is where the size hides.
    let hidden_in_a_stride = Value::List(Bounds::Stride(huge, 2), Box::new(Value::Unit(2 * huge)));
    assert_eq!(hidden_in_a_stride.len(), 2);
    assert_eq!(declared_rows(&hidden_in_a_stride), 2 * huge as u64);

    // And it does not overflow on a stride that would.
    let overflowing = Value::List(Bounds::Stride(usize::MAX, usize::MAX), Box::new(Value::Unit(0)));
    // (u64::MAX where `usize` is 64 bits; where it is 32 bits the product fits in a u64.)
    assert_eq!(declared_rows(&overflowing), (usize::MAX as u64).saturating_mul(usize::MAX as u64));

    // On ordinary columns it agrees with `len`.
    for v in corpus() {
        if matches!(v, Value::List(..)) {
            continue; // a list's values are legitimately longer than its rows
        }
        assert!(declared_rows(&v) >= v.len() as u64, "{v:?}");
    }
}

/// Truncating a random shape is an error, never a panic or an out-of-bounds read.
#[test]
fn random_shapes_reject_truncation() {
    let mut rng = Rng(0xBADCAFE);
    for i in 0..100 {
        let v = random_value(&mut rng, 1 + i % 5, 3);
        let mut buf = Vec::new();
        write_to(&v, &mut buf).unwrap();
        for cut in (0..buf.len()).step_by(8) {
            assert!(read_from(&buf[..cut]).is_err(), "decoding {cut} of {} bytes should fail: {v:?}", buf.len());
        }
    }
}
