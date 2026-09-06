//! Measure this test thread only, with inputs built outside the interval. These
//! assertions constrain row-sized scratch, not incidental control objects.
use corgi::{NumOp, Op, OpLike, Tags, Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! { static BYTES: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Count;
fn record(n: usize) {
    let _ = BYTES.try_with(|c| {
        if let Some(b) = c.get() {
            c.set(Some(b + n));
        }
    });
}
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        record(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        record(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        record(n);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Count = Count;

fn measure(f: impl FnOnce() -> Value) -> (Value, usize) {
    // Reset the counter even if the operation panics.
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BYTES.with(|c| c.set(None));
        }
    }
    BYTES.with(|c| c.set(Some(0)));
    let _reset = Reset;
    let v = f();
    let bytes = BYTES.with(|c| c.get().unwrap());
    (v, bytes)
}

fn lift(v: Value) -> Value {
    Value::Sum(Tags::Const(0, v.len()), vec![v, Value::Unit(0)])
}

#[test]
fn successful_hoists_and_squash_allocate_no_row_sized_masks() {
    let n = 1 << 20;
    let cases = [
        (
            "HoistProd",
            Op::HoistProd,
            Value::Prod(vec![lift(Value::Unit(n)), lift(Value::Unit(n))]),
            lift(Value::Prod(vec![Value::Unit(n), Value::Unit(n)])),
        ),
        (
            "HoistList",
            Op::HoistList,
            Value::List(vec![n].into(), Box::new(lift(Value::Unit(n)))),
            lift(Value::List(vec![n].into(), Box::new(Value::Unit(n)))),
        ),
        (
            "Squash",
            Op::Squash,
            lift(lift(Value::Unit(n))),
            lift(Value::Unit(n)),
        ),
        (
            "HoistSum",
            Op::HoistSum(vec![0]),
            Value::Sum(Tags::Const(0, n), vec![lift(Value::Unit(n))]),
            lift(Value::Sum(Tags::Const(0, n), vec![Value::Unit(n)])),
        ),
    ];
    let mut allocations = Vec::new();
    for (name, op, input, expected) in cases {
        let (actual, bytes) = measure(|| NumOp::Core(op).eval(input).unwrap());
        assert_eq!(actual, expected, "{name}");
        eprintln!("{name}: {bytes} allocated bytes for {n} rows");
        allocations.push((name, bytes));
    }
    assert!(
        allocations.iter().all(|(_, bytes)| *bytes < 1024),
        "successful structural operations allocated row-sized scratch: {allocations:?}"
    );
}
