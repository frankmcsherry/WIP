//! sprig: a columnar Joy.
//!
//! sprig is the smallest Joy/Forth we could find that still grows, in its own
//! language, up to a worst-case-optimal join. It keeps tada's two stacks
//! (`data` and `todo`) and tada's trick: a "combinator" is not a blessed object
//! that executes programs, it is a command that pushes more commands onto the
//! `todo` stack. Running a program is `todo.extend(body)`, nothing more.
//!
//! What changes from tada is the noun. tada's nouns were a bignum or a quoted
//! program. sprig's nouns are a quoted program or a `Data` (see the `sprig`
//! library crate): a tree of `i64` columns. There is no separate List, Product,
//! or Sum type -- those, and tries, are conventions built over `Data` in the
//! *language*, not cases in the kernel.
//!
//! This binary is the language: the parser, the verbs, and the REPL. The
//! columnar substrate and the primitive operations it dispatches to live in
//! `lib.rs`, so the benchmarks measure exactly the code this runs.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use sprig::{argsort, diff2, gather, iota, locate, locate_gallop, map_into, merge2, prefix_sum, reduce_sum, scatter_add, searchsorted, sort_unique, Data};

/// A command manipulates both the `data` and `todo` stacks.
#[derive(Clone, Debug)]
enum Command {
    /// A literal: pushes itself onto the data stack.
    Noun(Noun),
    /// An action: does as it likes to both stacks.
    Verb(Verb),
    /// `=x`: pop the top of the data stack and bind it to the name `x` in the
    /// value environment. The let-binding the stack alone could not give us: a
    /// value computed once and referred to many times, by name, without the
    /// positional bookkeeping of `pick`/`roll` and without recomputation.
    Store(String),
    /// `$x`: push the value bound to `x`. Panics if `x` is unbound (so a typo'd
    /// verb still fails, since only `$`-prefixed tokens are value loads).
    Load(String),
}

/// The two kinds of literal: a column tree, or a quoted program.
#[derive(Clone, Debug)]
enum Noun {
    Data(Data),
    /// Quoted programs are stored in run order; migrating to a stack reverses
    /// them, so display and the parser reverse to compensate (as in tada).
    Program(Vec<Command>),
}

impl Command {
    fn data(d: Data) -> Self {
        Command::Noun(Noun::Data(d))
    }
    fn program(p: Vec<Command>) -> Self {
        Command::Noun(Noun::Program(p))
    }
    /// Evaluate the command against the two stacks and the value environment.
    #[inline(always)]
    fn eval(self, data: &mut Vec<Command>, todo: &mut Vec<Command>, env: &mut BTreeMap<String, Noun>) {
        match self {
            Command::Noun(_) => data.push(self),
            Command::Verb(a) => a.eval(data, todo),
            Command::Store(name) => {
                let v = pop_noun(data);
                env.insert(name, v);
            }
            Command::Load(name) => {
                let v = env.get(&name).unwrap_or_else(|| panic!("unbound name: {}", name)).clone();
                data.push(Command::Noun(v));
            }
        }
    }
}

/// Drain `todo`, letting each command act, until it is empty. Returns the count.
fn reduce_stacks(data: &mut Vec<Command>, todo: &mut Vec<Command>, env: &mut BTreeMap<String, Noun>) -> usize {
    let mut counter = 0;
    while let Some(op) = todo.pop() {
        op.eval(data, todo, env);
        counter += 1;
    }
    counter
}

/// Actions on the two stacks.
///
/// The Joy spine (stack shuffles, program surgery, combinators) is tada's,
/// unchanged: it is noun-agnostic and treats `Data` and `Program` alike. The
/// columnar verbs dispatch to the kernel functions in the `sprig` library.
#[derive(Clone, Debug)]
enum Verb {
    // Stack shuffles, noun-agnostic.
    Dup,    // [a]        -> [a a]
    Pop,    // [a]        -> []
    Swap,   // [a b]      -> [b a]
    Rot,    // [a b c]    -> [c a b]
    // Program surgery and combinators (the Joy spine).
    Concat, // [[a] [b]]  -> [[a b]]
    Cons,   // [a [b]]    -> [[a b]]
    Cadr,   // [[a b]]    -> [a [b]]
    Eval,   // [[body]]   -> []      and pushes body onto todo
    Ifz,    // [a [t] [e]]-> []      and pushes t or e onto todo (a is a scalar)
    Print,  // [a]        -> []      and prints a
    // map: elementwise scalar ops over a column (length-1 broadcasts).
    Add,    // [b a]      -> [b+a]
    Sub,    // [b a]      -> [b-a]
    Mul,    // [b a]      -> [b*a]
    Eq,     // [b a]      -> [b==a]
    Lt,     // [b a]      -> [b<a]
    Shr,    // [b a]      -> [b >> a]  (logical shift on the 64-bit pattern)
    And,    // [b a]      -> [b & a]   (digit extraction: `key shr 8d and 255`)
    // scan: prefix-reduce and reduce by the + monoid.
    Scan,   // [c]        -> [inclusive prefix sums of c]
    Sum,    // [c]        -> [sum of c]   (length 1)
    // gather / scatter: read and write at a column of indices.
    Gather, // [v i]      -> [ v[i[0]] v[i[1]] ... ]
    Scatter,// [idx len vals] -> out  where out[idx[i]] += vals[i]  (`1 scatter` = bincount)
    // seek: batched segmented membership -- the WCO join's galloping core.
    Find,   // [hay beg end needle] -> [m]  m[i]=1 if needle[i] in hay[beg[i]..end[i]]
    Locate, // [hay beg end needle] -> [p]  p[i]=lower_bound index of needle[i] in hay[beg..end]
    Gallop, // [hay beg end needle] -> [p]  like Locate but galloping outward from beg[i]
    // sort: the permutation that stably orders a column.
    Sort,   // [c]        -> [argsort c]
    // generators / measures.
    Iota,   // [n]        -> [ 0 1 ... n-1 ]   (n a scalar)
    Len,    // [c]        -> [c.len()]         (length 1)
    // wiring: nest and unnest layers.
    Layer,  // [parent child] -> [parent with child appended as a layer]
    Unlayer,// [parent]   -> [parent-without-last-layer  last-layer]
    Nlayers,// [parent]   -> [count of layers]   (drives a fold over runs)
    Cat,    // [a b]      -> [a ++ b]   (append two columns; relation union)
    Merge2, // [a b]      -> [a U b]    (linear merge of two SORTED columns, dedup)
    Vsort,  // [c]        -> [sorted-unique c]  (direct value radix sort + dedup)
    Diff2,  // [a b]      -> [a \ b]   (sorted-merge antijoin; a,b sorted)
}

impl Verb {
    fn name(&self) -> &'static str {
        match self {
            Verb::Dup => "dup", Verb::Pop => "pop", Verb::Swap => "swap", Verb::Rot => "rot",
            Verb::Concat => "concat", Verb::Cons => "cons", Verb::Cadr => "cadr",
            Verb::Eval => "eval", Verb::Ifz => "ifz", Verb::Print => "print",
            Verb::Add => "add", Verb::Sub => "sub", Verb::Mul => "mul",
            Verb::Eq => "eq", Verb::Lt => "lt", Verb::Shr => "shr", Verb::And => "and",
            Verb::Scan => "scan", Verb::Sum => "sum", Verb::Gather => "gather",
            Verb::Scatter => "scatter", Verb::Find => "find", Verb::Locate => "locate", Verb::Gallop => "gallop", Verb::Sort => "sort",
            Verb::Iota => "iota", Verb::Len => "len", Verb::Layer => "layer", Verb::Unlayer => "unlayer",
            Verb::Nlayers => "nlayers", Verb::Cat => "cat", Verb::Merge2 => "merge2", Verb::Vsort => "vsort", Verb::Diff2 => "diff2",
        }
    }

    #[inline(always)]
    fn eval(&self, data: &mut Vec<Command>, todo: &mut Vec<Command>) {
        match self {
            // --- Stack shuffles (noun-agnostic) ---
            Verb::Dup => {
                let a = data.pop().expect("dup: empty stack");
                data.push(a.clone());
                data.push(a);
            }
            Verb::Pop => {
                data.pop().expect("pop: empty stack");
            }
            Verb::Swap => {
                let b = data.pop().expect("swap: empty stack");
                let a = data.pop().expect("swap: empty stack");
                data.push(b);
                data.push(a);
            }
            Verb::Rot => {
                let c = data.pop().expect("rot: empty stack");
                let b = data.pop().expect("rot: empty stack");
                let a = data.pop().expect("rot: empty stack");
                data.push(c);
                data.push(a);
                data.push(b);
            }

            // --- Joy spine (tada, unchanged in spirit) ---
            Verb::Concat => {
                let mut b = pop_prog(data);
                let a = pop_prog(data);
                b.extend(a);
                data.push(Command::program(b));
            }
            Verb::Cons => {
                let mut b = pop_prog(data);
                let a = pop_noun(data);
                b.push(Command::Noun(a));
                data.push(Command::program(b));
            }
            Verb::Cadr => {
                let mut a = pop_prog(data);
                let x = a.pop().expect("cadr: empty program");
                data.push(x);
                data.push(Command::program(a));
            }
            Verb::Eval => {
                let body = pop_prog(data);
                todo.extend(body);
            }
            Verb::Ifz => {
                let e = pop_prog(data);
                let t = pop_prog(data);
                let a = pop_data(data);
                if a.values.first().copied().unwrap_or(0) == 0 {
                    todo.extend(t);
                } else {
                    todo.extend(e);
                }
            }
            Verb::Print => {
                let a = pop_noun(data);
                println!("{}", Command::Noun(a));
            }

            // --- map: elementwise scalar ops (kernel: map_into) ---
            Verb::Add => bin(data, |x, y| x.wrapping_add(y)),
            Verb::Sub => bin(data, |x, y| x.wrapping_sub(y)),
            Verb::Mul => bin(data, |x, y| x.wrapping_mul(y)),
            Verb::Eq => bin(data, |x, y| (x == y) as i64),
            Verb::Lt => bin(data, |x, y| (x < y) as i64),
            Verb::Shr => bin(data, |x, y| if (y as u64) >= 64 { 0 } else { ((x as u64) >> y) as i64 }),
            Verb::And => bin(data, |x, y| x & y),

            // --- scan / reduce (kernel: prefix_sum / reduce_sum) ---
            Verb::Scan => {
                let c = pop_data(data);
                data.push(Command::data(Data::leaf(prefix_sum(&c.values))));
            }
            Verb::Sum => {
                let c = pop_data(data);
                data.push(Command::data(Data::scalar(reduce_sum(&c.values))));
            }

            // --- gather / scatter (kernel: gather / scatter_add) ---
            Verb::Gather => {
                let idx = pop_data(data);
                let vals = pop_data(data);
                data.push(Command::data(Data::leaf(gather(&vals.values, &idx.values))));
            }
            Verb::Scatter => {
                let vals = pop_data(data);
                let len = pop_data(data);
                let idx = pop_data(data);
                let n = *len.values.first().expect("scatter: empty length") as usize;
                data.push(Command::data(Data::leaf(scatter_add(&idx.values, n, &vals.values))));
            }

            // --- seek: batched segmented membership (kernel: searchsorted) ---
            Verb::Find => {
                let needle = pop_data(data);
                let end = pop_data(data);
                let beg = pop_data(data);
                let hay = pop_data(data);
                data.push(Command::data(Data::leaf(
                    searchsorted(&hay.values, &beg.values, &end.values, &needle.values),
                )));
            }
            Verb::Locate => {
                let needle = pop_data(data);
                let end = pop_data(data);
                let beg = pop_data(data);
                let hay = pop_data(data);
                data.push(Command::data(Data::leaf(
                    locate(&hay.values, &beg.values, &end.values, &needle.values),
                )));
            }
            Verb::Gallop => {
                let needle = pop_data(data);
                let end = pop_data(data);
                let beg = pop_data(data);
                let hay = pop_data(data);
                data.push(Command::data(Data::leaf(
                    locate_gallop(&hay.values, &beg.values, &end.values, &needle.values),
                )));
            }

            // --- sort: the ordering permutation (kernel: argsort, stable radix) ---
            Verb::Sort => {
                let c = pop_data(data);
                data.push(Command::data(Data::leaf(argsort(&c.values))));
            }

            // --- generators / measures ---
            Verb::Iota => {
                let n = pop_data(data);
                let n = *n.values.first().expect("iota: empty scalar");
                data.push(Command::data(Data::leaf(iota(n))));
            }
            Verb::Len => {
                let c = pop_data(data);
                data.push(Command::data(Data::scalar(c.values.len() as i64)));
            }

            // --- wiring: layers ---
            Verb::Layer => {
                let child = pop_data(data);
                let mut parent = pop_data(data);
                parent.layers.push(child);
                data.push(Command::data(parent));
            }
            Verb::Unlayer => {
                let mut parent = pop_data(data);
                let child = parent.layers.pop().expect("unlayer: no layers");
                data.push(Command::data(parent));
                data.push(Command::data(child));
            }
            Verb::Nlayers => {
                let p = pop_data(data);
                data.push(Command::data(Data::scalar(p.layers.len() as i64)));
            }
            Verb::Cat => {
                let b = pop_data(data);
                let mut a = pop_data(data);
                Arc::make_mut(&mut a.values).extend(b.values.iter().copied());
                data.push(Command::data(a));
            }
            Verb::Merge2 => {
                let b = pop_data(data);
                let a = pop_data(data);
                data.push(Command::data(Data::leaf(merge2(&a.values, &b.values))));
            }
            Verb::Vsort => {
                let c = pop_data(data);
                data.push(Command::data(Data::leaf(sort_unique(&c.values))));
            }
            Verb::Diff2 => {
                let b = pop_data(data);
                let a = pop_data(data);
                data.push(Command::data(Data::leaf(diff2(&a.values, &b.values))));
            }
        }
    }
}

/// Elementwise binary map of `b` (deeper) and `a` (top), length-1 broadcasting.
///
/// We own both popped columns, so we compute in place into whichever buffer is
/// full length -- no allocation. The equal-length path is the kernel's
/// `map_into`; the two broadcast paths are their own branchless loops. (When
/// `Data` becomes Arc-shared this will need a make-mut.)
fn bin(data: &mut Vec<Command>, f: impl Fn(i64, i64) -> i64) {
    let mut a = pop_data(data);
    let mut b = pop_data(data);
    let (lb, la) = (b.values.len(), a.values.len());
    let result = if lb == la {
        map_into(Arc::make_mut(&mut b.values).as_mut_slice(), &a.values, &f);
        b
    } else if la == 1 {
        let y = a.values[0];
        for x in Arc::make_mut(&mut b.values).iter_mut() { *x = f(*x, y); }
        b
    } else if lb == 1 {
        let x = b.values[0];
        for y in Arc::make_mut(&mut a.values).iter_mut() { *y = f(x, *y); }
        a
    } else {
        panic!("length mismatch in elementwise op: {} vs {}", lb, la);
    };
    data.push(Command::data(result));
}

fn pop_noun(stack: &mut Vec<Command>) -> Noun {
    match stack.pop() {
        Some(Command::Noun(n)) => n,
        other => panic!("expected a noun, found {:?}", other),
    }
}
fn pop_data(stack: &mut Vec<Command>) -> Data {
    match pop_noun(stack) {
        Noun::Data(d) => d,
        Noun::Program(p) => panic!("expected data, found program {:?}", p),
    }
}
fn pop_prog(stack: &mut Vec<Command>) -> Vec<Command> {
    match pop_noun(stack) {
        Noun::Program(p) => p,
        Noun::Data(d) => panic!("expected program, found data {}", d),
    }
}

// --- Display ---

fn fmt_program(prog: &[Command], f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[")?;
    for command in prog.iter().rev() {
        write!(f, " {}", command)?;
    }
    write!(f, " ]")
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Command::Noun(Noun::Data(d)) => write!(f, "{}", d),
            Command::Noun(Noun::Program(p)) => fmt_program(p, f),
            Command::Verb(v) => write!(f, "{}", v.name()),
            Command::Store(n) => write!(f, "={}", n),
            Command::Load(n) => write!(f, "${}", n),
        }
    }
}

fn print_stack(data: &[Command]) {
    print!("data:");
    for c in data {
        print!(" {}", c);
    }
    println!();
}

// --- Parsing ---

fn parse_definition(text: &str, commands: &BTreeMap<String, Vec<Command>>)
    -> Result<(String, Vec<Command>), String>
{
    let mut tokens = text.split_whitespace();
    let name = tokens.next().ok_or("name absent")?;
    let eq = tokens.next().ok_or(":= missing")?;
    if eq != ":=" {
        return Err(":= missing".into());
    }
    parse_commands(tokens, commands).map(|c| (name.to_string(), c))
}

/// Parse a sequence of tokens into a program.
///
/// `[ ... ]` quotes a program, `{ ... }` builds a `Data` leaf from integers,
/// a bare integer is a scalar leaf, and any other token expands a defined word.
/// The reversal bookkeeping (words expanded reversed, each `]` and the final
/// result reversed) is tada's, so quoted programs end up in run order.
fn parse_commands<'a>(
    tokens: impl Iterator<Item = &'a str>,
    commands: &BTreeMap<String, Vec<Command>>,
) -> Result<Vec<Command>, String> {
    let mut spine: Vec<Vec<Command>> = vec![vec![]];
    let mut leaf: Option<Vec<i64>> = None;
    for token in tokens {
        // Inside `{ ... }` we collect integers into a leaf.
        if let Some(acc) = leaf.as_mut() {
            if token == "}" {
                let values = std::mem::take(acc);
                leaf = None;
                spine.last_mut().unwrap().push(Command::data(Data::leaf(values)));
            } else {
                let v: i64 = token.parse().map_err(|_| format!("non-integer in {{}}: {}", token))?;
                acc.push(v);
            }
            continue;
        }
        match token {
            "{" => leaf = Some(Vec::new()),
            "[" => spine.push(Vec::new()),
            "]" => {
                let mut top = spine.pop().ok_or("imbalanced ]")?;
                top.reverse();
                spine.last_mut().ok_or("imbalanced ]")?.push(Command::program(top));
            }
            "[]" => spine.last_mut().unwrap().push(Command::program(Vec::new())),
            foo => {
                if let Some(name) = foo.strip_prefix('=') {
                    spine.last_mut().unwrap().push(Command::Store(name.to_string()));
                } else if let Some(name) = foo.strip_prefix('$') {
                    spine.last_mut().unwrap().push(Command::Load(name.to_string()));
                } else if let Ok(x) = foo.parse::<i64>() {
                    spine.last_mut().unwrap().push(Command::data(Data::scalar(x)));
                } else {
                    let def = commands.get(foo).ok_or(format!("unknown word: {}", foo))?;
                    spine.last_mut().unwrap().extend(def.iter().rev().cloned());
                }
            }
        }
    }
    if leaf.is_some() {
        return Err("imbalanced {".into());
    }
    if spine.len() != 1 {
        return Err("imbalanced [".into());
    }
    let mut top = spine.pop().unwrap();
    top.reverse();
    Ok(top)
}

/// `.load name path`: read a CSV of integers and bind each column to the value
/// environment as `name0`, `name1`, ... so the language can recall them with
/// `$name0` etc. This is how a relation enters the world -- the only data input
/// besides `{...}` literals and generators like `arc`. The path is a command
/// argument, not a language value, so the noun type needs no string case.
fn load_relation(name: &str, path: &str, env: &mut BTreeMap<String, Noun>) -> Result<(usize, usize), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {}", path, e))?;
    let mut cols: Vec<Vec<i64>> = Vec::new();
    let mut rows = 0;
    for line in text.lines() {
        if line.trim().is_empty() { continue; }
        for (i, field) in line.split(',').enumerate() {
            let v: i64 = field.trim().parse().map_err(|_| format!("non-integer {:?}", field))?;
            if i == cols.len() { cols.push(Vec::new()); }
            cols[i].push(v);
        }
        rows += 1;
    }
    let arity = cols.len();
    for (i, col) in cols.into_iter().enumerate() {
        env.insert(format!("{}{}", name, i), Noun::Data(Data::leaf(col)));
    }
    Ok((rows, arity))
}

// --- REPL ---

fn main() {
    let mut commands: BTreeMap<String, Vec<Command>> = BTreeMap::new();

    // Each primitive is a one-verb program, so words and verbs share a namespace.
    let prims = [
        ("dup", Verb::Dup), ("pop", Verb::Pop), ("swap", Verb::Swap), ("rot", Verb::Rot),
        ("concat", Verb::Concat), ("cons", Verb::Cons), ("cadr", Verb::Cadr),
        ("eval", Verb::Eval), ("ifz", Verb::Ifz), ("print", Verb::Print),
        ("add", Verb::Add), ("sub", Verb::Sub), ("mul", Verb::Mul),
        ("eq", Verb::Eq), ("lt", Verb::Lt), ("shr", Verb::Shr), ("and", Verb::And),
        ("scan", Verb::Scan), ("sum", Verb::Sum), ("gather", Verb::Gather),
        ("scatter", Verb::Scatter), ("find", Verb::Find), ("locate", Verb::Locate), ("gallop", Verb::Gallop), ("sort", Verb::Sort),
        ("iota", Verb::Iota), ("len", Verb::Len), ("layer", Verb::Layer), ("unlayer", Verb::Unlayer),
        ("nlayers", Verb::Nlayers), ("cat", Verb::Cat), ("merge2", Verb::Merge2), ("vsort", Verb::Vsort), ("diff2", Verb::Diff2),
    ];
    for (name, verb) in prims {
        commands.insert(name.to_string(), vec![Command::Verb(verb)]);
    }

    // Words built in the language, grouped into the sections below: tada's scalar
    // Joy (which still runs because a scalar is a length-one column), the
    // query-agnostic columnar toolkit, then two applications -- the triangle demo
    // and the GALEN program. Packing constants live with the section that uses them.
    // tada module: the original scalar Joy/Forth core (runs unchanged -- a scalar is a length-one column).
    let tada = [
        // tada's recursion machinery, verbatim.
        "quote := [ ] cons",
        "fix  := [ dup cons ] swap concat dup cons",
        "iter := swap dup rot rot [ swap ] swap [ [ pop ] ] swap [ swap concat eval ] cons quote [ ifz ] concat concat concat fix eval",
        "then := 1 sub rot dup rot add rot rot",
        "body := dup [ ] [ then ] ifz dup",
        "fib  := 0 1 rot rot [ body ] iter pop pop",
        "mul10 := [ 0 rot [ 1 sub rot dup rot add rot dup ] iter pop pop ] eval",
        "quine := [ dup cons ] dup cons",
    ];
    // Columnar toolkit module: the query-agnostic vocabulary -- column ops, indexing, joins, set/LSM ops, transitive closure.
    let toolkit = [
        // Column utilities.
        "sorted := dup sort gather",            // values in sorted order
        "range  := iota",                       // 0 .. n
        "over   := swap dup rot",               // Joy: copy the 2nd stack item to the top
        // One byte-radix level: `lowbyte` masks the low 8 bits, `bucket` sorts by
        // them (R=256, the measured sweet spot; higher bytes via `<shift> shr`).
        "lowbyte := 255 and",
        "bucket  := dup lowbyte sort gather",
        // Column constructors from a count N (used to build `arc`).
        "zeros   := iota 0 mul",
        "succ    := iota 1 add",
        "succ2   := iota 2 add",
        // `shift1`: right-shift-by-one neighbour view [c0 c0 c1 ... c(n-2)] (gather
        // index iota-1 clamped at 0 via the iota==0 mask -- we have no `max`).
        // `boundaries`: group starts in a SORTED column -- position 0, and any value
        // greater than its predecessor (sortedness lets `lt` stand in for `!=`).
        "shift1     := dup len iota dup 0 eq swap 1 sub add gather",
        "boundaries := dup len iota 0 eq swap dup shift1 swap lt add",
        // `not` negates a 0/1 mask; `headk` takes the first k (no slice verb needed);
        // `where` lists a mask's set positions (negate so 1s sort first, take the
        // prefix); `distinct` keeps the first of each equal run; `trielevel` pairs
        // the distinct keys with their group-start offsets as a child layer.
        "not        := 1 swap sub",
        "headk      := iota gather",
        "where      := dup sum swap not sort swap headk",
        "distinct   := dup boundaries where gather",
        "trielevel  := dup distinct swap boundaries where layer",
        "bincount := 1 scatter",                // histogram: scatter 1s into a dense domain
        "excl     := dup scan swap sub",        // exclusive prefix sum (segment offsets)
        "le       := 1 add lt",                 // b <= a, elementwise
        "gt0      := 0 swap lt",                // 0 < x, elementwise
        // `segids`: expand per-group counts into a group-id per output element -- the
        // run-length expand under probe/extend/plook/the cross-products. Handles 0s.
        "segids  := =cnt $cnt sum =M $cnt excl =noff \
                    $noff $M 1 add 1 scatter $M headk scan 1 sub",
        // --- The WCOJ vocabulary, mirroring datatoad's constructs as words. ---
        // The triangle above is one fused query; these are the reusable pieces,
        // so other cyclic queries (e.g. GALEN's p(x,z):-c(y,w,z),p(x,w),p(x,y))
        // compose from the same words. Datatoad names in brackets.
        //
        // An INDEX is datatoad's Forest/Layer: a relation keyed by one column,
        // its other column sorted within each key, plus offsets+degrees. We bundle
        // it as one columnar value -- values = the sorted neighbours, child layers
        // = [offsets, degrees] -- so it travels the stack as a unit.
        // index: value-sort the composite key*K+val (`vsort` = radix sort + dedup),
        // so duplicate pairs collapse (a relation is a SET -- e.g. GALEN q has many
        // (z,u) with different o, so u repeats per z) and the sort MOVES the data
        // rather than returning a permutation to gather through. Decode val and key
        // straight out of the sorted composite by mask/shift -- no satellite gather,
        // the fields ride in the sorted key -- then bundle the vals with per-key
        // offsets+degrees. arc's edges are already distinct, so the dedup is a no-op
        // there. (`boundaries where` did the dedup before; `vsort` now subsumes it.)
        "index := =D =val =key \
                  $key 2097152 mul $val add vsort =c \
                  $c 2097151 and =vals \
                  $c 21 shr $D bincount =deg \
                  $vals $deg excl layer $deg layer",
        "parts := unlayer swap unlayer rot rot",   // index -> vals off deg
        // propval [join + semijoin]: for the rows in `sel`, PROPOSE candidate
        // values from the smaller index S (expand its segments) and VALIDATE each
        // against the larger index L (the `find` seek). Binds RID (the surviving
        // row of the prefix batch) and CV (the new value). Reads the S*/L*/sel
        // names bound by `meet`.
        "propval := $skey $sel gather =sk $lkey $sel gather =lk \
                    $sdeg $sk gather =cnt $soff $sk gather =sbeg \
                    $loff $lk gather =lbeg $lbeg $ldeg $lk gather add =lend \
                    $cnt segids =grp \
                    $cnt sum iota $sbeg $cnt excl sub $grp gather add =nidx \
                    $svals $nidx gather =ndl \
                    $lvals $lbeg $grp gather $lend $grp gather $ndl find where =keep \
                    $sel $grp gather $keep gather =RID $ndl $keep gather =CV",
        // meet [the per-variable wco_join, 2 atoms]: extend a new variable that
        // two indexes bind, keyed per prefix-row by key1 (into idx1) and key2
        // (into idx2). count both [count], route each row to its smaller side
        // [the argmin partition, here two `where` masks], propose+validate each
        // shard, and union. Leaves (rowid cval): for every surviving extension,
        // which prefix row it came from and the new value. Stack: idx1 idx2 key1 key2.
        "meet := =key2 =key1 \
                 parts =deg2 =off2 =vals2  parts =deg1 =off1 =vals1 \
                 $deg1 $key1 gather =d1 $deg2 $key2 gather =d2 \
                 $vals1 =svals $off1 =soff $deg1 =sdeg $key1 =skey \
                 $vals2 =lvals $off2 =loff $deg2 =ldeg $key2 =lkey \
                 $d1 $d2 le $d1 gt0 mul where =sel propval $RID =rowidA $CV =cvalA \
                 $vals2 =svals $off2 =soff $deg2 =sdeg $key2 =skey \
                 $vals1 =lvals $off1 =loff $deg1 =ldeg $key1 =lkey \
                 $d2 $d1 lt $d2 gt0 mul where =sel propval $RID =rowidB $CV =cvalB \
                 $rowidA $rowidB cat $cvalA $cvalB cat",
        // --- LSM / fixpoint verbs (toward recursive Datalog). ---
        // A relation of pairs is one composite column x*K+z (K=2^21); set ops are
        // sort+dedup over it. `merge` is the LSM union; `diff` is the antijoin
        // (a \ b, via the `find` seek); `extend` is the one-index join (propose
        // each prefix's matches and expand, no validate -- propval minus find).
        "merge := cat sorted distinct",                        // sorted union + dedup
        "diff  := =b =a $a sorted distinct =a $b sorted =bs $bs 0 $bs len $a find not where =kp $a $kp gather",
        "extend := =pay =key parts =deg =off =vals \
                   $deg $key gather =cnt  $off $key gather =sbeg \
                   $cnt segids =grp \
                   $cnt sum iota $sbeg $cnt excl sub $grp gather add =nidx \
                   $vals $nidx gather =nz  $pay $grp gather =nx  $nx $nz",
        // Transitive closure, semi-naive. State lives in env: eidx (edge keyed by
        // first column), Pcomp (closure so far, as a composite set), and the
        // frontier (dx,dy). One round: extend the frontier along edges, keep only
        // genuinely-new pairs (diff vs Pcomp), make them the next frontier, and
        // merge them in. path(x,z) :- edge(x,z) ; path(x,z) :- path(x,y),edge(y,z).
        "tcinit := $e0 $e1 $D index =eidx \
                   $e0 2097152 mul $e1 add sorted distinct =Pcomp \
                   $e0 =dx  $e1 =dy",
        "tcstep := $eidx $dy $dx extend =nz =nx \
                   $nx 2097152 mul $nz add =ncomp \
                   $ncomp $Pcomp diff =dcomp \
                   $dcomp 21 shr =dx  $dcomp 2097151 and =dy \
                   $Pcomp $dcomp merge =Pcomp",
        // The fixpoint: iterate tcstep until the frontier is empty. `iter` is
        // tada's loop -- it runs the closure and repeats while the number on top
        // is nonzero -- so the body leaves the new frontier's size, and we prime
        // it with the initial size. `iter` leaves that priming number behind, so
        // `pop` it. Needs $e0,$e1,$D bound (the edge relation and a key bound).
        // Returns the closure size. This is the Datalog fixpoint, as words.
        "tc := tcinit $dx len [ tcstep $dx len ] iter pop $Pcomp len",
        // --- LSM as a list of sorted runs (a Data whose layers are the runs). ---
        // The win: appending a delta is one O(1) `layer`, never a merge of the
        // small new batch into the big closure. Membership (`member`) folds the
        // query over the runs with `iter`: peel a run with `unlayer`, `find` the
        // query in it, OR the hits in (runs are disjoint here, so add suffices).
        // `nlayers` drives the fold. No consolidation yet -- run count = rounds.
        "member := =P =q $q 0 mul =acc $P =Pw $P nlayers =k \
                   $k [ $Pw unlayer =run =Pw \
                        $run 0 $run len $q find $acc add =acc \
                        $k 1 sub =k $k ] iter pop  $acc",
        "diffL := =P =q $q vsort =q  $P =Pw $P nlayers =k \
                  $k [ $Pw unlayer =run =Pw  $q $run diff2 =q  $k 1 sub =k $k ] iter pop  $q",  // distinct q \ runs(P)
        // Transitive closure with the run-list closure (no re-sort of P each round;
        // the new delta becomes a fresh sorted run). `total` tracks the closure
        // size incrementally (runs are disjoint), so no final fold is needed.
        "tcinitL := $e0 $e1 $D index =eidx \
                    $e0 2097152 mul $e1 add sorted distinct =baserun \
                    { } $baserun layer =Pruns  $baserun len =total \
                    $baserun 21 shr =dx  $baserun 2097151 and =dy",
        // Consolidation: the binary-counter cascade that earns the LSM its name.
        // `mergetop` unions the top two runs with `merge2` -- a linear pass over two
        // already-sorted runs, not a re-sort of their concatenation. `mflag` is the
        // merge test: 1 iff there are >=2 runs and the top is more than half the
        // second (2*top > second) -- so a merge only ever combines comparable sizes,
        // and the survivors at least double going down, keeping the run count
        // O(log N). `consolidate` runs the cascade with `iter`. We peek run sizes by
        // `unlayer`ing a cheap (Arc) dup of Pruns.
        "mergetop := $Pruns unlayer =a =P1 $P1 unlayer =b =P2 \
                     $P2 $a $b merge2 layer =Pruns",
        "mflag := $Pruns nlayers 2 lt \
                  [ $Pruns =Pd $Pd unlayer =t1 =Pd $Pd unlayer =t2 =Pd \
                    $t2 len $t1 len 2 mul lt ] [ 0 ] ifz",
        "consolidate := =Pruns mflag [ mergetop mflag ] iter pop $Pruns",  // stack: P -> consolidated P
        // Collapsing consolidation, for a QUERY-heavy closure (the Datalog fixpoint
        // probes its closures many times per round). The geometric rule above keeps
        // appends O(1) but leaves a stack of runs -- and since fixpoint deltas shrink
        // over rounds, that rule rarely fires, so the stack grows ~one run per round.
        // Every seek then binary-searches every run. `consolidateC` instead merges
        // all the way down to a single run (`mflagC` fires while >=2 runs remain), so
        // each seek folds over one run, datatoad-style. `merge2` makes the per-round
        // full merge linear, so collapsing is cheap. Stack: P -> single-run P.
        "mflagC := $Pruns nlayers 2 lt not",
        "consolidateC := =Pruns mflagC [ mergetop mflagC ] iter pop $Pruns",
        "tcstepL := $eidx $dy $dx extend =nz =nx \
                    $nx 2097152 mul $nz add =ncomp \
                    $ncomp $Pruns diffL =dcomp \
                    $dcomp 21 shr =dx  $dcomp 2097151 and =dy \
                    $Pruns $dcomp sorted layer consolidate =Pruns \
                    $total $dcomp len add =total",
        "tcL := tcinitL $dx len [ tcstepL $dx len ] iter pop $total",
        // --- Sorted index for sparse (e.g. pair) keys, via `locate`. ---
        // `pindex` builds, from a key column K and value column V, a Data bundling
        // the values sorted-by-key with [distinct-keys, offsets, degrees] -- the
        // sorted analogue of `index` (which needs dense small keys). The distinct
        // keys get a huge sentinel and degrees a 0, so `locate`'s position (0..nd)
        // is always a safe gather. `plook` looks queries Q up in such an index and
        // expands each hit's segment: returns (rowid, value), rowid linking back
        // to the query -- so payloads keyed by a pair are recovered in batch.
        "pindex := =V =K \
                   $K sort =perm  $K $perm gather =Ks  $V $perm gather =Vs \
                   $Ks boundaries where =bp \
                   $Ks $bp gather =DKr  $DKr len =nd \
                   $bp $Vs len cat =OFFp \
                   $OFFp $nd iota 1 add gather $bp sub =DEGr \
                   $Vs $DKr 1152921504606846976 cat layer $OFFp layer $DEGr 0 cat layer",
        "plook := =pidx =Q \
                  $pidx unlayer =DEG unlayer =OFF unlayer =DK =VALS \
                  $DK len 1 sub =nd \
                  $DK 0 $nd $Q locate =pos \
                  $DK $pos gather $Q eq =mat \
                  $DEG $pos gather $mat mul =cnt \
                  $OFF $pos gather =sbeg \
                  $cnt segids =grp \
                  $cnt sum iota $sbeg $cnt excl sub $grp gather add =nidx \
                  $grp  $VALS $nidx gather",
        // GALEN rule 6's FULL head: q(x,e,o) :- q(x,y,z), q(z,u,o), r(y,u,e).
        // `meet` gives the (x,y,z,u) triangle survivors; then look up the o's by
        // pair (z,u) in q and the e's by pair (y,u) in r (sorted pair-indexes),
        // and cross-product o x e per survivor (expand the o-table by each
        // survivor's e-count, pick the e by local index -- no division). Dedup
        // the (x,e,o) triples. Run `.load q Q.csv` and `.load r R.csv` first.
        // GALEN rule 4: p(x,z) :- p(y,w), u(w,r,z), q(x,r,y). Same shape as rule 6:
        // triangle on (y,w,r) seeded by p-edges, extend r in u-by-w intersect
        // q-by-y; payloads z by (w,r) in u and x by (r,y) in q, cross-product to
        // p(x,z). Binary output (x*2^19+z). Run .load p/.load u/.load q first.
        // GALEN rule 5: p(x,z) :- c(y,w,z), p(x,w), p(x,y). Triangle on (x,w,y)
        // seeded by c; x in in_p(w) intersect in_p(y) (both from p-by-target), and
        // z is the seed's payload (no cross-product). Dormant on base p (=0), so
        // verify on a synthetic case. Binary output p(x,z).
        // `flatten` folds `cat` over a closure's runs back into one column (runs are
        // disjoint, so this is the whole distinct relation); `galeng` uses it to size
        // the final closures.
        "flatten := =P { } =acc $P =Pw $P nlayers =k \
                    $k [ $Pw unlayer =run =Pw  $acc $run cat =acc  $k 1 sub =k $k ] iter pop  $acc",
        // --- Incremental index: the closure runs ARE the index. ---
        // A run sorted by composite key*K+val is already a first-column index: the
        // tuples whose first field is y occupy the contiguous range [y*K, (y+1)*K).
        // So we need not flatten+rebuild an index from the closure each round; we
        // range-`locate` directly into the runs. `probe` reads a batch of query
        // keys Q against a whole run-LIST P (a Data whose layers are sorted runs),
        // locates each key's span in every run, and expands the matches. It returns
        // (rid, val): rid indexes back into Q (so any payload gathers by it), val is
        // the matched second field. It is `member` (fold over runs) and `extend`
        // (segment-expand) fused into the run-list probe. Stack: Q K P -> rid val.
        "probe := =P =K =Q \
                  $Q $K mul =qlo  $qlo $K add =qhi  $K 1 sub =kmask \
                  { } =rid  { } =val \
                  $P =Pw  $P nlayers =k \
                  $k [ $Pw unlayer =R =Pw \
                       $R 0 $R len $qlo locate =beg \
                       $R $beg $R len $qhi gallop =end \
                       $end $beg sub =cnt \
                       $cnt segids =grp \
                       $cnt sum iota  $beg $cnt excl sub $grp gather add =nidx \
                       $R $nidx gather $kmask and =vv \
                       $rid $grp cat =rid  $val $vv cat =val \
                       $k 1 sub =k $k ] iter pop \
                  $rid $val",
        // --- meet over run-lists: the WCOJ core, no index bundle, no re-locate. ---
        // `meetr` extends a new variable bound by two relations, keyed per prefix-row
        // by key1 (into P1) and key2 (into P2); returns (rowid, cval). The meet's two
        // indexes are always a single run (collapsed closures / static `runs1`), so it
        // takes that run directly. It locates each key's [beg,end) span ONCE: the count
        // end-beg routes each row to its smaller side (the two `where` masks), and the
        // SAME beg/end expand that side's candidates -- no re-locate, the redundancy
        // the old degr+probe pair carried. A candidate c proposed from P1 by key1
        // survives iff (key2,c) is in P2: compose key2*K+c and `find` it in P2's run.
        // Stack: P1 P2 K key1 key2 -> rowid cval.
        "meetr := =mk2 =mk1 =mK =mP2 =mP1  $mK 1 sub =kmask \
                  $mP1 unlayer =mr1 pop  $mP2 unlayer =mr2 pop \
                  $mr1 0 $mr1 len $mk1 $mK mul locate =b1  $mr1 $b1 $mr1 len $mk1 $mK mul $mK add gallop =e1  $e1 $b1 sub =mc1 \
                  $mr2 0 $mr2 len $mk2 $mK mul locate =b2  $mr2 $b2 $mr2 len $mk2 $mK mul $mK add gallop =e2  $e2 $b2 sub =mc2 \
                  $mc1 $mc2 le $mc1 gt0 mul where =selA \
                  $b1 $selA gather =bA  $e1 $selA gather =eA  $eA $bA sub =cntA  $cntA segids =grpA \
                  $cntA sum iota  $bA $cntA excl sub $grpA gather add =nidxA \
                  $mr1 $nidxA gather $kmask and =cvA \
                  $mk2 $selA gather $grpA gather $mK mul $cvA add =compA \
                  $mr2 0 $mr2 len $compA find where =kpA \
                  $selA $grpA gather $kpA gather =rowidA  $cvA $kpA gather =cvalA \
                  $mc2 $mc1 lt $mc2 gt0 mul where =selB \
                  $b2 $selB gather =bB  $e2 $selB gather =eB  $eB $bB sub =cntB  $cntB segids =grpB \
                  $cntB sum iota  $bB $cntB excl sub $grpB gather add =nidxB \
                  $mr2 $nidxB gather $kmask and =cvB \
                  $mk1 $selB gather $grpB gather $mK mul $cvB add =compB \
                  $mr1 0 $mr1 len $compB find where =kpB \
                  $selB $grpB gather $kpB gather =rowidB  $cvB $kpB gather =cvalB \
                  $rowidA $rowidB cat  $cvalA $cvalB cat",
        // Build a one-run run-list from a relation's (key,val) columns: composite
        // key*K+val, sorted+deduped, wrapped as a single layer. The run IS the
        // composite, so no decode is needed; `vsort` (value radix sort + dedup) is
        // exactly `sorted distinct` here, but moves the data instead of building a
        // permutation to gather through. The test harness for `meetr` -- the real
        // cyclic round feeds it the maintained closure run-lists.
        "runs1 := =K =V =Kc  $Kc $K mul $V add vsort =c1r  { } $c1r layer",
    ];
    // Triangle module: the worst-case-optimal triangle join on `arc` (a demo query).
    let triangle = [
        // Binary pair radix: a pair (a,b) packed as a*rk+b, rk = 2^21 > any arc node
        // id. `tri3r`/`gr6corer` use it; the LSM/TC use the same 2^21 written inline.
        "rk := 2097152",
        // `arc(N)`: the triangle-query relation -- edges 0->(1..N), (1..N)->0,
        // (1..N)->(2..N+1), unioned. Leaves two columns, src (deeper) and dst, 3N each.
        "arcsrc  := dup zeros over succ cat swap succ cat",
        "arcdst  := dup succ over zeros cat swap succ2 cat",
        "arc     := dup arcsrc swap arcdst",
        // Adjacency (CSR): `outnbr` reorders dst into src order (out-neighbour lists),
        // `innbr` reorders src into dst order; degrees are `<keys> D bincount`.
        "outnbr  := swap sort gather",
        "innbr   := sort gather",
        "isect   := over len 0 rot swap find sum",   // |sorted A in B|: gallop each A into B, count
        // The triangle, now composed from the vocabulary. out = arc keyed by src
        // (out-neighbours), in = arc keyed by dst (in-neighbours). Per edge (a,b),
        // extend c in out(b) intersect in(a): idx1=out keyed by b=dst, idx2=in
        // keyed by a=src. rowid indexes the edge batch, so a,b are gathered back.
        "tri3 := =n $n arc =dst =src $n 2 add =d \
                 $src $dst $d index =out $dst $src $d index =in \
                 $out $in $dst $src meet =cval =rowid $rowid len",
        "tri3p := =n $n arc =dst =src $n 2 add =d \
                  $src $dst $d index =out $dst $src $d index =in \
                  $out $in $dst $src meet =cval =rowid \
                  $src $rowid gather $dst $rowid gather layer $cval layer",
        // The triangle through `meetr`, to check it against tri3 (6, then 2999997).
        // out = arc keyed by src (src*rk+dst); in = arc keyed by dst (dst*rk+src).
        // Extend c per edge (a=src,b=dst): out(b) intersect in(a). key1=dst, key2=src.
        "tri3r := =n $n arc =dst =src \
                  $src $dst rk runs1 =outR  $dst $src rk runs1 =inR \
                  $outR $inR rk $dst $src meetr =cv =rid $rid len",
    ];
    // GALEN module: the six-rule mutually-recursive ontology program (`galeng`).
    let galen = [
        // Packing constants: a ternary fact is one base-2^19 key in an i64,
        // x*pkk + r*pk + z (pk = 2^19 > any GALEN node id; pkk = pk^2; pmask = pk-1
        // the low-field mask; pbits/pbits2 the field shifts; nd a node-id domain
        // bound for the dense bincount indexes). Decode via shr/and by 19 and 38.
        "pk := 524288", "pkk := 274877906944", "pmask := 524287",
        "pbits := 19", "pbits2 := 38", "nd := 1048576",
        // GALEN rule 6's join core, on loaded base data (run `.load q .../Q.csv`
        // and `.load r .../R.csv` first). The rule is
        //   q(x,e,o) :- q(x,y,z), q(z,u,o), r(y,u,e).
        // a triangle on (y,z,u): seed with q's rows (x,y,z), extend u in
        // q-by-z intersect r-by-y. idxzu = q keyed by z (=col0 of the q(z,u,o)
        // use) giving u; idxyu = r keyed by y giving u. The SAME relation q
        // supplies the seed and one index -- the "q twice" of a cyclic rule.
        // This is the meet core (the (x,y,z,u) survivors); the payload head
        // (o,e cross-product + dedup -> 25753) needs sorted-index lookups still
        // to come. D = 2^20 > any GALEN node id. Returns the survivor count.
        "gr6core := nd =D \
                    $q0 $q1 $D index =idxzu  $r0 $r1 $D index =idxyu \
                    $idxzu $idxyu $q2 $q1 meet =u =rowid $rowid len",
        // GALEN rule 6 core through `meetr`, to check it against gr6core (24796).
        // idxzu = q keyed by z giving u (q0*rk+q1); idxyu = r keyed by y giving u.
        // key1=z=q2, key2=y=q1.
        "gr6corer := $q0 $q1 rk runs1 =idxzuR  $r0 $r1 rk runs1 =idxyuR \
                     $idxzuR $idxyuR rk $q2 $q1 meetr =u =rid $rid len",
        // The full cyclic-rule heads over run-lists -- single-application verifiers
        // (one round on base data) that check `meetr` plus `probe`-as-payload-lookup
        // plus the cross-product against datatoad's reference counts. `runs1` gives
        // the distinct meet-side projections, and `probe K=pk` (a pair-key range scan)
        // is the payload lookup -- it returns (rid, val) expanded. The pair-key payload
        // index is a `runs1` whose key is itself a packed pair: `(a*pk+b) c pk runs1`
        // keys by (a,b) and yields c.
        // Rule 6: q(x,e,o):-q(x,y,z),q(z,u,o),r(y,u,e). o by (z,u) in q, e by (y,u)
        // in r, cross-product, dedup -> 25753.
        "gr6r := $q0 $q1 pk runs1 =idxzu  $r0 $r1 pk runs1 =idxyu \
                 $idxzu $idxyu pk $q2 $q1 meetr =uu =rid0 \
                 $q0 $rid0 gather =sx  $q2 $rid0 gather =sz  $q1 $rid0 gather =sy \
                 $q0 pk mul $q1 add $q2 pk runs1 =zuix \
                 $r0 pk mul $r1 add $r2 pk runs1 =yuix \
                 $sz pk mul $uu add pk $zuix probe =oval =orid \
                 $sy pk mul $uu add pk $yuix probe =eval =erid \
                 $rid0 len =S  $erid $S bincount =ce  $ce excl =eoff \
                 $ce $orid gather =ocnt  $ocnt segids =grp2 \
                 $ocnt sum iota  $ocnt excl $grp2 gather sub =locale \
                 $orid $grp2 gather =sout  $oval $grp2 gather =oout \
                 $eval $eoff $sout gather $locale add gather =eout \
                 $sx $sout gather =xout \
                 $xout pkk mul $eout pk mul add $oout add sorted distinct len",
        // Rule 4: p(x,z):-p(y,w),u(w,r,z),q(x,r,y). r in u-by-w intersect q-by-y;
        // z by (w,r) in u, x by (r,y) in q, cross-product -> p(x,z). -> 112207.
        "gr4r := $u0 $u1 pk runs1 =uwr  $q2 $q1 pk runs1 =qyr \
                 $uwr $qyr pk $p1 $p0 meetr =rr =rid0 \
                 $p0 $rid0 gather =sy  $p1 $rid0 gather =sw \
                 $u0 pk mul $u1 add $u2 pk runs1 =uzix \
                 $q1 pk mul $q2 add $q0 pk runs1 =qxix \
                 $sw pk mul $rr add pk $uzix probe =zval =zrid \
                 $rr pk mul $sy add pk $qxix probe =xval =xrid \
                 $rid0 len =S  $xrid $S bincount =cx  $cx excl =xoff \
                 $cx $zrid gather =zcnt  $zcnt segids =grp2 \
                 $zcnt sum iota  $zcnt excl $grp2 gather sub =locale \
                 $zrid $grp2 gather =sout  $zval $grp2 gather =zout \
                 $xval $xoff $sout gather $locale add gather =xout \
                 $xout pk mul $zout add sorted distinct len",
        // Rule 5: p(x,z):-c(y,w,z),p(x,w),p(x,y). x in in_p(w) intersect in_p(y),
        // z from the seed c (no cross-product). Dormant on base p, so -> 0.
        "gr5r := $p1 $p0 pk runs1 =inp \
                 $inp $inp pk $c1 $c0 meetr =xx =rid0  $c2 $rid0 gather =zz \
                 $xx pk mul $zz add sorted distinct len",

        // --- Full GALEN, truly O(delta): all six rules to the joint fixpoint, every
        // firing driven by a delta atom against the maintained run-list closures.
        // No round rebuilds an index or materializes a full closure column. GALEN
        // ids fit pk=2^19, so p and q share the pk radix and `meetr` needs one K.
        // Closures (env): gpab/gpba (p by 1st/2nd col), gq012 (q full triple, by 1st
        // col or by (col0,col1) pair), gq01 (distinct col0,col1), gq21 (distinct
        // col2,col1), gq120 (q reordered col1,col2,col0). Static base indexes built
        // once: sR, r01R/r10R/r012R, u01R/u10R/u012R, c10R/c01R/c012R.
        //
        // The easy rules (one-index joins; the delta drives, payloads ride):
        //   g1a/g1b: rule 1 (p:-p,p), probe gpab/gpba.
        //   g2a: Dp,q -- probe gq012 by y (K=pkk gives packed (r,z)); g2b: p,Dq --
        //        probe gpba by y for predecessors.
        //   g3: rule 3 (q:-q,s), the delta drives the static s-index.
        "g1a := $dp1 pk $gpab probe =z1 =rid1  $dp0 $rid1 gather pk mul $z1 add",
        "g1b := $dp0 pk $gpba probe =x2 =rid2  $x2 pk mul  $dp1 $rid2 gather add",
        "g2a := $dp1 pkk $gq012 probe =rzA =ridA  $dp0 $ridA gather pkk mul $rzA add",
        "g2b := $dq0 pk $gpba probe =xB =ridB  $xB pkk mul  $dq1 $ridB gather pk mul add  $dq2 $ridB gather add",
        "g3  := $dq1 pk $sR probe =eE =ridE  $dq0 $ridE gather pkk mul  $eE pk mul add  $dq2 $ridE gather add",
        // Rule 6 q(x,e,o):-q(x,y,z),q(z,u,o),r(y,u,e), triangle on (y,z,u), delta in
        // each q-atom. g6a: Dq is the seed q(x,y,z) -- meet u in q-by-z (gq01) and
        // r-by-y (r01R), then o by (z,u) in gq012, e by (y,u) in r012R, cross o x e.
        // g6b: Dq is the q(z,u,o) atom -- now (z,u) is fixed and y is the free var:
        // meet y in q-by-z (gq21, distinct col2,col1) and r-by-u (r10R), then x by
        // (y,z) in gq120, e by (y,u) in r012R, cross x x e; o rides from the delta.
        "g6a := $gq01 $r01R pk $dq2 $dq1 meetr =uu =rid0 \
                $dq0 $rid0 gather =sx  $dq2 $rid0 gather =sz  $dq1 $rid0 gather =sy \
                $sz pk mul $uu add pk $gq012 probe =oval =orid \
                $sy pk mul $uu add pk $r012R probe =eval =erid \
                $rid0 len =S  $erid $S bincount =ce  $ce excl =eoff \
                $ce $orid gather =ocnt  $ocnt segids =grp2 \
                $ocnt sum iota  $ocnt excl $grp2 gather sub =locale \
                $orid $grp2 gather =sout  $oval $grp2 gather =oout \
                $eval $eoff $sout gather $locale add gather =eout \
                $sx $sout gather =xout \
                $xout pkk mul $eout pk mul add $oout add",
        "g6b := $gq21 $r10R pk $dq0 $dq1 meetr =yy =rid0 \
                $dq0 $rid0 gather =sz  $dq2 $rid0 gather =so \
                $yy pk mul $sz add pk $gq120 probe =xval =xrid \
                $yy pk mul $dq1 $rid0 gather add pk $r012R probe =eval =erid \
                $rid0 len =S  $erid $S bincount =ce  $ce excl =eoff \
                $ce $xrid gather =xcnt  $xcnt segids =grp2 \
                $xcnt sum iota  $xcnt excl $grp2 gather sub =locale \
                $xrid $grp2 gather =sout  $xval $grp2 gather =xout2 \
                $eval $eoff $sout gather $locale add gather =eout \
                $so $sout gather =oout \
                $xout2 pkk mul $eout pk mul add $oout add",
        // Rule 4 p(x,z):-p(y,w),u(w,r,z),q(x,r,y), triangle on (w,r,y). g4a: Dp is
        // the seed p(y,w) -- meet r in u-by-w (u01R) and q-by-y (gq21), then z by
        // (w,r) in u012R, x by (r,y) in gq120, cross z x x. g4b: Dq is q(x,r,y) --
        // (r,y) fixed, w free: meet w in u-by-r (u10R) and p-by-y (gpab), z by (w,r)
        // in u012R, x rides from the delta (single payload, no cross-product).
        // CONTIGUITY INVARIANT (the subtle one): a cross-product's offset+locale
        // indexing (`vals[off[survivor]+locale]`) assumes that side's `probe` output
        // is laid out contiguously per survivor. `probe` folds over runs and `cat`s
        // their outputs, so a multi-run closure's output is NOT contiguous per
        // survivor -- only a single-run (static base) index is. So the offset-indexed
        // side MUST be the static one; the closure side is gathered by `grp2` (which
        // needs no contiguity). g6a/g6b offset-index eval (static r); g4a offset-
        // indexes zval (static u) and grp2-gathers xval (the q closure).
        "g4a := $u01R $gq21 pk $dp1 $dp0 meetr =rr =rid0 \
                $dp1 $rid0 gather =sw  $dp0 $rid0 gather =sy \
                $sw pk mul $rr add pk $u012R probe =zval =zrid \
                $rr pk mul $sy add pk $gq120 probe =xval =xrid \
                $rid0 len =S  $zrid $S bincount =cz  $cz excl =zoff \
                $cz $xrid gather =xcnt  $xcnt segids =grp2 \
                $xcnt sum iota  $xcnt excl $grp2 gather sub =locale \
                $xrid $grp2 gather =sout  $xval $grp2 gather =xout \
                $zval $zoff $sout gather $locale add gather =zout \
                $xout pk mul $zout add",
        "g4b := $u10R $gpab pk $dq1 $dq2 meetr =ww =rid0 \
                $dq1 $rid0 gather =sr  $dq0 $rid0 gather =sx \
                $ww pk mul $sr add pk $u012R probe =zval =zrid \
                $sx $zrid gather pk mul $zval add",
        // Rule 5 p(x,z):-c(y,w,z),p(x,w),p(x,y), triangle on (x,w,y); z rides from
        // the static c seed, so neither firing cross-products. g5a: Dp is p(x,w) --
        // meet y in c-by-w (c10R) and p-by-x (gpab); g5b: Dp is p(x,y) -- meet w in
        // c-by-y (c01R) and p-by-x (gpab). z by (y,w) in c012R either way.
        "g5a := $c10R $gpab pk $dp1 $dp0 meetr =yy =rid0 \
                $dp1 $rid0 gather =sw  $dp0 $rid0 gather =sx \
                $yy pk mul $sw add pk $c012R probe =zval =zrid \
                $sx $zrid gather pk mul $zval add",
        "g5b := $c01R $gpab pk $dp1 $dp0 meetr =ww =rid0 \
                $dp1 $rid0 gather =sy  $dp0 $rid0 gather =sx \
                $sy pk mul $ww add pk $c012R probe =zval =zrid \
                $sx $zrid gather pk mul $zval add",
        // One round: decode the two deltas, fire the rules, keep genuinely-new facts
        // (diffL vs the closures, which already include last round's delta), append
        // the survivors to every ordering (projecting/reordering as needed), and
        // consolidate each. Returns the round's new-fact count.
        "roundg := $gdp pbits shr =dp0  $gdp pmask and =dp1 \
                   $gdq pbits2 shr =dq0  $gdq pbits shr pmask and =dq1  $gdq pmask and =dq2 \
                   g1a g1b cat g4a cat g4b cat g5a cat g5b cat =np \
                   g2a g2b cat g3 cat g6a cat g6b cat =nq \
                   $np $gpab diffL =dpnew  $nq $gq012 diffL =dqnew \
                   $gpab $dpnew layer consolidateC =gpab \
                   $dpnew pbits shr =np0  $dpnew pmask and =np1  $np1 pk mul $np0 add vsort =dpba \
                   $gpba $dpba layer consolidateC =gpba \
                   $dqnew pbits2 shr =nq0  $dqnew pbits shr pmask and =nq1  $dqnew pmask and =nq2 \
                   $gq012 $dqnew layer consolidateC =gq012 \
                   $nq1 pkk mul $nq2 pk mul add $nq0 add vsort =d120  $gq120 $d120 layer consolidateC =gq120 \
                   $nq0 pk mul $nq1 add vsort =proj01  $proj01 $gq01 diffL =d01  $gq01 $d01 layer consolidateC =gq01 \
                   $nq2 pk mul $nq1 add vsort =proj21  $proj21 $gq21 diffL =d21  $gq21 $d21 layer consolidateC =gq21 \
                   $dpnew =gdp  $dqnew =gdq \
                   $dpnew len $dqnew len add",
        // Seed all closures + deltas from base, build static indexes.
        "ginit := $p0 pk mul $p1 add sorted distinct =pb  { } $pb layer =gpab  $pb =gdp \
                  $p1 pk mul $p0 add sorted distinct =pbb  { } $pbb layer =gpba \
                  $q0 pkk mul $q1 pk mul add $q2 add sorted distinct =qb  { } $qb layer =gq012  $qb =gdq \
                  $q1 pkk mul $q2 pk mul add $q0 add sorted distinct =qb120  { } $qb120 layer =gq120 \
                  $q0 pk mul $q1 add sorted distinct =qb01  { } $qb01 layer =gq01 \
                  $q2 pk mul $q1 add sorted distinct =qb21  { } $qb21 layer =gq21 \
                  $s0 $s1 pk runs1 =sR \
                  $r0 $r1 pk runs1 =r01R  $r1 $r0 pk runs1 =r10R  $r0 pk mul $r1 add $r2 pk runs1 =r012R \
                  $u0 $u1 pk runs1 =u01R  $u1 $u0 pk runs1 =u10R  $u0 pk mul $u1 add $u2 pk runs1 =u012R \
                  $c1 $c0 pk runs1 =c10R  $c0 $c1 pk runs1 =c01R  $c0 pk mul $c1 add $c2 pk runs1 =c012R",
        "galeng := ginit 1 [ roundg ] iter pop  $gpab flatten len  $gq012 flatten len",
    ];
    // Modules load in dependency order: each only references words from
    // itself or an earlier module.
    let modules: [(&str, &[&str]); 4] = [
        ("tada", &tada), ("toolkit", &toolkit),
        ("triangle", &triangle), ("galen", &galen),
    ];

    for (name, defs) in modules {
        println!("module {} ({} defs):", name, defs.len());
        for &line in defs {
            match parse_definition(line, &commands) {
                Ok((nm, prog)) => { commands.insert(nm, prog); }
                Err(err) => println!("  {}: error: {}  [{}]", name, err, line),
            }
        }
    }

    let mut data: Vec<Command> = Vec::new();
    let mut todo: Vec<Command> = Vec::new();
    // The value environment: names bound by `=x`, read by `$x`. Persists across
    // REPL lines, so you can bind on one line and use it on the next.
    let mut env: BTreeMap<String, Noun> = BTreeMap::new();

    use std::io::Write;
    println!();
    print!("> ");
    let _ = std::io::stdout().flush();

    let mut text = String::new();
    while let Ok(size) = std::io::stdin().read_line(&mut text) {
        if size == 0 {
            break;
        }
        if let Some(rest) = text.trim().strip_prefix(".load") {
            let mut it = rest.split_whitespace();
            match (it.next(), it.next()) {
                (Some(nm), Some(path)) => match load_relation(nm, path, &mut env) {
                    Ok((rows, arity)) => println!("loaded {} rows, {} cols into {}0..{}{}",
                                                  rows, arity, nm, nm, arity - 1),
                    Err(e) => println!("error: {}", e),
                },
                _ => println!("usage: .load <name> <path>"),
            }
            print!("> ");
            let _ = std::io::stdout().flush();
            text.clear();
            continue;
        }
        if let Ok((name, prog)) = parse_definition(&text, &commands) {
            commands.insert(name, prog);
        } else {
            match parse_commands(text.split_whitespace(), &commands) {
                Ok(prog) => {
                    let timer = std::time::Instant::now();
                    todo.extend(prog);
                    let ops = reduce_stacks(&mut data, &mut todo, &mut env);
                    print_stack(&data);
                    println!("ops: {}  time: {:?}", ops, timer.elapsed());
                }
                Err(err) => println!("error: {}", err),
            }
        }
        print!("> ");
        let _ = std::io::stdout().flush();
        text.clear();
    }
}
