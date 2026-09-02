//! A small ML-flavoured expression surface, concatenative-by-juxtaposition: a value is followed by
//! its operator stages with no separator (`input iota fold_add`). `let` boils binding into shared
//! edges (no re-derivation), with product destructuring; lambdas (`x -> …`) are the map/match bodies;
//! sums are built/eliminated via `inject`/`match`. A stage-chain runs until a token that can't begin a
//! stage — notably the `let` body's `in`, the one identifier allowed to follow a complete chain.
//!
//!   expr   = 'let' pat '=' expr 'in' expr
//!          | 'enum' IDENT '=' IDENT shape? ('|' IDENT shape?)* 'in' expr  -- a compile-time table; names ERASE here
//!   shape  = 'u8'|'u16'|'u32'|'u64' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
//!          | pipe
//!   pat    = IDENT | '(' IDENT (',' IDENT)* ')'
//!   pipe   = proj apply*                               -- juxtaposition; chain ends before `in`
//!   apply  = 'map' '(' lambda ')'
//!          | ('fold' | 'scan') '(' lambda ')'                 -- (seed, list); lambda is (acc, x)
//!          | 'map_variant' tag '(' lambda ')'
//!          | 'match' '(' (tag '(' lambda ')')(',' …)* ')'   -- MapSum + Unwrap
//!          | 'inject' VARIANT                               -- sum construction (its enum fully shaped)
//!          | 'branch' (NUM | ENUM)                          -- lane count, literal or by enum name
//!          | 'split' STR                                    -- delimiter as a one-byte string
//!          | BINARY NUM                                     -- immediate: `x sub 1` ≡ `(x, x lit 1) sub`
//!          | IDENT NUM?
//!   tag    = NUM | VARIANT                              -- a variant name resolves to its tag
//!   lambda = pat '->' expr                              -- a tuple pattern destructures the parameter
//!   proj   = atom ('.' NUM)*
//!   atom   = '(' expr (',' expr)* ')' | IDENT          -- 'input' is the root
//!
//! e.g.  let (subj, vals) = input.1 transpose in vals fold_add
//!       e match (0 (lo -> lo), 1 (hi -> hi add_u64 100))   -- exhaustive ⇒ Unwrap types it
//!       enum Size = Lo | Hi in … match (Lo (l -> l), Hi (h -> h add 100))
//!       enum Opt = None () | Some u64 in xs inject Some  -- tag xs into Some; None is an empty unit lane

use super::{pair_imm, resolve, str_value, takes_num};
use crate::graph::{Builder, Graph, Node, NodeKind};
use crate::ops::{NumOp, Op};
use crate::shape::Shape;
use crate::value::Value;
use std::collections::HashMap;

// ----- tokens ------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Arrow, // ->
    Dot,
    LParen,
    RParen,
    Comma,
    Eq,
    Bar, // | — the variant separator in an `enum` declaration
    Ident(String),
    Num(u64),
    Str(Vec<u8>),
}

fn lex(s: &str) -> Result<Vec<Tok>, String> {
    let cs: Vec<char> = s.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '-' if cs.get(i + 1) == Some(&'>') => {
                toks.push(Tok::Arrow);
                i += 2;
            }
            '.' => {
                toks.push(Tok::Dot);
                i += 1;
            }
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '=' => {
                toks.push(Tok::Eq);
                i += 1;
            }
            '|' => {
                toks.push(Tok::Bar);
                i += 1;
            }
            '"' => {
                i += 1; // opening quote
                let mut bytes = Vec::new();
                loop {
                    match cs.get(i) {
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            bytes.extend_from_slice(ch.encode_utf8(&mut [0; 4]).as_bytes());
                            i += 1;
                        }
                        None => return Err("unterminated string literal".to_string()),
                    }
                }
                toks.push(Tok::Str(bytes));
            }
            c if c.is_ascii_digit() => {
                let mut n = 0u64;
                while i < cs.len() && cs[i].is_ascii_digit() {
                    n = n * 10 + cs[i].to_digit(10).unwrap() as u64;
                    i += 1;
                }
                toks.push(Tok::Num(n));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut w = String::new();
                while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                    w.push(cs[i]);
                    i += 1;
                }
                toks.push(Tok::Ident(w));
            }
            _ => return Err(format!("unexpected character '{c}'")),
        }
    }
    Ok(toks)
}

// ----- AST ---------------------------------------------------------------

enum Pat {
    Name(String),
    Tuple(Vec<String>),
}

enum Apply {
    Op(String, Option<u64>),
    BinImm(String, u64), // pair op + immediate: `x sub 1` desugars to `(x, x lit 1) sub`
    Str(Vec<u8>),
    Map(Pat, Box<E>),
    Fold(Pat, Box<E>), // (B, List<A>) folded by a binary body; the lambda's tuple pattern is (acc, x)
    Scan(Pat, Box<E>), // (B, List<A>) scanned by a binary body; inclusive running accumulator
    FoldScan(Pat, Box<E>), // (T, List<A>) -> (T, List<R>); body (acc, x) -> (new state, output R)
    MapVariant(usize, Pat, Box<E>),
    Match(Vec<(usize, Pat, E)>), // arms (tag, binding, body) -> MapSum + Unwrap
    Inject(usize, Vec<Shape>),    // tag + the declared sum's lane shapes -> Op::Inject
    Head, // `head`: sugar for `(lit 0, list) get` — the first element (an empty row errs)
}

enum E {
    Var(String),
    Tuple(Vec<E>),
    Proj(Box<E>, usize),
    Let(Pat, Box<E>, Box<E>),
    Pipe(Box<E>, Apply),
}

// ----- parser ------------------------------------------------------------

struct P {
    toks: Vec<Tok>,
    i: usize,
    // the `enum` declarations' compile-time tables — names resolve HERE and erase from the AST,
    // so the core stays positional. Variant names are global (one table), hence unique program-wide.
    variants: HashMap<String, (usize, String)>,   // variant name -> (tag, its enum)
    enums: HashMap<String, Vec<Option<Shape>>>,   // enum name -> per-variant payload shape (if declared)
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }
    fn bump(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.i).cloned();
        if t.is_some() {
            self.i += 1;
        }
        t
    }
    fn eat(&mut self, t: &Tok) -> Result<(), String> {
        if self.peek() == Some(t) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!("expected {t:?}, found {:?}", self.peek()))
        }
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.bump() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(format!("expected an identifier, found {other:?}")),
        }
    }
    fn num(&mut self) -> Result<u64, String> {
        match self.bump() {
            Some(Tok::Num(n)) => Ok(n),
            other => Err(format!("expected a number, found {other:?}")),
        }
    }
    fn is_kw(&self, s: &str) -> bool {
        self.peek() == Some(&Tok::Ident(s.to_string()))
    }

    fn expr(&mut self) -> Result<E, String> {
        if self.is_kw("let") {
            self.bump();
            let pat = self.pat()?;
            self.eat(&Tok::Eq)?;
            let bound = self.expr()?;
            self.kw_in()?;
            let body = self.expr()?;
            Ok(E::Let(pat, Box::new(bound), Box::new(body)))
        } else if self.is_kw("enum") {
            // `enum Name = V0 shape? | V1 shape? | … in body` — a declaration, not a value: it fills
            // the tables and parses on into the body, leaving no AST node behind. A payload shape is
            // needed only where the sum is BUILT by `inject` (every lane must then be declared);
            // `branch Name` / `match` / `map_variant` read just the tags.
            self.bump();
            let name = self.ident()?;
            self.eat(&Tok::Eq)?;
            let mut vs = vec![self.variant_decl()?];
            while self.peek() == Some(&Tok::Bar) {
                self.bump();
                vs.push(self.variant_decl()?);
            }
            self.kw_in()?;
            let shapes: Vec<Option<Shape>> = vs.iter().map(|(_, s)| s.clone()).collect();
            if self.enums.insert(name.clone(), shapes).is_some() {
                return Err(format!("duplicate enum '{name}'"));
            }
            for (tag, (v, _)) in vs.into_iter().enumerate() {
                if self.variants.insert(v.clone(), (tag, name.clone())).is_some() {
                    return Err(format!("duplicate variant '{v}'"));
                }
            }
            self.expr()
        } else {
            self.pipe()
        }
    }

    /// a binding pattern: a name, or a tuple of names (used by `let` and lambda parameters alike).
    fn pat(&mut self) -> Result<Pat, String> {
        if self.peek() == Some(&Tok::LParen) {
            self.bump();
            let mut names = vec![self.ident()?];
            while self.peek() == Some(&Tok::Comma) {
                self.bump();
                names.push(self.ident()?);
            }
            self.eat(&Tok::RParen)?;
            Ok(Pat::Tuple(names))
        } else {
            Ok(Pat::Name(self.ident()?))
        }
    }

    /// the `in` that closes a `let` or `enum` header (an identifier, not a token, so `eat` can't).
    fn kw_in(&mut self) -> Result<(), String> {
        if !self.is_kw("in") {
            return Err(format!("expected 'in', found {:?}", self.peek()));
        }
        self.bump();
        Ok(())
    }

    /// one `Name shape?` of an `enum` declaration.
    fn variant_decl(&mut self) -> Result<(String, Option<Shape>), String> {
        let v = self.ident()?;
        let shape = match self.peek() {
            Some(Tok::LParen) => Some(self.shape()?),
            Some(Tok::Ident(k)) if k != "in" => Some(self.shape()?),
            _ => None,
        };
        Ok((v, shape))
    }

    /// a payload shape in an `enum` declaration:
    ///   shape = 'u8' | 'u16' | 'u32' | 'u64' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
    /// where ENUM names an earlier, fully-shaped enum (so sums nest, but never recursively).
    fn shape(&mut self) -> Result<Shape, String> {
        match self.bump() {
            Some(Tok::LParen) => {
                if self.peek() == Some(&Tok::RParen) {
                    self.bump();
                    return Ok(Shape::Unit);
                }
                let mut fields = vec![self.shape()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    fields.push(self.shape()?);
                }
                self.eat(&Tok::RParen)?;
                Ok(Shape::Prod(fields))
            }
            Some(Tok::Ident(k)) => match k.as_str() {
                "u8" => Ok(Shape::Prim(8)),
                "u16" => Ok(Shape::Prim(16)),
                "u32" => Ok(Shape::Prim(32)),
                "u64" => Ok(Shape::Prim(64)),
                "List" => {
                    self.eat(&Tok::LParen)?;
                    let inner = self.shape()?;
                    self.eat(&Tok::RParen)?;
                    Ok(Shape::List(Box::new(inner)))
                }
                e => self.enum_shape(e).map(Shape::Sum),
            },
            other => Err(format!("expected a shape, found {other:?}")),
        }
    }

    /// the full lane shapes of a declared enum — an error if any variant left its payload undeclared.
    fn enum_shape(&self, e: &str) -> Result<Vec<Shape>, String> {
        let lanes = self.enums.get(e).ok_or_else(|| format!("unknown enum '{e}'"))?;
        lanes
            .iter()
            .enumerate()
            .map(|(k, s)| s.clone().ok_or_else(|| format!("enum '{e}': variant {k} declares no payload shape")))
            .collect()
    }

    /// a variant tag at a use site: a literal number, or a declared variant name — which also
    /// names its enum, so `inject` by name knows the whole sum it builds.
    fn variant(&mut self) -> Result<(usize, Option<String>), String> {
        match self.bump() {
            Some(Tok::Num(n)) => Ok((n as usize, None)),
            Some(Tok::Ident(v)) => {
                let (tag, e) = self.variants.get(&v).cloned().ok_or_else(|| format!("unknown variant '{v}'"))?;
                Ok((tag, Some(e)))
            }
            other => Err(format!("expected a variant tag, found {other:?}")),
        }
    }

    fn pipe(&mut self) -> Result<E, String> {
        let mut e = self.proj()?;
        // a value is followed by its stages by juxtaposition; the chain runs until a token that
        // cannot begin a stage — in particular the `let` body's `in`, the one identifier that can
        // legally follow a complete pipe without being an op.
        while self.starts_apply() {
            let ap = self.apply()?;
            e = E::Pipe(Box::new(e), ap);
        }
        Ok(e)
    }

    /// whether the next token can begin a pipe stage — a string constant, or any identifier other
    /// than the chain-terminating `in`.
    fn starts_apply(&self) -> bool {
        match self.peek() {
            Some(Tok::Str(_)) => true,
            Some(Tok::Ident(k)) => k != "in",
            _ => false,
        }
    }

    fn apply(&mut self) -> Result<Apply, String> {
        // a string literal as a stage is a constant, like `lit`: broadcast to the value.
        if let Some(Tok::Str(_)) = self.peek() {
            let Some(Tok::Str(bytes)) = self.bump() else { unreachable!() };
            return Ok(Apply::Str(bytes));
        }
        let name = self.ident()?;
        match name.as_str() {
            "map" => {
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::Map(x, Box::new(body)))
            }
            // fold / scan: the value is a pair (seed, list); the lambda destructures (acc, x).
            "fold" => {
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::Fold(x, Box::new(body)))
            }
            "scan" => {
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::Scan(x, Box::new(body)))
            }
            "foldscan" => {
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::FoldScan(x, Box::new(body)))
            }
            "map_variant" => {
                let (k, _) = self.variant()?;
                self.eat(&Tok::LParen)?;
                let (x, body) = self.lambda()?;
                self.eat(&Tok::RParen)?;
                Ok(Apply::MapVariant(k, x, Box::new(body)))
            }
            // match: one arm per variant — `match (k0 (x -> b0), k1 (y -> b1), …)`.
            "match" => {
                self.eat(&Tok::LParen)?;
                let mut arms = Vec::new();
                loop {
                    let (k, _) = self.variant()?;
                    self.eat(&Tok::LParen)?;
                    let (x, body) = self.lambda()?;
                    self.eat(&Tok::RParen)?;
                    arms.push((k, x, body));
                    if self.peek() == Some(&Tok::Comma) {
                        self.bump();
                    } else {
                        break;
                    }
                }
                self.eat(&Tok::RParen)?;
                Ok(Apply::Match(arms))
            }
            // inject: construct a sum — `inject Variant`, the payload going to that variant's lane
            // of its enum, whose every lane must declare a payload shape (the other lanes are built
            // empty at those shapes). No numeric form: a sum is only ever built from a declaration.
            "inject" => {
                let (tag, e) = self.variant()?;
                let Some(e) = e else { return Err("inject needs a declared variant name".into()) };
                Ok(Apply::Inject(tag, self.enum_shape(&e)?))
            }
            // head: first element, sugar for `get 0` — total (an empty row -> Oob, carried in the err-mask).
            "head" => Ok(Apply::Head),
            // split: the delimiter is a one-byte string literal (`split ","`), not a bare number —
            // it names a byte, not a count.
            "split" => match self.bump() {
                Some(Tok::Str(bytes)) if bytes.len() == 1 => {
                    Ok(Apply::Op(name, Some(bytes[0] as u64)))
                }
                other => Err(format!("split expects a one-byte string delimiter, found {other:?}")),
            },
            // branch: the lane count is a literal, or an enum name standing for its arity.
            "branch" => {
                let lanes = match self.bump() {
                    Some(Tok::Num(n)) => n,
                    Some(Tok::Ident(e)) => {
                        self.enums.get(&e).ok_or_else(|| format!("unknown enum '{e}'"))?.len() as u64
                    }
                    other => return Err(format!("expected a lane count or enum, found {other:?}")),
                };
                Ok(Apply::Op(name, Some(lanes)))
            }
            // a pair-eating binary followed by a number is the immediate form: `x sub 1` is the
            // lit-pair idiom `(x, x lit 1) sub` spelled tight (a bare number can't begin a stage,
            // so this claims unused syntax).
            _ if pair_imm(&name) && matches!(self.peek(), Some(Tok::Num(_))) => {
                Ok(Apply::BinImm(name, self.num()?))
            }
            _ if takes_num(&name) => Ok(Apply::Op(name, Some(self.num()?))),
            _ => Ok(Apply::Op(name, None)),
        }
    }

    fn lambda(&mut self) -> Result<(Pat, E), String> {
        // a lambda is `pat -> body`; the `->` is the marker (no `fun` keyword). The pattern mirrors
        // `let`: a tuple pattern destructures the parameter.
        let x = self.pat()?;
        self.eat(&Tok::Arrow)?;
        let body = self.expr()?;
        Ok((x, body))
    }

    fn proj(&mut self) -> Result<E, String> {
        let mut e = self.atom()?;
        while self.peek() == Some(&Tok::Dot) {
            self.bump();
            e = E::Proj(Box::new(e), self.num()? as usize);
        }
        Ok(e)
    }

    fn atom(&mut self) -> Result<E, String> {
        match self.peek() {
            Some(Tok::LParen) => {
                self.bump();
                let mut es = vec![self.expr()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    es.push(self.expr()?);
                }
                self.eat(&Tok::RParen)?;
                Ok(if es.len() == 1 { es.pop().unwrap() } else { E::Tuple(es) })
            }
            Some(Tok::Ident(_)) => Ok(E::Var(self.ident()?)),
            other => Err(format!("expected an expression, found {other:?}")),
        }
    }
}

// ----- lowering ----------------------------------------------------------

type Env = HashMap<String, usize>;

/// bind a pattern to a node: a name binds the node itself; a tuple pattern binds each name to a
/// `Field` projection of it.
fn bind(pat: &Pat, id: usize, env: &mut Env, b: &mut Builder<NumOp>) {
    match pat {
        Pat::Name(x) => {
            env.insert(x.clone(), id);
        }
        Pat::Tuple(names) => {
            for (i, name) in names.iter().enumerate() {
                let fid = b.add(Op::Field(i), vec![id]);
                env.insert(name.clone(), fid);
            }
        }
    }
}

/// lower a lambda body into a closed sub-graph (its parameter is the body's `Input`).
fn lower_body(pat: &Pat, body: &E) -> Result<Graph<NumOp>, String> {
    let mut bb = Builder::default();
    let bin = bb.input();
    let mut benv = Env::new();
    bind(pat, bin, &mut benv, &mut bb);
    let bout = lower(body, &benv, &mut bb)?;
    Ok(bb.finish(bout))
}

/// append `Tuple([out, out])` to a body `(T,A)->B`, making it `(T,A)->(B,B)` — the `FoldScan` body
/// that re-expresses `scan`: the new state and the emitted output are both the running accumulator.
fn dup_output(mut g: Graph<NumOp>) -> Graph<NumOp> {
    let out = g.output;
    let tup = g.nodes.len();
    g.nodes.push(Node { kind: NodeKind::Tuple, inputs: vec![out, out] });
    Graph { nodes: g.nodes, output: tup }
}

fn lower(e: &E, env: &Env, b: &mut Builder<NumOp>) -> Result<usize, String> {
    match e {
        E::Var(name) => env.get(name).copied().ok_or_else(|| format!("unbound variable '{name}'")),
        E::Tuple(es) => {
            let ids = es.iter().map(|x| lower(x, env, b)).collect::<Result<Vec<_>, _>>()?;
            Ok(b.tuple(ids))
        }
        E::Proj(e, i) => {
            let id = lower(e, env, b)?;
            Ok(b.add(Op::Field(*i), vec![id]))
        }
        E::Let(pat, bound, body) => {
            let id = lower(bound, env, b)?;
            let mut env2 = env.clone();
            bind(pat, id, &mut env2, b);
            lower(body, &env2, b)
        }
        E::Pipe(e, ap) => {
            let id = lower(e, env, b)?;
            match ap {
                Apply::Op(name, arg) => Ok(b.add(resolve(name, *arg)?, vec![id])),
                Apply::BinImm(name, n) => {
                    let lit = b.add(Op::Lit(Value::u64(vec![*n])), vec![id]);
                    let pair = b.tuple(vec![id, lit]);
                    Ok(b.add(resolve(name, None)?, vec![pair]))
                }
                Apply::Str(bytes) => Ok(b.add(Op::Lit(str_value(bytes.clone())), vec![id])),
                Apply::Map(x, body) => Ok(b.add(Op::MapList(Box::new(lower_body(x, body)?)), vec![id])),
                Apply::Fold(x, body) => Ok(b.add(Op::Fold(Box::new(lower_body(x, body)?)), vec![id])),
                // scan IS foldscan: a body `(a,x) -> b` becomes `(a,x) -> (b, b)` (state = output), and
                // the running-accumulator list is field 1 of the result. Measured identical to a
                // dedicated Scan, so `Op::Scan` is retired in favour of this lowering.
                Apply::Scan(x, body) => {
                    let fs = b.add(Op::FoldScan(Box::new(dup_output(lower_body(x, body)?))), vec![id]);
                    Ok(b.add(Op::Field(1), vec![fs]))
                }
                Apply::FoldScan(x, body) => {
                    Ok(b.add(Op::FoldScan(Box::new(lower_body(x, body)?)), vec![id]))
                }
                Apply::MapVariant(k, x, body) => {
                    Ok(b.add(Op::MapSum(vec![(*k, lower_body(x, body)?)]), vec![id]))
                }
                Apply::Match(arms) => {
                    let lowered = arms
                        .iter()
                        .map(|(k, x, body)| Ok((*k, lower_body(x, body)?)))
                        .collect::<Result<Vec<(usize, Graph<NumOp>)>, String>>()?;
                    let ms = b.add(Op::MapSum(lowered), vec![id]);
                    Ok(b.add(Op::Unwrap, vec![ms]))
                }
                Apply::Inject(tag, shapes) => Ok(b.add(Op::Inject(*tag, shapes.clone()), vec![id])),
                // first element = index 0 of the row: build the (0, list) pair and scalar-`get` it.
                Apply::Head => {
                    // `head` lowers to `get` (GetTry) — the get FailOp; an empty row is an Oob carried
                    // in the err-mask, observed by a downstream TRY, not a panic.
                    let zero = b.add(Op::Lit(Value::u64(vec![0])), vec![id]);
                    let pair = b.tuple(vec![zero, id]);
                    Ok(b.add(Op::TryGet, vec![pair]))
                }
            }
        }
    }
}

/// parse an ML-flavoured expression into a `Graph` (with `input` bound to the root).
pub fn parse_ml(src: &str) -> Result<Graph<NumOp>, String> {
    let toks = lex(src)?;
    let mut p = P { toks, i: 0, variants: HashMap::new(), enums: HashMap::new() };
    let e = p.expr()?;
    if p.i != p.toks.len() {
        return Err(format!("trailing tokens from index {}", p.i));
    }
    let mut b = Builder::default();
    let input = b.input();
    let mut env = Env::new();
    env.insert("input".to_string(), input);
    let out = lower(&e, &env, &mut b)?;
    Ok(b.finish(out))
}
