//! A small ML-flavoured expression surface, concatenative-by-juxtaposition: a value is followed by
//! its operator stages with no separator (`input iota fold_add`). `let` boils binding into shared
//! edges (no re-derivation), with product destructuring; lambdas (`x -> …`) are the map/match bodies;
//! sums are built/eliminated via `inject`/`match`. A stage-chain runs until a token that can't begin a
//! stage — notably the `let` body's `in`, the one identifier allowed to follow a complete chain.
//!
//! This surface is a notation for the core graph, not a language over it. A construct belongs here
//! when it lowers to core nodes fixed by its syntax alone, adds no work the text doesn't show, and
//! is not a second way to write something already writable. Anything that decides which op to use
//! from shapes or kinds, or inserts ops the text doesn't name, belongs to a language that lowers
//! to this one.
//!
//!   expr   = 'let' pat '=' expr 'in' expr
//!          | 'enum' IDENT '=' IDENT shape? ('|' IDENT shape?)* 'in' expr  -- a compile-time table; names ERASE here
//!   shape  = 'u8'|'u16'|'u32'|'u64' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
//!          | pipe
//!   pat    = IDENT | '_' | '(' pat (',' pat)* ')'      -- irrefutable: names, wildcards, tuples
//!   pipe   = atom apply*                               -- juxtaposition; chain ends before `in`
//!   apply  = 'map' '(' lambda ')'
//!          | ('fold' | 'scan') '(' lambda ')'                 -- (seed, list); lambda is (acc, x)
//!          | 'map_variant' tag '(' lambda ')'
//!          | 'match' '(' (tag '(' lambda ')')(',' …)* ')'   -- MapSum + Unwrap
//!          | 'inject' VARIANT                               -- sum construction (its enum fully shaped)
//!          | 'branch' (NUM | ENUM)                          -- lane count, literal or by enum name
//!          | 'split' STR                                    -- delimiter as a one-byte string
//!          | '.' NUM                                        -- projection
//!          | IDENT NUM?
//!   tag    = NUM | VARIANT                              -- a variant name resolves to its tag
//!   lambda = pat '->' expr                              -- a tuple pattern destructures the parameter
//!   atom   = '(' expr (',' expr)* ')' | IDENT | LIT | STR   -- 'input' is the root
//!   LIT    = '-'? DIGITS ('.' DIGITS)? (('e'|'E') [+-]? DIGITS)? SUFFIX
//!   SUFFIX = ('u'|'i') ('8'|'16'|'32'|'64') | 'f' ('32'|'64')   -- required: kind and width
//!
//! A literal is a column of one constant, as long as the input of the scope it appears in (a
//! lambda's parameter, or `input`). Bodies are closed, so that is the length of every value in
//! the scope. A bare NUM is an op's parameter (`shr 3`, `branch 2`), never a value. `#` starts a
//! comment to the end of the line. (What runs is not always what is written here: `Program` turns a
//! binary op on a pair holding a literal, `(x, 1u64) sub`, into one op that carries the constant, so
//! no column of the constant is built; see `corgi::immediates`.)
//!
//! A literal's suffix chooses how its bits are laid down: `u` as the value, `i` in the
//! order-preserving signed encoding, `f` in the total-order float encoding. The encodings keep
//! order, so sorting, comparison, `min`/`max` and `find` are right for every kind. Nothing tracks
//! a kind past the literal: arithmetic takes its kind from the op's name (`add` is unsigned,
//! `add_f64` is float), so `(1.5f64, 1.5f64) add` type-checks and adds the encodings. Checking
//! kinds belongs to a language that lowers to this one.
//!
//! e.g.  let (subj, vals) = input.1 transpose in vals fold_add
//!       e match (0 (lo -> lo), 1 (hi -> (hi, 100u64) add))   -- exhaustive ⇒ Unwrap types it
//!       enum Size = Lo | Hi in … match (Lo (l -> l), Hi (h -> (h, 100u64) add))
//!       enum Opt = None () | Some u64 in xs inject Some  -- tag xs into Some; None is an empty unit lane

use super::{parse_kw, resolve, str_value, takes_num};
use crate::graph::{Builder, Graph, Node, NodeKind};
use crate::ops::numeric::{enc_f32, enc_f64};
use crate::ops::{lit_value, Kind, NumOp, Op};
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
    Num(u64),   // an op's numeric parameter: `shr 3`, `branch 2`
    Lit(Value), // a typed constant: `5u64`, `-3i64`, `0.7f64`
    Str(Vec<u8>),
}

/// `line:column` of char offset `at` in `cs`, both 1-based, for error messages.
fn position(cs: &[char], at: usize) -> String {
    let before = &cs[..at.min(cs.len())];
    let line = before.iter().filter(|&&c| c == '\n').count() + 1;
    let col = before.iter().rev().take_while(|&&c| c != '\n').count() + 1;
    format!("{line}:{col}")
}

/// a typed constant from its text: `digits` (with an optional leading `-`, and for floats a
/// fraction or exponent) and a suffix naming its kind and width. The suffix is required and says
/// how the bits are laid down: `u` as the value, `i` in the order-preserving signed encoding, `f`
/// in the total-order float encoding.
fn typed_lit(digits: &str, suffix: &str) -> Result<Value, String> {
    let (kind, width) = parse_kw(suffix).ok_or_else(|| format!("unknown literal suffix '{suffix}'"))?;
    let bad = || format!("'{digits}{suffix}' is not a {suffix}");
    match kind {
        Kind::F => {
            let x: f64 = digits.parse().map_err(|_| bad())?;
            Ok(if width == 32 { Value::u32(vec![enc_f32(x as f32)]) } else { Value::u64(vec![enc_f64(x)]) })
        }
        Kind::U | Kind::I => {
            let v: i128 = digits.parse().map_err(|_| bad())?;
            let (lo, hi) = match kind {
                Kind::U => (0i128, (1i128 << width) - 1),
                _ => (-(1i128 << (width - 1)), (1i128 << (width - 1)) - 1),
            };
            if v < lo || v > hi {
                return Err(format!("{v} does not fit in {suffix}"));
            }
            // two's complement truncated to the width; `lit_value` applies the kind's encoding.
            Ok(lit_value(kind, width, v as u64))
        }
    }
}

/// the tokens of `s`, and the char offset where each begins. `#` starts a comment to end of line.
fn lex(s: &str) -> Result<(Vec<Tok>, Vec<usize>), String> {
    let cs: Vec<char> = s.chars().collect();
    let mut toks = Vec::new();
    let mut starts = Vec::new();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        let start = i;
        let tok = match c {
            c if c.is_whitespace() => {
                i += 1;
                continue;
            }
            '#' => {
                while i < cs.len() && cs[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            '-' if cs.get(i + 1) == Some(&'>') => {
                i += 2;
                Tok::Arrow
            }
            '.' => {
                i += 1;
                Tok::Dot
            }
            '(' => {
                i += 1;
                Tok::LParen
            }
            ')' => {
                i += 1;
                Tok::RParen
            }
            ',' => {
                i += 1;
                Tok::Comma
            }
            '=' => {
                i += 1;
                Tok::Eq
            }
            '|' => {
                i += 1;
                Tok::Bar
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
                        None => return Err(format!("{}: unterminated string literal", position(&cs, start))),
                    }
                }
                Tok::Str(bytes)
            }
            c if c.is_ascii_digit() || (c == '-' && cs.get(i + 1).is_some_and(|d| d.is_ascii_digit())) => {
                let digit = |j: usize| cs.get(j).is_some_and(|d| d.is_ascii_digit());
                let mut text = String::from(c);
                i += 1;
                while digit(i) {
                    text.push(cs[i]);
                    i += 1;
                }
                // A fraction or exponent makes a float, and only counts when an `f` suffix follows:
                // otherwise `x.0.1` would read `0.1` as a number rather than two projections.
                let mut j = i;
                let mut float = String::new();
                if cs.get(j) == Some(&'.') && digit(j + 1) {
                    float.push('.');
                    j += 1;
                    while digit(j) {
                        float.push(cs[j]);
                        j += 1;
                    }
                }
                if matches!(cs.get(j), Some('e') | Some('E'))
                    && (digit(j + 1) || (matches!(cs.get(j + 1), Some('+') | Some('-')) && digit(j + 2)))
                {
                    float.push('e');
                    j += 1;
                    if !digit(j) {
                        float.push(cs[j]);
                        j += 1;
                    }
                    while digit(j) {
                        float.push(cs[j]);
                        j += 1;
                    }
                }
                if !float.is_empty() {
                    match cs.get(j) {
                        Some('f') => {
                            text.push_str(&float);
                            i = j;
                        }
                        Some(c) if c.is_ascii_alphabetic() => {
                            let at = position(&cs, start);
                            return Err(format!("{at}: '{text}{float}' has a fraction, so needs an f32 or f64 suffix"));
                        }
                        _ => {}
                    }
                }
                let mut suffix = String::new();
                while i < cs.len() && cs[i].is_ascii_alphanumeric() {
                    suffix.push(cs[i]);
                    i += 1;
                }
                let at = position(&cs, start);
                if suffix.is_empty() {
                    let n: u64 = text
                        .parse()
                        .map_err(|_| format!("{at}: '{text}' needs a type suffix, such as {text}i64"))?;
                    Tok::Num(n)
                } else {
                    Tok::Lit(typed_lit(&text, &suffix).map_err(|e| format!("{at}: {e}"))?)
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut w = String::new();
                while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                    w.push(cs[i]);
                    i += 1;
                }
                Tok::Ident(w)
            }
            _ => return Err(format!("{}: unexpected character '{c}'", position(&cs, start))),
        };
        toks.push(tok);
        starts.push(start);
    }
    Ok((toks, starts))
}

// ----- AST ---------------------------------------------------------------

/// an irrefutable pattern: a name, `_`, or a tuple of patterns. Refutable matching (a sum's lane)
/// is `match`'s job, never a pattern's.
enum Pat {
    Name(String),
    Wild,
    Tuple(Vec<Pat>),
}

enum Apply {
    Op(String, Option<u64>),
    Field(usize), // `.N`
    Map(Pat, Box<E>),
    Fold(Pat, Box<E>), // (B, List<A>) folded by a binary body; the lambda's tuple pattern is (acc, x)
    Scan(Pat, Box<E>), // (B, List<A>) scanned by a binary body; inclusive running accumulator
    FoldScan(Pat, Box<E>), // (T, List<A>) -> (T, List<R>); body (acc, x) -> (new state, output R)
    MapVariant(usize, Pat, Box<E>),
    Match(Vec<(usize, Pat, E)>), // arms (tag, binding, body) -> MapSum + Unwrap
    Inject(usize, Vec<Shape>),    // tag + the declared sum's lane shapes -> Op::Inject
    Head, // `head`: sugar for `(0u64, list) get` — the first element (an empty row errs)
}

enum E {
    Var(String, String), // the name, and its source position for errors
    Lit(Value),          // a typed constant or a string, filled to the length of its scope's input
    Tuple(Vec<E>),
    Let(Pat, Box<E>, Box<E>),
    Pipe(Box<E>, Apply),
}

// ----- parser ------------------------------------------------------------

struct P {
    toks: Vec<Tok>,
    i: usize,
    src: Vec<char>,
    starts: Vec<usize>, // char offset of each token
    seen: std::cell::Cell<usize>, // the last token looked at, where a parse error is reported
    // the `enum` declarations' compile-time tables — names resolve HERE and erase from the AST,
    // so the core stays positional. Variant names are global (one table), hence unique program-wide.
    variants: HashMap<String, (usize, String)>,   // variant name -> (tag, its enum)
    enums: HashMap<String, Vec<Option<Shape>>>,   // enum name -> per-variant payload shape (if declared)
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.seen.set(self.i);
        self.toks.get(self.i)
    }
    fn bump(&mut self) -> Option<Tok> {
        self.seen.set(self.i);
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

    /// the source position of token `k` (or of the end of input).
    fn at(&self, k: usize) -> String {
        position(&self.src, self.starts.get(k).copied().unwrap_or(self.src.len()))
    }

    /// a binding pattern, used by `let` and lambda parameters alike.
    fn pat(&mut self) -> Result<Pat, String> {
        if self.peek() == Some(&Tok::LParen) {
            self.bump();
            let mut pats = vec![self.pat()?];
            while self.peek() == Some(&Tok::Comma) {
                self.bump();
                pats.push(self.pat()?);
            }
            self.eat(&Tok::RParen)?;
            Ok(Pat::Tuple(pats))
        } else {
            let name = self.ident()?;
            Ok(if name == "_" { Pat::Wild } else { Pat::Name(name) })
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
        let mut e = self.atom()?;
        // a value is followed by its stages by juxtaposition; the chain runs until a token that
        // cannot begin a stage — in particular the `let` body's `in`, the one identifier that can
        // legally follow a complete pipe without being an op.
        while self.starts_apply() {
            let ap = self.apply()?;
            e = E::Pipe(Box::new(e), ap);
        }
        Ok(e)
    }

    /// whether the next token can begin a pipe stage — a `.N` projection, or any identifier
    /// other than the chain-terminating `in`.
    fn starts_apply(&self) -> bool {
        match self.peek() {
            Some(Tok::Dot) => true,
            Some(Tok::Ident(k)) => k != "in",
            _ => false,
        }
    }

    fn apply(&mut self) -> Result<Apply, String> {
        // `.N` after any value projects field N.
        if let Some(Tok::Dot) = self.peek() {
            self.bump();
            return Ok(Apply::Field(self.num()? as usize));
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
            _ if takes_num(&name) => Ok(Apply::Op(name, Some(self.num()?))),
            "field" => Err("projection is `.N`, as in x .1".into()),
            _ if name == "lit" || name.starts_with("lit_") => {
                Err("a constant is a typed literal, as in 5u64 or -3i64".into())
            }
            _ if matches!(self.peek(), Some(Tok::Num(_))) => {
                Err(format!("'{name}' takes no number; a constant operand is a typed literal, as in (x, 1u64) {name}"))
            }
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
            Some(Tok::Ident(_)) => {
                let at = self.at(self.i);
                Ok(E::Var(self.ident()?, at))
            }
            Some(Tok::Lit(_)) => {
                let Some(Tok::Lit(v)) = self.bump() else { unreachable!() };
                Ok(E::Lit(v))
            }
            Some(Tok::Str(_)) => {
                let Some(Tok::Str(bytes)) = self.bump() else { unreachable!() };
                Ok(E::Lit(str_value(bytes)))
            }
            Some(Tok::Num(n)) => Err(format!("a constant needs a type suffix, as in {n}u64")),
            other => Err(format!("expected an expression, found {other:?}")),
        }
    }
}

// ----- lowering ----------------------------------------------------------

type Env = HashMap<String, usize>;

/// The environment key of the scope's input, which constants are filled to the length of. It
/// can't collide with a name: identifiers never contain a space.
const ROOT: &str = " root";

/// bind a pattern to a node: a name binds the node itself, `_` binds nothing, and a tuple pattern
/// binds each sub-pattern to a `Field` projection of it.
fn bind(pat: &Pat, id: usize, env: &mut Env, b: &mut Builder<NumOp>) {
    match pat {
        Pat::Name(x) => {
            env.insert(x.clone(), id);
        }
        Pat::Wild => {}
        Pat::Tuple(pats) => {
            for (i, sub) in pats.iter().enumerate() {
                let fid = b.add(Op::Field(i), vec![id]);
                bind(sub, fid, env, b);
            }
        }
    }
}

/// lower a lambda body into a closed sub-graph (its parameter is the body's `Input`).
fn lower_body(pat: &Pat, body: &E) -> Result<Graph<NumOp>, String> {
    let mut bb = Builder::default();
    let bin = bb.input();
    let mut benv = Env::new();
    benv.insert(ROOT.to_string(), bin);
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
        E::Var(name, at) => env.get(name).copied().ok_or_else(|| format!("{at}: unbound variable '{name}'")),
        // A body is closed, so every value in it has the length of the body's input: filling a
        // constant to that length is always right.
        E::Lit(v) => Ok(b.add(Op::Lit(v.clone()), vec![env[ROOT]])),
        E::Tuple(es) => {
            let ids = es.iter().map(|x| lower(x, env, b)).collect::<Result<Vec<_>, _>>()?;
            Ok(b.tuple(ids))
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
                Apply::Field(i) => Ok(b.add(Op::Field(*i), vec![id])),
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
    let (toks, starts) = lex(src)?;
    let mut p = P {
        toks,
        i: 0,
        src: src.chars().collect(),
        starts,
        seen: std::cell::Cell::new(0),
        variants: HashMap::new(),
        enums: HashMap::new(),
    };
    let e = p.expr().map_err(|e| format!("{}: {e}", p.at(p.seen.get())))?;
    if p.i != p.toks.len() {
        return Err(format!("{}: unexpected {:?}", p.at(p.i), p.toks[p.i]));
    }
    let mut b = Builder::default();
    let input = b.input();
    let mut env = Env::new();
    env.insert("input".to_string(), input);
    env.insert(ROOT.to_string(), input);
    let out = lower(&e, &env, &mut b)?;
    Ok(b.finish(out))
}
