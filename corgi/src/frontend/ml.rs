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
//!   shape  = 'int' | 'float' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
//!          | pipe
//!   pat    = IDENT | '_' | '(' (pat (',' pat)*)? ')'   -- irrefutable: names, wildcards, tuples
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
//!   atom   = '(' (expr (',' expr)*)? ')' | IDENT | LIT | STR   -- 'input' is the root; '(' … ')' is a tuple
//!   LIT    = '-'? DIGITS                                        -- an Int
//!          | '-'? DIGITS ('.' DIGITS)? (('e'|'E') [+-]? DIGITS)?  -- a Float, with a fraction or exponent
//!
//! Parentheses build a tuple wherever they appear: `(e)` in an expression is a one-field tuple, as
//! `(x)` is in a pattern and `(int)` in a shape, and `()` is the tuple with no fields, the unit, in
//! all three (in a pattern it binds nothing). Nothing needs parentheses for grouping, as stages
//! apply by juxtaposition.
//!
//! A literal is a column of one constant, as long as the input of the scope it appears in (a
//! lambda's parameter, or `input`). Bodies are closed, so that is the length of every value in
//! the scope. A NUM after an op that takes one is its parameter (`chunk 3`, `branch 2`); anywhere
//! else it is an Int, as `-3` is. `#` starts a comment to the end of the line. (What runs is not
//! always what is written here: `Program` turns a binary op on a pair holding a literal,
//! `(x, 1) sub`, into one op that carries the constant, so no column of the constant is built; see
//! `corgi::immediates`.)
//!
//! An Int literal is an integer and a Float literal an `f64`; arithmetic takes its kind from its
//! operands, so `(1.5, 2.5) add` is a float add and `(1, 2.5) add` a shape error.
//!
//! e.g.  let (subj, vals) = input.1 transpose in vals fold_add
//!       e match (0 (lo -> lo), 1 (hi -> (hi, 100) add))   -- exhaustive ⇒ Unwrap types it
//!       enum Size = Lo | Hi in … match (Lo (l -> l), Hi (h -> (h, 100) add))
//!       enum Opt = None () | Some int in xs inject Some  -- tag xs into Some; None is an empty unit lane

use super::{resolve, retired, str_value, takes_num};
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
    Num(u64),   // a non-negative integer: an op's parameter (`chunk 3`, `branch 2`), or an Int
    Lit(Value), // a constant no parameter can be: a negative Int (`-3`) or a Float (`0.7`)
    Str(Vec<u8>),
}

/// `line:column` of char offset `at` in `cs`, both 1-based, for error messages.
fn position(cs: &[char], at: usize) -> String {
    let before = &cs[..at.min(cs.len())];
    let line = before.iter().filter(|&&c| c == '\n').count() + 1;
    let col = before.iter().rev().take_while(|&&c| c != '\n').count() + 1;
    format!("{line}:{col}")
}

/// an Int constant, held as a byte when it is one (so a byte column meeting it stays bytes, as text
/// meeting `40` does) and as an `i64` otherwise.
fn int_lit(n: i64) -> Value {
    match u8::try_from(n) {
        Ok(b) => Value::u8(vec![b]),
        Err(_) => Value::i64(vec![n]),
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
                // A fraction or exponent makes a float, but not right after a `.`: there a number
                // is a projection, and `x.0.1` is two of them.
                let projecting = toks.last() == Some(&Tok::Dot);
                let mut float = false;
                if !projecting && cs.get(i) == Some(&'.') && digit(i + 1) {
                    float = true;
                    text.push('.');
                    i += 1;
                    while digit(i) {
                        text.push(cs[i]);
                        i += 1;
                    }
                }
                if !projecting
                    && matches!(cs.get(i), Some('e') | Some('E'))
                    && (digit(i + 1) || (matches!(cs.get(i + 1), Some('+') | Some('-')) && digit(i + 2)))
                {
                    float = true;
                    text.push('e');
                    i += 1;
                    if !digit(i) {
                        text.push(cs[i]);
                        i += 1;
                    }
                    while digit(i) {
                        text.push(cs[i]);
                        i += 1;
                    }
                }
                let at = position(&cs, start);
                if i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                    let mut suffix = String::new();
                    while i < cs.len() && cs[i].is_ascii_alphanumeric() {
                        suffix.push(cs[i]);
                        i += 1;
                    }
                    return Err(format!("{at}: '{text}{suffix}': a literal takes no suffix (integers have no width), so write {text}"));
                }
                if float {
                    let x: f64 = text.parse().map_err(|_| format!("{at}: '{text}' is not a float"))?;
                    Tok::Lit(Value::f64(vec![x]))
                } else if text.starts_with('-') {
                    let n: i64 = text.parse().map_err(|_| format!("{at}: '{text}' does not fit in an Int (an i64)"))?;
                    Tok::Lit(int_lit(n))
                } else {
                    let n: u64 = text.parse().map_err(|_| format!("{at}: '{text}' does not fit in an Int (an i64)"))?;
                    Tok::Num(n)
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
    Head, // `head`: sugar for `(0, list) get` — the first element (an empty row errs)
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
            // `()`, the tuple with no fields: the unit, which binds nothing
            if self.peek() == Some(&Tok::RParen) {
                self.bump();
                return Ok(Pat::Tuple(Vec::new()));
            }
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
    ///   shape = 'int' | 'float' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
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
                "int" => Ok(Shape::Int),
                "float" => Ok(Shape::Float),
                "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
                    Err(format!("'{k}' is retired: an integer is `int` (its width is storage), a float `float`"))
                }
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
            // head: first element, sugar for `get 0` — checked (an empty row errs, carried in the err-mask).
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
            // a retired spelling points at its replacement, whatever follows it (`cast 32`)
            _ if retired(&name).is_some() => Err(retired(&name).unwrap_or_default()),
            // `and` takes its mask optionally: `x and 255`, or the pair form `(x, y) and`.
            "and" if matches!(self.peek(), Some(Tok::Num(_))) => Ok(Apply::Op(name, Some(self.num()?))),
            "field" => Err("projection is `.N`, as in x .1".into()),
            _ if name == "lit" || name.starts_with("lit_") => {
                Err("a constant is a literal, as in 5, -3 or 0.5".into())
            }
            _ if matches!(self.peek(), Some(Tok::Num(_))) => {
                Err(format!("'{name}' takes no number; a constant operand goes in the pair, as in (x, 1) {name}"))
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
                // `()`, the tuple with no fields: the unit value
                if self.peek() == Some(&Tok::RParen) {
                    self.bump();
                    return Ok(E::Tuple(Vec::new()));
                }
                let mut es = vec![self.expr()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    es.push(self.expr()?);
                }
                self.eat(&Tok::RParen)?;
                Ok(E::Tuple(es))
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
            Some(&Tok::Num(n)) => {
                self.bump();
                let n = i64::try_from(n).map_err(|_| format!("{n} does not fit in an Int (an i64)"))?;
                Ok(E::Lit(int_lit(n)))
            }
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
        // the tuple with no fields is the unit, as long as the scope's input
        E::Tuple(es) if es.is_empty() => Ok(b.add(Op::Unit, vec![env[ROOT]])),
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
                Apply::Op(name, _) if name == "slices" => Ok(slices_word(b, id)),
                Apply::Op(name, arg) if name.starts_with("try_") => try_word(b, id, name, *arg),
                Apply::Op(name, _) if matches!(name.as_str(), "sort" | "dedup" | "group") => Ok(sort_word(b, id, name)),
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
                // first element = index 0 of the row: build the (0, list) pair and gather it. An empty
                // row reads the zero of the element's shape, as `get` does.
                Apply::Head => {
                    let zero = b.add(Op::Lit(int_lit(0)), vec![id]);
                    let pair = b.tuple(vec![zero, id]);
                    Ok(b.add(Op::Gather, vec![pair]))
                }
            }
        }
    }
}

/// `(ranges, list) slices`: each `(lo, hi)` range of row r becomes the sub-list `list[r][lo..hi)`.
/// The word `map(range); gather`: the ranges become nested position lists, and `gather` keeps their
/// structure. A range with `lo >= hi` is empty; a position past the row reads the zero of its shape.
fn slices_word(b: &mut Builder<NumOp>, pair: usize) -> usize {
    let ranges = b.add(Op::Field(0), vec![pair]);
    let list = b.add(Op::Field(1), vec![pair]);
    let range = {
        let mut rb = Builder::default();
        let r = rb.input();
        let out = rb.add(Op::Range, vec![r]);
        rb.finish(out)
    };
    let positions = b.add(Op::MapList(Box::new(range)), vec![ranges]);
    let args = b.tuple(vec![positions, list]);
    b.add(Op::Gather, vec![args])
}

/// the checked ops, as words over the lossy ones: mark each row the op would lose something on,
/// `branch` the input on the mark, run the op on lane 0 and make lane 1 `()`. The result is a
/// `Sum{T | ()}` for the program to `match`. A row is marked when
/// - `(i, xs) try_get`: `i` is outside `0 .. xs len`;
/// - `(ps, xs) try_gather`: a position in `ps` is outside it (the least below 0, or the greatest at
///   `xs len` or past it);
/// - `(xs, ys) try_zip`: `xs len` and `ys len` differ;
/// - `xs try_chunk k`: `xs len` leaves a remainder by `k` (the remainder is the mark).
fn try_word(b: &mut Builder<NumOp>, x: usize, name: &str, arg: Option<u64>) -> Result<usize, String> {
    use crate::ops::{ArithOp, BinOp, BitOp, CmpOp, Pred, Red};
    fn on(b: &mut Builder<NumOp>, op: impl Into<NumOp>, l: usize, r: usize) -> usize {
        let pair = b.tuple(vec![l, r]);
        b.add(op, vec![pair])
    }
    fn lit(b: &mut Builder<NumOp>, n: i64, like: usize) -> usize {
        b.add(Op::Lit(int_lit(n)), vec![like])
    }
    // `i` outside `0 .. n`
    fn outside(b: &mut Builder<NumOp>, i: usize, n: usize) -> usize {
        let zero = lit(b, 0, i);
        let below = on(b, CmpOp::Rel(Pred::Lt), i, zero);
        let above = on(b, CmpOp::Rel(Pred::Ge), i, n);
        on(b, ArithOp::Bits(BitOp::Or), below, above)
    }
    let (op, mark): (NumOp, usize) = match name {
        "try_get" | "try_gather" => {
            let (ps, xs) = (b.add(Op::Field(0), vec![x]), b.add(Op::Field(1), vec![x]));
            let n = b.add(Op::Len, vec![xs]);
            let mark = if name == "try_get" {
                outside(b, ps, n)
            } else {
                // the least and the greatest position; an empty `ps` has neither, and its 0s are no mark
                let lo = b.add(ArithOp::Reduce(Red::Min), vec![ps]);
                let hi = b.add(ArithOp::Reduce(Red::Max), vec![ps]);
                let zero = lit(b, 0, lo);
                let below = on(b, CmpOp::Rel(Pred::Lt), lo, zero);
                let above = on(b, CmpOp::Rel(Pred::Ge), hi, n);
                let count = b.add(Op::Len, vec![ps]);
                let some = on(b, CmpOp::Rel(Pred::Gt), count, zero);
                let above = on(b, CmpOp::Min, above, some);
                on(b, ArithOp::Bits(BitOp::Or), below, above)
            };
            (Op::Gather.into(), mark)
        }
        "try_zip" => {
            let (xs, ys) = (b.add(Op::Field(0), vec![x]), b.add(Op::Field(1), vec![x]));
            let (nx, ny) = (b.add(Op::Len, vec![xs]), b.add(Op::Len, vec![ys]));
            (Op::Zip.into(), on(b, CmpOp::Rel(Pred::Ne), nx, ny))
        }
        "try_chunk" => {
            let k = arg.ok_or("try_chunk needs a width")?;
            if k == 0 {
                return Err("try_chunk width must be positive".into());
            }
            let n = b.add(Op::Len, vec![x]);
            let width = lit(b, k as i64, n);
            (Op::Chunk(k as usize).into(), on(b, ArithOp::Bin(BinOp::Rem), n, width))
        }
        other => return Err(format!("unknown op '{other}' (the checked ops are try_get, try_gather, try_zip and try_chunk)")),
    };
    let arm = |op: NumOp| {
        let mut bb = Builder::default();
        let i = bb.input();
        let out = bb.add(op, vec![i]);
        bb.finish(out)
    };
    let routed = on(b, Op::Branch(2), x, mark);
    Ok(b.add(Op::MapSum(vec![(0, arm(op)), (1, arm(Op::Unit.into()))]), vec![routed]))
}

/// `sort`, `dedup` and `group` as words over `sort_by` (stable by key, a payload carried along, and
/// each element's run of equal keys), `adjacent`, `filter` and `cut`. `dedup` and `group` sort a
/// list key by reference, so that only the keys they keep are copied out:
/// - `xs sort` = `sort_by` with a unit payload;
/// - `xs dedup` = the sorted elements that start a run;
/// - `kvs group` = the keys that start a run, and the values cut where a run starts.
fn sort_word(b: &mut Builder<NumOp>, xs: usize, name: &str) -> usize {
    use crate::ops::CmpOp;
    let pair_up = |b: &mut Builder<NumOp>, l: usize, r: usize| {
        let pair = b.tuple(vec![l, r]);
        b.add(Op::Zip, vec![pair])
    };
    // an op on each element: `ref` makes a list element a reference (a leaf stays itself)
    let each = |b: &mut Builder<NumOp>, op: NumOp, xs: usize| {
        let mut bb = Builder::default();
        let i = bb.input();
        let u = bb.add(op, vec![i]);
        b.add(Op::MapList(Box::new(bb.finish(u))), vec![xs])
    };
    // (k, v) -> the sorted keys, the values carried along, and a mark where each run starts (its
    // run number changes, or a row starts)
    let sort_by = |b: &mut Builder<NumOp>, k: usize, v: usize, marks: bool| {
        let kv = pair_up(b, k, v);
        let s = b.add(CmpOp::SortBy, vec![kv]);
        let t = b.add(Op::Transpose, vec![s]);
        let (sk, sv) = (b.add(Op::Field(0), vec![t]), b.add(Op::Field(1), vec![t]));
        let mark = marks.then(|| {
            let runs = b.add(Op::Field(2), vec![t]);
            b.add(CmpOp::Adjacent, vec![runs])
        });
        (sk, sv, mark.unwrap_or(t))
    };
    match name {
        "sort" => {
            let units = each(b, Op::Unit.into(), xs);
            sort_by(b, xs, units, false).0
        }
        "dedup" => {
            let r = each(b, Op::Ref.into(), xs);
            let units = each(b, Op::Unit.into(), xs);
            let (sk, _, mark) = sort_by(b, r, units, true);
            let marked = pair_up(b, mark, sk);
            let kept = b.add(Op::Filter, vec![marked]);
            each(b, Op::Clone.into(), kept)
        }
        _ => {
            let kv = b.add(Op::Transpose, vec![xs]);
            let (k, v) = (b.add(Op::Field(0), vec![kv]), b.add(Op::Field(1), vec![kv]));
            let kr = each(b, Op::Ref.into(), k);
            let (sk, sv, mark) = sort_by(b, kr, v, true);
            let mk = pair_up(b, mark, sk);
            let kept = b.add(Op::Filter, vec![mk]);
            let keys = each(b, Op::Clone.into(), kept);
            let mv = pair_up(b, mark, sv);
            let pieces = b.add(Op::Cut, vec![mv]);
            pair_up(b, keys, pieces)
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
