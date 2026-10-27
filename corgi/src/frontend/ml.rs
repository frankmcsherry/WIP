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
//!          | pipe
//!   shape  = 'int' | 'float' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM
//!          | '_'                                       -- a hole: any shape (not in a declaration)
//!   pat    = (IDENT | '_' | '(' (pat (',' pat)*)? ')') (':' shape)?   -- irrefutable: names, wildcards, tuples
//!   pipe   = atom apply*                               -- juxtaposition; chain ends before `in`
//!   apply  = 'map' '(' lambda ')'
//!          | ('fold' | 'scan') '(' lambda ')'                 -- (seed, list); lambda is (acc, x)
//!          | 'map_variant' tag '(' lambda ')'
//!          | 'match' '(' (tag '(' lambda ')')(',' …)* ')'   -- MapSum + Unwrap
//!          | 'inject' VARIANT                               -- sum construction (its other lanes shaped)
//!          | 'branch' (NUM | ENUM)                          -- lane count, literal or by enum name
//!          | 'split' STR                                    -- delimiter as a one-byte string
//!          | 'call' IDENT                                   -- a word the host bound in the `Scope`
//!          | '.' NUM                                        -- projection
//!          | ':' shape                                      -- ascription: the value has this shape
//!          | IDENT NUM?
//!   tag    = NUM | VARIANT                              -- a variant name resolves to its tag
//!   VARIANT = IDENT | ENUM '::' IDENT                   -- `Foo::Bar` where two enums declare a `Bar`
//!   lambda = pat '->' expr                              -- a tuple pattern destructures the parameter
//!   atom   = '(' (expr (',' expr)*)? ')' | IDENT | LIT | STR   -- 'input' is the root; '(' … ')' is a tuple
//!          | '[' (const (',' const)*)? ']'              -- a list literal: one constant list
//!   const  = (NUM | LIT | STR | '(' (const (',' const)*)? ')' | '[' (const (',' const)*)? ']') (':' shape)?
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
//! A list literal is a constant too, one list filled like any literal, as a string is: `"ab"` is
//! `[97, 98]`, the same node holding the same value. Its elements are constants of one shape:
//! numbers, strings, `()`, and tuples and lists of these (`[(0, "GET"), (1, "PUT")]`). An empty
//! list takes its shape from the elements beside it (`[[1], []]`) or from an ascription
//! (`[] : List(int)`). A list of computed values is not a literal: it is an `append` of enlisted
//! columns, `(a enlist, b enlist, (a, b) add enlist) append`, one pass whatever its length.
//!
//! An ascription `: S` says a value has the shape `S`, as a stage (`xs : List(int)`) or on a
//! pattern (`(acc : (int, int), x : int) -> …`), `_` being a hole and an enum naming its sum (a
//! variant declared without a payload is a hole). It is a check, made when the program is typed:
//! one `Op::Pattern`, which a tuple pattern's own nesting joins. It converts nothing and steers
//! nothing; the one place it gives a shape instead of checking one is the empty list, which has
//! none of its own.
//!
//! A program is parsed in a `Scope` (`parse_ml_in`; `parse_ml` is the empty one): names the host
//! binds, as it binds `input`. `x call name` grafts the graph the host bound to `name` with `x` as
//! its input — a host kernel is a graph of one node, and a word may also be written in corgi — and
//! the scope's enums are declared in every program parsed in it. Only `call` reaches the host's
//! words, so they and corgi's own never collide.
//!
//! e.g.  let (subj, vals) = input.1 transpose in vals fold_add
//!       e match (0 (lo -> lo), 1 (hi -> (hi, 100) add))   -- exhaustive ⇒ Unwrap types it
//!       enum Size = Lo | Hi in … match (Lo (l -> l), Hi (h -> (h, 100) add))
//!       enum Opt = None () | Some int in xs inject Some  -- tag xs into Some; None is an empty unit lane
//!       enum Option = None () | Some in (3 inject Some, "dog" inject Some)  -- Some takes its payload's shape

use super::{resolve, retired, str_value, takes_num};
use crate::graph::{Builder, Graph, Node, NodeKind};
use crate::ops::{CmpOp, NumOp, Op, Pattern, Pred};
use crate::shape::Shape;
use crate::value::{Prim, Scalar, Value};
use std::collections::HashMap;
use std::sync::Arc;

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
    PathSep, // :: — between an enum and its variant, `Foo::Bar`
    Colon, // : — before a shape, in an ascription
    LBracket, // [ — a list literal
    RBracket, // ]
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
            ':' if cs.get(i + 1) == Some(&':') => {
                i += 2;
                Tok::PathSep
            }
            ':' => {
                i += 1;
                Tok::Colon
            }
            '[' => {
                i += 1;
                Tok::LBracket
            }
            ']' => {
                i += 1;
                Tok::RBracket
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
                    Tok::Lit(Value::int([n]))
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

/// an irrefutable pattern: a name, `_`, or a tuple of patterns, each perhaps ascribed a shape.
/// Refutable matching (a sum's lane) is `match`'s job, never a pattern's.
enum Pat {
    Name(String),
    Wild,
    Tuple(Vec<Pat>),
    Typed(Box<Pat>, Pattern), // `p : S`: the pattern, and what it and its ascription want together
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
    Inject(usize, Vec<Option<Shape>>), // tag + the declared lane shapes (the filled one may be open) -> Op::Inject
    Head, // `head`: sugar for `(0, list) get` — the first element (an empty row errs)
    Call(Graph<NumOp>), // `call name`: the graph the scope binds to `name`, grafted onto the value
    Check(Pattern), // `: S`: the value has this shape (`_` a hole); typing fails otherwise
}

/// a constant in a list literal, before the literal's shape is known: an empty list's element
/// shape may come from the elements beside it.
enum Const {
    Leaf(Scalar),
    Tuple(Vec<Const>), // `()` is the unit
    List(Vec<Const>),
    Typed(Box<Const>, Pattern), // `c : S`
}

enum E {
    Var(String, String), // the name, and its source position for errors
    Lit(Value),          // a typed constant or a string, filled to the length of its scope's input
    Tuple(Vec<E>),
    Let(Pat, Box<E>, Box<E>),
    Pipe(Box<E>, Apply),
}

// ----- parser ------------------------------------------------------------

/// The `enum` declarations: compile-time tables, so names resolve at parse and erase from the AST,
/// and the core stays positional. Two enums may declare the same variant name; a use of a shared
/// name says which enum it means, `Foo::Bar`, as nothing is typed while parsing.
#[derive(Clone, Default)]
struct Enums {
    decls: HashMap<String, Vec<(String, Option<Shape>)>>, // enum -> its variants and payload shapes (if declared), in tag order
    owners: HashMap<String, Vec<String>>,                 // variant -> the enums declaring it, in order
}

impl Enums {
    fn declare(&mut self, name: String, vs: Vec<(String, Option<Shape>)>) -> Result<(), String> {
        if self.decls.contains_key(&name) {
            return Err(format!("duplicate enum '{name}'"));
        }
        for (k, (v, _)) in vs.iter().enumerate() {
            if vs[..k].iter().any(|(w, _)| w == v) {
                return Err(format!("enum '{name}' declares '{v}' twice"));
            }
        }
        for (v, _) in &vs {
            self.owners.entry(v.clone()).or_default().push(name.clone());
        }
        self.decls.insert(name, vs);
        Ok(())
    }

    /// the tag of variant `v`, and its enum: enum `e` if the use names it (`e::v`), else the one
    /// enum that declares `v`.
    fn tag(&self, e: Option<&str>, v: &str) -> Result<(usize, &str), String> {
        let e = match (e, self.owners.get(v).map(Vec::as_slice)) {
            (Some(e), _) => e,
            (None, Some([e])) => e.as_str(),
            (None, Some(es)) => {
                let paths: Vec<String> = es.iter().map(|e| format!("{e}::{v}")).collect();
                return Err(format!("'{v}' is a variant of {}: write {}", es.join(" and "), paths.join(" or ")));
            }
            (None, None) => return Err(format!("unknown variant '{v}'")),
        };
        let (e, vs) = self.decls.get_key_value(e).ok_or_else(|| format!("unknown enum '{e}'"))?;
        let tag = vs.iter().position(|(w, _)| w == v).ok_or_else(|| format!("enum '{e}' has no variant '{v}'"))?;
        Ok((tag, e.as_str()))
    }

    /// enum `e` as a pattern: a sum of its lanes, where a variant declared without a payload shape
    /// is a hole.
    fn pattern(&self, e: &str) -> Result<Pattern, String> {
        let vs = self.decls.get(e).ok_or_else(|| format!("unknown enum '{e}'"))?;
        Ok(Pattern::Sum(vs.iter().map(|(_, s)| s.as_ref().map_or(Pattern::Any, Pattern::from)).collect()))
    }

    /// the lane shapes of enum `e` for an `inject` into lane `tag`: that lane's as declared, if it
    /// is, and every other lane's, which must be.
    fn lanes(&self, e: &str, tag: usize) -> Result<Vec<Option<Shape>>, String> {
        let vs = self.decls.get(e).ok_or_else(|| format!("unknown enum '{e}'"))?;
        for (k, (v, s)) in vs.iter().enumerate() {
            if k != tag && s.is_none() {
                let filled = &vs[tag].0;
                return Err(format!(
                    "inject {filled}: enum '{e}' declares no payload shape for '{v}', whose empty lane needs one"
                ));
            }
        }
        Ok(vs.iter().map(|(_, s)| s.clone()).collect())
    }
}

struct P<'a> {
    toks: Vec<Tok>,
    i: usize,
    src: Vec<char>,
    starts: Vec<usize>, // char offset of each token
    seen: std::cell::Cell<usize>, // the last token looked at, where a parse error is reported
    enums: Enums,                             // the scope's, and then the program's own
    words: &'a HashMap<String, Graph<NumOp>>, // the scope's words, which `call` reaches
}

impl<'a> P<'a> {
    /// a parser over `src`, its tables starting as `scope`'s.
    fn new(src: &str, scope: &'a Scope) -> Result<Self, String> {
        let (toks, starts) = lex(src)?;
        Ok(P {
            toks,
            i: 0,
            src: src.chars().collect(),
            starts,
            seen: std::cell::Cell::new(0),
            enums: scope.enums.clone(),
            words: &scope.words,
        })
    }

    /// `f` over the whole input: an error is placed at the last token looked at, and tokens left
    /// over are an error.
    fn whole<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T, String>) -> Result<T, String> {
        let t = f(self).map_err(|e| format!("{}: {e}", self.at(self.seen.get())))?;
        if self.i != self.toks.len() {
            return Err(format!("{}: unexpected {:?}", self.at(self.i), self.toks[self.i]));
        }
        Ok(t)
    }

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
            // the tables and parses on into the body, leaving no AST node behind.
            self.enum_decl()?;
            self.kw_in()?;
            self.expr()
        } else {
            self.pipe()
        }
    }

    /// `enum Name = V0 shape? | V1 shape? | …`, into the tables. A payload shape is needed only
    /// where the sum is BUILT by `inject`, and then only for the lanes it does not fill (they are
    /// built empty at those shapes; the filled lane may take its payload's); `branch Name` /
    /// `match` / `map_variant` read just the tags.
    fn enum_decl(&mut self) -> Result<(), String> {
        if !self.is_kw("enum") {
            return Err(format!("expected 'enum', found {:?}", self.peek()));
        }
        self.bump();
        let name = self.ident()?;
        self.eat(&Tok::Eq)?;
        let mut vs = vec![self.variant_decl()?];
        while self.peek() == Some(&Tok::Bar) {
            self.bump();
            vs.push(self.variant_decl()?);
        }
        self.enums.declare(name, vs)
    }

    /// the source position of token `k` (or of the end of input).
    fn at(&self, k: usize) -> String {
        position(&self.src, self.starts.get(k).copied().unwrap_or(self.src.len()))
    }

    /// a binding pattern, used by `let` and lambda parameters alike, perhaps ascribed a shape:
    /// `(acc : (int, int), x : int)`. The ascription and the pattern's own nesting make one check.
    fn pat(&mut self) -> Result<Pat, String> {
        let p = self.bare_pat()?;
        if self.peek() != Some(&Tok::Colon) {
            return Ok(p);
        }
        self.bump();
        let s = self.shape_pattern()?;
        let both = p.pattern().meet(&s).ok_or_else(|| format!("the pattern {} cannot be {s}", p.pattern()))?;
        Ok(Pat::Typed(Box::new(p), both))
    }

    /// a pattern without its ascription.
    fn bare_pat(&mut self) -> Result<Pat, String> {
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

    /// a payload shape in an `enum` declaration: a shape with no holes, where an ENUM names an
    /// earlier, fully-shaped enum (so sums nest, but never recursively).
    fn shape(&mut self) -> Result<Shape, String> {
        let p = self.shape_pattern()?;
        p.whole().ok_or_else(|| {
            format!("the payload shape {p} has a hole: a `_`, or an enum variant with no declared payload")
        })
    }

    /// a shape, perhaps with holes, as an ascription writes it:
    ///   shape = 'int' | 'float' | '()' | '(' shape (',' shape)* ')' | 'List' '(' shape ')' | ENUM | '_'
    /// where ENUM names an enum, a variant declared without a payload shape being a hole.
    fn shape_pattern(&mut self) -> Result<Pattern, String> {
        match self.bump() {
            Some(Tok::LParen) => {
                if self.peek() == Some(&Tok::RParen) {
                    self.bump();
                    return Ok(Pattern::Tuple(Vec::new()));
                }
                let mut fields = vec![self.shape_pattern()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.bump();
                    fields.push(self.shape_pattern()?);
                }
                self.eat(&Tok::RParen)?;
                Ok(Pattern::Tuple(fields))
            }
            Some(Tok::Ident(k)) => match k.as_str() {
                "_" => Ok(Pattern::Any),
                "int" => Ok(Pattern::Int),
                "float" => Ok(Pattern::Float),
                "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
                    Err(format!("'{k}' is retired: an integer is `int` (its width is storage), a float `float`"))
                }
                "List" => {
                    self.eat(&Tok::LParen)?;
                    let inner = self.shape_pattern()?;
                    self.eat(&Tok::RParen)?;
                    Ok(Pattern::List(Box::new(inner)))
                }
                e => self.enums.pattern(e),
            },
            other => Err(format!("expected a shape, found {other:?}")),
        }
    }

    /// a variant tag at a use site: a literal number, or a declared variant name — which also
    /// names its enum, so `inject` by name knows the whole sum it builds. A name two enums declare
    /// is written with its enum, `Foo::Bar`.
    fn variant(&mut self) -> Result<(usize, Option<String>), String> {
        match self.bump() {
            Some(Tok::Num(n)) => Ok((n as usize, None)),
            Some(Tok::Ident(v)) => {
                let (e, v) = if self.peek() == Some(&Tok::PathSep) {
                    self.bump();
                    (Some(v), self.ident()?)
                } else {
                    (None, v)
                };
                let (tag, e) = self.enums.tag(e.as_deref(), &v)?;
                Ok((tag, Some(e.to_string())))
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

    /// whether the next token can begin a pipe stage — a `.N` projection, an ascription `: S`, or
    /// any identifier other than the chain-terminating `in`.
    fn starts_apply(&self) -> bool {
        match self.peek() {
            Some(Tok::Dot) | Some(Tok::Colon) => true,
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
        // `: S` after any value checks its shape.
        if let Some(Tok::Colon) = self.peek() {
            self.bump();
            return Ok(Apply::Check(self.shape_pattern()?));
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
            // of its enum, whose every other lane must declare a payload shape (they are built empty
            // at those shapes). The filled lane's shape, if the enum leaves it open, is the
            // payload's. No numeric form: a sum is only ever built from a declaration.
            "inject" => {
                let (tag, e) = self.variant()?;
                let Some(e) = e else { return Err("inject needs a declared variant name".into()) };
                Ok(Apply::Inject(tag, self.enums.lanes(&e, tag)?))
            }
            // head: first element, sugar for `get 0` — checked (an empty row errs, carried in the err-mask).
            "head" => Ok(Apply::Head),
            // call: a word the host bound in the scope (`Scope::word`). Only `call` reaches the
            // scope's words, so they and corgi's own never collide.
            "call" => {
                let w = self.ident()?;
                let g = self.words.get(&w).ok_or_else(|| format!("no word '{w}' in scope"))?;
                Ok(Apply::Call(g.clone()))
            }
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
                        self.enums.decls.get(&e).ok_or_else(|| format!("unknown enum '{e}'"))?.len() as u64
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
                Ok(E::Lit(Value::int([n])))
            }
            // a list literal: one constant list, built here
            Some(Tok::LBracket) => {
                let list = self.constant()?;
                let p = list.pattern()?;
                let shape = p.whole().ok_or_else(|| {
                    format!("the list literal's shape {p} has a hole: an empty list takes its shape from the elements beside it, or from an ascription, as in [] : List(int)")
                })?;
                Ok(E::Lit(column(&[&list], &shape)))
            }
            other => Err(format!("expected an expression, found {other:?}")),
        }
    }

    /// a constant in a list literal, perhaps ascribed: a number, a string, `()`, or a tuple or
    /// list of constants.
    fn constant(&mut self) -> Result<Const, String> {
        let c = match self.bump() {
            Some(Tok::Num(n)) => {
                Const::Leaf(Scalar::Int(i64::try_from(n).map_err(|_| format!("{n} does not fit in an Int (an i64)"))?))
            }
            Some(Tok::Lit(Value::Prim(p))) => Const::Leaf(Scalar::of(&p, 0)),
            Some(Tok::Str(bytes)) => Const::List(bytes.into_iter().map(|b| Const::Leaf(Scalar::Int(b as i64))).collect()),
            Some(Tok::LParen) => Const::Tuple(self.constants(Tok::RParen)?),
            Some(Tok::LBracket) => Const::List(self.constants(Tok::RBracket)?),
            other => {
                return Err(format!(
                    "a list literal holds constants (numbers, strings, `()`, and tuples and lists of these), not {other:?}; build a list of computed values with append, as in (a enlist, b enlist) append"
                ))
            }
        };
        if self.peek() != Some(&Tok::Colon) {
            return Ok(c);
        }
        self.bump();
        Ok(Const::Typed(Box::new(c), self.shape_pattern()?))
    }

    /// the constants up to the closing token, separated by commas (the opening one is read).
    fn constants(&mut self, close: Tok) -> Result<Vec<Const>, String> {
        let mut cs = Vec::new();
        if self.peek() == Some(&close) {
            self.bump();
            return Ok(cs);
        }
        loop {
            cs.push(self.constant()?);
            match self.bump() {
                Some(Tok::Comma) => {}
                Some(t) if t == close => return Ok(cs),
                other => {
                    let close = if close == Tok::RBracket { ']' } else { ')' };
                    return Err(format!(
                        "expected ',' or '{close}' in a list literal, found {other:?}; a list literal holds constants, and a list of computed values is built with append, as in (a enlist, b enlist) append"
                    ));
                }
            }
        }
    }
}

// ----- lowering ----------------------------------------------------------

type Env = HashMap<String, usize>;

/// The environment key of the scope's input, which constants are filled to the length of. It
/// can't collide with a name: identifiers never contain a space.
const ROOT: &str = " root";

/// bind a pattern to a node: a name binds the node itself, `_` binds nothing, and a tuple pattern
/// binds each sub-pattern to a `Field` projection of it. A pattern that wants anything of the
/// value (a tuple pattern takes apart exactly its n fields, at every level, and an ascription
/// wants its shape) is one check of the whole pattern, which its names read through; that check
/// is returned.
fn bind(pat: &Pat, id: usize, env: &mut Env, b: &mut Builder<NumOp>) -> Option<usize> {
    let p = pat.pattern();
    if p == Pattern::Any {
        bind_fields(pat, id, env, b);
        return None;
    }
    let checked = b.add(Op::Pattern(p), vec![id]);
    bind_fields(pat, checked, env, b);
    Some(checked)
}

/// bind a checked tuple pattern's names to its fields, and its sub-patterns' to theirs.
fn bind_fields(pat: &Pat, id: usize, env: &mut Env, b: &mut Builder<NumOp>) {
    match pat {
        Pat::Name(x) => {
            env.insert(x.clone(), id);
        }
        Pat::Wild => {}
        Pat::Typed(p, _) => bind_fields(p, id, env, b),
        Pat::Tuple(pats) => {
            for (i, sub) in pats.iter().enumerate() {
                let fid = b.add(Op::Field(i), vec![id]);
                bind_fields(sub, fid, env, b);
            }
        }
    }
}

impl Pat {
    /// what this pattern wants of a value: the nesting of products it takes apart, and the
    /// shapes it is ascribed.
    fn pattern(&self) -> Pattern {
        match self {
            Pat::Name(_) | Pat::Wild => Pattern::Any,
            Pat::Tuple(pats) => Pattern::Tuple(pats.iter().map(Pat::pattern).collect()),
            Pat::Typed(_, p) => p.clone(),
        }
    }
}

impl Const {
    /// the constant without its ascriptions.
    fn bare(&self) -> &Const {
        match self {
            Const::Typed(c, _) => c.bare(),
            c => c,
        }
    }

    /// the shape the constant's text fixes: an empty list's elements are a hole until the
    /// elements beside it, or an ascription, fill it.
    fn pattern(&self) -> Result<Pattern, String> {
        Ok(match self {
            Const::Leaf(Scalar::Int(_)) => Pattern::Int,
            Const::Leaf(Scalar::Float(_)) => Pattern::Float,
            Const::Tuple(cs) => Pattern::Tuple(cs.iter().map(Const::pattern).collect::<Result<_, _>>()?),
            Const::List(cs) => {
                let mut elems = Pattern::Any;
                for c in cs {
                    let p = c.pattern()?;
                    elems = elems
                        .meet(&p)
                        .ok_or_else(|| format!("a list literal's elements differ in shape: {elems} and {p}"))?;
                }
                Pattern::List(Box::new(elems))
            }
            Const::Typed(c, s) => {
                let p = c.pattern()?;
                p.meet(s).ok_or_else(|| format!("a constant of shape {p} is not {s}"))?
            }
        })
    }
}

/// the column of the constants `cs`, each of the whole shape `shape`. An Int column is held as
/// [`Value::int`] holds it, as an Int literal is, so `[97, 98]` is the value `"ab"` is.
fn column(cs: &[&Const], shape: &Shape) -> Value {
    let leaf = |c: &&Const| match c.bare() {
        Const::Leaf(x) => *x,
        _ => unreachable!("a constant of a leaf shape is a leaf"),
    };
    match shape {
        Shape::Int => Value::int(cs.iter().map(|c| match leaf(c) {
            Scalar::Int(x) => x,
            Scalar::Float(_) => unreachable!("an Int constant"),
        })),
        Shape::Float => Value::Prim(Prim::F64(Arc::new(cs.iter().map(|c| match leaf(c) {
            Scalar::Float(k) => k,
            Scalar::Int(_) => unreachable!("a Float constant"),
        }).collect()))),
        Shape::Unit => Value::Unit(cs.len()),
        Shape::Prod(fields) => Value::Prod(
            fields
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    let field: Vec<&Const> = cs.iter().map(|c| match c.bare() {
                        Const::Tuple(fs) => &fs[i],
                        _ => unreachable!("a constant of a product shape is a tuple"),
                    }).collect();
                    column(&field, s)
                })
                .collect(),
        ),
        Shape::List(elem) => {
            let (mut items, mut ends) = (Vec::new(), Vec::with_capacity(cs.len()));
            for c in cs {
                let Const::List(xs) = c.bare() else { unreachable!("a constant of a list shape is a list") };
                items.extend(xs);
                ends.push(items.len());
            }
            Value::List(ends.into(), Box::new(column(&items, elem)))
        }
        Shape::Sum(_) | Shape::Ref(_) => unreachable!("no constant is a sum or a reference"),
    }
}

/// lower a lambda body into a closed sub-graph (its parameter is the body's `Input`).
fn lower_body(pat: &Pat, body: &E) -> Result<Graph<NumOp>, String> {
    let mut bb = Builder::default();
    let bin = bb.input();
    let mut benv = Env::new();
    benv.insert(ROOT.to_string(), bin);
    // the body's constants are as long as the checked parameter, so a closed body reads it
    if let Some(checked) = bind(pat, bin, &mut benv, &mut bb) {
        benv.insert(ROOT.to_string(), checked);
    }
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
            // past a tuple pattern, the value it took apart is its checked form: the body's
            // constants are as long as it, and a name it was bound to now means the checked value
            if let Some(checked) = bind(pat, id, &mut env2, b) {
                env2.insert(ROOT.to_string(), checked);
                if let E::Var(name, _) = &**bound {
                    env2.insert(name.clone(), checked);
                }
            }
            lower(body, &env2, b)
        }
        // `(c1, c2, ..) all`: every condition holds; `any`: one does. Normalize nonzero masks
        // to 1 before taking `min` or `max` along the tuple as written.
        E::Pipe(e, Apply::Op(name, None)) if name == "all" || name == "any" => {
            let E::Tuple(conds) = &**e else {
                return Err(format!("'{name}' takes a tuple of conditions, written out: (c1, c2, ..) {name}"));
            };
            let op = if name == "all" { CmpOp::Min } else { CmpOp::Max };
            let normalize = CmpOp::RelImm(Pred::Ne, Scalar::Int(0));
            let (first, rest) = conds.split_first().ok_or_else(|| format!("'{name}' of no conditions"))?;
            let first = lower(first, env, b)?;
            let mut acc = b.add(normalize.clone(), vec![first]);
            for c in rest {
                let c = lower(c, env, b)?;
                let c = b.add(normalize.clone(), vec![c]);
                let pair = b.tuple(vec![acc, c]);
                acc = b.add(op.clone(), vec![pair]);
            }
            Ok(acc)
        }
        E::Pipe(e, ap) => {
            let id = lower(e, env, b)?;
            match ap {
                Apply::Op(name, _) if name == "slices" => Ok(slices_word(b, id)),
                Apply::Op(name, arg) if name.starts_with("try_") => {
                    let arity = match &**e { E::Tuple(es) => Some(es.len()), _ => None };
                    try_word(b, id, name, *arg, arity)
                }
                Apply::Op(name, _) if matches!(name.as_str(), "sort" | "dedup" | "group") => Ok(sort_word(b, id, name)),
                Apply::Op(name, _) if name == "hash_group" => Ok(b.graft(&parse_ml(HASH_GROUP)?, id)),
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
                    let zero = b.add(Op::Lit(Value::int([0])), vec![id]);
                    let pair = b.tuple(vec![zero, id]);
                    Ok(b.add(Op::Gather, vec![pair]))
                }
                Apply::Call(g) => Ok(b.graft(g, id)),
                Apply::Check(Pattern::Any) => Ok(id),
                Apply::Check(p) => Ok(b.add(Op::Pattern(p.clone()), vec![id])),
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
/// - `(xs, ys, ..) try_zip`: any list's length differs from the first. A tuple not written out
///   must be a pair, checked by a pattern before reading its fields;
/// - `xs try_chunk k`: `xs len` leaves a remainder by `k` (the remainder is the mark).
fn try_word(b: &mut Builder<NumOp>, mut x: usize, name: &str, arg: Option<u64>, arity: Option<usize>) -> Result<usize, String> {
    use crate::ops::{ArithOp, BinOp, BitOp, CmpOp, Pred, Red};
    fn on(b: &mut Builder<NumOp>, op: impl Into<NumOp>, l: usize, r: usize) -> usize {
        let pair = b.tuple(vec![l, r]);
        b.add(op, vec![pair])
    }
    fn lit(b: &mut Builder<NumOp>, n: i64, like: usize) -> usize {
        b.add(Op::Lit(Value::int([n])), vec![like])
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
            let n = arity.unwrap_or_else(|| {
                x = b.add(Op::Pattern(Pattern::Tuple(vec![Pattern::Any; 2])), vec![x]);
                2
            });
            let first = b.add(Op::Field(0), vec![x]);
            let len = b.add(Op::Len, vec![first]);
            let mut mark = None;
            for i in 1..n {
                let field = b.add(Op::Field(i), vec![x]);
                let next = b.add(Op::Len, vec![field]);
                let differs = on(b, CmpOp::Rel(Pred::Ne), len, next);
                mark = Some(match mark {
                    None => differs,
                    Some(prev) => on(b, CmpOp::Max, prev, differs),
                });
            }
            // One list always agrees with itself; `len` still checks that it is a list.
            (Op::Zip.into(), mark.unwrap_or_else(|| lit(b, 0, len)))
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

/// `kvs hash_group`: `group` by hashing, the word written in corgi. Each row's `(key, value)`
/// pairs, grouped by key: the groups in the order of their keys' hashes, not of the keys, and each
/// key a reference into the row's keys (`clone` it to copy it out). Sorting the 64-bit hashes is
/// much cheaper than sorting keys such as strings. The groups are exact: a group of equal hashes
/// holds one key unless two keys share a hash, and its keys then change inside it. A row where
/// that happens is grouped by sorting the keys themselves.
const HASH_GROUP: &str = "
    let (k, v) = input transpose in
    let refs = k map (x -> x ref) in
    let (_, kvs, runs) = (k map (x -> x hash), (refs, v) zip) zip sort_by transpose in
    let starts = runs adjacent in
    let changes = kvs map ((r, _) -> r) adjacent in
    (((kvs, starts), (refs, v)), (changes, starts) ne) branch 2 match (
        0 (((kvs, starts), _) ->
            ((starts, kvs map ((r, _) -> r)) zip filter, (starts, kvs map ((_, x) -> x)) zip cut) zip),
        1 ((_, (refs, v)) ->
            let (keys, kvs, runs) = (refs, (refs, v) zip) zip sort_by transpose in
            let starts = runs adjacent in
            ((starts, keys) zip filter, (starts, kvs map ((_, x) -> x)) zip cut) zip))
";

/// `sort`, `dedup` and `group` as words over `sort_by` (stable by key, a payload carried along, and
/// each element's run of equal keys), `adjacent`, `filter` and `cut`. `dedup` and `group` sort a
/// list key by reference, so that only the keys they keep are copied out:
/// - `xs sort` = `sort_by` with a unit payload;
/// - `xs dedup` = the sorted elements that start a run;
/// - `kvs group` = the keys that start a run, and the values cut where a run starts.
fn sort_word(b: &mut Builder<NumOp>, xs: usize, name: &str) -> usize {
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

/// What a program may name without defining it, as it names `input`: words the host binds, which
/// `call` reaches, and enums declared once for every program parsed in the scope. Both resolve at
/// parse and erase, so a program parsed in a scope is the graph it would be had it written the
/// declarations and the words' graphs itself.
#[derive(Clone, Default)]
pub struct Scope {
    words: HashMap<String, Graph<NumOp>>,
    enums: Enums,
}

impl Scope {
    /// Bind `name` to `graph`: `x call name` grafts `graph`, its input `x`. A host kernel is the
    /// graph of one `NumOp::Host` node over the input.
    pub fn word(&mut self, name: &str, graph: Graph<NumOp>) -> Result<(), String> {
        let mut cs = name.chars();
        let ident = cs.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && cs.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !ident {
            return Err(format!("'{name}' is not a name `call` can reach"));
        }
        if self.words.contains_key(name) {
            return Err(format!("duplicate word '{name}'"));
        }
        self.words.insert(name.to_string(), graph);
        Ok(())
    }

    /// Declare an enum for every program parsed in this scope: `enum Name = V0 shape? | …`, as a
    /// program writes it but without the `in`. Enum names are unique across the scope and every
    /// program parsed in it, as within one program; a variant name two enums share is written
    /// `Foo::Bar` where it is used. On an error the scope is unchanged.
    pub fn declare(&mut self, src: &str) -> Result<(), String> {
        let mut p = P::new(src, self)?;
        p.whole(P::enum_decl)?;
        let P { enums, .. } = p;
        self.enums = enums;
        Ok(())
    }

    /// The tag of a variant, and its enum, resolved as a program resolves them: `Bar`, which one
    /// enum declares, or `Foo::Bar`.
    pub fn variant(&self, name: &str) -> Result<(usize, &str), String> {
        match name.split_once("::") {
            Some((e, v)) => self.enums.tag(Some(e), v),
            None => self.enums.tag(None, name),
        }
    }

    /// Enum `name`'s variants and their payload shapes (`None` where a variant declares none), in
    /// tag order.
    pub fn variants(&self, name: &str) -> Option<&[(String, Option<Shape>)]> {
        self.enums.decls.get(name).map(Vec::as_slice)
    }
}

/// parse an ML-flavoured expression into a `Graph` (with `input` bound to the root).
pub fn parse_ml(src: &str) -> Result<Graph<NumOp>, String> {
    parse_ml_in(src, &Scope::default())
}

/// `parse_ml` in `scope`: the program may `call` its words and use its enums.
pub fn parse_ml_in(src: &str, scope: &Scope) -> Result<Graph<NumOp>, String> {
    let mut p = P::new(src, scope)?;
    let e = p.whole(P::expr)?;
    let mut b = Builder::default();
    let input = b.input();
    let mut env = Env::new();
    env.insert("input".to_string(), input);
    env.insert(ROOT.to_string(), input);
    let out = lower(&e, &env, &mut b)?;
    Ok(b.finish(out))
}
