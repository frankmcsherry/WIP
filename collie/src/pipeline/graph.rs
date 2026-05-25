//! Term-graph IR — the vocabulary a parsed program lowers into.
//!
//! A [`Graph`] is a topologically ordered list of [`Term`]s. Each term is
//! one operator applied to input edges; each edge is an [`OutRef`] naming a
//! specific output of an earlier term. Acyclic by construction (children
//! reference lower indices), pure-functional (a term's output, once
//! produced, is never mutated), and free of any notion of evaluation order
//! beyond the data dependencies the edges express.
//!
//! This module is *just the data*. The stages that produce, transform, and
//! run a graph live in `crate::pipeline` (lower / optimize / execute).

use crate::pipeline::sysop::SystemOp;

/// Reference to one output of a term. Ops can push multiple values
/// (e.g. `flatten` → 2, `split` → N+1), so a flat TermId wouldn't
/// disambiguate which one a consumer wants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutRef {
    pub term: usize,
    /// Output position, in stack order (0 = pushed first).
    pub idx: usize,
}

/// One op applied to its input edges.
#[derive(Debug)]
pub struct Term {
    pub op: SystemOp,
    /// Inputs (the op's arguments), in stack order (bottom-to-top as
    /// `op.run` sees them after popping).
    pub children: Vec<OutRef>,
    /// How many values `op.run` pushes; bounds valid `OutRef.idx` values
    /// that reference this term.
    pub n_outputs: usize,
}

#[derive(Debug, Default)]
pub struct Graph {
    /// Topologically ordered: `children` always reference earlier indices.
    pub terms: Vec<Term>,
    /// Final stack state, bottom-to-top.
    pub roots: Vec<OutRef>,
}
