//! Binary-only utilities: not part of the language, just the runner's
//! supporting infrastructure (REPL, bench harness, pretty-printer,
//! serialization, demo glue, external-op registration example).
//!
//! Library consumers of `collie` shouldn't need anything here.

pub mod bench;
pub mod demos;
pub mod examples_runner;
pub mod ops_extra;
pub mod pretty;
pub mod repl;
pub mod serialize;
