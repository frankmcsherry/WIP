//! Op vocabularies. `core` is the structural + comparison core (`Op<L>`, generic over
//! its body layer); `numeric` is the first layer above it (`NumOp`, embedding
//! `core::Op` plus arithmetic). Future buckets/layers (cmp, swizzle, …) live here too.

pub(crate) mod cmp;
pub(crate) mod core;
pub(crate) mod numeric;

pub use self::cmp::{CmpOp, Pred};
pub use self::core::Op;
pub use self::numeric::{dec_i64, enc_i64, ArithOp, BinOp, Kind, NumOp};
