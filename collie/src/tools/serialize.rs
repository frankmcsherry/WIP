//! Self-describing binary format. Tags:
//!   0x01..0x04 — Prim widths P8/P16/P32/P64
//!   0x10       — Prod
//!   0x11       — Sum
//!   0x12       — List

use std::sync::Arc;
use crate::ir::value::{Value, Prim, prim_p8, prim_p64, prod, sum, list};

const TAG_P8:   u8 = 0x01;
const TAG_P16:  u8 = 0x02;
const TAG_P32:  u8 = 0x03;
const TAG_P64:  u8 = 0x04;
const TAG_PROD: u8 = 0x10;
const TAG_SUM:  u8 = 0x11;
const TAG_LIST: u8 = 0x12;

pub fn encode(v: &Value, out: &mut Vec<u8>) {
    macro_rules! prim_bytes {
        ($tag:expr, $arr:expr) => {{
            out.push($tag);
            let n = $arr.len() as u64;
            out.extend_from_slice(&n.to_le_bytes());
            let bytes: &[u8] = bytemuck::cast_slice(&$arr[..]);
            out.extend_from_slice(bytes);
        }};
    }
    match v {
        Value::Prim(p) => match p {
            Prim::P8(a)  => { out.push(TAG_P8); let n = a.len() as u64; out.extend_from_slice(&n.to_le_bytes()); out.extend_from_slice(&a[..]); }
            Prim::P16(a) => prim_bytes!(TAG_P16, a),
            Prim::P32(a) => prim_bytes!(TAG_P32, a),
            Prim::P64(a) => prim_bytes!(TAG_P64, a),
        }
        Value::Prod(fs) => {
            out.push(TAG_PROD);
            out.push(fs.len() as u8);
            for f in fs.iter() { encode(f, out); }
        }
        Value::Sum { disc, lanes } => {
            out.push(TAG_SUM);
            out.push(lanes.len() as u8);
            // Currently we serialize disc + lanes as separate values inline.
            // Disc is encoded first as a Prim (its own tag), then each lane.
            encode(&Value::Prim(disc.clone()), out);
            for l in lanes.iter() { encode(l, out); }
        }
        Value::List { bounds, values } => {
            out.push(TAG_LIST);
            // Materialize Stride to its P64 view on the wire; the reader
            // reconstructs as Var.
            encode(&Value::Prim(bounds.to_prim()), out);
            encode(values, out);
        }
        Value::View { .. } => {
            // Serialization eagerly materializes Views — the wire format has
            // no notion of deferred indexing, and the receiver shouldn't be
            // forced to keep the source alive. Gather + encode the result.
            match crate::ops::helpers::materialize_ref(v) {
                Ok(mat) => encode(mat.as_ref(), out),
                Err(_) => {
                    // Materialize failure shouldn't happen for well-formed
                    // Views, but to keep the encoder infallible we fall back
                    // to writing a 0-byte placeholder. Decoders will reject.
                    out.push(0u8);
                }
            }
        }
    }
}

pub fn decode(bytes: &[u8], pos: &mut usize) -> Result<Value, String> {
    if *pos >= bytes.len() { return Err("decode: unexpected end".into()); }
    let tag = bytes[*pos]; *pos += 1;
    let read_u64_le = |bytes: &[u8], pos: &mut usize| -> Result<u64, String> {
        if *pos + 8 > bytes.len() { return Err("decode: short length".into()); }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[*pos..*pos + 8]);
        *pos += 8;
        Ok(u64::from_le_bytes(buf))
    };
    macro_rules! read_prim {
        ($n:expr, $ty:ty, $stride:expr, $from_le:expr) => {{
            let n = $n;
            let byte_len = n * $stride;
            if *pos + byte_len > bytes.len() {
                return Err(format!("decode: short {} payload", stringify!($ty)));
            }
            let slice = &bytes[*pos..*pos + byte_len];
            *pos += byte_len;
            match bytemuck::try_cast_slice::<u8, $ty>(slice) {
                Ok(view) => view.to_vec(),
                Err(_) => {
                    let mut out: Vec<$ty> = Vec::with_capacity(n);
                    for i in 0..n {
                        let mut buf = [0u8; $stride];
                        buf.copy_from_slice(&slice[i*$stride..(i+1)*$stride]);
                        out.push($from_le(buf));
                    }
                    out
                }
            }
        }};
    }
    match tag {
        TAG_P8 => {
            let n = read_u64_le(bytes, pos)? as usize;
            if *pos + n > bytes.len() { return Err("decode: short P8 payload".into()); }
            let v = bytes[*pos..*pos + n].to_vec();
            *pos += n;
            Ok(Value::Prim(prim_p8(v)))
        }
        TAG_P16 => {
            let n = read_u64_le(bytes, pos)? as usize;
            let v = read_prim!(n, u16, 2, u16::from_le_bytes);
            Ok(Value::Prim(Prim::P16(Arc::new(v))))
        }
        TAG_P32 => {
            let n = read_u64_le(bytes, pos)? as usize;
            let v = read_prim!(n, u32, 4, u32::from_le_bytes);
            Ok(Value::Prim(Prim::P32(Arc::new(v))))
        }
        TAG_P64 => {
            let n = read_u64_le(bytes, pos)? as usize;
            let v = read_prim!(n, u64, 8, u64::from_le_bytes);
            Ok(Value::Prim(prim_p64(v)))
        }
        TAG_PROD => {
            if *pos >= bytes.len() { return Err("decode: short Prod arity".into()); }
            let arity = bytes[*pos] as usize; *pos += 1;
            let mut fs = Vec::with_capacity(arity);
            for _ in 0..arity { fs.push(decode(bytes, pos)?); }
            Ok(prod(fs))
        }
        TAG_SUM => {
            if *pos >= bytes.len() { return Err("decode: short Sum header".into()); }
            let n_lanes = bytes[*pos] as usize; *pos += 1;
            // Decode disc as a Prim value; extract the Prim from the wrapper.
            let disc_val = decode(bytes, pos)?;
            let disc = match disc_val {
                Value::Prim(p) => p,
                _ => return Err("decode: Sum disc must be a Prim".into()),
            };
            let mut lanes = Vec::with_capacity(n_lanes);
            for _ in 0..n_lanes { lanes.push(decode(bytes, pos)?); }
            Ok(sum(disc, lanes))
        }
        TAG_LIST => {
            let bounds_val = decode(bytes, pos)?;
            let bounds = match bounds_val {
                Value::Prim(p) => p,
                _ => return Err("decode: List bounds must be a Prim".into()),
            };
            let inner = decode(bytes, pos)?;
            Ok(list(crate::ir::value::BoundsRepr::Var(bounds), inner))
        }
        other => Err(format!("decode: unknown tag 0x{:02x}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::ir::value::{from_vec, prim_p8, bounds_var_from_ends};

    #[test]
    fn round_trip_nested_prod_sum_list() {
        let original = prod(vec![
            sum(
                prim_p8(vec![0, 1, 0, 2, 1]),
                vec![
                    from_vec::<u64>(vec![100, 300]),
                    from_vec::<f64>(vec![2.5, 4.5]),
                    from_vec::<u8>(vec![42]),
                ],
            ),
            Value::List {
                bounds: bounds_var_from_ends(vec![2, 5, 6]),
                values: Arc::new(from_vec::<i64>(vec![-1, -2, 10, 20, 30, 999])),
            },
        ]);
        let mut bytes = Vec::new();
        encode(&original, &mut bytes);
        let mut pos = 0;
        let decoded = decode(&bytes, &mut pos).expect("decode");
        assert_eq!(pos, bytes.len(), "decode did not consume all bytes");
        assert_eq!(original, decoded);
    }
}
