use rtdb_types::{Anomaly,LogicalType as L,Result,ScalarType as S};

pub(crate) fn signature(name: &str,arguments: &[L]) -> Option<Result<L>> {
    if name != "like_regex" { return None; }
    Some((|| {
        if arguments.iter().any(|ty| ty.legs().is_empty()) { return Ok(L::Nothing); }
        if arguments.iter().any(|ty| ty.legs().iter().all(|leg| *leg == L::Null)) { return Ok(L::Null); }
        if arguments.len() != 3 || arguments.iter().any(|ty| ty.legs().iter().any(|leg| !matches!(leg,L::Null | L::Scalar(S::Utf8)))) {
            return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch","like_regex requires three UTF8 arguments"));
        }
        let result = L::Scalar(S::Bool);
        Ok(if arguments.iter().any(L::nullable) { result.maybe() } else { result })
    })())
}
