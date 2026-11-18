use std::{alloc::Layout,sync::Arc};
use rtdb_types::Result;
use crate::{admission::Admission,numeric::overflow};
use super::{Map,Object,T,Throwable,Time,render};

pub(crate) enum FormatArgument { Semantic(String),Wire(T) }

pub(crate) fn format_argument(value: T,admission: Admission<'_>) -> Result<FormatArgument> {
    if let T::Tagged { tag,representation } = &value
        && matches!(tag.as_str(),"time/date" | "time/time")
        && let T::String(source) = representation.as_ref()
        && let Some(time) = Time::read_text(tag,source)?
    {
        return render::string(&Object::Time(time),admission).map(FormatArgument::Semantic);
    }
    let Some(message_bytes) = message_bytes(&value) else { return Ok(FormatArgument::Wire(value)); };
    let retained = Layout::new::<Throwable>().size().checked_add(32).and_then(|bytes|bytes.checked_add(message_bytes)).ok_or_else(overflow)?;
    admission.reserve(retained)?;
    let arguments = arguments(value);
    let throwable = Object::Throwable(Arc::new(Throwable::exception_info(&arguments)?));
    let text = render::string(&throwable,admission)?;
    drop(throwable);drop(arguments);
    Ok(FormatArgument::Semantic(text))
}

fn message_bytes(value: &T) -> Option<usize> {
    let T::Tagged { tag,representation } = value else { return None; };
    if tag != "xtdb/exception-info" { return None; }
    let T::Array(values) = representation.as_ref() else { return None; };
    let [message,data] = values.as_slice() else { return None; };
    let bytes = match message { T::Null => 0,T::String(value) => value.len(),_ => return None };
    match data { T::Null => Some(bytes),T::Map(entries) if entries.is_empty() => Some(bytes),_ => None }
}

fn arguments(value: T) -> [Object;2] {
    let T::Tagged { representation,.. } = value else { unreachable!() };
    let T::Array(values) = *representation else { unreachable!() };
    let mut values = values.into_iter();
    let message = match values.next().unwrap() { T::Null => Object::Null,T::String(value) => Object::String(value),_ => unreachable!() };
    let data = match values.next().unwrap() { T::Null => Object::Null,T::Map(entries) if entries.is_empty() => Object::Map(Map::new()),_ => unreachable!() };
    [message,data]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvaluationScope;
    use rtdb_types::MemoryBudget;

    fn tag(name: &str,representation: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(representation) } }
    fn info(message: T,data: T) -> T { tag("xtdb/exception-info",T::Array(vec![message,data])) }
    fn semantic(value: T,admission: Admission<'_>) -> String {
        match format_argument(value,admission).unwrap() { FormatArgument::Semantic(value) => value,FormatArgument::Wire(_) => panic!("semantic text") }
    }
    fn failure(value: T,admission: Admission<'_>) -> rtdb_types::Anomaly {
        match format_argument(value,admission) { Err(error) => error,Ok(_) => panic!("admission failure") }
    }

    #[test]
    fn bounded_exception_info_shapes_share_scalar_text_in_both_admission_modes() {
        let memory = MemoryBudget::new(1 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            for (message,expected) in [
                (T::Null,"clojure.lang.ExceptionInfo: null {}"),
                (T::String(String::new()),"clojure.lang.ExceptionInfo:  {}"),
                (T::String("quote ' \"\nλ😀".into()),"clojure.lang.ExceptionInfo: quote ' \"\nλ😀 {}"),
            ] {
                for data in [T::Null,T::Map(vec![])] {
                    let value = info(message.clone(),data);
                    assert_eq!(semantic(value.clone(),Admission::Standalone),expected);
                    let retained = memory.used_bytes();
                    let output = semantic(value,Admission::Tracked(&scope));
                    assert_eq!(output,expected);
                    assert!(memory.used_bytes() > retained + output.len());
                    let charged = memory.used_bytes();drop(output);
                    assert_eq!(memory.used_bytes(),charged);
                }
            }
        }
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn excluded_shapes_return_the_original_wire_value_without_admission() {
        let nested = info(T::String("nested".into()),T::Map(vec![]));
        let values = vec![
            T::Null,T::Bool(true),T::String("plain".into()),T::Array(vec![]),
            tag("other",T::Array(vec![T::Null,T::Null])),
            tag("xtdb/exception-info",T::Null),
            tag("xtdb/exception-info",T::Array(vec![])),
            tag("xtdb/exception-info",T::Array(vec![T::Null])),
            tag("xtdb/exception-info",T::Array(vec![T::Null,T::Null,T::Integer(1)])),
            tag("xtdb/exception-info",tag("list",T::Array(vec![T::Null,T::Null]))),
            tag("with-meta",T::Array(vec![info(T::Null,T::Null),T::Map(vec![])])),
            info(T::Integer(1),T::Map(vec![])),info(T::Null,T::Bool(false)),
            info(T::Null,tag("m",T::String("0".into()))),
            info(T::Null,T::Map(vec![(T::String("date".into()),tag("m",T::String("0".into())))])),
            info(T::Null,T::Map(vec![(T::String("error".into()),nested)])),
        ];
        let memory = MemoryBudget::new(4096);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            for value in values {
                let expected = value.clone();
                for admission in [Admission::Standalone,Admission::Tracked(&scope)] {
                    match format_argument(value.clone(),admission).unwrap() {
                        FormatArgument::Wire(value) => assert_eq!(value,expected),
                        FormatArgument::Semantic(_) => panic!("unchanged wire value"),
                    }
                    assert_eq!(memory.used_bytes(),4096);
                    assert_eq!(memory.peak_bytes(),4096);
                }
            }
            let source = String::from("unchanged backing");let pointer = source.as_ptr();
            match format_argument(T::String(source),Admission::Tracked(&scope)).unwrap() {
                FormatArgument::Wire(T::String(value)) => assert_eq!(value.as_ptr(),pointer),
                _ => panic!("original string"),
            }
        }
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn selected_message_backing_moves_to_stack_arguments_before_the_shared_copy() {
        let mut message = String::with_capacity(128);message.push_str("message");
        let pointer = message.as_ptr();let capacity = message.capacity();
        let value = info(T::String(message),T::Null);
        assert_eq!(message_bytes(&value),Some(7));
        let inputs = arguments(value);
        let Object::String(source) = &inputs[0] else { panic!("source message") };
        assert_eq!(source.as_ptr(),pointer);assert_eq!(source.capacity(),capacity);
        let Throwable::ExceptionInfo { message: Object::String(copied),data: Object::Map(data) } = Throwable::exception_info(&inputs).unwrap() else { panic!("ExceptionInfo") };
        assert_eq!(copied,"message");assert_ne!(copied.as_ptr(),pointer);assert!(data.is_empty());
    }

    #[test]
    fn constructor_and_text_denials_retain_only_successful_charges_until_scope_drop() {
        let retained = Layout::new::<Throwable>().size() + 32 + 7;
        let value = ||info(T::String("message".into()),T::Map(vec![]));
        let memory = MemoryBudget::new(4096 + retained + 256 - 1);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let error = failure(value(),Admission::Tracked(&scope));
            assert_eq!(error.code,"rtdb/memory-budget-exceeded");
            assert_eq!(error.message.as_deref(),Some(format!("Cannot reserve {} bytes from budget {} with 4096 retained",retained + 256,4096 + retained + 256 - 1).as_str()));
            assert_eq!(memory.used_bytes(),4096);assert_eq!(memory.peak_bytes(),4096);
        }
        assert_eq!(memory.used_bytes(),0);
        let memory = MemoryBudget::new(4096 + retained + 256);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let error = failure(value(),Admission::Tracked(&scope));
            assert_eq!(error.code,"rtdb/memory-budget-exceeded");
            assert_eq!(error.message.as_deref(),Some(format!("Cannot reserve 320 bytes from budget {} with {} retained",4096 + retained + 256,4096 + retained + 256).as_str()));
            assert_eq!(memory.used_bytes(),4096 + retained + 256);
        }
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn local_time_scalar_reserves_temporary_text_before_output_growth() {
        let value = ||tag("time/time",T::String("08:09:00".into()));
        let scope_bytes = 4096;
        let temporary_bytes = 4096 + 256;
        let output_bytes = 64 + 256;
        for (limit,retained,requested) in [
            (scope_bytes + temporary_bytes - 1,scope_bytes,temporary_bytes),
            (scope_bytes + temporary_bytes + output_bytes - 1,scope_bytes + temporary_bytes,output_bytes),
        ] {
            let memory = MemoryBudget::new(limit);
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                let error = failure(value(),Admission::Tracked(&scope));
                assert_eq!(error.code,"rtdb/memory-budget-exceeded");
                let message = format!("Cannot reserve {requested} bytes from budget {limit} with {retained} retained");
                assert_eq!(error.message.as_deref(),Some(message.as_str()));
                assert_eq!(memory.used_bytes(),retained);
            }
            assert_eq!(memory.used_bytes(),0);
        }
        let limit = scope_bytes + temporary_bytes + output_bytes;
        let memory = MemoryBudget::new(limit);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            assert_eq!(semantic(value(),Admission::Tracked(&scope)),"08:09");
            assert_eq!(memory.used_bytes(),limit);
        }
        assert_eq!(memory.used_bytes(),0);
    }
}
