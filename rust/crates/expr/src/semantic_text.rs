use rtdb_types::Result;

pub(crate) trait Sink {
    fn push(&mut self,text: &str) -> Result<()>;
}

#[derive(Clone,Copy)]
pub(crate) enum Mode { Str,Readable }

#[derive(Clone,Copy)]
pub(crate) enum Atom<'a> {
    Nil,Boolean(bool),String(&'a str),Symbol(&'a str),Keyword(&'a str),Character(char),
}

#[derive(Clone,Copy)]
pub(crate) enum SequenceKind { List,Vector,Set }

pub(crate) fn atom<S: Sink + ?Sized>(sink: &mut S,value: Atom<'_>,mode: Mode) -> Result<()> {
    match value {
        Atom::Nil => match mode { Mode::Str => Ok(()),Mode::Readable => sink.push("nil") },
        Atom::Boolean(value) => sink.push(if value { "true" } else { "false" }),
        Atom::String(value) => match mode { Mode::Str => sink.push(value),Mode::Readable => quoted(sink,value) },
        Atom::Symbol(value) => sink.push(value),
        Atom::Keyword(value) => { sink.push(":")?;sink.push(value) }
        Atom::Character(value) => match mode {
            Mode::Str => character(sink,value),
            Mode::Readable => {
                let name = match value { '\n' => "newline",'\t' => "tab",' ' => "space",'\u{8}' => "backspace",'\u{c}' => "formfeed",'\r' => "return",_ => "" };
                sink.push("\\")?;if name.is_empty() { character(sink,value) } else { sink.push(name) }
            }
        },
    }
}

pub(crate) fn quoted<S: Sink + ?Sized>(sink: &mut S,value: &str) -> Result<()> {
    sink.push("\"")?;
    for ch in value.chars() {
        match ch {
            '\n' => sink.push("\\n"),'\t' => sink.push("\\t"),'\r' => sink.push("\\r"),'"' => sink.push("\\\""),
            '\\' => sink.push("\\\\"),'\u{c}' => sink.push("\\f"),'\u{8}' => sink.push("\\b"),_ => character(sink,ch),
        }?;
    }
    sink.push("\"")
}

pub(crate) fn sequence<S: Sink + ?Sized,T>(
    sink: &mut S,kind: SequenceKind,values: impl IntoIterator<Item=T>,mut write: impl FnMut(&mut S,T) -> Result<()>,
) -> Result<()> {
    let (start,end) = match kind { SequenceKind::List => ("(",")"),SequenceKind::Vector => ("[","]"),SequenceKind::Set => ("#{","}") };
    sink.push(start)?;
    let mut first = true;
    for value in values { if !first { sink.push(" ")?; }first = false;write(sink,value)?; }
    sink.push(end)
}

pub(crate) fn map<S: Sink + ?Sized,T>(
    sink: &mut S,entries: impl IntoIterator<Item=(T,T)>,mut write: impl FnMut(&mut S,T) -> Result<()>,
) -> Result<()> {
    sink.push("{")?;
    for (index,(key,value)) in entries.into_iter().enumerate() {
        if index != 0 { sink.push(", ")?; }write(sink,key)?;sink.push(" ")?;write(sink,value)?;
    }
    sink.push("}")
}

fn character<S: Sink + ?Sized>(sink: &mut S,value: char) -> Result<()> { sink.push(value.encode_utf8(&mut [0;4])) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use rtdb_types::Anomaly;

    #[derive(Default)]
    struct Recording { attempts: Vec<String>,failure: Option<usize> }
    impl Sink for Recording {
        fn push(&mut self,text: &str) -> Result<()> {
            let index = self.attempts.len();self.attempts.push(text.into());
            if self.failure == Some(index) { Err(sink_error()) } else { Ok(()) }
        }
    }
    fn sink_error() -> Anomaly { Anomaly::incorrect("test/text-sink","Text sink rejected the append") }

    #[test]
    fn scalar_and_readable_atoms_keep_clojure_spelling_and_append_boundaries() {
        for (value,plain,readable) in [
            (Atom::Nil,"","nil"),(Atom::Boolean(false),"false","false"),(Atom::Boolean(true),"true","true"),
            (Atom::String("\n\t\r\"\\\u{c}\u{8}é😀"),"\n\t\r\"\\\u{c}\u{8}é😀",r#""\n\t\r\"\\\f\bé😀""#),
            (Atom::Symbol("ns/x"),"ns/x","ns/x"),(Atom::Keyword("ns/k"),":ns/k",":ns/k"),
            (Atom::Character('\n'),"\n",r"\newline"),(Atom::Character('\t'),"\t",r"\tab"),
            (Atom::Character(' ')," ",r"\space"),(Atom::Character('\u{8}'),"\u{8}",r"\backspace"),
            (Atom::Character('\u{c}'),"\u{c}",r"\formfeed"),(Atom::Character('\r'),"\r",r"\return"),
            (Atom::Character('é'),"é",r"\é"),
        ] {
            for (mode,expected) in [(Mode::Str,plain),(Mode::Readable,readable)] {
                let mut sink = Recording::default();atom(&mut sink,value,mode).unwrap();assert_eq!(sink.attempts.concat(),expected);
            }
        }
        let mut sink = Recording::default();atom(&mut sink,Atom::Nil,Mode::Str).unwrap();assert!(sink.attempts.is_empty());
        atom(&mut sink,Atom::Keyword("ns/k"),Mode::Readable).unwrap();assert_eq!(sink.attempts,[":","ns/k"]);
        let mut sink = Recording::default();quoted(&mut sink,"a\né").unwrap();assert_eq!(sink.attempts,["\"","a","\\n","é","\""]);
    }

    #[test]
    fn sequence_punctuation_keeps_borrowed_child_order() {
        for (kind,expected) in [(SequenceKind::List,"(b a)"),(SequenceKind::Vector,"[b a]"),(SequenceKind::Set,"#{b a}")] {
            let mut sink = Recording::default();sequence(&mut sink,kind,["b","a"],|sink,value|sink.push(value)).unwrap();
            assert_eq!(sink.attempts.concat(),expected);
        }
        let mut sink = Recording::default();map(&mut sink,[("b","2"),("a","1")],|sink,value|sink.push(value)).unwrap();
        assert_eq!(sink.attempts.concat(),"{b 2, a 1}");
    }

    #[test]
    fn sequence_sink_failures_stop_before_another_child_callback() {
        for (failure,pulled,written) in [(0,0,0),(2,2,1),(4,2,2)] {
            let fetched = Cell::new(0);let mut calls = 0;
            let values = ["a","b"].into_iter().inspect(|_|fetched.set(fetched.get() + 1));
            let mut sink = Recording { failure: Some(failure),..Default::default() };
            let error = sequence(&mut sink,SequenceKind::Vector,values,|sink,value| { calls += 1;sink.push(value) }).unwrap_err();
            assert_eq!(error,sink_error());assert_eq!(fetched.get(),pulled);assert_eq!(calls,written);assert_eq!(sink.attempts.len(),failure + 1);
        }
    }

    #[test]
    fn map_sink_failures_stop_between_keys_values_and_entries() {
        for (failure,pulled,written) in [(0,0,0),(2,1,1),(4,2,2),(8,2,4)] {
            let fetched = Cell::new(0);let mut calls = 0;
            let entries = [("k","v"),("later","last")].into_iter().inspect(|_|fetched.set(fetched.get() + 1));
            let mut sink = Recording { failure: Some(failure),..Default::default() };
            let error = map(&mut sink,entries,|sink,value| { calls += 1;sink.push(value) }).unwrap_err();
            assert_eq!(error,sink_error());assert_eq!(fetched.get(),pulled);assert_eq!(calls,written);assert_eq!(sink.attempts.len(),failure + 1);
        }
    }

    #[test]
    fn child_errors_propagate_without_rendering_remaining_values_or_closing_delimiters() {
        let expected = Anomaly::fault("test/child","Child rendering failed");
        let fetched = Cell::new(0);let mut calls = Vec::new();let mut sink = Recording::default();
        let values = ["first","bad","later"].into_iter().inspect(|_|fetched.set(fetched.get() + 1));
        let error = sequence(&mut sink,SequenceKind::List,values,|sink,value| {
            calls.push(value);if value == "bad" { Err(expected.clone()) } else { sink.push(value) }
        }).unwrap_err();
        assert_eq!(error,expected);assert_eq!(fetched.get(),2);assert_eq!(calls,["first","bad"]);assert_eq!(sink.attempts,["(","first"," "]);
        for (failure,visited,attempts) in [("key",vec!["key"],vec!["{"]),("value",vec!["key","value"],vec!["{","key"," "])] {
            let fetched = Cell::new(0);let mut calls = Vec::new();let mut sink = Recording::default();
            let entries = [("key","value"),("later","last")].into_iter().inspect(|_|fetched.set(fetched.get() + 1));
            let error = map(&mut sink,entries,|sink,value| { calls.push(value);if value == failure { Err(expected.clone()) } else { sink.push(value) } }).unwrap_err();
            assert_eq!(error,expected);assert_eq!(fetched.get(),1);assert_eq!(calls,visited);assert_eq!(sink.attempts,attempts);
        }
    }
}
