use alloc::{collections::BTreeSet, string::String, vec, vec::Vec};
use core::convert::TryFrom;
use crate::{analyze::Info, Assertion, CompileError, Error, Expr, LookAround, RegexOptions, Result, RuntimeError};

#[path = "java_class.rs"]
mod class;
use class::CharacterClass;

const MAX_CALL_DEPTH: usize = 512;

fn on_stack<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "std")]
    { stacker::maybe_grow(64 * 1024, 1024 * 1024, work) }
    #[cfg(not(feature = "std"))]
    { work() }
}

pub(crate) fn requires_java_nodes(expr: &Expr) -> bool {
    match expr {
        Expr::GraphemeBoundary | Expr::LineEnding => true,
        Expr::Concat(children) | Expr::Alt(children) => children.iter().any(requires_java_nodes),
        Expr::Group(child) | Expr::JavaGroup(child) | Expr::JavaAtom(child) | Expr::JavaPossessive(child)
        | Expr::AtomicGroup(child) | Expr::LookAround(child, _) | Expr::JavaLookBehind(child, _) | Expr::CanonicalClass(child)
        | Expr::Repeat { child, .. } => requires_java_nodes(child),
        _ => false,
    }
}

fn deterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Alt(_) | Expr::Grapheme | Expr::CanonicalClass(_) => false,
        Expr::Concat(children) => children.iter().all(deterministic),
        Expr::Group(child) | Expr::JavaGroup(child) | Expr::AtomicGroup(child) | Expr::JavaPossessive(child) => deterministic(child),
        Expr::Repeat { child, lo, hi, .. } => lo == hi && deterministic(child),
        _ => true,
    }
}

fn grouped(expr: &Expr) -> bool { matches!(expr, Expr::Group(_) | Expr::JavaGroup(_)) }
fn character(expr: &Expr) -> bool { matches!(expr, Expr::Literal { .. } | Expr::Any { .. } | Expr::Delegate { size: 1, .. }) }

#[derive(Clone, Copy, Debug)]
enum Order { Greedy, Lazy, Possessive }

#[derive(Clone, Debug)]
enum Node {
    Accept,
    End,
    BehindEnd,
    Fail,
    Atom { atom: Atom, next: usize },
    Boundary { next: usize },
    Branch { alternatives: Vec<usize> },
    GroupHead { local: usize, next: usize },
    GroupTail { local: usize, group: usize, next: usize },
    Look { body: usize, kind: LookAround, min: usize, max: i32, supplementary: bool, next: usize },
    Independent { body: usize, next: usize, opaque: bool },
    Ques { body: usize, order: Order, next: usize },
    Curly { body: usize, min: usize, max: usize, order: Order, next: usize },
    GroupCurly { body: usize, local: usize, group: Option<usize>, min: usize, max: usize, order: Order, next: usize },
    CharGreedy { atom: Atom, min: usize, next: usize },
    Prolog { body: usize, count: usize, min: usize, max: usize, lazy: bool, next: usize },
    Loop { body: usize, local: usize, count: usize, min: usize, max: usize, lazy: bool, memo: Option<usize>, next: usize },
}

#[derive(Clone, Debug)]
enum Atom {
    Literal { units: Vec<u16>, casei: bool },
    Class(CharacterClass),
    Any(bool),
    Assertion(Assertion),
    PreviousEnd,
    Backref { group: usize, casei: bool, ascii: bool },
    Grapheme,
    LineEnding,
    Canonical(CharacterClass),
}

#[derive(Clone, Debug)]
pub(crate) struct Program {
    nodes: Vec<Node>,
    root: usize,
    groups: usize,
    locals: usize,
    memos: usize,
    anchored: bool,
    minimum: usize,
    supplementary: bool,
}

struct Builder<'a> {
    program: Program,
    options: &'a RegexOptions,
    has_backrefs: bool,
}

impl Program {
    pub(crate) fn compile(info: &Info<'_>, anchored: bool, options: &RegexOptions) -> Result<Self> {
        let mut study = Study::new(); study.visit(info, false);
        let minimum = usize::try_from(study.min).map_err(|_| Error::CompileError(CompileError::PatternSizeOverflow))?;
        let mut builder = Builder {
            program: Self { nodes: vec![Node::Accept, Node::End, Node::BehindEnd], root: 0, groups: info.end_group, locals: 0, memos: 0, anchored, minimum, supplementary: options.java_supplementary },
            options,
            has_backrefs: references(info.expr),
        };
        builder.program.root = builder.build(info, 1, true)?;
        Ok(builder.program)
    }

    pub(crate) fn run_utf16(&self, text: &[u16], position: usize, previous_end: usize, options: &RegexOptions) -> Result<Option<Vec<usize>>> {
        self.run_positions(text, position, previous_end, previous_end, options)
    }

    fn run_positions(&self, text: &[u16], position: usize, last: usize, old_last: usize, options: &RegexOptions) -> Result<Option<Vec<usize>>> {
        let mut matcher = Matcher {
            program: self, text, captures: vec![usize::MAX; self.groups * 2], locals: vec![usize::MAX; self.locals],
            failed: vec![BTreeSet::new(); self.memos], last, first: position, old_last, behind: 0,
            depth: 0, steps: 0, options,
        };
        let Some(guard) = text.len().checked_sub(self.minimum) else { return Ok(None); };
        let mut start = position;
        while start <= guard {
            matcher.first = start;
            if matcher.matches(self.root, start)? { return Ok(Some(matcher.captures)); }
            if self.anchored { break; }
            start = if self.supplementary { point(text, start).map_or(start + 1, |(_, end)| end) } else { start + 1 };
        }
        Ok(None)
    }

    pub(crate) fn run(&self, text: &str, position: usize, previous_end: Option<usize>, options: &RegexOptions) -> Result<Option<Vec<usize>>> {
        self.run_bytes(text, position, previous_end, options, false)
    }

    pub(crate) fn find(&self, text: &str, position: usize, previous_end: Option<usize>, options: &RegexOptions) -> Result<Option<Vec<usize>>> {
        self.run_bytes(text, position, previous_end, options, true)
    }

    fn run_bytes(&self, text: &str, position: usize, previous_end: Option<usize>, options: &RegexOptions, whole_only: bool) -> Result<Option<Vec<usize>>> {
        let units: Vec<u16> = text.encode_utf16().collect();
        let position = text[..position].encode_utf16().count();
        let previous_end = previous_end.map(|end| text[..end].encode_utf16().count());
        let Some(mut captures) = self.run_positions(&units, position, previous_end.unwrap_or(0), previous_end.unwrap_or(position), options)? else { return Ok(None); };
        if whole_only { captures.truncate(2); }
        let mut offsets = vec![usize::MAX; units.len() + 1];
        let mut unit = 0;
        for (byte, scalar) in text.char_indices() { offsets[unit] = byte; unit += scalar.len_utf16(); }
        offsets[unit] = text.len();
        for capture in &mut captures {
            if *capture != usize::MAX {
                *capture = offsets[*capture];
                if *capture == usize::MAX { return Err(Error::RuntimeError(RuntimeError::NonUtf8Capture)); }
            }
        }
        Ok(Some(captures))
    }
}

fn references(expr: &Expr) -> bool {
    match expr {
        Expr::Backref { .. } | Expr::BackrefAscii { .. } => true,
        Expr::Concat(children) | Expr::Alt(children) => children.iter().any(references),
        Expr::Group(child) | Expr::JavaGroup(child) | Expr::JavaAtom(child) | Expr::JavaPossessive(child)
        | Expr::AtomicGroup(child) | Expr::LookAround(child, _) | Expr::JavaLookBehind(child, _) | Expr::CanonicalClass(child)
        | Expr::Repeat { child, .. } => references(child),
        _ => false,
    }
}

impl Builder<'_> {
    fn add(&mut self, node: Node) -> usize { let index = self.program.nodes.len(); self.program.nodes.push(node); index }
    fn local(&mut self) -> usize { let index = self.program.locals; self.program.locals += 1; index }

    fn atom(&self, expr: &Expr) -> Result<Atom> {
        Ok(match expr {
            Expr::Literal { val, casei } => Atom::Literal { units: val.encode_utf16().collect(), casei: *casei },
            Expr::Any { newline } => Atom::Any(*newline),
            Expr::Delegate { inner, casei, .. } => Atom::Class(CharacterClass::compile(inner, *casei, self.options)?),
            Expr::Assertion(assertion) => {
                if matches!(assertion, Assertion::LeftWordBoundary | Assertion::RightWordBoundary | Assertion::WordBoundary | Assertion::NotWordBoundary)
                    && regex_syntax::try_is_word_character('a').is_err() {
                    return Err(Error::CompileError(CompileError::FeatureNotYetSupported("Unicode word assertions require Unicode tables".into())));
                }
                Atom::Assertion(*assertion)
            },
            Expr::ContinueFromPreviousMatchEnd => Atom::PreviousEnd,
            Expr::Backref { group, casei } => Atom::Backref { group: *group, casei: *casei, ascii: false },
            Expr::BackrefAscii { group } => Atom::Backref { group: *group, casei: true, ascii: true },
            Expr::Grapheme => Atom::Grapheme,
            Expr::LineEnding => Atom::LineEnding,
            Expr::CanonicalClass(child) => {
                let mut pattern = String::new(); child.to_str(&mut pattern, 0);
                Atom::Canonical(CharacterClass::compile(&pattern, false, self.options)?)
            }
            _ => return Err(Error::CompileError(CompileError::FeatureNotYetSupported("Java node atom".into()))),
        })
    }

    fn build(&mut self, info: &Info<'_>, next: usize, top: bool) -> Result<usize> {
        Ok(match info.expr {
            Expr::Empty => next,
            Expr::Concat(_) => {
                let mut head = next;
                for child in info.children.iter().rev() { head = self.build(child, head, top)?; }
                head
            }
            Expr::Alt(_) => {
                let alternatives = info.children.iter().map(|child| self.build(child, next, top)).collect::<Result<Vec<_>>>()?;
                self.add(Node::Branch { alternatives })
            }
            Expr::Group(_) | Expr::JavaGroup(_) => {
                let local = self.local();
                let group = if matches!(info.expr, Expr::Group(_)) { info.start_group } else { 0 };
                let tail = self.add(Node::GroupTail { local, group, next });
                let body = self.build(&info.children[0], tail, top)?;
                self.add(Node::GroupHead { local, next: body })
            }
            Expr::JavaAtom(_) | Expr::AtomicGroup(_) => {
                let body = self.build(&info.children[0], 0, top)?;
                self.add(Node::Independent { body, next, opaque: matches!(info.expr, Expr::JavaAtom(_)) })
            }
            Expr::JavaPossessive(_) => self.repeat(&info.children[0], next, Order::Possessive, top)?,
            Expr::Repeat { greedy, .. } => self.repeat(info, next, if *greedy { Order::Greedy } else { Order::Lazy }, top)?,
            Expr::LookAround(_, kind) | Expr::JavaLookBehind(_, kind) => {
                let behind = matches!(kind, LookAround::LookBehind | LookAround::LookBehindNeg);
                let (min, max) = if behind { bounds(&info.children[0])? } else { (0, 0) };
                let body = self.build(&info.children[0], if behind { 2 } else { 0 }, top && !behind)?;
                self.add(Node::Look { body, kind: *kind, min, max, supplementary: matches!(info.expr, Expr::JavaLookBehind(_, _)), next })
            }
            Expr::GraphemeBoundary => self.add(Node::Boundary { next }),
            _ => { let atom = self.atom(info.expr)?; self.add(Node::Atom { atom, next }) }
        })
    }

    fn repeat(&mut self, info: &Info<'_>, next: usize, order: Order, top: bool) -> Result<usize> {
        let Expr::Repeat { lo, hi, .. } = info.expr else { unreachable!() };
        let child = &info.children[0];
        let min = *lo; let max = *hi;
        if min == 0 && max == 1 {
            if grouped(child.expr) && !matches!(order, Order::Possessive) {
                let body = self.build(child, next, false)?;
                let alternatives = if matches!(order, Order::Lazy) { vec![next, body] } else { vec![body, next] };
                return Ok(self.add(Node::Branch { alternatives }));
            }
            let body = self.build(child, 0, top && !grouped(child.expr))?;
            return Ok(self.add(Node::Ques { body, order, next }));
        }
        if matches!(order, Order::Greedy) && max == usize::MAX && character(child.expr) {
            let atom = self.atom(child.expr)?;
            return Ok(self.add(Node::CharGreedy { atom, min, next }));
        }
        if grouped(child.expr) && !matches!(order, Order::Possessive) {
            let local = self.local();
            let group = matches!(child.expr, Expr::Group(_)).then_some(child.start_group);
            if deterministic(child.expr) {
                let tail = self.add(Node::GroupTail { local, group: group.unwrap_or(0), next: 0 });
                let body = self.build(&child.children[0], tail, false)?;
                return Ok(self.add(Node::GroupCurly { body, local, group, min, max, order, next }));
            }
            let count = self.local();
            let lazy = matches!(order, Order::Lazy);
            let memo = if top && !self.has_backrefs && !lazy && max == usize::MAX {
                let index = self.program.memos; self.program.memos += 1; Some(index)
            } else { None };
            let loop_node = self.add(Node::Fail);
            let tail = self.add(Node::GroupTail { local, group: group.unwrap_or(0), next: loop_node });
            let inner = self.build(&child.children[0], tail, false)?;
            let body = self.add(Node::GroupHead { local, next: inner });
            self.program.nodes[loop_node] = Node::Loop { body, local, count, min, max, lazy, memo, next };
            return Ok(self.add(Node::Prolog { body, count, min, max, lazy, next }));
        }
        let body = self.build(child, 0, top && !grouped(child.expr))?;
        Ok(self.add(Node::Curly { body, min, max, order, next }))
    }
}

fn bounds(info: &Info<'_>) -> Result<(usize, i32)> {
    let mut study = Study::new(); study.visit(info, false);
    if !study.valid { return Err(Error::CompileError(CompileError::LookBehindNotConst)); }
    Ok((usize::try_from(study.min).map_err(|_| Error::CompileError(CompileError::PatternSizeOverflow))?, study.max))
}

struct Study { min: i32, max: i32, valid: bool, stopped: bool }
impl Study {
    fn new() -> Self { Self { min: 0, max: 0, valid: true, stopped: false } }
    fn visit(&mut self, info: &Info<'_>, possessive: bool) {
        if self.stopped { return; }
        match info.expr {
            Expr::Concat(_) => for child in &info.children { self.visit(child, false); },
            Expr::Group(_) | Expr::JavaGroup(_) => self.visit(&info.children[0], false),
            Expr::AtomicGroup(_) | Expr::JavaAtom(_) => { self.visit(&info.children[0], false); self.stopped = false; },
            Expr::JavaPossessive(_) => self.visit(&info.children[0], true),
            Expr::Alt(_) => {
                let mut minimum = i32::MAX; let mut maximum = -1;
                for child in &info.children {
                    let mut branch = Self::new(); branch.visit(child, false);
                    minimum = minimum.min(branch.min); maximum = maximum.max(branch.max); self.valid &= branch.valid;
                }
                self.min = self.min.wrapping_add(minimum); self.max = self.max.wrapping_add(maximum);
            }
            Expr::Repeat { lo, hi, greedy, .. } => {
                let child = &info.children[0];
                if *lo == 0 && *hi == 1 {
                    let saved_minimum = self.min;
                    if grouped(child.expr) && !possessive {
                        let mut atom = Self::new(); atom.visit(child, false);
                        self.max = self.max.wrapping_add(atom.max.max(0)); self.valid &= atom.valid;
                    } else { self.visit(child, false); self.stopped = false; }
                    self.min = saved_minimum; return;
                }
                if !possessive && !(*lo == 0 && *hi == 1) && grouped(child.expr) && !deterministic(child.expr) {
                    self.valid = false; self.stopped = true; return;
                }
                if !possessive && *greedy && *hi == usize::MAX && character(child.expr) {
                    self.min = self.min.wrapping_add(*lo as i32);
                    if self.valid { self.max = self.max.wrapping_add(i32::MAX); }
                    return;
                }
                let mut atom = Self::new(); atom.visit(child, false);
                let minimum = if *lo == 0 && *hi == 1 { self.min } else { atom.min.wrapping_mul(*lo as i32).wrapping_add(self.min) };
                self.min = if minimum < self.min { 0x0fff_ffff } else { minimum };
                if self.valid && atom.valid {
                    let maximum = atom.max.wrapping_mul((*hi).min(i32::MAX as usize) as i32).wrapping_add(self.max);
                    if maximum < self.max { self.valid = false; }
                    self.max = maximum;
                } else { self.valid = false; }
            }
            Expr::Grapheme | Expr::CanonicalClass(_) => self.min = self.min.wrapping_add(1),
            Expr::LineEnding => { self.min = self.min.wrapping_add(1); self.max = self.max.wrapping_add(2); },
            Expr::Backref { .. } | Expr::BackrefAscii { .. } => self.valid = false,
            Expr::LookAround(_, _) | Expr::JavaLookBehind(_, _) | Expr::GraphemeBoundary | Expr::Empty | Expr::Assertion(_) | Expr::ContinueFromPreviousMatchEnd => {},
            _ => { self.min = self.min.wrapping_add(info.min_size as i32); self.max = self.max.wrapping_add(info.min_size as i32); }
        }
    }
}

struct Matcher<'a> {
    program: &'a Program,
    text: &'a [u16],
    captures: Vec<usize>,
    locals: Vec<usize>,
    failed: Vec<BTreeSet<usize>>,
    last: usize,
    first: usize,
    old_last: usize,
    behind: usize,
    depth: usize,
    steps: usize,
    options: &'a RegexOptions,
}

impl Matcher<'_> {
    fn matches(&mut self, node: usize, position: usize) -> Result<bool> {
        if self.depth >= self.options.backtrack_stack_limit.min(MAX_CALL_DEPTH) { return Err(Error::RuntimeError(RuntimeError::StackOverflow)); }
        self.depth += 1;
        let result = on_stack(|| self.execute(node, position));
        self.depth -= 1;
        result
    }

    fn execute(&mut self, mut node: usize, mut position: usize) -> Result<bool> {
        let program = self.program;
        loop {
            self.steps += 1;
            if self.steps > self.options.backtrack_limit { return Err(Error::RuntimeError(RuntimeError::BacktrackLimitExceeded)); }
            match &program.nodes[node] {
                Node::Fail => return Ok(false),
                Node::Accept | Node::End => {
                    self.last = position; self.captures[0] = self.first; self.captures[1] = position;
                    return Ok(true);
                }
                Node::BehindEnd => return Ok(position == self.behind),
                Node::Atom { atom, next } => {
                    if matches!(atom, Atom::LineEnding) && self.text.get(position..position.saturating_add(2)) == Some(&[13, 10][..]) {
                        if self.matches(*next, position + 2)? { return Ok(true); }
                        position += 1; node = *next; continue;
                    }
                    if let Atom::Canonical(class) = atom {
                        for end in IntoIterator::into_iter(crate::canonical::candidates_utf16(self.text, position, |value| class.contains(value))).flatten() {
                            if self.matches(*next, end)? { return Ok(true); }
                        }
                        return Ok(false);
                    }
                    let Some(end) = self.atom(atom, position)? else { return Ok(false); };
                    position = end; node = *next;
                }
                Node::Boundary { next } => {
                    if position != 0 && position < self.text.len() {
                        if high(self.text[position - 1]) && low(self.text[position]) { return Ok(false); }
                        let end = crate::grapheme::next_boundary_utf16(self.text, self.last)
                            .ok_or(Error::RuntimeError(RuntimeError::GraphemeBoundaryStart))?;
                        if end > position { return Ok(false); }
                    }
                    node = *next;
                }
                Node::Branch { alternatives } => {
                    for alternative in alternatives { if self.matches(*alternative, position)? { return Ok(true); } }
                    return Ok(false);
                }
                Node::GroupHead { local, next } => {
                    let saved = self.locals[*local]; self.locals[*local] = position;
                    let result = self.matches(*next, position);
                    self.locals[*local] = saved;
                    return result;
                }
                Node::GroupTail { local, group, next } => {
                    let start = self.locals[*local];
                    if start == usize::MAX { self.last = position; return Ok(true); }
                    let slot = group * 2;
                    let saved = (self.captures[slot], self.captures[slot + 1]);
                    self.captures[slot] = start; self.captures[slot + 1] = position;
                    if self.matches(*next, position)? { return Ok(true); }
                    self.captures[slot] = saved.0; self.captures[slot + 1] = saved.1;
                    return Ok(false);
                }
                Node::Independent { body, next, opaque } => {
                    let saved = self.last;
                    let matched = self.matches(*body, position)?;
                    let end = self.last;
                    if *opaque { self.last = saved; }
                    if !matched { return Ok(false); }
                    position = end; node = *next;
                }
                Node::Look { body, kind, min, max, supplementary, next } => {
                    let matched = if matches!(kind, LookAround::LookBehind | LookAround::LookBehindNeg) {
                        let saved = self.behind; self.behind = position;
                        let mut matched = false;
                        let first = if *supplementary { Some(backwards(self.text, position, *min).unwrap_or(0)) } else { position.checked_sub(*min) };
                        if let Some(mut start) = first {
                            let lower = if *supplementary { position.saturating_sub(behind_distance(self.text, position, *max)) }
                                else { (position as i32).wrapping_sub(*max).max(0) as usize };
                            while start >= lower {
                                if self.matches(*body, start)? { matched = true; break; }
                                if start == 0 { break; } start = if *supplementary { previous(self.text, start) } else { start - 1 };
                            }
                        }
                        self.behind = saved; matched
                    } else { self.matches(*body, position)? };
                    if matched == matches!(kind, LookAround::LookAheadNeg | LookAround::LookBehindNeg) { return Ok(false); }
                    node = *next;
                }
                Node::Ques { body, order, next } => {
                    return match order {
                        Order::Greedy => Ok((self.matches(*body, position)? && self.matches(*next, self.last)?) || self.matches(*next, position)?),
                        Order::Lazy => Ok(self.matches(*next, position)? || (self.matches(*body, position)? && self.matches(*next, self.last)?)),
                        Order::Possessive => { if self.matches(*body, position)? { position = self.last; } self.matches(*next, position) }
                    };
                }
                Node::Curly { body, min, max, order, next } => {
                    for _ in 0..*min { if !self.matches(*body, position)? { return Ok(false); } position = self.last; }
                    return self.curly(*body, position, *min, *max, *order, *next);
                }
                Node::GroupCurly { body, local, group, min, max, order, next } => {
                    let saved_local = self.locals[*local]; self.locals[*local] = usize::MAX;
                    let saved_capture = group.map(|group| (self.captures[group * 2], self.captures[group * 2 + 1]));
                    let mut matched = true;
                    for _ in 0..*min {
                        if !self.matches(*body, position)? { matched = false; break; }
                        self.capture(*group, position, self.last); position = self.last;
                    }
                    if matched { matched = self.group_curly(*body, position, *min, *max, *order, *group, *next)?; }
                    if !matched {
                        self.locals[*local] = saved_local;
                        if let (Some(group), Some((start, end))) = (group, saved_capture) { self.capture(Some(*group), start, end); }
                    }
                    return Ok(matched);
                }
                Node::CharGreedy { atom, min, next } => {
                    let start = position; let mut count = 0;
                    while let Some(end) = self.atom(atom, position)? { position = end; count += 1; }
                    while count >= *min {
                        if self.matches(*next, position)? { return Ok(true); }
                        if count == *min { break; }
                        position = previous(self.text, position).max(start); count -= 1;
                    }
                    return Ok(false);
                }
                Node::Prolog { body, count, min, max, lazy, next } => {
                    let saved = self.locals[*count];
                    let matched = if *min > 0 { self.locals[*count] = 1; self.matches(*body, position)? }
                    else if *lazy {
                        if self.matches(*next, position)? { true }
                        else if *max > 0 { self.locals[*count] = 1; self.matches(*body, position)? }
                        else { false }
                    }
                    else if *max > 0 { self.locals[*count] = 1; self.matches(*body, position)? || self.matches(*next, position)? }
                    else { self.matches(*next, position)? };
                    self.locals[*count] = saved; return Ok(matched);
                }
                Node::Loop { body, local, count, min, max, lazy, memo, next } => {
                    if position <= self.locals[*local] { node = *next; continue; }
                    let repetitions = self.locals[*count];
                    if repetitions < *min {
                        self.locals[*count] = repetitions + 1;
                        let matched = self.matches(*body, position)?;
                        if !matched { self.locals[*count] = repetitions; }
                        return Ok(matched);
                    }
                    if *lazy && self.matches(*next, position)? { return Ok(true); }
                    if repetitions < *max {
                        if memo.is_some_and(|index| self.failed[index].contains(&position)) { node = *next; continue; }
                        self.locals[*count] = repetitions + 1;
                        if self.matches(*body, position)? { return Ok(true); }
                        self.locals[*count] = repetitions;
                        if let Some(index) = memo { self.failed[*index].insert(position); }
                    }
                    if *lazy { return Ok(false); }
                    node = *next;
                }
            }
        }
    }

    fn capture(&mut self, group: Option<usize>, start: usize, end: usize) {
        if let Some(group) = group { self.captures[group * 2] = start; self.captures[group * 2 + 1] = end; }
    }

    fn curly(&mut self, body: usize, position: usize, count: usize, max: usize, order: Order, next: usize) -> Result<bool> {
        if self.depth >= self.options.backtrack_stack_limit.min(MAX_CALL_DEPTH) { return Err(Error::RuntimeError(RuntimeError::StackOverflow)); }
        self.depth += 1;
        let result = on_stack(|| self.curly_inner(body, position, count, max, order, next));
        self.depth -= 1;
        result
    }

    fn curly_inner(&mut self, body: usize, mut position: usize, mut count: usize, max: usize, order: Order, next: usize) -> Result<bool> {
        match order {
            Order::Lazy => loop {
                if self.matches(next, position)? { return Ok(true); }
                if count >= max || !self.matches(body, position)? || position == self.last { return Ok(false); }
                position = self.last; count += 1;
            },
            Order::Possessive => {
                while count < max && self.matches(body, position)? { if position == self.last { break; } position = self.last; count += 1; }
                self.matches(next, position)
            }
            Order::Greedy => {
                if count >= max { return self.matches(next, position); }
                let minimum = count;
                if self.matches(body, position)? {
                    let width = self.last - position;
                    if width == 0 { return self.matches(next, position); }
                    position = self.last; count += 1;
                    while count < max {
                        if !self.matches(body, position)? { break; }
                        if position + width != self.last {
                            if self.curly(body, self.last, count + 1, max, order, next)? { return Ok(true); }
                            break;
                        }
                        position += width; count += 1;
                    }
                    loop {
                        if self.matches(next, position)? { return Ok(true); }
                        if count == minimum { break; }
                        position -= width; count -= 1;
                    }
                    return Ok(false);
                }
                self.matches(next, position)
            }
        }
    }

    fn group_curly(&mut self, body: usize, position: usize, count: usize, max: usize, order: Order, group: Option<usize>, next: usize) -> Result<bool> {
        if self.depth >= self.options.backtrack_stack_limit.min(MAX_CALL_DEPTH) { return Err(Error::RuntimeError(RuntimeError::StackOverflow)); }
        self.depth += 1;
        let result = on_stack(|| self.group_curly_inner(body, position, count, max, order, group, next));
        self.depth -= 1;
        result
    }

    fn group_curly_inner(&mut self, body: usize, mut position: usize, mut count: usize, max: usize, order: Order, group: Option<usize>, next: usize) -> Result<bool> {
        match order {
            Order::Lazy => loop {
                if self.matches(next, position)? { return Ok(true); }
                if count >= max || !self.matches(body, position)? || position == self.last { return Ok(false); }
                self.capture(group, position, self.last); position = self.last; count += 1;
            },
            Order::Possessive => unreachable!(),
            Order::Greedy => {
                let minimum = count;
                let saved = group.map(|group| (self.captures[group * 2], self.captures[group * 2 + 1]));
                if count < max && self.matches(body, position)? {
                    let width = self.last - position;
                    if width == 0 { self.capture(group, position, position); }
                    else {
                        loop {
                            self.capture(group, position, position + width);
                            position += width; count += 1;
                            if count >= max || !self.matches(body, position)? { break; }
                            if position + width != self.last {
                                if self.group_curly(body, position, count, max, order, group, next)? { return Ok(true); }
                                break;
                            }
                        }
                        while count > minimum {
                            if self.matches(next, position)? { self.capture(group, position - width, position); return Ok(true); }
                            position -= width; self.capture(group, position.wrapping_sub(width), position); count -= 1;
                        }
                    }
                }
                if let Some((start, end)) = saved { self.capture(group, start, end); }
                self.matches(next, position)
            }
        }
    }

    fn atom(&self, atom: &Atom, position: usize) -> Result<Option<usize>> {
        Ok(match atom {
            Atom::Literal { units, casei } => {
                if position + units.len() > self.text.len() { None }
                else if !*casei { (&self.text[position..position + units.len()] == units.as_slice()).then_some(position + units.len()) }
                else { match_units(self.text, position, units, false) }
            }
            Atom::Class(class) => point(self.text, position).and_then(|(value, end)| class.contains(value).then_some(end)),
            Atom::Any(newline) => point(self.text, position).and_then(|(value, end)| (*newline || value != 10).then_some(end)),
            Atom::PreviousEnd => (position == self.old_last).then_some(position),
            Atom::Assertion(assertion) => assertion_match(*assertion, self.text, position).then_some(position),
            Atom::Backref { group, casei, ascii } => {
                let slot = group * 2;
                if slot + 1 >= self.captures.len() || self.captures[slot] == usize::MAX { None }
                else {
                    let reference = &self.text[self.captures[slot]..self.captures[slot + 1]];
                    if *casei { match_reference(self.text, position, self.captures[slot], reference.len(), *ascii) }
                    else { self.text.get(position..position.saturating_add(reference.len())).filter(|candidate| *candidate == reference).map(|_| position + reference.len()) }
                }
            }
            Atom::Grapheme => crate::grapheme::next_boundary_utf16(self.text, position),
            Atom::LineEnding => self.text.get(position).filter(|unit| matches!(**unit, 10..=13 | 0x85 | 0x2028 | 0x2029)).map(|_| position + 1),
            Atom::Canonical(_) => unreachable!(),
        })
    }
}

fn high(unit: u16) -> bool { (0xd800..=0xdbff).contains(&unit) }
fn low(unit: u16) -> bool { (0xdc00..=0xdfff).contains(&unit) }
pub(super) fn point(text: &[u16], position: usize) -> Option<(u32, usize)> {
    let unit = *text.get(position)?;
    if high(unit) { if let Some(second) = text.get(position + 1).filter(|unit| low(**unit)) {
        return Some((0x10000 + ((u32::from(unit) - 0xd800) << 10) + u32::from(*second) - 0xdc00, position + 2));
    } }
    Some((u32::from(unit), position + 1))
}
fn previous(text: &[u16], position: usize) -> usize {
    if position >= 2 && low(text[position - 1]) && high(text[position - 2]) { position - 2 } else { position - 1 }
}
fn backwards(text: &[u16], mut position: usize, count: usize) -> Option<usize> {
    for _ in 0..count { if position == 0 { return None; } position = previous(text, position); }
    Some(position)
}
fn behind_distance(text: &[u16], position: usize, count: i32) -> usize {
    if count >= 0 { return position - backwards(text, position, count as usize).unwrap_or(0); }
    if count == i32::MIN { return 0; }
    let mut end = position;
    for _ in 0..count.unsigned_abs() { let Some((_, next)) = point(text, end) else { break; }; end = next; }
    end - position
}
fn match_reference(text: &[u16], position: usize, mut reference: usize, size: usize, ascii: bool) -> Option<usize> {
    if position + size > text.len() { return None; }
    let mut actual = position; let mut points = size; let mut index = 0;
    while index < points {
        let (left, end) = point(text, actual)?; let (right, reference_end) = point(text, reference)?;
        if !case_equal(left, right, ascii) { return None; }
        actual = end; reference = reference_end;
        if left >= 0x10000 { points -= 1; }
        index += 1;
    }
    Some(position + size)
}
fn case_equal(left: u32, right: u32, ascii: bool) -> bool {
    left == right || if ascii { left < 128 && right < 128 && (left as u8).eq_ignore_ascii_case(&(right as u8)) }
        else { char::from_u32(left).zip(char::from_u32(right)).is_some_and(|(left, right)| crate::unicode_case::equivalent(left, right)) }
}
fn match_units(text: &[u16], position: usize, reference: &[u16], ascii: bool) -> Option<usize> {
    if position + reference.len() > text.len() { return None; }
    let mut actual = position; let mut expected = 0;
    while expected < reference.len() {
        let (left, end) = point(text, actual)?; let (right, reference_end) = point(reference, expected)?;
        if !case_equal(left, right, ascii) { return None; }
        actual = end; expected = reference_end;
    }
    Some(position + reference.len())
}
fn assertion_match(assertion: Assertion, text: &[u16], position: usize) -> bool {
    match assertion {
        Assertion::StartText => position == 0,
        Assertion::EndText => position == text.len(),
        Assertion::StartLine { crlf: false } => position == 0 || text[position - 1] == 10,
        Assertion::EndLine { crlf: false } => position == text.len() || text[position] == 10,
        Assertion::StartLine { crlf: true } => position == 0 || text[position - 1] == 10 || text[position - 1] == 13 && text.get(position) != Some(&10),
        Assertion::EndLine { crlf: true } => position == text.len() || text[position] == 13 || text[position] == 10 && (position == 0 || text[position - 1] != 13),
        Assertion::LeftWordBoundary | Assertion::RightWordBoundary | Assertion::WordBoundary | Assertion::NotWordBoundary => {
            let word = |value| char::from_u32(value).is_some_and(|value| regex_syntax::try_is_word_character(value).unwrap_or(false));
            let left = position > 0 && point(text, previous(text, position)).is_some_and(|(value, _)| word(value));
            let right = point(text, position).is_some_and(|(value, _)| word(value));
            match assertion {
                Assertion::LeftWordBoundary => !left && right,
                Assertion::RightWordBoundary => left && !right,
                Assertion::WordBoundary => left != right,
                _ => left == right,
            }
        }
    }
}
