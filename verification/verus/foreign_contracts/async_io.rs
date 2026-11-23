use vstd::prelude::*;

verus! {

pub struct TaskLocal<T> {
    pub value: Option<T>,
}

pub struct Scoped<T, F> {
    pub local: TaskLocal<T>,
    pub work: F,
}

impl<T> TaskLocal<T> {
    pub fn try_with<R, F: FnOnce(&T) -> R>(&self, read: F) -> (out: Result<R, ()>)
        requires self.value is Some ==> read.requires((&self.value->Some_0,)),
        ensures
            out is Ok <==> self.value is Some,
            out is Ok ==> read.ensures((&self.value->Some_0,), out->Ok_0),
    {
        match &self.value {
            Some(value) => Ok(read(value)),
            None => Err(()),
        }
    }

    pub fn scope<F>(&self, value: T, work: F) -> (out: Scoped<T, F>)
        ensures out.local.value == Some(value), out.work == work,
    {
        Scoped { local: TaskLocal { value: Some(value) }, work }
    }
}

pub struct JoinHandle<T> {
    pub captures: T,
}

pub struct Detached<T, B> {
    pub captures: T,
    pub local: TaskLocal<B>,
}

pub fn spawn<T>(captures: T) -> (out: JoinHandle<T>)
    ensures out.captures == captures,
{
    JoinHandle { captures }
}

pub fn detach<T, B>(handle: JoinHandle<T>) -> (out: Detached<T, B>)
    ensures out.captures == handle.captures, out.local.value.is_none(),
{
    Detached { captures: handle.captures, local: TaskLocal { value: None } }
}

pub struct Arc;

impl Arc {
    pub fn new<T>(value: T) -> (out: T)
        ensures out == value,
    {
        value
    }

    pub fn clone<T: Copy>(value: &T) -> (out: T)
        ensures out == *value,
    {
        *value
    }
}

pub struct PermitBalance {
    pub strong: u64,
    pub returned: u64,
}

pub fn release_strong_reference(balance: PermitBalance) -> (out: PermitBalance)
    requires balance.strong > 0, balance.returned == 0,
    ensures
        out.strong == balance.strong - 1,
        out.returned == if balance.strong == 1 { 1u64 } else { 0u64 },
{
    PermitBalance {
        strong: balance.strong - 1,
        returned: if balance.strong == 1 { 1 } else { 0 },
    }
}

pub struct MutexSection<T> {
    pub value: T,
}

impl<T> MutexSection<T> {
    pub fn lock(&mut self) -> (guard: &mut T)
        ensures *guard == old(self).value, final(self).value == *final(guard),
    {
        &mut self.value
    }
}

pub struct Pin {
    pub pending: u64,
}

pub struct PollContext {
    pub completed: bool,
}

pub enum Poll {
    Pending,
    Ready,
}

impl Pin {
    pub fn as_mut(&mut self) -> (out: &mut Self)
        ensures *out == *old(self), *final(self) == *final(out),
    {
        self
    }

    pub fn poll(&mut self, cx: &mut PollContext) -> (out: Poll)
        ensures
            final(self).pending == if old(self).pending > 0 { old(self).pending - 1 } else { 0 },
            final(cx).completed == (old(self).pending == 0),
            out is Ready <==> old(self).pending == 0,
    {
        cx.completed = self.pending == 0;
        if self.pending == 0 {
            Poll::Ready
        } else {
            self.pending -= 1;
            Poll::Pending
        }
    }
}

impl Poll {
    pub fn is_ready(&self) -> (out: bool)
        ensures out <==> self is Ready,
    {
        matches!(self, Poll::Ready)
    }
}

}
