use std::future::Future;
use std::pin::Pin;

/// Builds `make`'s future in this function's frame and boxes it.
/// An unoptimised build gives every call site its own stack slot for the future it constructs, even under `Box::pin`,
/// so a poll that dispatches to many large futures needs the sum of their sizes; through here it needs a pointer each.
pub fn boxed<F: Future>(make: impl FnOnce() -> F) -> Pin<Box<F>> {
    Box::pin(make())
}

/// [`boxed`] for a future that borrows `receiver`, typically `&mut self`, which a capturing closure could not return.
pub fn boxed_with<T, F: Future>(receiver: T, make: impl FnOnce(T) -> F) -> Pin<Box<F>> {
    Box::pin(make(receiver))
}
