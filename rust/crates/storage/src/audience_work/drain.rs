#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

macro_rules! provider_custody {
    ($owner:expr, $work:expr; $pin:ident, $poll_fn:ident) => {{
        let mut owner = Some($owner);
        let mut work = $pin!($work);
        $poll_fn!(owner, work, cx => {
            let result = work.as_mut().poll(cx);
            owner = retain_until_drained(owner.take(), result.is_ready());
            result
        })
    }};
}

#[cfg_attr(verus_keep_ghost, verus_spec(retained => ensures
    retained == if drained { None } else { owner },
))]
pub(super) fn retain_until_drained<T>(owner: Option<T>, drained: bool) -> Option<T> {
    if drained { None } else { owner }
}

#[cfg(all(not(verus_keep_ghost), any(feature = "remote", test)))]
pub(crate) async fn hold_provider_custody<T>(
    owner: impl Send,
    work: impl std::future::Future<Output = T>,
) -> T {
    macro_rules! pin { ($work:expr) => { std::pin::pin!($work) }; }
    macro_rules! poll_fn { ($owner:ident, $work:ident, $cx:ident => $poll:block) => {
        std::future::poll_fn(|$cx| $poll).await
    }; }
    provider_custody!(owner, work; pin, poll_fn)
}
