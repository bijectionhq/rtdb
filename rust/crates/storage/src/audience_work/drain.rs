#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

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
    let mut owner = Some(owner);
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| {
        let result = work.as_mut().poll(cx);
        owner = retain_until_drained(owner.take(), result.is_ready());
        result
    }).await
}
