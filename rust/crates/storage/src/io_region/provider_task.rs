#[cfg(any(feature = "remote", test, verus_keep_ghost))]
macro_rules! spawn_provider_task {
    ($operation:ident, $work:ident, $sender:ident, $audience:ident;
     $current:expr, $spawn:ident, $hold:ident, $inherit:ident, $deliver:ident) => {{
        let budget = $current;
        $spawn!(worker; ($operation, budget, $work, $sender, $audience) => {
            let custody = ($operation, budget.clone());
            $hold!(custody, {
                let work = $inherit!(worker, budget, $work);
                $deliver!($sender, $audience, work)
            })
        })
    }};
}

macro_rules! retain_admitted_owner {
    ($current:expr) => {
        $current.try_with(Arc::clone).ok()
    };
}

macro_rules! scope_io_owner {
    ($current:expr, $supplied:expr, $work:expr; $combine:ident) => {{
        let supplied = $supplied;
        let owner = match $current.try_with(Arc::clone) {
            Ok(parent) => $combine!(parent, supplied),
            Err(_) => supplied,
        };
        $current.scope(owner, $work)
    }};
}
