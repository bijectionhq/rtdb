macro_rules! capture_audience_io_owner {
    ($call:expr) => {{
        $call.session.audience_source()?;
        Some(Arc::new((
            Arc::clone(&$call.admission),
            Arc::clone(&$call.session),
            $call._active_call.clone(),
            Arc::clone(&$call._reservation),
        )))
    }};
}
