macro_rules! select_audience_budget {
    ($existing:expr, $limits:expr, $turn:expr) => {{
        let existing = $existing;
        match decision::new_action(existing.is_some(), $limits) {
            Some(limits) => Some(Budget { action: Arc::new(Mutex::new(Some(limits))), turn: $turn }),
            None => existing,
        }
    }};
}

macro_rules! inherit_audience_budget {
    ($current:expr, $budget:expr, $work:expr, $await:ident) => {{
        match $budget {
            Some(budget) => $await!(scoped, $current.scope(budget, $work)),
            None => $await!(unscoped, $work),
        }
    }};
}

macro_rules! capture_current_budget {
    ($current:expr) => {
        $current.try_with(Budget::clone).ok()
    };
}

macro_rules! debit_audience_allowance {
    ($allowance:expr, $cost:expr) => {{
        let mut available = $allowance.lock();
        *available = decision::debit(*available, $cost);
        available.as_ref().map(|_| ()).ok_or(Error::AudienceWorkLimit)
    }};
}

macro_rules! charge_audience_budget {
    ($action:expr, $turn:expr, $work:expr, $debit:ident) => {{
        let cost = decision::cost($work);
        $debit!($action, cost)?;
        if let Some(remaining) = $turn {
            $debit!(remaining, cost)?;
        }
        Ok(())
    }};
}
