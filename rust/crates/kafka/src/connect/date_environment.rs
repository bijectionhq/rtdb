use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::MemoryBudget;
use super::{DateEnvironment, DateEnvironmentInput};

pub(super) fn test_environment(language: &str,region: &str,timezone: &str) -> Arc<DateEnvironment> {
    DateEnvironment::from_inputs(DateEnvironmentInput { language: language.into(),script: String::new(),region: region.into(),variant: String::new(),unicode: BTreeMap::new(),timezone: timezone.into(),initialized_at: 1_600_000_000_000,observed_at: 1_600_000_000_000,constructed_at: 1_600_000_000_000 },&MemoryBudget::new(256 << 20)).unwrap()
}
