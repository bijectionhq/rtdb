use rtdb_types::Anomaly;
use super::super::{Failure, FailureKind, Result, overflow};

const MAX_STRING_UNITS: usize = 20_000_000;

pub(super) struct TextBufferDemand { capacity: Option<usize>, occupied: usize, completed: usize }
impl TextBufferDemand {
    pub(super) fn new(capacity: Option<usize>, at: usize) -> Result<Self> {
        if capacity.is_some_and(|capacity| capacity == 0 || capacity > i32::MAX as usize) {
            return Err(unavailable(at, "Observed text-buffer capacity is outside the Java array range"));
        }
        Ok(Self { capacity, occupied: 0, completed: 0 })
    }

    pub(super) fn before_character(&mut self, at: usize) -> Result<()> {
        if let Some(capacity) = self.capacity {
            if self.occupied == capacity {
                self.completed = self.completed.checked_add(capacity).ok_or_else(overflow)?;
                self.occupied = 0;
                validate(self.completed, at)?;
                self.capacity = Some(capacity.checked_add(capacity >> 1).ok_or_else(overflow)?.clamp(500, 65_536));
            }
        }
        Ok(())
    }

    pub(super) fn unit(&mut self, at: usize) -> Result<()> {
        self.occupied = self.occupied.checked_add(1).ok_or_else(overflow)?;
        if self.capacity.is_none() && self.occupied > MAX_STRING_UNITS {
            return Err(unavailable(at, "Over-limit string demand requires the source text-buffer capacity and reader profile"));
        }
        Ok(())
    }

    pub(super) fn complete(&self, at: usize) -> Result<()> {
        validate(self.completed.checked_add(self.occupied).ok_or_else(overflow)?, at)
    }
}

fn validate(units: usize, at: usize) -> Result<()> {
    if units > MAX_STRING_UNITS {
        return Err(Failure::at(FailureKind::Constraint, at, "JSON string length exceeds 20000000"));
    }
    Ok(())
}

fn unavailable(at: usize, detail: &'static str) -> Failure {
    Failure::Native(Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-source-profile", format!("Registry response at byte {at}: {detail}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_segments_grow_from_the_observed_capacity_with_java_bounds() {
        let mut buffer = TextBufferDemand::new(Some(200), 0).unwrap();
        buffer.occupied = 200;
        buffer.before_character(0).unwrap();
        assert_eq!((buffer.completed, buffer.occupied, buffer.capacity), (200, 0, Some(500)));
        buffer.occupied = 500;
        buffer.before_character(0).unwrap();
        assert_eq!((buffer.completed, buffer.occupied, buffer.capacity), (700, 0, Some(750)));
        let mut reused = TextBufferDemand::new(Some(50_000), 0).unwrap();
        reused.occupied = 50_000;
        reused.before_character(0).unwrap();
        assert_eq!(reused.capacity, Some(65_536));
    }

    #[test]
    fn over_limit_partial_segments_are_checked_on_completion_and_final_contents_demand() {
        let mut buffer = TextBufferDemand { capacity: Some(65_536), occupied: 14_425, completed: 19_985_576 };
        buffer.before_character(0).unwrap();
        assert!(matches!(buffer.complete(0), Err(Failure::Decode { kind: FailureKind::Constraint, .. })));
        buffer.occupied = 65_536;
        assert!(matches!(buffer.before_character(0), Err(Failure::Decode { kind: FailureKind::Constraint, .. })));
    }

    #[test]
    fn missing_or_invalid_capacity_is_an_outer_capability_failure() {
        let mut buffer = TextBufferDemand::new(None, 0).unwrap();
        buffer.occupied = MAX_STRING_UNITS;
        match buffer.unit(0) {
            Err(Failure::Native(cause)) => {
                assert_eq!(cause.category, rtdb_types::AnomalyCategory::Unsupported);
                assert!(cause.code.ends_with("/unsupported-source-profile"));
            }
            _ => panic!("Expected an outer source-profile failure"),
        }
        assert!(matches!(TextBufferDemand::new(Some(0), 0), Err(Failure::Native(_))));
        assert!(matches!(TextBufferDemand::new(Some(i32::MAX as usize + 1), 0), Err(Failure::Native(_))));
    }
}
