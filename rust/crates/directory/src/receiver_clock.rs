//! The cell directory owns the clock frontier independently of deployment histories.
use rtdb_types::{Anomaly, Result, Timestamp};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The publisher must advance the clock within this interval to retain its epoch.
const LEASE_MICROS: i64 = 3_000_000;

pub(crate) const CLOCK_PREFIX: &str = "~receiver-clock/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverClockSample {
    pub domain: Uuid,
    pub publisher: Uuid,
    pub epoch: u64,
    pub sequence: u64,
    pub instant: Timestamp,
    pub expires: Timestamp,
}

impl ReceiverClockSample {
    pub fn admits(self, domain: Uuid, elapsed: Timestamp) -> bool {
        self.domain == domain && self.instant <= elapsed && elapsed < self.expires
    }

    pub(crate) fn advance(
        previous: Option<Self>, domain: Uuid, publisher: Uuid, observed: Timestamp,
    ) -> Result<Option<Self>> {
        if domain.is_nil() || publisher.is_nil() || previous.is_some_and(|last| last.domain != domain) {
            return Err(Anomaly::incorrect("rtdb.directory/clock-domain", "Invalid receiver clock authority"));
        }
        let (epoch, sequence) = match previous {
            Some(last) => {
                if observed <= last.instant || publisher != last.publisher && observed < last.expires {
                    return Ok(None);
                }
                let epoch = if publisher == last.publisher && observed < last.expires {
                    last.epoch
                } else {
                    last.epoch.checked_add(1).ok_or_else(exhausted)?
                };
                (epoch, last.sequence.checked_add(1).ok_or_else(exhausted)?)
            }
            None => (1, 1),
        };
        let expires = observed.0.checked_add(LEASE_MICROS).map(Timestamp).ok_or_else(exhausted)?;
        Ok(Some(Self { domain, publisher, epoch, sequence, instant: observed, expires }))
    }
}

fn exhausted() -> Anomaly { Anomaly::fault("rtdb.directory/clock-exhausted", "Receiver clock authority exhausted") }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stalled_or_replaced_publisher_cannot_rewind_or_extend_a_sample() {
        let domain = Uuid::from_u128(1);
        let first = Uuid::from_u128(2);
        let later = Uuid::from_u128(3);
        let original = ReceiverClockSample::advance(None, domain, first, Timestamp(100)).unwrap().unwrap();
        assert!(!original.admits(domain, Timestamp(99)));
        assert!(!original.admits(domain, original.expires));
        assert_eq!(ReceiverClockSample::advance(Some(original), domain, first, Timestamp(100)).unwrap(), None);
        assert_eq!(ReceiverClockSample::advance(Some(original), domain, later, Timestamp(101)).unwrap(), None);
        let successor = ReceiverClockSample::advance(Some(original), domain, later, original.expires).unwrap().unwrap();
        assert_eq!((successor.sequence, successor.epoch), (2, 2));
        assert_eq!(ReceiverClockSample::advance(Some(successor), domain, first, successor.instant).unwrap(), None);
    }
}
