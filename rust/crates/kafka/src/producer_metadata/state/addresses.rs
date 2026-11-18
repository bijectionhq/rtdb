use std::{mem, sync::Arc};

use rtdb_types::{Anomaly, MemoryBudget, Result};

use super::{Budget, Vector};

/// A qualified Java InetAddress result projected into socket address bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionAddress {
    /// IPv4 address bytes in network order.
    V4([u8; 4]),
    /// IPv6 address bytes in network order and the source numeric scope, zero for unscoped.
    V6 { address: [u8; 16], scope_id: u32 },
}

impl ConnectionAddress {
    fn same_family(self, other: Self) -> bool { mem::discriminant(&self) == mem::discriminant(&other) }

    fn same_ip(self, other: Self) -> bool {
        match (self, other) {
            (Self::V4(a), Self::V4(b)) => a == b,
            (Self::V6 { address: a, .. }, Self::V6 { address: b, .. }) => a == b,
            _ => false,
        }
    }
}

/// Holds one connection's admitted preferred-family addresses and last attempted address.
/// The caller serializes this cursor with the connection's original hostname and state.
pub struct ConnectionAddresses {
    addresses: Vector<ConnectionAddress>,
    position: usize,
    attempted: Option<ConnectionAddress>,
    budget: Budget,
}

impl ConnectionAddresses {
    /// Creates an unresolved entry for a new connection ID or changed hostname.
    pub fn new(budget: Arc<MemoryBudget>) -> Self {
        Self { addresses: Vector::new(), position: 0, attempted: None, budget: Budget(budget) }
    }

    /// Clears the retained list after an established disconnect while preserving its last attempted address.
    /// A connecting failure keeps the list for the next same-host advance.
    pub fn clear(&mut self) {
        self.addresses = Vector::new();
        self.position = 0;
    }

    /// Advances the cached list for a same-host connection attempt and clears it at exhaustion.
    /// An unresolved entry stays unresolved until the next call to [`Self::current`].
    pub fn advance(&mut self) {
        if self.addresses.len() == 0 {
            return;
        }
        self.position += 1;
        if self.position == self.addresses.len() {
            self.clear();
        }
    }

    /// Returns and records the current address, resolving only when the retained list is empty.
    /// Repeated calls keep the same selection until advance or clear.
    /// The caller returns its original qualified Java resolver order and keeps that borrowed input admitted.
    /// A resolver result must be nonempty and fit the signed Java array-length domain.
    /// Rejected results preserve the previous attempt.
    /// The first result chooses the family, and the output retains its original IPv6 scope.
    /// If another preferred result exists, a fresh list skips a first address matching the previous attempt's IP bytes.
    /// A resolver or admission failure leaves the prior attempt intact.
    pub fn current<'source>(
        &mut self, resolve: impl FnOnce() -> Result<&'source [ConnectionAddress]>,
    ) -> Result<ConnectionAddress> {
        if self.addresses.len() == 0 {
            let source = resolve()?;
            if source.len() > i32::MAX as usize {
                return Err(Anomaly::unsupported("rtdb/kafka-address-list-size", "Java address arrays require a signed int length"));
            }
            let first = *source.first().ok_or_else(|| Anomaly::incorrect(
                "rtdb/kafka-empty-address-list", "Source resolver returned an empty address list",
            ))?;
            let preferred = source.iter().copied().filter(|address| address.same_family(first));
            let count = preferred.clone().count();
            let mut addresses = Vector::with_capacity(count, &mut self.budget)?;
            for address in preferred {
                addresses.push(address, &mut self.budget)?;
            }
            self.position = usize::from(count > 1 && self.attempted.is_some_and(|last| last.same_ip(first)));
            self.addresses = addresses;
        }
        let selected = self.addresses.as_slice()[self.position];
        self.attempted = Some(selected);
        Ok(selected)
    }
}

#[cfg(test)]
#[path = "addresses/tests.rs"]
mod tests;
