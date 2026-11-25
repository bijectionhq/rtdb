#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
pub(super) struct ForkPin {
    pub incarnation: u64,
    pub log_epoch: i32,
    pub pin: u128,
    pub reservation: u128,
}

#[derive(Clone, Copy)]
pub(super) enum ForkDisposition {
    NeverSubmitted,
    Ordered { registered: bool, certified: bool },
}

pub(super) enum ForkPinWrite {
    Publish(u128),
    Release(u128),
}

#[cfg_attr(verus_keep_ghost, verus_spec(write => ensures
    match write {
        Some(ForkPinWrite::Publish(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && matches!(disposition, ForkDisposition::Ordered { registered: true, certified: true }),
        Some(ForkPinWrite::Release(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && matches!(disposition, ForkDisposition::NeverSubmitted
                    | ForkDisposition::Ordered { registered: false, certified: true }),
        None => !binding_matches || !same_reservation(original, current)
            || matches!(disposition, ForkDisposition::Ordered { certified: false, .. }),
    },
))]
pub(super) fn select_pin_write(
    original: ForkPin, current: ForkPin, binding_matches: bool, disposition: ForkDisposition,
) -> Option<ForkPinWrite> {
    if !binding_matches || original.incarnation != current.incarnation
        || original.log_epoch != current.log_epoch || original.pin != current.pin
        || original.reservation != current.reservation
    {
        return None;
    }
    match disposition {
        ForkDisposition::NeverSubmitted => Some(ForkPinWrite::Release(original.pin)),
        ForkDisposition::Ordered { registered: true, certified: true } => Some(ForkPinWrite::Publish(original.pin)),
        ForkDisposition::Ordered { registered: false, certified: true } => Some(ForkPinWrite::Release(original.pin)),
        ForkDisposition::Ordered { certified: false, .. } => None,
    }
}

#[cfg(verus_keep_ghost)]
verus! {
pub(super) open spec fn same_reservation(original: ForkPin, current: ForkPin) -> bool {
    original.incarnation == current.incarnation && original.log_epoch == current.log_epoch
        && original.pin == current.pin && original.reservation == current.reservation
}
}
