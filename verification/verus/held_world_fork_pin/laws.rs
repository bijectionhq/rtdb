use crate::production::{ForkDisposition, ForkPin, ForkPinWrite, same_reservation, select_pin_write};
use vstd::prelude::*;

verus! {

fn unconfirmed_decision_preserves_pin(original: ForkPin, current: ForkPin, registered: bool) {
    let write = select_pin_write(original, current, true, ForkDisposition::Ordered { registered, certified: false });
    assert(write.is_none());
}

fn replacement_reservation_is_preserved(original: ForkPin, current: ForkPin, disposition: ForkDisposition)
    requires !same_reservation(original, current),
{
    let write = select_pin_write(original, current, true, disposition);
    assert(write.is_none());
}

fn certified_refusal_releases_the_original_pin(original: ForkPin) {
    let write = select_pin_write(original, original, true, ForkDisposition::Ordered { registered: false, certified: true });
    assert(matches!(write, Some(ForkPinWrite::Release(pin)) if pin == original.pin));
}

}
