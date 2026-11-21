import Std

namespace RTDB.MessagePosition

def offsetLimit : Int := 281474976710656
def epochLimit : Int := 32768
def signedLimit : Int := 9223372036854775808

def pack (epoch offset : Int) : Int := epoch * offsetLimit + offset

theorem supported_positions_fit_signed_order (epoch offset : Int)
    (eligibleEpoch : 0 ≤ epoch ∧ epoch < epochLimit)
    (eligibleOffset : 0 ≤ offset ∧ offset < offsetLimit) :
    0 ≤ pack epoch offset ∧ pack epoch offset < signedLimit := by
  unfold pack offsetLimit epochLimit signedLimit at *
  omega

theorem an_epoch_fits_exactly_within_the_supported_range (epoch offset : Int)
    (eligibleOffset : 0 ≤ offset ∧ offset < offsetLimit) :
    (0 ≤ pack epoch offset ∧ pack epoch offset < signedLimit) ↔
      (0 ≤ epoch ∧ epoch < epochLimit) := by
  unfold pack offsetLimit epochLimit signedLimit at *
  omega

theorem order_is_lexicographic (leftEpoch leftOffset rightEpoch rightOffset : Int)
    (left : 0 ≤ leftOffset ∧ leftOffset < offsetLimit)
    (right : 0 ≤ rightOffset ∧ rightOffset < offsetLimit) :
    pack leftEpoch leftOffset < pack rightEpoch rightOffset ↔
      leftEpoch < rightEpoch ∨ (leftEpoch = rightEpoch ∧ leftOffset < rightOffset) := by
  unfold pack offsetLimit at *
  omega

theorem packed_round_trip (epoch offset : Int) (eligibleOffset : 0 ≤ offset ∧ offset < offsetLimit) :
    pack epoch offset / offsetLimit = epoch ∧ pack epoch offset % offsetLimit = offset := by
  unfold pack offsetLimit at *
  omega

theorem the_start_sentinel_precedes_the_first_position (epoch : Int) :
    pack epoch (-1) + 1 = pack epoch 0 := by
  unfold pack
  omega

#print axioms supported_positions_fit_signed_order
#print axioms an_epoch_fits_exactly_within_the_supported_range
#print axioms order_is_lexicographic
#print axioms packed_round_trip
#print axioms the_start_sentinel_precedes_the_first_position

end RTDB.MessagePosition
