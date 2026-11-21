import History

namespace RTDB

def forBucket (view : List Event) (bucketOf : Nat → Nat) (bucket : Nat) : List Event :=
  view.filter (fun event => decide (bucketOf event.fact.key = bucket))

theorem key_projection_through_its_bucket (view : List Event)
    (bucketOf : Nat → Nat) (key : Nat) :
    forKey (forBucket view bucketOf (bucketOf key)) key = forKey view key := by
  simp only [forKey, forBucket, List.filter_filter]
  apply congrArg (fun predicate => view.filter predicate)
  funext event
  by_cases same : event.fact.key = key
  · simp [same]
  · simp [same]

def partitionedPoint (view : List Event) (bucketOf : Nat → Nat)
    (system key : Nat) (valid : Int) : Option Nat :=
  point (forBucket view bucketOf (bucketOf key)) system key valid

theorem partitioned_point_refines_the_reference (view : List Event)
    (bucketOf : Nat → Nat) (system key : Nat) (valid : Int) :
    partitionedPoint view bucketOf system key valid = point view system key valid := by
  unfold partitionedPoint
  rw [← scan_refines_point, ← scan_refines_point]
  rw [← key_partition_preserves_answer (forBucket view bucketOf (bucketOf key)) system key valid]
  rw [key_projection_through_its_bucket, key_partition_preserves_answer]

#print axioms key_projection_through_its_bucket
#print axioms partitioned_point_refines_the_reference

end RTDB
