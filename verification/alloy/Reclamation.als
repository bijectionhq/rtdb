module Reclamation

sig Object {}
sig Member { uses: one Object }
sig Snapshot { captured: set Member }
sig View {
  catalog: set Member,
  readers: set Snapshot,
  stored: set Object
}

fun reachable [v: View]: set Object {
  (v.catalog + v.readers.captured).uses
}

pred healthy [v: View] {
  reachable[v] in v.stored
}

pred detach [prior, result: View, member: Member] {
  member in prior.catalog
  result.catalog = prior.catalog - member
  result.readers = prior.readers
}

pred collect [prior, result: View, member: Member] {
  detach[prior, result, member]
  let object = member.uses |
    result.stored = prior.stored - (object - reachable[result])
}

pred collectOnFirstRemoval [prior, result: View, member: Member] {
  detach[prior, result, member]
  result.stored = prior.stored - member.uses
}

assert SurvivingReferences {
  all prior, result: View, member: Member |
    healthy[prior] and collect[prior, result, member] implies healthy[result]
}

assert FirstRemovalIsEnough {
  all prior, result: View, member: Member |
    healthy[prior] and collectOnFirstRemoval[prior, result, member] implies healthy[result]
}

pred SharedFileSurvives {
  some disj prior, result: View, disj left, right: Member |
    right in prior.catalog and left.uses = right.uses and
    healthy[prior] and collect[prior, result, left] and left.uses in result.stored
}

check SurvivingReferences for 4
check FirstRemovalIsEnough for 4
run SharedFileSurvives for 4
