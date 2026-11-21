(set-logic QF_FPBV)

(define-fun valid ((bits (_ BitVec 64))) Bool
  (or (= bits #x8000000000000000) (bvule bits #x7ff0000000000000)))
(define-fun key ((bits (_ BitVec 64))) (_ BitVec 64)
  (ite (= bits #x8000000000000000) #x0000000000000000 bits))

(declare-const left (_ BitVec 64))
(declare-const right (_ BitVec 64))
(assert (and (valid left) (valid right)))
(assert (not (= (fp.gt ((_ to_fp 11 53) left) ((_ to_fp 11 53) right))
                (bvugt (key left) (key right)))))
(check-sat)
