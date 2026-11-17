(ns rtdb.conformance.table-column-test
  (:require [clojure.test :as t]
            [jsonista.core :as json]
            [rtdb.conformance.table-column :as table-column]))

(def ^:dynamic *fixture*)

(defn- values [cases]
  (into {} (map (fn [{:keys [id executed outcome]}]
                  (t/is (true? executed))
                  (t/is (= "returned" (:kind outcome)))
                  [id (:typed_value outcome)]))
        (table-column/observe cases)))

(t/deftest original-column-table-observations-are-independent-of-fixture-expectations
  (let [cases (:cases *fixture*)
        expected (into {} (map (fn [{:keys [id column expected]}] [id {column expected}])) cases)]
    (t/is (= expected (values cases)))
    (t/is (= expected (values (mapv #(assoc % :expected "fabricated") cases))))
    (let [changed (values (assoc-in cases [0 :input :List 0] {:I64 9}))]
      (t/is (= {:I64 9} (get-in changed ["integers" "x5" :rows 0 0])))
      (t/is (= {:I64 1} (get-in expected ["integers" "x5" :rows 0 0])))
      (t/is (= (get expected "mixed-numbers") (get changed "mixed-numbers"))))))

(t/deftest negative-floating-inputs-preserve-unsigned-ieee-bits
  (let [bits 13832806255468478464N
        cases [{:id "negative", :column "v", :parameter false, :input {:List [{:F64 bits}]}}]]
    (t/is (= {"negative" {"v" {:data_type {:Scalar "F64"}, :rows [[{:F64 bits}]]}}}
             (values cases)))))

(t/deftest unsupported-fixture-shapes-fail-before-producing-observations
  (doseq [input [{:I32 1} {:List [{:List []}]} {:List [{:Utf8 "not in profile"}]}]]
    (t/is (thrown-with-msg? RuntimeException #"Column-table fixtures"
                           (table-column/observe [{:id "unsupported", :column "v", :parameter false, :input input}])))))

(defn -main [fixture-path]
  (binding [*fixture* (json/read-value (slurp fixture-path) json/keyword-keys-object-mapper)]
    (let [{:keys [fail error]} (t/run-tests 'rtdb.conformance.table-column-test)]
      (when (pos? (+ fail error)) (System/exit 1)))))
