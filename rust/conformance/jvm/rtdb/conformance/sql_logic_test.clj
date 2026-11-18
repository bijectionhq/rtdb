(ns rtdb.conformance.sql-logic-test
  (:require [clojure.test :as t]
            [rtdb.conformance.sql-logic :as adapter]
            [xtdb.error :as err]
            [xtdb.sql.logic-test.runner :as slt])
  (:import (xtdb.api.storage Storage)))

(def options {:max_rows 100, :max_result_bytes 10000})
(def empty-engine
  (reify slt/DbEngine
    (get-engine-name [_] "xtdb")
    (execute-statement [this _ _] this)
    (execute-query [_ _ _] [[1]])))

(t/deftest execution-environment-records-storage-version-and-utc-query-configuration
  (let [environment (#'adapter/execution-environment)]
    (t/is (integer? (:storage_version environment)))
    (t/is (= Storage/VERSION (:storage_version environment)))
    (t/is (= "UTC" (:timezone environment)))))

(t/deftest pinned-halt-context-update-reports-error-and-does-not-stop
  (binding [slt/*opts* {:script-mode :validation}]
    (let [[ctx receipt] (adapter/execute-one {:db-engine empty-engine, :queries-run 0} {:type :halt} {} options)
          [_ query] (adapter/execute-one ctx {:type :query, :query "SELECT 1", :type-string "I", :sort-mode :nosort, :result-set "1\n"} {} options)]
      (t/is (= "halt_error" (:disposition receipt)))
      (t/is (= 1 (get-in ctx [:results :error])))
      (t/is (= "query_ok" (:disposition query))))))

(t/deftest suppressed-error-does-not-increment-pinned-error-count
  (let [engine (reify slt/DbEngine
                 (get-engine-name [_] "xtdb")
                 (execute-statement [_ _ _] (throw (err/incorrect ::rejected "rejected operation")))
                 (execute-query [_ _ _] []))
        [ctx receipt] (adapter/execute-one {:db-engine engine, :queries-run 0, :results {}}
                                          {:type :statement, :mode :ok, :statement "bad", :skipif ["postgresql"]} {} options)]
    (t/is (= "suppressed_postgresql_error" (:disposition receipt)))
    (t/is (= {} (:results ctx)))))

(t/deftest plan-and-option-limits-reject-unbounded-inputs
  (doseq [options [{:direct_sql true, :decorrelate true, :max_rows 0, :max_result_bytes 1}
                   {:direct_sql true, :decorrelate true, :max_rows 1, :max_result_bytes 536870913}
                   {:direct_sql true, :decorrelate true, :max_rows 1, :max_result_bytes 1, :query_limit -1}]]
    (t/is (thrown? Exception (#'adapter/validate-options options))))
  (let [path (java.nio.file.Files/createTempFile "rtdb-slt-plan" ".json" (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (java.nio.file.Files/writeString path "{\"oversized\":true}" (make-array java.nio.file.OpenOption 0))
      (with-redefs-fn {#'adapter/maximum-plan 2}
        #(t/is (thrown? Exception (#'adapter/read-plan path))))
      (finally (java.nio.file.Files/delete path)))))

(t/deftest engine-skip-consumes-query-limit-counter
  (let [[ctx receipt] (adapter/execute-one {:db-engine empty-engine, :queries-run 0}
                                         {:type :query, :query "SELECT 1", :type-string "I", :sort-mode :nosort, :skipif ["xtdb"]} {} options)]
    (t/is (= 1 (:queries-run ctx)))
    (t/is (= "engine_condition" (:disposition receipt)))
    (t/is (false? (:engine_executed receipt)))))

(t/deftest canonical-parse-retains-hash-count-label-and-case
  (t/is (= {:type "query", :sql "SELECT 1", :type_string "I", :sort "nosort", :label "same",
            :expected {:count 100, :md5 "ABCDEF0123456789abcdef0123456789"}}
           (adapter/canonical-record (slt/parse-record ["query I nosort same" "SELECT 1" "----" "100 values hashing to ABCDEF0123456789abcdef0123456789"])))))

(t/deftest pinned-real-rounding-and-zip-truncation
  (t/is (= "1.235\n-0.000\nNaN\n" (#'slt/format-result-str :nosort "R" [[1.2345] [-0.0001] [Double/NaN]])))
  (t/is (= "1\n" (#'slt/format-result-str :nosort "I" [[1 2]])))
  (t/is (= "1\n" (#'slt/format-result-str :nosort "ITT" [[1]])))
  (t/is (= "\n" (#'slt/format-result-str :nosort "I" []))))

(t/deftest expected-exception-retains-actual-error-evidence
  (let [engine (reify slt/DbEngine
                 (get-engine-name [_] "xtdb")
                 (execute-statement [_ _ _] (throw (err/incorrect ::rejected "rejected operation")))
                 (execute-query [_ _ _] []))
        [_ receipt] (adapter/execute-one {:db-engine engine, :queries-run 0}
                                        {:type :statement, :mode :error, :statement "bad"} {} options)]
    (t/is (= "statement_expected_error" (:disposition receipt)))
    (t/is (= "rejected operation" (get-in receipt [:error :message])))))

(defn -main [& _]
  (let [{:keys [fail error]} (t/run-tests 'rtdb.conformance.sql-logic-test)]
    (when (pos? (+ fail error)) (throw (err/fault ::tests-failed "SQL logic adapter assertions failed")))))
