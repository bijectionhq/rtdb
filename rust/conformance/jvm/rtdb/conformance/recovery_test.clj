(ns rtdb.conformance.recovery-test
  (:require [clojure.test :as t]
            [rtdb.conformance.recovery :as recovery]))

(t/deftest manifest-and-request-identities-must-agree
  (let [fixture {:schema_version 1, :kind "historical_prices", :timezone "UTC",
                 :transactions [{:operation "put", :system_time "2026-04-01T00:00:00Z", :prices {:A 12 :B 7}, :valid_from "2026-04-01T00:00:00Z"}],
                 :prefixes [{:id "april", :transactions 1}],
                 :queries [{:id "read", :prefix "april", :valid_time "2026-04-01T00:00:00Z", :system_time "2026-04-01T00:00:00Z"}]}
        request {:schema_version 1, :action "recover", :prefix "april", :fixture_sha256 "digest", :canary {:id "C" :price 991}}
        manifest {:schema_version 1, :closed true, :prefix "april", :fixture_sha256 "digest", :storage_version 6, :storage_epoch 0, :partitions 1, :external_source_token nil}]
    (t/is (= request (recovery/validate-request request fixture manifest)))
    (doseq [changed [(assoc manifest :closed false) (assoc manifest :storage_version 7) (assoc manifest :storage_epoch 1)
                     (assoc manifest :prefix "other") (assoc manifest :fixture_sha256 "different") (assoc manifest :external_source_token "wal")]]
      (t/is (thrown? clojure.lang.ExceptionInfo (recovery/validate-request request fixture changed))))
    (t/is (thrown? clojure.lang.ExceptionInfo (recovery/validate-request (assoc request :action "canary") fixture manifest)))
    (let [retry (assoc request :retry_compaction true :compaction_target "l01-rc-b01")
          l0 {:path "storage/v06/tables/public$products/data/l00-rc-b01.arrow"}
          orphan {:path "storage/v06/tables/public$products/data/l01-rc-b01.arrow"}]
      (t/is (= retry (recovery/validate-request retry fixture (assoc manifest :files [l0]))))
      (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"unoccupied compaction keys"
                            (recovery/validate-request retry fixture (assoc manifest :files [l0 orphan]))))
      (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid recovery compaction target"
                            (recovery/validate-request (dissoc retry :compaction_target) fixture manifest))))))

(defn -main [& _]
  (let [result (t/run-tests 'rtdb.conformance.recovery-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "Recovery adapter assertions failed" result)))))
