(ns rtdb.conformance.recovery
  (:require [clojure.java.io :as io]
            [jsonista.core :as json]
            [rtdb.conformance.history :as history]
            [xtdb.compactor :as compactor]
            [xtdb.db-catalog :as db]
            [xtdb.log :as log]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)
           (java.time Duration Instant ZoneId)
           (xtdb.api Xtdb Xtdb$Config)
           (xtdb.database Database)))

(set! *warn-on-reflection* true)

(defn- compacted-object? [path]
  (boolean (re-matches #"storage/v06/tables/[^/]+/(?:data|meta)/l(?!00-)[^/]+\.arrow" path)))

(defn validate-request [request fixture manifest]
  (history/validate-fixture fixture)
  (when-not (and (= 1 (:schema_version request))
                 (contains? #{"capture" "recover"} (:action request))
                 (some #(= (:prefix request) (:id %)) (:prefixes fixture))
                 (= 1 (:schema_version manifest)) (true? (:closed manifest))
                 (= (:prefix request) (:prefix manifest)) (= (:fixture_sha256 request) (:fixture_sha256 manifest))
                 (= 6 (:storage_version manifest)) (= 0 (:storage_epoch manifest)) (= 1 (:partitions manifest)) (nil? (:external_source_token manifest))
                 (= "C" (get-in request [:canary :id])) (= 991 (get-in request [:canary :price])))
    (throw (ex-info "Invalid recovery request or protected manifest" {})))
  (when (or (:retry_compaction request) (:compaction_target request))
    (let [target (:compaction_target request)]
      (when-not (and (= "recover" (:action request)) (true? (:retry_compaction request))
                     (string? target) (re-matches #"l01-rc-b[0-9a-f]{2,17}" target))
        (throw (ex-info "Invalid recovery compaction target" {:target target})))
      (when-let [occupied (some #(when (compacted-object? (:path %)) (:path %)) (:files manifest))]
        (throw (ex-info "JVM local compaction requires unoccupied compaction keys" {:target target, :occupied occupied})))))
  request)

(defn- open-node ^Xtdb [^Path directory clock read-only?]
  (let [^Xtdb$Config config (node/->config {:log [:local {:path (.resolve directory "log"), :instant-src (tu/->settable-clock clock), :instant-source-for-non-tx-msgs? false}]
                                         :storage [:local {:path (.resolve directory "storage"), :epoch 0}]
                                         :compactor {:threads 1, :ree-iids? false, :within-file-recency-branching? false}
                                         :garbage-collector {:enabled? false}, :default-tz (ZoneId/of "UTC")})]
    (.readOnlyDatabases config (boolean read-only?))
    (binding [compactor/*ignore-signal-block?* true] (node/start-node config))))

(defn- recovered [^Xtdb node manifest capture?]
  (let [^Database database (db/primary-db node), target (long (get-in manifest [:log_heads :replica])), started (System/nanoTime)]
    (loop []
      (when-let [error (.getIngestionError database)] (throw error))
      (let [position (long (first (.getLatestReplicaMsgIds database)))]
        (when (> position target) (throw (ex-info "Recovery passed protected replica head" {:actual position, :expected target})))
        (when (< position target)
          (when (> (- (System/nanoTime) started) 30000000000) (throw (ex-info "Recovery did not reach protected replica head" {})))
          (Thread/sleep 10) (recur))))
    (let [actual (#'history/checkpoint node), expected (:checkpoint manifest)]
      (when-not (and (= (:tx_key actual) (:tx_key expected))
                     (or capture? (= (select-keys actual [:source_position :adopted_block :open_block])
                                    (select-keys expected [:source_position :adopted_block :open_block]))))
        (throw (ex-info "Recovered checkpoint differs from protected boundary" {:actual actual, :expected expected})))
      actual)))

(defn- observations [node fixture request manifest]
  (let [prefix (:prefix request)
        current (.toString (Instant/ofEpochSecond (quot (long (get-in manifest [:checkpoint :tx_key :system_time_micros])) 1000000)
                                                 (* (rem (long (get-in manifest [:checkpoint :tx_key :system_time_micros])) 1000000) 1000)))
        query {:id "current", :prefix prefix, :valid_time current, :system_time current}]
    (into (vec (for [query (:queries fixture) :when (= prefix (:prefix query)), bypass? [false true], product ["A" "B"]]
                 (#'history/observe node prefix "recovery" query product bypass?)))
          (mapv #(#'history/observe node prefix "recovery" query % false) ["A" "B" "C"]))))

(defn- execute [request fixture manifest]
  (let [^Path directory (.toPath (io/file (:directory request)))
        clock (atom (Instant/ofEpochSecond (quot (long (get-in manifest [:checkpoint :tx_key :system_time_micros])) 1000000)))
        result (util/with-open [node (open-node directory clock true)]
                 {:checkpoint (recovered node manifest (= "capture" (:action request))), :observations (observations node fixture request manifest)})]
    (if-not (:retry_compaction request)
      result
      (util/with-open [node (open-node directory clock false)]
        (log/sync-node node (Duration/ofSeconds 30))
        (compactor/compact-all! node (Duration/ofSeconds 60))
        (log/sync-node node (Duration/ofSeconds 30))
        (let [checkpoint (#'history/checkpoint node)]
          (assoc result :after_retry {:checkpoint checkpoint, :observations (observations node fixture request (assoc manifest :checkpoint checkpoint))}))))))

(defn -main [request-path output-path]
  (let [input (Files/readAllBytes (.toPath (io/file request-path)))
        request (json/read-value input json/keyword-keys-object-mapper)
        ^Path output (.toPath (io/file output-path))
        artifacts (#'history/runtime-artifacts)]
    (Files/createDirectory output (make-array FileAttribute 0))
    (#'history/write-new! (.resolve output "runtime-artifacts.json") artifacts)
    (let [identity {:schema_version 1, :engine "jvm", :request_sha256 (#'history/sha256 input), :fixture_sha256 (:fixture_sha256 request),
                    :binary_sha256 (#'history/sha256 (json/write-value-as-bytes artifacts)), :artifacts artifacts, :complete_parity false}
          result (try
                   (let [fixture-bytes (Files/readAllBytes (.toPath (io/file (:fixture request))))
                         manifest-bytes (Files/readAllBytes (.toPath (io/file (:manifest request))))
                         fixture (json/read-value fixture-bytes json/keyword-keys-object-mapper)
                         manifest (json/read-value manifest-bytes json/keyword-keys-object-mapper)
                         ^Path directory (.toPath (io/file (:directory request)))]
                     (validate-request request fixture manifest)
                     (when-not (and (= (:fixture_sha256 request) (#'history/sha256 fixture-bytes))
                                    (= (:manifest_sha256 request) (#'history/sha256 manifest-bytes))
                                    (= (:files manifest) (#'history/files directory)))
                       (throw (ex-info "Recovery inputs differ from protected manifest" {})))
                     (assoc identity :execution_complete true :result (execute request fixture manifest)))
                   (catch Throwable error
                     (assoc identity :execution_complete false :error (str error), :suppressed (mapv str (.getSuppressed error)))))
          result (try (assoc result :closure "graceful", :files (#'history/files (.toPath (io/file (:directory request)))))
                      (catch Throwable error (assoc result :closure "graceful", :execution_complete false, :artifact_error (str error))))]
      (when-not (= artifacts (#'history/runtime-artifacts)) (throw (ex-info "JVM runtime artifacts changed during recovery" {})))
      (#'history/write-new! (.resolve output "execution.json") result)
      (when-not (:execution_complete result) (throw (ex-info "Recovery adapter failed; inspect retained receipt" {}))))))
