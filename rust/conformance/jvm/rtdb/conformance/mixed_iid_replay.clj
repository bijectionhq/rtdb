(ns rtdb.conformance.mixed-iid-replay
  (:require [clojure.java.io :as io]
            [jsonista.core :as json]
            [rtdb.conformance.embedded-parser :as embedded-parser]
            [rtdb.conformance.history :as history]
            [rtdb.conformance.mixed-iid-reference :as reference]
            [xtdb.api :as xt]
            [xtdb.compactor :as compactor]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.log :as log]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (clojure.lang RT)
           (java.io InputStream)
           (java.lang ProcessHandle ProcessHandle$Info)
           (java.nio.file Files LinkOption OpenOption Path)
           (java.time Duration)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.vector FixedSizeBinaryVector)
           (xtdb.api TransactionKey Xtdb)
           (xtdb.catalog TableCatalog$State)
           (xtdb.database Database)
           (xtdb.indexer LiveIndex$State)))

(set! *warn-on-reflection* true)

(def ^:private source-file "xtdb/mixed_iid_encoding_test.clj")
(def ^:private source-sha "7cdd0bf680e9311f9d814c4c0b07fd6db2282995528d80510ebc5b7f80fe9d39")
(def ^:private original-namespace 'xtdb.mixed-iid-encoding-test)
(def ^:private original-lines {'ingest! 79, 'queries 101, 'patch! 119, 'flight-sql-iids 124, 'pgwire-iids 143})
(def ^:private classes [RT Xtdb Database FlightSqlClient FixedSizeBinaryVector])
(def ^:private case-directory "target/mixed-iid-encoding")
(def ^:private case-id "[engine:dev.clojurephant.jovial.clojure-test]/[namespace:xtdb.mixed-iid-encoding-test]/[name:a-scan-over-plain-and-run-end-encoded-iid-pages-answers-as-over-plain-ones-6258]")
(def ^:private source-path (str "src/test/clojure/" source-file))
(def ^:private prefix-time 1578679200000000)
(def ^:private max-request-bytes (* 8 1024 1024))
(def ^:private ^"[Ljava.nio.file.LinkOption;" resolve-links (make-array LinkOption 0))
(def ^:private ^"[Ljava.nio.file.OpenOption;" regular-read (into-array OpenOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private ^Duration wait-limit (Duration/ofSeconds 30))

(defn- verify! [condition message data]
  (when-not condition
    (throw (err/incorrect ::replay-input message data))))

(defn- loaded-source-sha []
  (let [source (io/resource source-file)]
    (verify! source "Pinned mixed IID source is missing from the worker classpath" {})
    (with-open [^InputStream input (io/input-stream source)]
      (#'history/sha256 (.readNBytes input 65537)))))

(defn- original-vars []
  (verify! (= source-sha (loaded-source-sha)) "Worker loaded different mixed IID source bytes" {})
  (require original-namespace)
  (into {}
        (map (fn [[symbol line]]
               (let [original (ns-resolve original-namespace symbol)]
                 (verify! (and (var? original) (= {:file source-file :line line}
                                                   (select-keys (meta original) [:file :line])))
                          "Could not resolve the pinned mixed IID helper" {:helper symbol})
                 [(keyword symbol) original])))
        original-lines))

(defn- tx-key [^TransactionKey key]
  (when key
    {:tx_id (.getTxId key) :system_time_micros (#'history/micros (.getSystemTime key))}))

(defn- await-replica [^Database database ^long target]
  (let [start (System/nanoTime)
        timeout (.toNanos wait-limit)]
    (loop []
      (when-let [error (.getIngestionError database)] (throw error))
      (let [position (first (.getLatestReplicaMsgIds database))]
        (if (and (integer? position) (>= (long position) target))
          position
          (do
            (verify! (< (- (System/nanoTime) start) timeout)
                     "JVM replay did not read the archived replica tip" {:actual position, :expected target})
            (Thread/sleep 10)
            (recur)))))))

(defn- observe-basis [^Xtdb node {:keys [source_position replica_position tx adopted_block] :as expected}]
  (let [^Database database (db/primary-db node)
        source-tip (.latestSubmittedMsgId (.getSourceLog database) 0)]
    (verify! (and (<= 0 (:tx_id tx) source_position) (= prefix-time (:system_time_micros tx))
                  (= source_position source-tip))
             "JVM replay opened a different source checkpoint" {:actual source-tip :expected expected})
    (log/await-source database source_position wait-limit)
    (await-replica database replica_position)
    (let [^LiveIndex$State live (.getState (.getLiveIndex database))
          ^TableCatalog$State catalog (.snap (.getTableCatalog database))
          actual {:source_tip source-tip
                  :source_position (.getLatestProcessedMsgId database)
                  :replica_position (first (.getLatestReplicaMsgIds database))
                  :public_tx (tx-key (first (get (.latestCompletedTxs node) "xtdb")))
                  :live_tx (tx-key (.getLatestCompletedTx live))
                  :adopted_tx (tx-key (.getLatestCompletedTx catalog))
                  :adopted_block (.getBlockIdx catalog)
                  :open_block (.getBlockIdx live)}]
      (verify! (and (= source_position (:source_position actual))
                    (<= (long replica_position) (long (:replica_position actual)))
                    (= tx (:public_tx actual) (:live_tx actual) (:adopted_tx actual))
                    (= adopted_block (:adopted_block actual))
                    (= (inc (long adopted_block)) (:open_block actual)))
               "JVM replay recovered a different tick-39 basis" {:actual actual :expected expected})
      actual)))

(defn- flight [result]
  {:declared (#'reference/flight-field (:declared result))
   :batch_fields (mapv #'reference/flight-field (sort-by str (:batch-fields result)))
   :batch_count (:batches result)
   :iids (mapv #'reference/iid (:iids result))})

(defn- observe-continuation [^Path workspace original control mixed]
  (let [{:keys [ingest! queries pgwire-iids patch! flight-sql-iids]} original]
    (ingest! control (range 40 70))
    (ingest! mixed (range 40 70))
    (let [live (->> (xt/q mixed "SELECT trie_key FROM xt.trie_stats WHERE table_name = 'readings' AND trie_state = 'live' AND level > 0")
                    (into (sorted-set) (map :trie-key)))
          live-files (#'reference/read-live-files workspace live)
          control-queries (queries control)
          mixed-queries (queries mixed)
          control-pgwire (pgwire-iids control)
          mixed-pgwire (pgwire-iids mixed)
          control-patch (patch! control)
          mixed-patch (patch! mixed)
          control-flight (flight-sql-iids control)
          mixed-flight (flight-sql-iids mixed)
          typed-queries (fn [values patch]
                          (into (sorted-map :patch (#'reference/rows patch))
                                (map (fn [[id rows]] [id (#'reference/rows rows)]))
                                values))]
      {:live_keys (vec live)
       :live_files live-files
       :control {:observations (typed-queries control-queries control-patch)
                 :pgwire (#'reference/rows control-pgwire)
                 :flight (flight control-flight)}
       :mixed {:observations (typed-queries mixed-queries mixed-patch)
               :pgwire (#'reference/rows mixed-pgwire)
               :flight (flight mixed-flight)}})))

(defn- canonical-path ^Path [^Path path]
  (verify! (and (.isAbsolute path) (= path (.toRealPath path resolve-links)))
           "Mixed IID replay path is not canonical" {:path (str path)})
  path)

(defn- read-request ^bytes [^Path path]
  (canonical-path path)
  (with-open [^InputStream input (Files/newInputStream path regular-read)]
    (let [bytes (.readNBytes input (int (inc max-request-bytes)))]
      (verify! (<= (alength bytes) max-request-bytes) "Mixed IID replay request exceeds its size limit" {})
      bytes)))

(defn- validate-input [request ^Path copies]
  (let [{:keys [schema_version mode case source source_sha256 reference_revision checkpoint basis]} request]
    (verify! (and (= #{:schema_version :mode :case :source :source_sha256 :reference_revision
                       :native_execution_sha256 :native_binary_sha256 :reference_result_sha256 :checkpoint :basis}
                     (set (keys request)))
                  (= 1 schema_version) (= "native-to-jvm" mode)
                  (= case-id case) (= source-path source) (= source-sha source_sha256)
                  (= "c84f9334eb060ceb92d28ecdd4a06e6b625932fd" reference_revision)
                  (every? #(and (string? %) (re-matches #"[0-9a-f]{64}" %))
                          (map request [:native_execution_sha256 :native_binary_sha256 :reference_result_sha256]))
                  (= #{:control :mixed} (set (keys basis)))
                  (= 1 (:schema_version checkpoint))
                  (= "after-rewrite-before-reopen" (:stage checkpoint)))
             "Invalid native-to-JVM mixed IID transfer request" {})
    (canonical-path copies)
    (verify! (= (sort-by :path (:files checkpoint)) (#'history/files copies))
             "Staged JVM checkpoint files differ from the verified transfer" {})))

(defn- execute! [^Path workspace ^Path copies {:keys [basis]} original]
  (binding [cat/*file-size-target* (* 16 1024)
            compactor/*ignore-signal-block?* true]
    (util/with-open [control (tu/->local-node {:node-dir (.resolve copies "control") :rows-per-block 200})
                     mixed (tu/->local-node {:node-dir (.resolve copies "mixed") :rows-per-block 200})]
      (let [recovered {:control (observe-basis control (:control basis))
                       :mixed (observe-basis mixed (:mixed basis))}]
        (assoc (observe-continuation workspace original control mixed) :recovered recovered)))))

(defn- canonical-runtime-path [^String path]
  (str (.toRealPath (Path/of path (make-array String 0)) resolve-links)))

(defn- runtime-closure []
  (let [java-home (canonical-runtime-path (System/getProperty "java.home"))
        ^ProcessHandle$Info process (.info (ProcessHandle/current))
        executable (.command process)
        runtime (embedded-parser/runtime-closure classes nil)]
    (verify! (.isPresent executable) "JVM replay process did not report its Java executable" {})
    (let [java-executable (canonical-runtime-path (.get executable))
          java-modules (canonical-runtime-path (str java-home "/lib/modules"))]
      (verify! (and (= java-executable (canonical-runtime-path (str java-home "/bin/java")))
                    (= [java-executable java-modules] (:jdk runtime)))
               "JVM replay artifacts differ from the executing JDK" {:java_home java-home})
      (assoc runtime :java_home java-home, :java_executable java-executable, :java_modules java-modules))))

(defn -main [request-path output-path]
  (let [request-path (Path/of ^String request-path (make-array String 0))
        output (canonical-path (Path/of ^String output-path (make-array String 0)))
        workspace (canonical-path (.normalize (.toAbsolutePath (Path/of "" (make-array String 0)))))
        copies (.resolve workspace case-directory)
        input (read-request request-path)
        request (json/read-value input json/keyword-keys-object-mapper)]
    (validate-input request copies)
    (let [original (original-vars)
          runtime (runtime-closure)
          execution (execute! workspace copies request original)]
      (verify! (and (= runtime (runtime-closure))
                    (= source-sha (loaded-source-sha))
                    (= (#'history/sha256 input) (#'history/sha256 (read-request request-path))))
               "JVM replay input or runtime changed while observing the checkpoint" {})
      (let [runtime-bytes (json/write-value-as-bytes runtime)]
        (#'embedded-parser/write-bytes! output "runtime-artifacts.json" runtime-bytes)
        (#'history/write-new!
          (.resolve output "execution.json")
          (assoc execution :schema_version 1 :engine "jvm" :mode "native-to-jvm"
                 :case case-id :source source-path :source_sha256 source-sha
                 :original_helpers (into {} (map (fn [[sym line]] [(name sym) line])) original-lines) :continuation_ticks (vec (range 40 70))
                 :request_sha256 (#'history/sha256 input)
                 :runtime_sha256 (#'history/sha256 runtime-bytes)
                 :execution_complete true :complete_parity false))))))
