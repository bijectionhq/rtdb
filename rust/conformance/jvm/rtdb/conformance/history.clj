(ns rtdb.conformance.history
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.compactor :as compactor]
            [xtdb.db-catalog :as db]
            [xtdb.log :as log]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import (java.io File InputStream)
           (java.lang.reflect InvocationHandler InvocationTargetException Method Proxy)
           (java.nio.file Files LinkOption OpenOption Path StandardOpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.security MessageDigest)
           (java.time Duration Instant ZoneId)
           (java.util HexFormat Map$Entry)
           (java.util.function Consumer)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           (org.apache.arrow.vector.types TimeUnit)
           (org.apache.arrow.vector.types.pojo ArrowType$Int ArrowType$Null ArrowType$Timestamp)
           (xtdb NodeBase)
           (xtdb.api ResultCursor TransactionKey Xtdb)
           (xtdb.api.query PrepareOpts QueryOpts)
           (xtdb.arrow RelationReader VectorReader VectorType)
           (xtdb.catalog TableCatalog$State)
           (xtdb.database Database Database$Catalog PartitionStorage)
           (xtdb.indexer LiveIndex$State)
           (xtdb.metadata PageMetadata$Factory)
           (xtdb.query IQuerySource IQuerySource$QueryCatalog IQuerySource$QueryDatabase PreparedQuery SqlParser)
           (xtdb.storage BufferPool)))

(set! *warn-on-reflection* true)

(def ^:private products #xt/table products)
(def ^:private layouts ["live" "flushed" "compacted" "restarted"])
(def ^:private projection ["price" "valid_from" "valid_to" "system_from" "system_to"])
(def ^:private no-links (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn- micros ^long [^Instant instant]
  (Math/addExact (Math/multiplyExact (.getEpochSecond instant) 1000000)
                 (quot (.getNano instant) 1000)))
(defn- instant ^Instant [^String value] (Instant/parse value))
(defn- sha256 [^bytes bytes]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)))
(defn- file-digest [^Path path]
  (util/with-open [^InputStream stream (Files/newInputStream path (make-array OpenOption 0))]
    (let [digest (MessageDigest/getInstance "SHA-256") buffer (byte-array 65536)]
      (loop []
        (let [n (.read stream buffer)]
          (when (pos? n) (.update digest buffer 0 n) (recur))))
      (.formatHex (HexFormat/of) (.digest digest)))))

(defn- files [^Path root]
  (util/with-open [stream (Files/walk root (make-array java.nio.file.FileVisitOption 0))]
    (->> (.iterator ^java.util.stream.Stream stream)
         iterator-seq
         (remove #(= root %))
         (keep (fn [^Path path]
                 (cond
                   (Files/isSymbolicLink path) (throw (ex-info "Fixture artifact is a symbolic link" {}))
                   (Files/isDirectory path no-links) nil
                   (Files/isRegularFile path no-links)
                   {:path (str (.relativize root path)), :bytes (Files/size path), :sha256 (file-digest path)}
                   :else (throw (ex-info "Fixture artifact is not a regular file" {})))))
         (sort-by :path)
         vec)))

(defn runtime-artifacts
  "Returns a SHA-256 manifest for the running JVM's classpath entries, bin/java and lib/modules."
  []
  (mapv (fn [^File file]
          (let [path (.toPath file)]
            (if (.isDirectory file)
              {:path (.getCanonicalPath file), :kind "directory", :files (files path)}
              {:path (.getCanonicalPath file), :kind "file", :bytes (Files/size path), :sha256 (file-digest path)})))
        (map io/file
             (concat (str/split (System/getProperty "java.class.path")
                               (re-pattern (java.util.regex.Pattern/quote File/pathSeparator)))
                     [(str (System/getProperty "java.home") "/bin/java")
                      (str (System/getProperty "java.home") "/lib/modules")]))))

(defn- write-new! [^Path path value]
  (let [bytes (json/write-value-as-bytes value)]
    (util/with-open [channel (java.nio.channels.FileChannel/open path
                             (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))]
      (let [buffer (java.nio.ByteBuffer/wrap bytes)]
        (while (.hasRemaining buffer) (.write ^java.nio.channels.FileChannel channel buffer)))
      (.force ^java.nio.channels.FileChannel channel true))))

(defn- record-read! [counter ^long bytes]
  (swap! counter (fn [[count-bytes count-calls]]
                   [(Math/addExact (long count-bytes) bytes)
                    (Math/addExact (long count-calls) 1)])))

(defn counting-pool
  "Borrows the node pool; close never releases it.
  Counts successful byte arrays and record-batch metadata/body spans, including cache hits, excluding footer-only requests.
  Suspend reads complete through the pool's synchronous bridge before returning; the caller owns the returned record batch."
  ^BufferPool [^BufferPool delegate counter]
  (Proxy/newProxyInstance
    (.getClassLoader ^Class BufferPool)
    (into-array Class [BufferPool])
    (reify InvocationHandler
      (invoke [_ proxy method args]
        (let [^Method method method
              args (or args (object-array 0))]
          (case (.getName method)
            "close" nil
            "toString" "HistoryCountingBufferPool"
            "hashCode" (System/identityHashCode proxy)
            "equals" (identical? proxy (aget ^objects args 0))
            "getByteArray"
            (let [bytes (.getByteArray delegate (aget ^objects args 0))]
              (record-read! counter (alength bytes)) bytes)
            ("getRecordBatch" "getRecordBatchSync")
            (let [^Path key (aget ^objects args 0), index (int (aget ^objects args 1))
                  ^ArrowFooter footer (.getFooter delegate key)
                  ^ArrowBlock block (.get (.getRecordBatches footer) index)
                  ^ArrowRecordBatch batch (.getRecordBatchSync delegate key index)]
              (try
                (record-read! counter (Math/addExact (long (.getMetadataLength block)) (.getBodyLength block)))
                batch
                (catch Throwable error
                  (try (.close batch) (catch Throwable close (.addSuppressed error close)))
                  (throw error))))
            ("putObject" "putObjectSync" "copyObject" "copyObjectSync" "deleteIfExists" "deleteIfExistsSync" "openArrowWriter")
            (throw (ex-info "Query measurement cannot publish storage mutations" {}))
            (try (.invoke method delegate ^objects args)
                 (catch InvocationTargetException error (throw (.getCause error))))))))))

(defn- catalog-view ^IQuerySource$QueryCatalog [^Database database ^PartitionStorage storage]
  (let [view (reify IQuerySource$QueryDatabase
               (getName [_] (.getName database))
               (getStorage [_] storage)
               (getQueryState [_] (.getQueryState database))
               (openSnapshot [_ basis] (.openSnapshot database basis)))]
    (reify IQuerySource$QueryCatalog
      (getDatabaseNames [_] [(.getName database)])
      (getTxScoped [_] false)
      (databaseOrNull [_ name] (when (= name (.getName database)) view)))))

(defn- arrow-tag [type]
  (cond
    (instance? ArrowType$Null type) {:kind "null"}
    (and (instance? ArrowType$Int type) (= 64 (.getBitWidth ^ArrowType$Int type)) (.getIsSigned ^ArrowType$Int type)) {:kind "i64"}
    (and (instance? ArrowType$Timestamp type) (= TimeUnit/MICROSECOND (.getUnit ^ArrowType$Timestamp type)))
    {:kind "timestamp", :unit "microsecond", :timezone (.getTimezone ^ArrowType$Timestamp type)}
    :else (throw (ex-info "Unexpected history projection type" {:type (str type)}))))
(defn type-tags [^VectorType type]
  (->> (.getLegs type) (map #(arrow-tag (.getArrowType ^VectorType %))) (sort-by json/write-value-as-string) vec))
(defn- cell [^VectorReader vector ^long row]
  (if (.isNull vector (int row))
    {:kind "null"}
    (let [tags (remove #(= "null" (:kind %)) (type-tags (.getType vector)))]
      (when-not (= 1 (count tags)) (throw (ex-info "History value requires a single non-null type" {})))
      (assoc (first tags) :value (.getLong vector (int row))))))

(defn- observe [^Xtdb node prefix layout query product bypass?]
  (let [pruning (if bypass? "bypassed" "normal")
        sql (str "SELECT price, _valid_from AS valid_from, _valid_to AS valid_to, _system_from AS system_from, _system_to AS system_to FROM products "
                 "FOR VALID_TIME AS OF TIMESTAMP '" (:valid_time query) "' FOR SYSTEM_TIME AS OF TIMESTAMP '" (:system_time query) "' WHERE _id = '" product "'")
        ^Database database (db/primary-db node)
        ^NodeBase base (util/node-base node)
        counter (atom [0 0])
        pool (counting-pool (.getBufferPool database) counter)
        token (.snapshotToken ^Database$Catalog (db/<-node node))
        now (.getSystemTime ^TransactionKey (first (get (.latestCompletedTxs node) "xtdb")))]
    (util/with-open [metadata (PageMetadata$Factory. (.getAllocator base) pool 1024)]
      (let [storage (PartitionStorage. (.getLogs database) pool metadata 0)
            catalog (catalog-view database storage)
            ^IQuerySource source (.getQuerySource base)
            execute (fn []
                      (let [^PreparedQuery prepared (.prepareQuery source (first (SqlParser/parseStatements sql)) catalog
                                                    (PrepareOpts. now (ZoneId/of "UTC") "xtdb" nil false false token))]
                        (util/with-open [^ResultCursor cursor (.openQuery prepared nil (QueryOpts. now (ZoneId/of "UTC") token nil))]
                          (let [schema (mapv (fn [^Map$Entry entry] {:name (.getKey entry), :types (type-tags (.getValue entry))})
                                             (.entrySet (.getResultTypes cursor)))
                                rows (atom [])]
                            (when-not (= projection (mapv :name schema)) (throw (ex-info "History schema has unexpected projection order" {})))
                            (while (.tryAdvance cursor
                                     (reify Consumer
                                       (accept [_ relation]
                                         (let [^RelationReader relation relation]
                                           (doseq [row (range (.getRowCount relation))]
                                             (when (= 2 (count @rows)) (throw (ex-info "History point query returned more than two rows" {})))
                                             (swap! rows conj (mapv #(cell (.vectorFor relation %) row) projection))))))))
                            {:schema schema, :rows @rows}))))
            result (if bypass? (with-redefs [trie/filter-pages (fn [pages _] pages)] (execute)) (execute))
            [bytes calls] @counter]
        (merge result {:id (str prefix "/" layout "/" pruning "/" (:id query) "/" product), :executed true,
                       :query (:id query), :product product, :valid_time (:valid_time query), :system_time (:system_time query),
                       :pruning pruning, :sql sql, :read_bytes bytes, :read_calls calls, :read_boundary "buffer_pool_logical_payload"})))))

(defn- checkpoint [^Xtdb node]
  (let [^Database database (db/primary-db node)
        ^LiveIndex$State live (.getState (.getLiveIndex database))
        ^TableCatalog$State table (.snap (.getTableCatalog database))
        ^TransactionKey tx (.getLatestCompletedTx live)
        tries (trie-cat/current-tries (.tableState (.getState (.getTrieCatalog database)) products))]
    {:source_position (.getLatestProcessedMsgId database), :replica_position (first (.getLatestReplicaMsgIds database)),
     :tx_key (when tx {:tx_id (.getTxId tx), :system_time_micros (micros (.getSystemTime tx))}),
     :open_block (.getBlockIdx live),
     :adopted_block (when-some [index (.getBlockIdx table)]
                      {:index index, :source_position (.getLatestProcessedMsgId table),
                       :boundary_replica_position (.getBoundaryReplicaMsgId table), :term (.getBoundaryTermId table),
                       :term_sequence (.getBoundaryTermSeq table)}),
     :live_product_segments (if (contains? (.getTables live) products) 1 0),
     :live_product_tries (mapv (fn [trie] {:key (:trie-key trie), :level (:level trie), :data_file_size (:data-file-size trie)}) tries)}))

(defn- observations [^Xtdb node fixture prefix layout]
  (let [checkpoint (checkpoint node)
        expected-time (-> fixture :transactions (nth (dec (long (:transactions prefix)))) :system_time instant micros)
        tries (:live_product_tries checkpoint)]
    (when-not (= expected-time (get-in checkpoint [:tx_key :system_time_micros]))
      (throw (ex-info "Physical checkpoint changed the user transaction prefix" {})))
    (when (and (= layout "live") (or (zero? (long (:live_product_segments checkpoint))) (seq tries)))
      (throw (ex-info "Live fixture lost its unflushed layout" {})))
    (when (and (= layout "flushed")
               (or (nil? (:adopted_block checkpoint)) (pos? (long (:live_product_segments checkpoint)))
                   (not-any? #(zero? (long (:level %))) tries)))
      (throw (ex-info "Flushed fixture has no adopted L0 products layout" {})))
    (when (and (contains? #{"compacted" "restarted"} layout)
               (not-any? #(pos? (long (:level %))) tries))
      (throw (ex-info "Compaction produced no live products trie above level zero" {})))
    {:checkpoint checkpoint
     :observations (vec (for [query (:queries fixture) :when (= (:prefix query) (:id prefix))
                              bypass? [false true], product ["A" "B"]]
                          (observe node (:id prefix) layout query product bypass?)))}))

(defn- submit! [node clock transaction]
  (let [time (instant (:system_time transaction))
        _ (reset! clock time)
        operation (case (:operation transaction)
                    "put" (into [:put-docs (cond-> {:into :products, :valid-from (instant (:valid_from transaction))}
                                            (:valid_to transaction) (assoc :valid-to (instant (:valid_to transaction))))]
                                (map (fn [[id price]] {:xt/id (name id), :price (long price)})) (:prices transaction))
                    "delete" [:delete-docs {:from :products, :valid-from (instant (:valid_from transaction))} (:id transaction)]
                    "erase" [:erase-docs :products (:id transaction)])
        ^TransactionKey key (xt/execute-tx node [operation] {:default-tz "UTC"})]
    (when-not (= time (.getSystemTime key)) (throw (ex-info "Transaction did not commit at the fixture clock reading" {})))
    {:tx_id (.getTxId key), :system_time_micros (micros time), :committed true}))

(defn- open-node ^Xtdb [^Path directory clock]
  (binding [compactor/*ignore-signal-block?* true]
    (tu/->local-node {:node-dir directory, :buffers-dir "storage", :instant-src (tu/->settable-clock clock),
                     :compactor-threads 1, :gc? false, :default-tz (ZoneId/of "UTC")})))

(defn- flush! [node]
  (let [^Database database (db/primary-db node)
        previous (long (or (.getBlockIdx (.snap (.getTableCatalog database))) -1))
        started (System/nanoTime)]
    (tu/flush-block! node (Duration/ofSeconds 30))
    (loop []
      (when-let [error (.getIngestionError database)] (throw error))
      (when-not (> (long (or (.getBlockIdx (.snap (.getTableCatalog database))) -1)) previous)
        (when (> (- (System/nanoTime) started) 30000000000)
          (throw (ex-info "Timed out awaiting adopted flushed block" {})))
        (Thread/sleep 10)
        (recur)))))

(defn- layout! [^Path directory fixture prefix layout]
  (Files/createDirectory directory (make-array FileAttribute 0))
  (let [clock (atom (instant (:system_time (first (:transactions fixture)))))
        {:keys [transactions result]}
        (util/with-open [node (open-node directory clock)]
          (let [transactions (mapv #(submit! node clock %) (take (:transactions prefix) (:transactions fixture)))]
            (when-not (= layout "live") (flush! node))
            (when (contains? #{"compacted" "restarted"} layout) (compactor/compact-all! node (Duration/ofSeconds 60)))
            {:transactions transactions, :result (when-not (= layout "restarted") (observations node fixture prefix layout))}))
        result (if (= layout "restarted")
                 (util/with-open [node (open-node directory clock)]
                   (log/sync-node node (Duration/ofSeconds 30))
                   (observations node fixture prefix layout))
                 result)]
    (assoc result :transactions transactions :artifacts (files directory) :prefix (:id prefix) :layout layout :status "observed")))

(defn validate-fixture [fixture]
  (let [valid-id? #(and (string? %) (boolean (re-matches #"[A-Za-z0-9_-]+" %)))
        prefixes (:prefixes fixture), queries (:queries fixture), transactions (:transactions fixture)
        prefix-ids (mapv :id prefixes), query-ids (mapv :id queries)]
    (when-not (and (= 1 (:schema_version fixture)) (= "historical_prices" (:kind fixture)) (= "UTC" (:timezone fixture))
                   (seq transactions) (seq prefixes) (seq queries)
                   (every? valid-id? (concat prefix-ids query-ids))
                   (= (count prefix-ids) (count (distinct prefix-ids))) (= (count query-ids) (count (distinct query-ids)))
                   (every? #(and (integer? (:transactions %)) (<= 1 (long (:transactions %)) (count transactions))) prefixes)
                   (every? #(some #{(:prefix %)} prefix-ids) queries)
                   (every? (fn [prefix] (some #(= prefix (:prefix %)) queries)) prefix-ids))
      (throw (ex-info "Invalid historical-price fixture identities" {})))
    (doseq [[prior next] (partition 2 1 transactions)]
      (when-not (.isBefore (instant (:system_time prior)) (instant (:system_time next)))
        (throw (ex-info "History transaction times must increase" {}))))
    (doseq [transaction transactions]
      (instant (:system_time transaction))
      (case (:operation transaction)
        "put" (let [from (instant (:valid_from transaction))]
                (when-not (and (seq (:prices transaction))
                               (every? #(contains? #{"A" "B"} (name %)) (keys (:prices transaction)))
                               (every? integer? (vals (:prices transaction)))
                               (or (nil? (:valid_to transaction)) (.isBefore from (instant (:valid_to transaction)))))
                  (throw (ex-info "Invalid history put" {}))))
        "delete" (do (when-not (= "A" (:id transaction)) (throw (ex-info "History delete must target A" {}))) (instant (:valid_from transaction)))
        "erase" (when-not (= "A" (:id transaction)) (throw (ex-info "History erase must target A" {})))
        (throw (ex-info "Unsupported history operation" {}))))
    (doseq [query queries] (instant (:valid_time query)) (instant (:system_time query)))
    fixture))

(defn -main [fixture-path output-path]
  (let [input (Files/readAllBytes (.toPath (io/file fixture-path)))
        fixture (validate-fixture (json/read-value input json/keyword-keys-object-mapper))
        artifacts (runtime-artifacts)
        ^Path output (.toPath (io/file output-path))]
    (Files/createDirectory output (make-array FileAttribute 0))
    (write-new! (.resolve output "runtime-artifacts.json") artifacts)
    (let [receipts (vec (for [prefix (:prefixes fixture), layout layouts]
                          (let [name (str (:id prefix) "-" layout)
                                result (try (layout! (.resolve output name) fixture prefix layout)
                                            (catch Throwable error
                                              {:prefix (:id prefix), :layout layout, :status "failed", :error (str error),
                                               :suppressed (mapv str (.getSuppressed error)), :observations []}))]
                            (write-new! (.resolve output (str name ".json")) result)
                            result)))
          complete? (every? #(= "observed" (:status %)) receipts)
          execution {:schema_version 1, :engine "jvm", :fixture_sha256 (sha256 input),
                     :binary_sha256 (sha256 (json/write-value-as-bytes artifacts)), :artifacts artifacts,
                     :execution_complete complete?, :complete_parity false,
                     :read_measurement "Successful query-local BufferPool byte-array lengths and record-batch metadata/body spans; footer-only requests excluded; includes warm-cache hits; not physical I/O or native ObjectStore payload bytes.",
                     :layouts receipts}]
      (when-not (= artifacts (runtime-artifacts))
        (throw (ex-info "JVM runtime artifacts changed during history execution" {})))
      (write-new! (.resolve output "execution.json") execution)
      (when-not complete? (throw (ex-info "Historical fixture execution failed; inspect retained layout receipts" {}))))))
