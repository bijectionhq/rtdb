(ns rtdb.conformance.mixed-iid-reference
  (:require [clojure.test :as t]
            [dev.clojurephant.jovial.engine.clojure-test :as jovial]
            [jsonista.core :as json]
            [rtdb.conformance.history :as history]
            [xtdb.test-util :as tu])
  (:import (java.io PrintWriter Writer)
           (java.nio.file Files LinkOption OpenOption Path StandardOpenOption)
           (java.security MessageDigest)
           (java.time Instant OffsetDateTime ZonedDateTime)
           (java.util Collections Date HexFormat List)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector.types.pojo ArrowType$FixedSizeBinary ArrowType$Int ArrowType$RunEndEncoded Field)
           (org.junit.platform.engine DiscoverySelector UniqueId)
           (org.junit.platform.engine.discovery DiscoverySelectors)
           (org.junit.platform.launcher Launcher TestExecutionListener TestIdentifier TestPlan)
           (org.junit.platform.launcher.core LauncherDiscoveryRequestBuilder LauncherFactory)
           (org.junit.platform.launcher.listeners SummaryGeneratingListener TestExecutionSummary)
           (xtdb.api.storage Storage)
           (xtdb.arrow Relation Relation$Loader RunEndEncodedVector)
           (xtdb.table TableSlug)))

(set! *warn-on-reflection* true)

(def ^:private test-namespace 'xtdb.mixed-iid-encoding-test)
(def ^:private test-name 'a-scan-over-plain-and-run-end-encoded-iid-pages-answers-as-over-plain-ones-6258)
(def ^:private test-file "xtdb/mixed_iid_encoding_test.clj")
(def ^:private mixed-directory "target/mixed-iid-encoding/mixed")
(def ^:private engine-id "dev.clojurephant.jovial.clojure-test")
(def ^:private query-ids {":sorted-iids" "sorted-iids", ":history" "history",
                         ":iid-join" "iid-join", ":iid-intersect" "iid-intersect"})

(def ^:private assertion-ids
  {'[(< runs rows) "the rewritten pages carry multi-row runs"] [165 "rewritten-runs"]
   '[(some (set (keys rewritten)) live) "a run-encoded file is still live"] [174 "run-encoded-live"]
   '[(some (complement (set (keys rewritten))) live) "a plain compacted file is live alongside it"] [175 "plain-live"]
   '[(seq expected) nil] [183 "pgwire-nonempty"]
   '[(= expected (pgwire-iids mixed)) "pgwire"] [184 "pgwire"]
   '[(= (patch! control) (patch! mixed)) "PATCH"] [186 "patch"]
   '[(= (ArrowType$FixedSizeBinary. 16) (.getType declared)) nil] [190 "flight-declared"]
   '[(= #{declared} batch-fields (:batch-fields actual)) "every batch is loaded into the one declared field"] [191 "flight-batch-fields"]
   '[(< 1 (:batches actual)) "the stream spans several pages"] [192 "flight-multiple-batches"]
   '[(= (:iids expected) (:iids actual)) "every batch's buffers read back as the iids"] [193 "flight-iids"]})

(defn- identify [{:keys [expected message]}]
  (if (= '(= (get expected k) (get actual k)) expected)
    (when-let [id (query-ids message)] [180 id])
    (assertion-ids [expected message])))

(defn- i64 [value]
  (when-not (integer? value)
    (throw (ex-info "Expected an integer comparison operand" {:value value})))
  {:kind "i64", :value (long value)})

(defn- iid [value]
  (when-not (and (vector? value) (= 16 (count value)) (every? #(instance? Byte %) value))
    (throw (ex-info "Expected a vector of sixteen signed bytes" {:value value})))
  {:kind "fixed-size-binary", :signed_bytes (mapv int value)})

(defn- timestamp [^Instant value zone]
  {:kind "timestamp", :epoch_second (.getEpochSecond value), :nanosecond (.getNano value), :zone zone})

(defn- cell [value]
  (cond
    (nil? value) {:kind "null"}
    (integer? value) (i64 value)
    (instance? ZonedDateTime value) (timestamp (.toInstant ^ZonedDateTime value) (str (.getZone ^ZonedDateTime value)))
    (instance? OffsetDateTime value) (timestamp (.toInstant ^OffsetDateTime value) (str (.getOffset ^OffsetDateTime value)))
    (instance? Instant value) (timestamp value nil)
    (instance? Date value) (timestamp (.toInstant ^Date value) nil)
    :else (throw (ex-info "Unexpected mixed IID row cell type" {:type (str (class value))}))))

(defn- rows [value]
  (mapv (fn [row]
          (when-not (map? row)
            (throw (ex-info "Expected a mixed IID result row" {:value row})))
          (into (sorted-map)
                (map (fn [[column value]]
                       (when-not (keyword? column)
                         (throw (ex-info "Expected a keyword result column" {:column column})))
                       [(subs (str column) 1) (if (= column :xt/iid) (iid value) (cell value))]))
                row))
        value))

(defn- fixed-binary [value]
  (when-not (instance? ArrowType$FixedSizeBinary value)
    (throw (ex-info "Expected a fixed-size binary Arrow type" {:type (str value)})))
  {:kind "fixed-size-binary", :byte_width (.getByteWidth ^ArrowType$FixedSizeBinary value)})

(defn- flight-field [^Field field]
  (when (or (seq (.getChildren field)) (.getDictionary field))
    (throw (ex-info "Unexpected children or dictionary in Flight IID field" {:field (str field)})))
  {:name (.getName field), :type (fixed-binary (.getType field)), :nullable (.isNullable field),
   :metadata (into (sorted-map) (.getMetadata field))})

(defn- capture-operands [id actual]
  (let [[left right] (rest actual)]
    (case id
      ("rewritten-runs" "flight-multiple-batches") [(i64 left) (i64 right)]
      "run-encoded-live" [(vec (sort left)) (vec (sort right))]
      "plain-live" [(vec (sort right))]
      "pgwire-nonempty" [(rows left)]
      "flight-declared" (mapv fixed-binary (rest actual))
      "flight-batch-fields" (mapv #(mapv flight-field %) (rest actual))
      "flight-iids" (mapv #(mapv iid %) (rest actual))
      ("sorted-iids" "history" "iid-join" "iid-intersect" "pgwire" "patch") [(rows left) (rows right)])))

(defn- encoding [^Field field]
  (let [arrow-type (.getType field)]
    (cond
      (= (ArrowType$FixedSizeBinary. 16) arrow-type) "plain"
      (instance? ArrowType$RunEndEncoded arrow-type)
      (let [[^Field ends ^Field values :as children] (.getChildren field)]
        (when-not (and (= 2 (count children))
                       (= (ArrowType$Int. 32 true) (.getType ends))
                       (= (ArrowType$FixedSizeBinary. 16) (.getType values)))
          (throw (ex-info "Unexpected run-end IID children" {:field (str field)})))
        "run-end-i32")
      :else (throw (ex-info "Unexpected stored IID field" {:field (str field)})))))

(defn- inspect-iid-file [^Path workspace ^Path file]
  (let [^Path file (.normalize (.toAbsolutePath file))]
    (when-not (and (.startsWith file workspace) (= file (.toRealPath file (make-array LinkOption 0))))
      (throw (ex-info "Mixed IID data is outside the case workspace or redirected" {:file (str file)})))
    (let [^bytes bytes (Files/readAllBytes file)]
      (with-open [^BufferAllocator al (RootAllocator.)
                  ^Relation$Loader loader (Relation/loader al bytes)]
        (let [encoding (encoding (.findField (.getSchema loader) "_iid"))
              pages (mapv (fn [page-index]
                            (with-open [page (.loadPage loader (int page-index) al)]
                              (cond-> {:rows (.getRowCount page)}
                                (= "run-end-i32" encoding)
                                (assoc :runs (.getRunCount ^RunEndEncodedVector (.vectorFor page "_iid"))))))
                          (range (.getPageCount loader)))]
          {:path (str (.relativize workspace file)), :bytes (alength bytes),
           :sha256 (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)),
           :encoding encoding, :pages pages})))))

(defn- read-live-files [^Path workspace live-keys]
  (let [^Path storage-root (Storage/storageRoot Storage/VERSION 0)
        ^Path table-data-directory (.dataFileDir (TableSlug/of #xt/table readings))
        ^Path data-directory (-> (.resolve workspace ^String mixed-directory)
                                (.resolve "objects")
                                (.resolve storage-root)
                                (.resolve table-data-directory))]
    (mapv (fn [key]
            (assoc (inspect-iid-file workspace (.resolve data-directory (str key ".arrow"))) :trie_key key))
          live-keys)))

(defn- record! [state operation capture]
  (try
    (capture)
    (catch Exception error
      (swap! state update :capture_errors conj {:operation operation, :message (str error)})
      nil)))

(defn- record-assertion! [state workspace {:keys [type expected actual message] :as report}]
  (if (= :pass type)
    (let [[line id :as location] (identify report)]
      (when-not location
        (throw (ex-info "Unexpected assertion in original mixed IID case" {:form expected, :message message})))
      (let [operands (capture-operands id actual)
            observation {:line line, :id id, :status "passed", :operands operands}]
        (swap! state update :assertions conj observation)
        (when (= "run-encoded-live" id)
          (swap! state assoc :live_files (read-live-files workspace (second operands))))))
    (swap! state update :assertion_failures conj {:status (name type), :form (pr-str expected), :message message})))

(defn- run-case [^Path workspace]
  (require test-namespace)
  (let [test-var (ns-resolve test-namespace test-name)
        rewrite-var (ns-resolve test-namespace 'rewrite-iid-as-runs!)
        rewrite @rewrite-var
        report jovial/jovial-report
        state (atom {:assertions [], :assertion_failures [], :capture_errors [], :rewritten_files [], :live_files []})
        ^UniqueId unique-id (-> (UniqueId/forEngine engine-id)
                               (.append "namespace" (str test-namespace))
                               (.append "name" (str test-name)))
        ^DiscoverySelector selector (DiscoverySelectors/selectUniqueId unique-id)
        ^List selectors (Collections/singletonList selector)
        request (-> (LauncherDiscoveryRequestBuilder/request)
                    (.selectors selectors)
                    (.build))
        ^Launcher launcher (LauncherFactory/create)
        ^TestPlan plan (.discover launcher request)
        discovered (into []
                         (comp (mapcat #(.getDescendants plan ^TestIdentifier %))
                               (filter #(.isTest ^TestIdentifier %))
                               (map #(.getUniqueId ^TestIdentifier %)))
                         (.getRoots plan))
        listener (SummaryGeneratingListener.)
        ^"[Lorg.junit.platform.launcher.TestExecutionListener;" listeners (into-array TestExecutionListener [listener])]
    (when-not (and (= test-file (:file (meta test-var))) (= 148 (:line (meta test-var)))
                   (= [(str unique-id)] discovered))
      (throw (ex-info "Jovial discovery did not resolve the original mixed IID case"
                      {:source (select-keys (meta test-var) [:file :line]), :discovered discovered})))
    (with-redefs-fn
      {#'jovial/jovial-report
       (fn [event]
         (report event)
         (when (and (= test-var (first t/*testing-vars*)) (#{:pass :fail :error} (:type event)))
           (record! state "assertion" #(record-assertion! state workspace event))))
       rewrite-var
       (fn [^BufferAllocator allocator ^Path file]
         (let [before (record! state "plain file" #(inspect-iid-file workspace file))
               result (rewrite allocator file)]
           (record! state "rewritten file"
                    #(swap! state update :rewritten_files conj
                            {:before before, :after (inspect-iid-file workspace file), :result result}))
           result))}
      #(.execute launcher plan listeners))
    (let [^TestExecutionSummary summary (.getSummary listener)]
      (when (pos? (.getTotalFailureCount summary))
        (.printFailuresTo summary (PrintWriter. ^Writer *err* true)))
      (assoc @state
             :junit {:unique_id (str unique-id), :found (.getTestsFoundCount summary), :started (.getTestsStartedCount summary),
                     :succeeded (.getTestsSucceededCount summary), :failed (.getTestsFailedCount summary),
                     :aborted (.getTestsAbortedCount summary), :skipped (.getTestsSkippedCount summary),
                     :failures (.getTotalFailureCount summary)}))))

(defn- write-new! [^Path path value]
  (let [^bytes bytes (json/write-value-as-bytes value)
        ^"[Ljava.nio.file.OpenOption;" options (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE])]
    (Files/write path bytes options)))

(defn -main [output-path]
  (let [output (Path/of ^String output-path (make-array String 0))
        workspace (.normalize (.toAbsolutePath (Path/of "" (make-array String 0))))
        artifacts (history/runtime-artifacts)
        receipt (run-case workspace)]
    (write-new! (.resolve output "runtime-artifacts.json") artifacts)
    (write-new! (.resolve output "execution.json")
                (assoc receipt :schema_version 1, :engine "jvm", :source (str "src/test/clojure/" test-file),
                       :runtime_unchanged (= artifacts (history/runtime-artifacts))))))
