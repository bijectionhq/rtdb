(ns xtdb.rust.property-corpus
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.results :as results]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.rust :as rust]
            [xtdb.test-util :as tu])
  (:import [java.io InputStream Writer]
           [java.net JarURLConnection]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files LinkOption Path]
           [java.security MessageDigest]
           [java.time Duration InstantSource]
           [java.util HexFormat]
           [xtdb.rust NativeDatabase]))

(defn- require! [condition message data]
  (when-not condition (throw (ex-info message data))))

(defn- file-hash [file]
  (let [sha (MessageDigest/getInstance "SHA-256")
        buffer (byte-array 8192)]
    (with-open [^InputStream in (io/input-stream file)]
      (loop []
        (let [n (.read in buffer)]
          (when (pos? n)
            (.update sha buffer 0 n)
            (recur)))))
    (.formatHex (HexFormat/of) (.digest sha))))

(defn- stable-value [value]
  (cond
    (nil? value) nil
    (map? value) [:map (sort-by pr-str (map (fn [[k v]] [(stable-value k) (stable-value v)]) value))]
    (set? value) [:set (sort-by pr-str (map stable-value value))]
    (sequential? value) [(if (vector? value) :vector :sequence) (mapv stable-value value)]
    (bytes? value) [:bytes (vec value)]
    (instance? ByteBuffer value) (let [buffer (.duplicate ^ByteBuffer value)
                                       data (byte-array (.remaining buffer))]
                                   (.get buffer data)
                                   [:byte-buffer (vec data)])
    (string? value) [:string (mapv int value)]
    :else [(.getName (class value)) (str value)]))

(defn- input-hash [args]
  (let [^String value (pr-str (stable-value args))]
    (.formatHex (HexFormat/of)
                (.digest (MessageDigest/getInstance "SHA-256") (.getBytes value StandardCharsets/UTF_8)))))

(defn- preview [value]
  (binding [*print-length* 12, *print-level* 5]
    (let [s (pr-str value)]
      (subs s 0 (min 1200 (count s))))))

(def ^:dynamic *observe!* nil)
(def ^:dynamic *scalar-node* nil)
(def ^:dynamic *local-storage* nil)

(def ^:private node-yaml "server: null\nflightSql: null\ncompactor: {threads: 0}\n")
(def ^:private native-memory (* 512 1024 1024))

(defn- open-node ^NativeDatabase []
  (NativeDatabase/open tu/*allocator* (str node-yaml "log: !InMemory {}\n") native-memory))

(defn- document-node ^NativeDatabase [{:keys [log indexer storage compactor] :as options}]
  (let [[kind log-opts] log]
    (require! (= {:threads 0} compactor) "Unmapped pinned compactor options" {})
    (case kind
      :in-memory
      (let [{:keys [instant-src]} log-opts]
        (require! (and (= #{:log :compactor} (set (keys options)))
                       (= #{:instant-src} (set (keys log-opts)))
                       (instance? InstantSource instant-src))
                  "Unmapped pinned in-memory node options" {})
        (NativeDatabase/open tu/*allocator* (str node-yaml "log: !InMemory {}\n") native-memory
                             ^InstantSource instant-src))
      :local
      (let [[storage-kind storage-opts] storage
            log-path (:path log-opts)
            storage-path (:path storage-opts)
            rows (:rows-per-block indexer)]
        (require! (and (= #{:log :compactor :storage :indexer} (set (keys options)))
                       (= :local storage-kind)
                       (= #{:path} (set (keys log-opts)) (set (keys storage-opts)))
                       (= #{:rows-per-block} (set (keys indexer)))
                       (instance? Path log-path) (instance? Path storage-path)
                       (pos-int? rows))
                  "Unmapped pinned local node options" {})
        (let [node (NativeDatabase/open
                    tu/*allocator*
                    (str node-yaml "log: !Local {path: " (json/write-value-as-string (str log-path)) "}\n"
                         "storage: !Local {path: " (json/write-value-as-string (str storage-path)) "}\n"
                         "indexer: {rowsPerBlock: " rows "}\n")
                    native-memory)]
          (vreset! *local-storage* storage-path)
          node))
      (throw (ex-info "Unmapped pinned document node options" {:kind kind})))))

(defn- native-node ^NativeDatabase [node]
  (require! (instance? NativeDatabase node) "Property observer received a non-native node" {})
  node)

(defn- observed-query [query-fn]
  (fn [node statement & [options]]
    (let [rows (query-fn (native-node node) statement (or options {}))]
      (*observe!* {:kind :sql_query, :statement statement, :row_count (count rows)
                   :invariant (t/testing-contexts-str), :sample (preview (take 2 rows))})
      rows)))

(defn- flush!
  ([node] (flush! node (Duration/ofSeconds 5)))
  ([node timeout]
   (.flushAll (native-node node) timeout)
   (*observe!* {:kind :flush})))

(defn- sync! [node timeout]
  (.syncAll (native-node node) timeout)
  (*observe!* {:kind :sync}))

(defn- local-files [node path]
  (native-node node)
  (require! (and (= "tables/public$docs/meta/" path) (some? @*local-storage*))
            "Unmapped pinned restart object listing" {:path path})
  (let [^Path root @*local-storage*
        prefix (.resolve root (str "v06/" path))
        keys (if (Files/isDirectory prefix (make-array LinkOption 0))
               (with-open [objects (Files/list prefix)]
                 (->> (iterator-seq (.iterator objects))
                      (filter #(Files/isRegularFile ^Path % (make-array LinkOption 0)))
                      (map #(str (.getFileName ^Path %)))
                      sort vec))
               [])]
    (*observe!* {:kind :local_store_list, :keys keys})
    keys))

(defn- logical-query
  ([plan] (logical-query plan {}))
  ([plan options]
   (let [rows (rust/query-ra (native-node *scalar-node*) plan options)]
     (require! (= 1 (count rows)) "Scalar property projection must yield one row" {:rows (count rows)})
     (*observe!* {:kind :logical_query, :plan (preview plan), :sample (preview (first rows))})
     rows)))

(defn- project1 [form doc]
  (let [args (volatile! {})]
    (letfn [(bind! [value]
              (let [key (keyword (str "arg_" (count @args)))]
                (vswap! args assoc key value)
                (symbol (str "?" (name key)))))
            (parameterize [expr]
              (cond
                (seq? expr) (apply list (first expr) (mapv parameterize (rest expr)))
                (symbol? expr) (let [column (keyword (name expr))]
                                 (require! (contains? doc column) "Unbound scalar property column" {:column column})
                                 (bind! (get doc column)))
                (bytes? expr) (bind! expr)
                :else expr))]
      (let [expr (parameterize form)]
        (:res (first (logical-query [:project {:projections [{'res expr}]}
                                     [:table {:rows [{}]}]]
                                    {:args @args})))))))

(defn capture-property
  "Gets the generated argument tuple and predicate from the selected, loaded upstream Var."
  [v]
  (let [captured (volatile! nil)
        capture! (fn [property]
                   (require! (and (::generators property) (ifn? (::predicate property)) (nil? @captured))
                             "Expected exactly one upstream for-all property" {:var (str v)})
                   (vreset! captured property))]
    (with-redefs [prop/for-all* (fn [generators predicate] {::generators generators, ::predicate predicate})
                  tc/quick-check (fn [_ property & _] (capture! property))
                  tu/run-property-test (fn ([_ property] (capture! property))
                                         ([property] (capture! property)))]
      (if (::tct/defspec (meta v))
        (@v)
        ((:test (meta v)))))
    (require! @captured "Selected upstream Var did not construct a property" {:var (str v)})
    {:generators (::generators @captured), :predicate (::predicate @captured)}))

(defn- document-observers [f]
  (let [method-too-large? (when-let [ns (find-ns 'xtdb.block-boundary-test)]
                            (ns-resolve ns 'ee-method-too-large?))
        primary-db db/primary-db]
    (with-redefs [xtn/start-node document-node
                  xt/q (observed-query xt/q)
                  tu/flush-block! flush!
                  tu/read-files-from-bp-path local-files
                  log/sync-node sync!]
      (tu/with-allocator
        (fn []
          (if method-too-large?
            (let [jvm-method-too-large? @method-too-large?]
              (with-redefs-fn {#'db/primary-db (fn [node]
                                               (if (instance? NativeDatabase node) node (primary-db node)))
                               method-too-large? (fn [database]
                                                   (if (instance? NativeDatabase database)
                                                     false
                                                     (jvm-method-too-large? database)))}
                f))
            (f)))))))

(defn with-native-observers [kind f]
  (case kind
    "test" (document-observers f)
    "defspec" (tu/with-allocator
                (fn []
                  (with-open [node (open-node)]
                    (binding [*scalar-node* node]
                      (with-redefs-fn {(ns-resolve 'xtdb.expression-test 'project1) project1
                                       #'tu/query-ra logical-query}
                        f)))))))

(defn observe [predicate args]
  (let [observations (volatile! [])
        verdict (binding [*observe!* #(vswap! observations conj %)
                          *local-storage* (volatile! nil)]
                  (try
                    (if (apply predicate args) {:outcome :passed} {:outcome :invariant_failed})
                    (catch ThreadDeath e (throw e))
                    (catch Throwable e
                      {:outcome :error, :class (.getName (class e)), :message (preview (ex-message e))})))
        read? (some #(#{:sql_query :logical_query} (:kind %)) @observations)]
    (assoc (if (and (not= :error (:outcome verdict)) (not read?))
             {:outcome :unobserved}
             verdict)
           :observations @observations)))

(defrecord Trial [evidence]
  results/Result
  (pass? [_] (= :passed (:outcome evidence)))
  (result-data [_] evidence))

(defn- quick-check [{:keys [generators]} {:keys [seed trials max_size]} predicate reporter]
  (tc/quick-check trials (prop/for-all* generators predicate)
                  :seed seed :max-size max_size :reporter-fn reporter))

(defn- input-id [args]
  {:input_sha256 (input-hash args), :input_preview (preview args)})

(defn- failing-inputs [{:keys [fail num-tests failing-size shrunk]}]
  {:trial num-tests
   :failing_size failing-size
   :first (input-id fail)
   :smallest (input-id (:smallest shrunk))
   :shrink_depth (:depth shrunk)
   :shrink_visits (:total-nodes-visited shrunk)})

(defn sweep [property settings emit!]
  (let [verdicts (volatile! [])
        result (quick-check property settings
                            (fn [& args]
                              (let [trial (->Trial (observe (:predicate property) args))]
                                (vswap! verdicts conj (results/pass? trial))
                                trial))
                            (fn [{:keys [type num-tests args fail result-data]}]
                              (when (#{:trial :failure} type)
                                (emit! {:event :trial, :trial num-tests, :evidence result-data
                                        :input (input-id (or fail args))}))))
        completed (:num-tests result)]
    (cond-> {:planned_trials (:trials settings)
             :passed_trials (if (:pass? result) completed (dec completed))
             :failed_trials (if (:pass? result) 0 1)
             :unobserved_trials (- (:trials settings) completed)}
      (not (:pass? result))
      (assoc :failure (merge (failing-inputs result)
                             {:first_evidence (:result-data result)
                              :smallest_evidence (get-in result [:shrunk :result-data])
                              :verdicts @verdicts})))))

(defn- failure-signature [{:keys [outcome class message observations]}]
  [outcome class message (select-keys (last observations) [:kind :invariant :statement :plan])])

(defn replay [property settings {:keys [verdicts] :as failure}]
  (require! (and (vector? verdicts) (seq verdicts) (every? boolean? verdicts))
            "Invalid property replay verdicts" {})
  (let [position (volatile! -1)
        synthesized (quick-check property settings
                                 (fn [& _]
                                   (let [i (vswap! position inc)]
                                     (boolean (get verdicts i))))
                                 (constantly nil))
        regenerated (when-not (:pass? synthesized) (failing-inputs synthesized))
        identity-fields [:trial :failing_size :shrink_depth :shrink_visits]]
    (require! (and regenerated
                   (= (inc @position) (count verdicts))
                   (= (select-keys regenerated identity-fields) (select-keys failure identity-fields))
                   (= (mapv #(get-in regenerated [% :input_sha256]) [:first :smallest])
                      (mapv #(get-in failure [% :input_sha256]) [:first :smallest])))
              "Property replay did not regenerate the recorded counterexamples" {})
    (let [first-evidence (observe (:predicate property) (:fail synthesized))
          smallest-evidence (observe (:predicate property) (get-in synthesized [:shrunk :smallest]))]
      (assoc regenerated
             :first_evidence first-evidence
             :smallest_evidence smallest-evidence
             :reproduced (= (mapv failure-signature [first-evidence smallest-evidence])
                            (mapv (comp failure-signature failure) [:first_evidence :smallest_evidence]))))))

(defn- json-file [file]
  (json/read-value (io/file file) json/keyword-keys-object-mapper))

(defn- check-source! [source-root path sha]
  (let [relative (str/replace-first path #"^src/(test|testFixtures)/clojure/" "")
        file (.getCanonicalFile (io/file source-root path))
        resource (io/resource relative)]
    (require! (and resource (= "file" (.getProtocol resource))
                   (= file (.getCanonicalFile (io/file (.toURI resource))))
                   (= sha (file-hash file)))
              "Property source differs from the pinned classpath input" {:source path})))

(defn- runtime [plan manifest]
  (let [library (io/file (System/getProperty "xtdb.native.library"))
        sha (file-hash library)
        ^JarURLConnection test-check (.openConnection (io/resource "clojure/test/check.cljc"))
        jar (io/file (.toURI (.getJarFileURL test-check)))
        version (:test_check_version manifest)]
    (require! (= sha (:native_library_sha256 plan)) "Native property library changed" {})
    (require! (= (.getName jar) (str "test.check-" version ".jar")) "Unexpected test.check runtime" {})
    {:native_library_sha256 sha
     :test_check_version version
     :test_check_sha256 (file-hash jar)
     :java_version (System/getProperty "java.runtime.version")
     :java_vendor (System/getProperty "java.vendor")
     :clojure_version (clojure-version)}))

(defn- load-case [{:keys [namespace kind name]}]
  (let [ns-name (symbol namespace)
        v (ns-resolve ns-name (symbol name))
        metadata (meta v)]
    (require! (and v (:test metadata)
                   (case kind
                     "test" (:property metadata)
                     "defspec" (::tct/defspec metadata)))
              "Selected Var is missing its pinned property metadata" {:namespace namespace :name name})
    (capture-property v)))

(defn- sweep-case [output identity runtime case settings emit!]
  (let [property (load-case case)
        result (with-native-observers (:kind case) #(sweep property settings emit!))]
    (when-let [failure (:failure result)]
      (spit (io/file output (str (:name case) ".replay.edn"))
            (pr-str {:schema_version 1, :case case, :identity identity, :runtime runtime
                     :settings settings, :failure failure})))
    (emit! (assoc (dissoc result :failure) :event :case_complete
                  :replay (when (:failure result) (str (:name case) ".replay.edn"))))))

(defn- replay-case [token identity runtime case emit!]
  (require! (and (= 1 (:schema_version token))
                 (= case (:case token))
                 (= identity (:identity token))
                 (= (:test_check_sha256 runtime) (get-in token [:runtime :test_check_sha256])))
            "Property replay belongs to another corpus or generator runtime" {})
  (let [{:keys [seed trials max_size] :as settings} (:settings token)]
    (require! (and (integer? seed) (<= Long/MIN_VALUE seed Long/MAX_VALUE)
                   (integer? trials) (<= 1 trials 1000)
                   (integer? max_size) (<= 1 max_size 200))
              "Invalid property replay generation settings" {})
    (let [property (load-case case)
          result (with-native-observers (:kind case) #(replay property settings (:failure token)))]
      (emit! (assoc result :event :replay_complete :original_runtime (:runtime token))))))

(defn- selected-cases [manifest]
  (for [{:keys [id namespace kind source cases]} (:families manifest)
        {:keys [unported] :as case} cases
        :when (nil? unported)]
    (assoc case :family id :namespace namespace :kind kind :source source)))

(defn- load-pinned! [loaded source-root path]
  (when-not (contains? @loaded path)
    (load-file (.getPath (io/file source-root path)))
    (vswap! loaded conj path)))

(defn -main [plan-file manifest-file source-root output]
  (let [plan (json-file plan-file)
        manifest (json-file manifest-file)
        token (when (= "replay" (:mode plan))
                (let [path (io/file output "replay.edn")]
                  (require! (= (:replay_sha256 plan) (file-hash path)) "Property replay input changed" {})
                  (edn/read-string (slurp path))))
        selected (if token [(:case token)] (mapv :case (:selected plan)))
        identity {:upstream_commit (:upstream_commit manifest), :manifest_sha256 (file-hash manifest-file)}
        runtime (runtime plan manifest)
        loaded (volatile! #{})]
    (require! (and (= 1 (:schema_version plan) (:schema_version manifest))
                   (= identity (select-keys plan [:upstream_commit :manifest_sha256]))
                   (= (:upstream_commit manifest) (.trim (slurp (io/file source-root ".commit"))))
                   (seq selected)
                   (every? (set (selected-cases manifest)) selected))
              "Property selection does not match the pinned corpus manifest" {})
    (doseq [[path sha] (:sources manifest)]
      (check-source! (io/file source-root) (subs (str path) 1) sha))
    (load-pinned! loaded source-root "src/testFixtures/clojure/xtdb/test_util.clj")
    (load-pinned! loaded source-root "src/testFixtures/clojure/xtdb/test_generators.clj")
    (with-open [^Writer writer (io/writer (io/file output "properties.jsonl"))]
      (let [emit! (fn [entry]
                    (.write writer (json/write-value-as-string entry))
                    (.write writer "\n")
                    (.flush writer))]
        (emit! {:event :start, :identity identity, :runtime runtime})
        (doseq [[i case] (map-indexed vector selected)]
          (let [emit-case! #(emit! (assoc % :case case))]
            (try
              (when (= "defspec" (:kind case))
                (load-pinned! loaded source-root "src/test/clojure/xtdb/expression_test.clj"))
              (load-pinned! loaded source-root (:source case))
              (if token
                (replay-case token identity runtime case emit-case!)
                (sweep-case output identity runtime case (get-in plan [:selected i :settings]) emit-case!))
              (catch Exception e
                (emit-case! {:event :case_error
                             :class (.getName (class e)), :message (preview (ex-message e))})))))
        (emit! {:event :complete})))
    (shutdown-agents)))
