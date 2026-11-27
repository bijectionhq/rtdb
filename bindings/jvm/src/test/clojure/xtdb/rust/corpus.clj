(ns xtdb.rust.corpus
  "Runs upstream XTDB's own test Vars against the native engine and records one outcome, and every failure, per Var.
   The test sources, test resources and xtdb.test-util come from the tree rust/conformance/upstream_tests.sh extracts
   at the pinned upstream commit, which the runner puts ahead of this repository's edited copies on the classpath."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.flight-sql :as fsql]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.operator.list :as list-operator]
            [xtdb.rust :as rust]
            [xtdb.serde.types :as st]
            [xtdb.test-util :as tu])
  (:import [clojure.lang Compiler]
           [java.io File StringReader]
           [java.nio.file Path]
           [java.time Duration InstantSource]
           [java.util.concurrent TimeUnit]
           [java.util.regex Pattern]
           [org.apache.arrow.memory RootAllocator]
           [xtdb.api PasswordHash PasswordHash$Argon2id PasswordHash$BCrypt]
           [xtdb.rust NativeDatabase NativeException]))


(defn- file->ns [^File root ^File file]
  (-> (.relativize (.toPath root) (.toPath file))
      str
      (str/replace #"\.clj$" "")
      (str/replace "/" ".")
      (str/replace "_" "-")
      symbol))

(defn test-namespaces [^File root]
  (->> (file-seq root)
       (filter #(str/ends-with? (.getName ^File %) "_test.clj"))
       (map #(file->ns root %))
       sort))

(defn source-file
  "The file `ns-sym` loads from, when the classpath resolves its source to a plain file."
  ^File [ns-sym]
  (let [path (str (-> (name ns-sym) (str/replace "-" "_") (str/replace "." "/")) ".clj")]
    (when-let [url (io/resource path)]
      (when (= "file" (.getProtocol url))
        (.getCanonicalFile (io/file (.toURI url)))))))

(defn loads-from?
  "Whether `ns-sym`'s source resolves to a file under `root`."
  [^File root ns-sym]
  (boolean (when-let [file (source-file ns-sym)]
             (.startsWith ^Path (.toPath file) ^Path (.toPath (.getCanonicalFile root))))))

(defn- truncate [s n]
  (let [s (str s)] (if (> (count s) n) (str (subs s 0 n) "…") s)))

(defn- describe [{:keys [type expected actual message]}]
  (truncate (str (name type)
                 (when (seq t/*testing-contexts*) (str " [" (t/testing-contexts-str) "]"))
                 (when message (str ": " message))
                 (if (instance? Throwable actual)
                   (let [^Throwable e actual]
                     (str " " (.getName (class e)) ": " (ex-message e)))
                   (str "\n  expected: " (pr-str expected) "\n  actual: " (pr-str actual))))
            2000))

(defn- classify [{:keys [pass fail error]} timed-out? {:keys [native wire]}]
  (cond
    timed-out? "timeout"
    (pos? error) "error"
    (pos? fail) "fail"
    (and (pos? pass) native) "pass"
    (and (pos? pass) wire) "pass-wire"
    (pos? pass) "pass-jvm-only"
    :else "no-assertions"))


(def ^:private config-key-aliases
  "Clojure node-option keys whose YAML spelling isn't their camel-case form."
  {:keystore "keyStore", :keystore-password "keyStorePassword"})

(defn- camel-case [k]
  (let [[head & more] (str/split (str/replace (name k) #"\?$" "") #"-")]
    (apply str head (map str/capitalize more))))

(defn- camel [k]
  (if-let [alias (config-key-aliases k)]
    alias
    (camel-case k)))

(defn- enum-constant
  "The Kotlin enum constant a Clojure keyword option names, e.g. `:device-auth` -> `DEVICE_AUTH`."
  [k]
  (str/upper-case (str/replace (name k) "-" "_")))

(defn- tag [k] (str "!" (str/join (map str/capitalize (str/split (name k) #"-")))))

(defn- yaml [v]
  (cond
    (map? v) (str "{" (str/join ", " (for [[k x] v]
                                       (str (json/write-value-as-string (if (keyword? k) (camel k) (str k))) ": "
                                            (yaml (cond-> x (and (= :method k) (keyword? x)) enum-constant)))))
                  "}")
    (and (vector? v) (= 2 (count v)) (keyword? (first v)) (map? (second v))) (str (tag (first v)) " " (yaml (second v)))
    (and (vector? v) (= 1 (count v)) (keyword? (first v))) (str (tag (first v)) " {}")
    (sequential? v) (str "[" (str/join ", " (map yaml v)) "]")
    (keyword? v) (json/write-value-as-string (name v))
    (instance? PasswordHash$Argon2id v) (str "!Argon2id " (json/write-value-as-string (.getEncoded ^PasswordHash v)))
    (instance? PasswordHash$BCrypt v) (str "!BCrypt " (json/write-value-as-string (.getEncoded ^PasswordHash v)))
    (or (instance? Path v) (instance? File v) (instance? Duration v)) (json/write-value-as-string (str v))
    (nil? v) "null"
    :else (json/write-value-as-string v)))

(defonce ^:private native-allocator (RootAllocator.))

(def ^:private open-nodes
  "Native nodes the running Var opened, closed if it times out so its native work stops competing with later Vars."
  (atom #{}))

(defn native-start-node
  "Opens a native node for a reference `xtdb.node/start-node` options map."
  ([] (native-start-node {}))
  ([opts]
   (when-not (map? opts)
     (throw (ex-info "Unsupported native start-node options" {:class (str (class opts))})))
   (let [clock (get-in opts [:log 1 :instant-src])
         opts (cond-> opts clock (update-in [:log 1] dissoc :instant-src))
         opts (cond-> opts
                (contains? (get-in opts [:log 1]) :instant-source-for-non-tx-msgs?)
                (update-in [:log 1] (fn [{use? :instant-source-for-non-tx-msgs? :as log}]
                                      (cond-> (dissoc log :instant-source-for-non-tx-msgs?)
                                        (= :local (get-in opts [:log 0])) (assoc :use-instant-source-for-non-tx (boolean use?))))))
         opts (cond-> opts
                (contains? opts :xtdb.pgwire/server)
                (-> (update :server merge (:xtdb.pgwire/server opts)) (dissoc :xtdb.pgwire/server)))
         opts (merge {:server {} :flight-sql {}} opts)
         opts (reduce (fn [opts listener]
                        (update opts listener #(if (map? %) (merge {:port 0} %) %)))
                      opts [:server :flight-sql])
         config (yaml opts)]
     (try
       (doto (if (instance? InstantSource clock)
               (NativeDatabase/open native-allocator config (* 512 1024 1024) ^InstantSource clock)
               (NativeDatabase/open native-allocator config (* 512 1024 1024)))
         (->> (swap! open-nodes conj)))
       (catch NativeException e
         (throw (case (.code e)
                  "xtdb/out-of-sync-log" (IllegalStateException. (ex-message e) e)
                  "xtdb.error/illegal-arg" (IllegalArgumentException. (ex-message e) e)
                  e)))))))

(defn- import-logical-inputs [query]
  (let [[operator options & operands] query]
    (case operator
      ::tu/pages (case (count operands)
                   0 [:literal-pages {:pages options}]
                   1 [:literal-pages {:pages (first operands) :types (update-vals options st/render-type)}]
                   (throw (ex-info "Invalid fixture page source" {:query query})))
      :list [:list (update options :batch-size #(or % list-operator/*batch-size*))]
      :mega-join [:mega-join options (mapv import-logical-inputs (first operands))]
      (if (qualified-keyword? operator)
        (throw (ex-info "The test plans a logical operator it defines on the JVM" {:operator operator}))
        (into [operator options] (map import-logical-inputs) operands)))))

(defn- native-query-ra [query {:keys [node] :as options}]
  (let [query (import-logical-inputs query)]
    (if node
      (rust/query-ra node query options)
      (with-open [node (native-start-node)]
        (rust/query-ra node query options)))))

(defn- native-with-node [f]
  (with-open [node (native-start-node (or tu/*node-opts* {}))]
    (binding [tu/*node* node]
      (f))))

(def ^:private touched (atom {}))

(defn native-listener-port ^long [^NativeDatabase node listener]
  (let [port (case listener
               :pgwire (.getServerPort node)
               :pgwire-read-only (.getServerReadOnlyPort node)
               :flight-sql (.getFlightSqlPort node)
               :healthz (.getHealthzPort node))]
    (swap! touched assoc :wire true)
    port))

(defn- native-latest-completed-txs [^NativeDatabase node]
  (let [partitions (.latestCompletedTxs node)]
    (swap! touched assoc :native true)
    (update-vals (into {} partitions) vec)))

(def ^:private native-connectables
  {:adbc (fn [node f] (f node))
   :pgwire (fn [node f] (f (xt/client {:port (native-listener-port node :pgwire)})))
   :flight-sql (fn [node f]
                 (with-open [conn (fsql/open-conn {:port (native-listener-port node :flight-sql)})]
                   (f conn)))})

(defn- with-root [v value f]
  (let [original (deref v)]
    (alter-var-root v (constantly value))
    (try (f) (finally (alter-var-root v (constantly original))))))

(defn- observing [k f]
  (fn [& args] (swap! touched assoc k true) (apply f args)))

(defn- with-native-engine
  "Routes the original fixtures and node constructors to the native engine; namespaces must load inside it."
  [f]
  (with-redefs [rust/plan (observing :native @#'rust/plan)
                rust/transact (observing :native @#'rust/transact)
                rust/execute-set! (observing :native @#'rust/execute-set!)
                rust/connect (observing :native @#'rust/connect)
                tu/with-node native-with-node
                xtn/start-node native-start-node
                xt-log/sync-node (let [jvm-sync-node @#'xt-log/sync-node]
                                   (fn [node timeout]
                                     (if (instance? NativeDatabase node)
                                       (.syncAll ^NativeDatabase node timeout)
                                       (jvm-sync-node node timeout))))]
    (with-redefs [tu/latest-completed-txs (let [jvm-latest-completed-txs @#'tu/latest-completed-txs]
                                          (fn [node]
                                            (if (instance? NativeDatabase node)
                                              (native-latest-completed-txs node)
                                              (jvm-latest-completed-txs node))))
                  tu/query-ra (let [run (observing :native native-query-ra)]
                                (fn ([query] (run query {}))
                                    ([query options] (run query options))))
                  tu/flush-block! (fn ([^NativeDatabase node] (.flushAll node (Duration/ofSeconds 5)))
                                      ([^NativeDatabase node timeout] (.flushAll node timeout)))]
      (with-root #'tu/connectables native-connectables f))))

(defn- port-fixture [port-var server-var]
  (fn [f]
    (let [^NativeDatabase node tu/*node*]
      (with-bindings (cond-> {(resolve port-var) (native-listener-port node :pgwire)}
                       server-var (assoc (resolve server-var) node))
        (f)))))

(def ^:private fixture-overrides
  "Original fixtures that read the JVM node's internal system map, by the Var that defines them."
  {'xtdb.pgwire-test/with-server-and-port (port-fixture 'xtdb.pgwire-test/*port* 'xtdb.pgwire-test/*server*)
   'xtdb.pgwire.pg2-test/with-server-and-port (port-fixture 'xtdb.pgwire.pg2-test/*port* 'xtdb.pgwire.pg2-test/*server*)
   'xtdb.pgwire-protocol-test/with-port (port-fixture 'xtdb.pgwire-protocol-test/*port* nil)
   'xtdb.pgwire.authn-test/with-port (port-fixture 'xtdb.pgwire.authn-test/*port* nil)
   'xtdb.sql.logic-test.runner/with-xtdb (fn [_] (throw (ex-info "The SQL logic XTDB engine plans through the JVM database catalog" {})))})

(defn- native-fixtures [fixtures]
  (let [overrides (into {} (keep (fn [[sym fixture]] (when-let [v (resolve sym)] [@v fixture]))) fixture-overrides)]
    (mapv #(get overrides % %) fixtures)))

(defn- direct-redefs []
  (if-let [with-xtdb (resolve 'xtdb.sql.logic-test.runner/with-xtdb)]
    {with-xtdb (fixture-overrides 'xtdb.sql.logic-test.runner/with-xtdb)}
    {}))

(defn- run-case [v]
  (let [fixtures (meta (:ns (meta v)))
        once (t/join-fixtures (native-fixtures (::t/once-fixtures fixtures)))
        each (t/join-fixtures (native-fixtures (::t/each-fixtures fixtures)))]
    (with-redefs-fn (direct-redefs)
      #(once (fn [] (tu/with-allocator (fn [] (each (:test (meta v))))))))))

(def ^:private max-recorded-failures 200)

(defn run-var [v timeout-ms]
  (let [counts (atom {:pass 0 :fail 0 :error 0})
        problems (atom [])
        report (fn [{:keys [type] :as m}]
                 (case type
                   :pass (swap! counts update :pass inc)
                   (:fail :error) (do (swap! counts update type inc)
                                      (swap! problems #(cond-> % (< (count %) max-recorded-failures) (conj (describe m)))))
                   nil))
        started (System/nanoTime)
        _ (reset! touched {})
        _ (reset! open-nodes #{})
        task (future
               (binding [t/report report
                         t/*testing-vars* (list v)]
                 (try
                   (run-case v)
                   (catch Throwable e
                     (report {:type :error :actual e})))))
        timed-out? (= ::timeout (deref task timeout-ms ::timeout))]
    (when timed-out?
      (future-cancel task)
      (let [closing (future (doseq [^NativeDatabase node @open-nodes] (try (.close node) (catch Throwable _))))]
        (deref closing 30000 nil)))
    (let [counts @counts
          result (classify counts timed-out? @touched)
          {:keys [fail error]} counts
          problems @problems]
      (merge {:var (str (symbol v))
              :result result
              :millis (.toMillis TimeUnit/NANOSECONDS (- (System/nanoTime) started))}
             counts
             (when (seq problems)
               (cond-> {:detail (first problems) :failures problems}
                 (or (> (+ fail error) (count problems)) (some #(str/ends-with? % "…") problems))
                 (assoc :failures-truncated true)))))))

(def ^:private listener-call-sites
  {'xtdb.api-flight-sql-test [[2 "(.getFlightSqlPort ^Xtdb *node*)" "*node*" :flight-sql]
                             [2 "(.getFlightSqlPort node)" "node" :flight-sql]]
   'xtdb.flight-sql-test [[1 "(.getFlightSqlPort ^Xtdb tu/*node*)" "tu/*node*" :flight-sql]]
   'xtdb.pgwire-test [[6 "(.getServerPort node)" "node" :pgwire]
                     [2 "(.getServerPort tu/*node*)" "tu/*node*" :pgwire]
                     [1 "(.getServerReadOnlyPort node)" "node" :pgwire-read-only]]
   'xtdb.healthz-test [[9 "(.getHealthzPort node)" "node" :healthz]]})

(defn- adapt-listener-ports [test-ns source]
  (reduce (fn [source [expected site receiver listener]]
            (when-not (= expected (count (re-seq (re-pattern (Pattern/quote site)) source)))
              (throw (ex-info "Unexpected upstream listener port call sites" {:namespace test-ns :site site})))
            (str/replace source site (str "(xtdb.rust.corpus/native-listener-port " receiver " " listener ")")))
          source
          (listener-call-sites test-ns)))

(defn- load-test-source [test-ns ^File file]
  (if (contains? listener-call-sites test-ns)
    (with-open [reader (StringReader. (adapt-listener-ports test-ns (slurp file)))]
      (Compiler/load reader (.getPath file) (.getName file)))
    (load-file (.getPath file))))

(defn- test-vars [^File root ns-sym]
  (let [test-ns (symbol (or (namespace ns-sym) (name ns-sym)))]
    (when-not (loads-from? root test-ns)
      (throw (ex-info (str "the test namespace's source resolves outside the upstream corpus root " root ": " (source-file test-ns)) {})))
    (load-test-source test-ns (source-file test-ns))
    (let [tests (->> (ns-interns test-ns) vals (filter (comp :test meta))
                     (sort-by (juxt (comp :line meta) (comp :name meta))))]
      (if (namespace ns-sym)
        (if-let [v (some #(when (= ns-sym (symbol %)) %) tests)]
          [v]
          (throw (ex-info (str ns-sym " is not an upstream test Var") {})))
        (vec tests)))))

(defn run-namespace
  "Reports discovered Vars before running any of them and stops on the first timeout."
  [^File root ns-sym timeout-ms emit!]
  (let [tests (try
                (test-vars root ns-sym)
                (catch Throwable e
                  (emit! {:event "load-error" :detail (truncate (ex-message e) 2000)})
                  nil))]
    (when tests
      (emit! {:event "namespace-start" :vars (mapv #(str (symbol %)) tests)})
      (loop [[v & more] tests]
        (if v
          (let [row (if (:integration (meta v))
                      {:var (str (symbol v)) :result "skipped-integration"}
                      (run-var v timeout-ms))]
            (emit! row)
            (when-not (= "timeout" (:result row))
              (recur more)))
          (do (emit! {:event "namespace-complete"}) true))))))

(defn- required-dir ^File [property]
  (let [dir (some-> (System/getProperty property) io/file)]
    (when-not (and dir (.isDirectory ^File dir))
      (binding [*out* *err*]
        (println (str "corpus needs -D" property " naming a tree from rust/conformance/upstream_tests.sh; use rust/conformance/run_corpus.sh")))
      (System/exit 2))
    dir))

(defn -main [& namespaces]
  (let [root (required-dir "xtdb.corpus.root")
        fixtures-root (required-dir "xtdb.corpus.fixtures-root")
        _ (when-not (loads-from? fixtures-root 'xtdb.test-util)
            (binding [*out* *err*]
              (println "xtdb.test-util loaded from" (str (source-file 'xtdb.test-util)) "rather than the upstream fixtures under" (str fixtures-root)))
            (System/exit 2))
        out (io/file (System/getProperty "xtdb.corpus.out" "build/corpus/results.jsonl"))
        timeout-ms (Long/parseLong (System/getProperty "xtdb.corpus.timeout-ms" "120000"))
        namespaces (if (seq namespaces) (map symbol namespaces) (test-namespaces root))]
    (load-file (.getPath (source-file 'xtdb.test-util)))
    (io/make-parents out)
    (let [complete? (with-open [w (io/writer out)]
                      (with-native-engine
                        #(every? (fn [ns-sym]
                                   (run-namespace root ns-sym timeout-ms
                                                  (fn [row]
                                                    (.write w (json/write-value-as-string (assoc row :ns (str ns-sym))))
                                                    (.write w "\n")
                                                    (.flush w))))
                                 namespaces)))]
      (shutdown-agents)
      (System/exit (if complete? 0 1)))))
