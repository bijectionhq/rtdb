(ns xtdb.rust.corpus
  "Runs original XTDB test Vars against the native engine and records one outcome per Var."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.flight-sql :as fsql]
            [xtdb.node :as xtn]
            [xtdb.operator.list :as list-operator]
            [xtdb.rust :as rust]
            [xtdb.serde.types :as st]
            [xtdb.test-util :as tu])
  (:import [java.io File]
           [java.nio.file Path]
           [java.time Duration InstantSource]
           [java.util.concurrent TimeUnit]
           [org.apache.arrow.memory RootAllocator]
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

(defn- classify [{:keys [pass fail error]} thrown timed-out? {:keys [native wire jvm]}]
  (cond
    timed-out? "timeout"
    jvm "jvm-node"
    (pos? error) "error"
    (pos? fail) "fail"
    (and (pos? pass) native) "pass"
    (and (pos? pass) wire) "pass-wire"
    (pos? pass) "jvm-internal"
    :else "no-assertions"))


(defn- camel [k]
  (let [[head & more] (str/split (str/replace (name k) #"\?$" "") #"-")]
    (apply str head (map str/capitalize more))))

(defn- tag [k] (str "!" (str/join (map str/capitalize (str/split (name k) #"-")))))

(defn- yaml [v]
  (cond
    (map? v) (str "{" (str/join ", " (for [[k x] v] (str (json/write-value-as-string (if (keyword? k) (camel k) (str k))) ": " (yaml x)))) "}")
    (and (vector? v) (= 2 (count v)) (keyword? (first v)) (map? (second v))) (str (tag (first v)) " " (yaml (second v)))
    (and (vector? v) (= 1 (count v)) (keyword? (first v))) (str (tag (first v)) " {}")
    (sequential? v) (str "[" (str/join ", " (map yaml v)) "]")
    (keyword? v) (json/write-value-as-string (name v))
    (or (instance? Path v) (instance? File v) (instance? Duration v)) (json/write-value-as-string (str v))
    (nil? v) "null"
    :else (json/write-value-as-string v)))

(defonce ^:private native-allocator (RootAllocator.))

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
         opts (merge {:server {:port 0} :flight-sql {:port 0}} opts)
         config (yaml opts)]
     (try
       (if (instance? InstantSource clock)
         (NativeDatabase/open native-allocator config (* 512 1024 1024) ^InstantSource clock)
         (NativeDatabase/open native-allocator config (* 512 1024 1024)))
       (catch NativeException e
         (throw (if (= "xtdb/out-of-sync-log" (.code e))
                  (IllegalStateException. (ex-message e) e)
                  e)))))))

(defn- import-logical-inputs [query]
  (let [[operator options & operands] query]
    (case operator
      ::tu/pages (case (count operands)
                   0 [:literal-pages {:pages options}]
                   1 [:literal-pages {:pages (first operands) :types (update-vals options st/render-type)}]
                   (throw (ex-info "Invalid fixture page source" {:query query})))
      :list [:list (update options :batch-size #(or % list-operator/*batch-size*))]
      (into [operator options] (map import-logical-inputs) operands))))

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

(def ^:private native-connectables
  {:adbc (fn [node f] (f node))
   :pgwire (fn [^NativeDatabase node f] (swap! touched assoc :wire true) (f (xt/client {:port (.getServerPort node)})))
   :flight-sql (fn [^NativeDatabase node f]
                 (swap! touched assoc :wire true)
                 (with-open [conn (fsql/open-conn {:port (.getFlightSqlPort node)})]
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
                xtn/start-node (observing :native-node native-start-node)]
    (binding [tu/*query-ra-executor* (observing :native native-query-ra)
              tu/*flush-block-executor* (fn [^NativeDatabase node timeout] (.flushAll node timeout))]
      (with-root #'tu/connectables native-connectables f))))

(defn- port-fixture [port-var server-var]
  (fn [f]
    (let [^NativeDatabase node tu/*node*]
      (swap! touched assoc :wire true)
      (with-bindings (cond-> {(resolve port-var) (.getServerPort node)}
                       server-var (assoc (resolve server-var) node))
        (f)))))

(def ^:private fixture-overrides
  "Original fixtures that read the JVM node's internal system map, by the Var that defines them."
  {'xtdb.pgwire-test/with-server-and-port (port-fixture 'xtdb.pgwire-test/*port* 'xtdb.pgwire-test/*server*)
   'xtdb.pgwire.pg2-test/with-server-and-port (port-fixture 'xtdb.pgwire.pg2-test/*port* 'xtdb.pgwire.pg2-test/*server*)
   'xtdb.pgwire-protocol-test/with-port (port-fixture 'xtdb.pgwire-protocol-test/*port* nil)
   'xtdb.pgwire.authn-test/with-port (port-fixture 'xtdb.pgwire.authn-test/*port* nil)})

(defn- native-fixtures [fixtures]
  (let [overrides (into {} (keep (fn [[sym fixture]] (when-let [v (resolve sym)] [@v fixture]))) fixture-overrides)]
    (mapv #(get overrides % %) fixtures)))

(defn- run-case [v]
  (let [fixtures (meta (:ns (meta v)))
        once (t/join-fixtures (native-fixtures (::t/once-fixtures fixtures)))
        each (t/join-fixtures (native-fixtures (::t/each-fixtures fixtures)))]
    (once #(tu/with-allocator (fn [] (each (:test (meta v))))))))

(defn run-var [v timeout-ms]
  (let [counts (atom {:pass 0 :fail 0 :error 0})
        first-problem (atom nil)
        thrown (atom nil)
        report (fn [{:keys [type] :as m}]
                 (case type
                   :pass (swap! counts update :pass inc)
                   (:fail :error) (do (swap! counts update type inc)
                                      (compare-and-set! first-problem nil (describe m)))
                   nil))
        started (System/nanoTime)
        _ (reset! touched {})
        task (future
               (binding [t/report report
                         t/*testing-vars* (list v)]
                 (try
                   (run-case v)
                   (catch Throwable e
                     (reset! thrown e)
                     (report {:type :error :actual e})))))
        timed-out? (= ::timeout (deref task timeout-ms ::timeout))]
    (when timed-out? (future-cancel task))
    (let [result (classify @counts @thrown timed-out? @touched)]
      (merge {:var (str (symbol v))
              :result result
              :millis (.toMillis TimeUnit/NANOSECONDS (- (System/nanoTime) started))}
             @counts
             (when-let [problem (or @first-problem (some-> @thrown ex-message))]
               (when-not (= result "pass") {:detail problem}))))))

(defn- test-vars [ns-sym]
  (->> (ns-interns ns-sym) vals (filter (comp :test meta)) (sort-by (comp :line meta))))

(defn run-namespace [ns-sym timeout-ms]
  (try
    (require ns-sym :reload)
    (vec (for [v (test-vars ns-sym)]
           (if (:integration (meta v))
             {:var (str (symbol v)) :result "skipped-integration"}
             (run-var v timeout-ms))))
    (catch Throwable e
      [{:var (str ns-sym) :result "load-error" :detail (truncate (ex-message e) 2000)}])))

(defn -main [& namespaces]
  (let [root (io/file (System/getProperty "xtdb.corpus.root"))
        out (io/file (System/getProperty "xtdb.corpus.out" "build/corpus/results.jsonl"))
        timeout-ms (Long/parseLong (System/getProperty "xtdb.corpus.timeout-ms" "120000"))
        namespaces (if (seq namespaces) (map symbol namespaces) (test-namespaces root))
        totals (atom (sorted-map))]
    (io/make-parents out)
    (with-open [w (io/writer out :append (Boolean/getBoolean "xtdb.corpus.append"))]
      (with-native-engine
        #(doseq [ns-sym namespaces
              row (run-namespace ns-sym timeout-ms)]
        (swap! totals update (:result row) (fnil inc 0))
        (.write w (json/write-value-as-string (assoc row :ns (str ns-sym))))
        (.write w "\n")
        (.flush w))))
    (println "corpus results" (str out) @totals)
    (shutdown-agents)
    (System/exit 0)))
