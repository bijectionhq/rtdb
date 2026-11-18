(ns xtdb.rust.corpus
  "Runs original XTDB test Vars against the native engine and records one outcome per Var."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.node :as xtn]
            [xtdb.rust]
            [xtdb.rust-test])
  (:import [java.io File]
           [java.nio.file Path]
           [java.time Duration InstantSource]
           [java.util.concurrent TimeUnit]
           [org.apache.arrow.memory RootAllocator]
           [xtdb.rust NativeDatabase]))

(def ^:private run-original-case @#'xtdb.rust-test/run-original-api-case)

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
                 (when message (str ": " message))
                 (if (instance? Throwable actual)
                   (let [^Throwable e actual]
                     (str " " (.getName (class e)) ": " (ex-message e)))
                   (str "\n  expected: " (pr-str expected) "\n  actual: " (pr-str actual))))
            2000))

(defn- classify [{:keys [pass fail error]} thrown timed-out? {:keys [native jvm]}]
  (cond
    timed-out? "timeout"
    jvm "jvm-node"
    (some-> ^Throwable thrown ex-message (= "Unsupported original API fixture")) "unsupported-fixture"
    (pos? error) "error"
    (pos? fail) "fail"
    (and (pos? pass) native) "pass"
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
         opts (merge {:server {:port 0} :flight-sql nil} opts)
         config (yaml opts)]
     (if (instance? InstantSource clock)
       (NativeDatabase/open native-allocator config (* 512 1024 1024) ^InstantSource clock)
       (NativeDatabase/open native-allocator config (* 512 1024 1024))))))

(def ^:private touched (atom {}))

(defn- observing [k f]
  (fn [& args] (swap! touched assoc k true) (apply f args)))

(defn- with-provenance [f]
  (with-redefs [xtdb.rust/plan (observing :native @#'xtdb.rust/plan)
                xtdb.rust/transact (observing :native @#'xtdb.rust/transact)
                xtdb.rust/execute-set! (observing :native @#'xtdb.rust/execute-set!)
                xtdb.rust/query-ra (observing :native xtdb.rust/query-ra)
                xtn/start-node (observing :native-node native-start-node)]
    (f)))

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
                   (run-original-case v)
                   (catch Throwable e
                     (reset! thrown e)
                     (when-not (= "Unsupported original API fixture" (ex-message e))
                       (report {:type :error :actual e}))))))
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
    (require ns-sym)
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
      (with-provenance
        #(doseq [ns-sym namespaces
              row (run-namespace ns-sym timeout-ms)]
        (swap! totals update (:result row) (fnil inc 0))
        (.write w (json/write-value-as-string (assoc row :ns (str ns-sym))))
        (.write w "\n")
        (.flush w))))
    (println "corpus results" (str out) @totals)
    (shutdown-agents)
    (System/exit 0)))
