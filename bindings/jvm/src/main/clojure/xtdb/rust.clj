(ns xtdb.rust
  "Embedded xtdb.api compatibility over the native RTDB engine."
  (:require [clojure.string :as str]
            [cognitect.transit :as transit]
            [xtdb.api]
            [xtdb.basis :as basis]
            [xtdb.error :as err]
            [xtdb.protocols :as protocols]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import [clojure.lang IReduceInit]
           [java.io ByteArrayOutputStream]
           [java.time Instant LocalDate LocalDateTime OffsetDateTime ZonedDateTime]
           [java.util Locale]
           [java.util.function Function]
           [org.apache.arrow.vector.types.pojo Field]
           [xtdb.api.query IKeyFn]
           [xtdb.arrow VectorType]
           [xtdb.rust NativeConnection NativeDatabase NativeException NativeRows NativeStatement]
           [xtdb.util NormalForm]))

(extend-protocol err/ToAnomaly
  NativeException
  (->anomaly [^NativeException error context]
    (let [wire (.transit error)]
      (if (pos? (alength wire))
        (err/->anomaly (serde/read-transit wire) context)
        (err/unavailable (keyword (.code error)) (ex-message error) context)))))

(def ^:private request-write-handlers
  (transit/write-handler-map
   (into serde/transit-write-handlers
         {Byte (transit/write-handler "i8" long)
          Short (transit/write-handler "i16" long)
          Integer (transit/write-handler "i32" long)
          Float (transit/write-handler "f32" double)})))

(defn ^:no-doc write-request ^bytes [value]
  (with-open [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :msgpack {:handlers request-write-handlers}) value)
    (.toByteArray out)))

(defn- execute-set! [^NativeConnection connection sql arguments]
  (with-open [^NativeStatement statement (.createStatement connection sql)]
    (when (some? arguments)
      (.bindTransit statement (write-request [(vec arguments)])))
    (.executeUpdate statement)))

(defn- current-await-token [^NativeConnection connection]
  (.getAwaitToken connection))

(defn- set-await-token! [^NativeConnection connection token]
  (if (and (.supportsTypedAwaitToken connection) (or (nil? token) (string? token)))
    (.setAwaitToken connection token)
    (execute-set! connection "SET AWAIT_TOKEN = ?" [token])))

(defn- native-read-time? [time]
  (or (nil? time) (string? time)
      (instance? LocalDate time) (instance? LocalDateTime time)
      (instance? ZonedDateTime time) (instance? OffsetDateTime time)
      (and (instance? Instant time)
           (< -9223372036854 (.getEpochSecond ^Instant time) 9223372036854))))

(defn- row-reader ^Function [^IKeyFn key-fn]
  (let [handlers (assoc serde/transit-read-handlers "rtdb/struct"
                        (transit/read-handler
                          #(into {} (map (fn [[key value]] [(.denormalize key-fn key) value])) %)))]
    (reify Function
      (apply [_ input] (transit/read (transit/reader input :json {:handlers handlers}))))))

(defn- with-read-settings [^NativeConnection connection {:keys [snapshot-token snapshot-time current-time default-tz]} f]
  (let [timezone (some-> default-tz str)]
    (if-not (or (some? timezone) (some? snapshot-token) (some? snapshot-time) (some? current-time))
      (f)
      (let [typed? (and (or (nil? snapshot-token) (string? snapshot-token))
                        (native-read-time? snapshot-time) (native-read-time? current-time))
            begun? (and typed? (.tryBeginReadOnly connection timezone snapshot-token snapshot-time current-time))]
        (when-not begun? (.begin connection))
        (try
          (when-not begun?
            (if (and typed? (.supportsTypedReadSettings connection))
              (.setReadSettings connection timezone snapshot-token snapshot-time current-time)
              (let [settings (filterv (comp some? second)
                                      [["TIMEZONE" timezone] ["SNAPSHOT_TOKEN" snapshot-token]
                                       ["SNAPSHOT_TIME" snapshot-time] ["CLOCK_TIME" current-time]])]
                (execute-set! connection
                              (str "SET TRANSACTION READ ONLY, " (str/join ", " (map #(str (first %) " = ?") settings)))
                              (mapv second settings)))))
          (f)
          (finally (.rollbackTx connection)))))))

(defn- plan [^NativeConnection connection sql args {:keys [await-token key-fn] :as options}]
  (let [read-page (row-reader (serde/read-key-fn (or key-fn :kebab-case-keyword)))]
    (reify IReduceInit
      (reduce [_ f initial]
        (err/wrap-anomaly {:sql sql}
          (when await-token
            (set-await-token! connection (basis/merge-tx-tokens (current-await-token connection) await-token)))
          (with-read-settings connection options
            (fn []
              (with-open [^NativeStatement statement (.createStatement connection sql)]
                (.prepare statement)
                (when (seq args)
                  (let [args (vec args)]
                    (when-not (.tryBindNativeArguments statement args)
                      (.bindTransit statement (write-request [args])))))
                (with-open [^NativeRows rows (.executeRows statement)]
                  (loop [acc initial]
                    (if (reduced? acc)
                      @acc
                      (if-let [page (.readNext rows read-page)]
                        (let [next (reduce (fn [acc row]
                                             (let [next (f acc row)]
                                               (if (reduced? next) (reduced next) next)))
                                           acc page)]
                          (recur next))
                        acc))))))))))))

(defn- operation [{:keys [op table-name docs doc-ids valid-from valid-to sql arg-rows]}]
  (if (= op :sql)
    {"op" "sql", "sql" sql, "arguments" arg-rows}
    (cond-> {"op" (name op), "schema" (or (namespace table-name) "public"), "table" (name table-name)}
      (some? docs) (assoc "documents" docs)
      (some? doc-ids) (assoc "ids" doc-ids)
      valid-from (assoc "valid_from" valid-from)
      valid-to (assoc "valid_to" valid-to))))

(defn- transact [^NativeConnection connection operations {:keys [default-tz system-time metadata]} asynchronous?]
  (err/wrap-anomaly {}
    (let [request {"asynchronous" asynchronous?, "operations" (mapv operation operations)
                   "options" (cond-> {}
                               default-tz (assoc "default_timezone" (str default-tz))
                               system-time (assoc "system_time" (time/->instant system-time {:default-tz default-tz}))
                               metadata (assoc "metadata" metadata))}
          result (serde/read-transit (.transact connection (write-request request)) :json)]
      (if asynchronous?
        {:tx-id (get result "tx_id")}
        (serde/->TxKey (get result "tx_id") (time/->instant (get result "system_time")))))))

(defn- connect ^NativeConnection [^NativeDatabase node {:keys [database application-name]}]
  (err/wrap-anomaly {}
    (let [conn (.connect node (if database (cond-> database (keyword? database) (-> symbol str NormalForm/normalForm)) "xtdb"))]
      (try
        (.setApplicationName conn (or application-name "XTDB Clojure API"))
        conn
        (catch Throwable t
          (try (.close conn) (catch Throwable close-error (.addSuppressed t close-error)))
          (throw t))))))

(defn- logical-arguments [args]
  (if (sequential? args)
    (into {} (map-indexed (fn [index value] [(str "?_" index) value])) args)
    (or args {})))

(defn- native-argument-name [key]
  (cond
    (string? key) key
    (keyword? key) (let [name (subs (str key) 1)]
                     (when (re-matches #"[\u0000-\u007f]+" name)
                       (-> (if (str/starts-with? name "xt/") (str "_" (subs name 3)) name)
                           (str/replace "-" "_")
                           (str/replace #"[./$]" "\\$")
                           (.toLowerCase Locale/ROOT))))))

(defn- native-logical-arguments [arguments]
  (when (map? arguments)
    (reduce-kv (fn [row key value]
                 (let [name (native-argument-name key)]
                   (if (and (some? name) (not (contains? row name)))
                     (assoc row name value)
                     (reduced nil))))
               {} arguments)))

(defn query-ra [^NativeDatabase node relation {:keys [args default-db await-token key-fn preserve-pages? with-types? explain? explain-analyze?] :as options
                                             :or {key-fn :kebab-case-keyword}}]
  (err/wrap-anomaly {}
    (with-open [connection (connect node (assoc options :database default-db))]
      (when-let [token (or await-token (.getAwaitToken node))]
        (set-await-token! connection token))
      (with-read-settings connection options
        (fn []
          (let [relation (cond
                           explain? [:explain {:mode :plan} relation]
                           explain-analyze? [:explain {:mode :analyze} relation]
                           :else relation)
                source (pr-str relation)
                arguments (logical-arguments args)]
            (with-open [^NativeRows result (or (when-let [row (native-logical-arguments arguments)] (.tryQueryLogicalRelation connection source row))
                                               (.queryLogicalRelation connection source (write-request arguments)))]
              (let [read-page (row-reader (serde/read-key-fn key-fn))
                    rows (loop [rows (transient [])]
                           (if-let [page (.readNext result read-page)]
                             (recur (if preserve-pages? (conj! rows page) (reduce conj! rows page)))
                             (persistent! rows)))]
                (if with-types?
                  {:res rows
                   :types (into {} (map (fn [^Field field] [(symbol (.getName field)) (VectorType/fromField field)]))
                                (.getFields (.schema result)))}
                  rows)))))))))

(defn- merge-node-token! [^NativeDatabase database next-token]
  (loop []
    (let [previous (.getAwaitToken database)
          next (basis/merge-tx-tokens previous next-token)]
      (when-not (.compareAndSetAwaitToken database previous next) (recur)))))

(defn- with-node [^NativeDatabase database options f]
  (with-open [connection (connect database options)]
    (when-let [token (.getAwaitToken database)] (set-await-token! connection token))
    (let [result (f connection)]
      (merge-node-token! database (current-await-token connection))
      result)))

(extend-protocol protocols/Connectable
  NativeConnection
  (-plan-q [connection sql args options]
    (protocols/check-no-database connection options)
    (plan connection sql args options))
  (-submit-tx [connection operations options]
    (protocols/check-no-database connection options)
    (transact connection operations options true))
  (-execute-tx [connection operations options]
    (protocols/check-no-database connection options)
    (transact connection operations options false))
  (-status [connection] (protocols/build-status connection))

  NativeDatabase
  (-plan-q [^NativeDatabase database sql args options]
    (reify IReduceInit
      (reduce [_ f initial]
        (with-open [connection (connect database options)]
          (when-let [token (.getAwaitToken database)] (set-await-token! connection token))
          (reduce f initial (plan connection sql args (dissoc options :database)))))))
  (-submit-tx [database operations options]
    (with-node database options #(transact % operations (dissoc options :database) true)))
  (-execute-tx [database operations options]
    (with-node database options #(transact % operations (dissoc options :database) false)))
  (-status [^NativeDatabase database]
    (with-open [connection (.connect database)] (protocols/build-status connection))))
