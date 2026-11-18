(ns rtdb.conformance.parser-wire
  (:require [jsonista.core :as json]
            [rtdb.conformance.embedded-parser :as embedded]
            [xtdb.error :as err]
            [xtdb.node :as xtn])
  (:import (java.io DataInputStream DataOutputStream InputStream)
           (java.lang ProcessHandle ProcessHandle$Info)
           (java.net InetSocketAddress Socket)
           (java.nio.charset StandardCharsets)
           (java.nio.file Files OpenOption Path StandardOpenOption)
           (java.security MessageDigest)
           (java.util HexFormat)
           (org.apache.arrow.flight CallHeaders CallOption CallStatus ErrorFlightMetadata FlightClient FlightClientMiddleware FlightClientMiddleware$Factory FlightEndpoint FlightInfo FlightRuntimeException FlightStream Location SetSessionOptionsRequest SetSessionOptionsResult SessionOptionValueFactory)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector BigIntVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Int Field Schema)
           (xtdb.api Xtdb)
           (xtdb.antlr Sql)
           (xtdb.flight_sql XtdbProducer)
           (xtdb.pgwire PgType)
           (xtdb.query SqlParser)))

(set! *warn-on-reflection* true)

(def ^:private expected-fixture
  {:schema_version 1, :statement_mismatch "SELEC 1", :unwanted_token "extra SELECT 1",
   :follow_up "SELECT 42::bigint AS answer"})

(def ^:private ^"[Ljava.nio.file.LinkOption;" resolve-links (make-array java.nio.file.LinkOption 0))
(def ^:private ^"[Ljava.nio.file.OpenOption;" read-options (make-array OpenOption 0))
(def ^:private ^"[Ljava.nio.file.OpenOption;" write-options (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))
(def ^:private ^"[Lorg.apache.arrow.flight.CallOption;" call-options (make-array CallOption 0))

(defn- sha256 [^bytes bytes]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)))

(defn- unsigned-bytes [^bytes bytes]
  (mapv #(bit-and 255 %) bytes))

(defn- pg-frame [^DataInputStream in]
  (let [tag (str (char (.readUnsignedByte in)))
        length (.readInt in)]
    (when-not (<= 4 length 65536)
      (throw (err/fault ::pg-frame "Invalid PostgreSQL frame length")))
    (let [body (byte-array (- length 4))]
      (.readFully in body)
      {:tag tag, :bytes (unsigned-bytes body)})))

(defn- pg-send! [^DataOutputStream out ^long tag ^bytes body]
  (.writeByte out (int tag))
  (.writeInt out (+ 4 (alength body)))
  (.write out body)
  (.flush out))

(defn- pg-startup! [^DataInputStream in ^DataOutputStream out]
  (let [^bytes parameters (.getBytes "user\u0000xtdb\u0000database\u0000xtdb\u0000\u0000" StandardCharsets/UTF_8)]
    (.writeInt out (+ 8 (alength parameters)))
    (.writeInt out 196608)
    (.write out parameters)
    (.flush out))
  (loop [remaining 128, authenticated? false, backend? false]
    (when (zero? remaining)
      (throw (err/fault ::pg-startup "PostgreSQL startup did not become ready")))
    (let [{:keys [tag bytes]} (pg-frame in)]
      (cond
        (and (= tag "R") (= bytes [0 0 0 0]) (not authenticated?)) (recur (dec remaining) true backend?)
        (and (= tag "K") (= 8 (count bytes)) authenticated? (not backend?)) (recur (dec remaining) true true)
        (and (= tag "S") authenticated?) (recur (dec remaining) true backend?)
        (and (= tag "Z") (= bytes [73]) authenticated? backend?) nil
        :else (throw (err/fault ::pg-startup "Unexpected PostgreSQL startup response" {:tag tag}))))))

(defn- pg-query! [^DataInputStream in ^DataOutputStream out ^String sql]
  (pg-send! out (int \Q) (.getBytes (str sql \u0000) StandardCharsets/UTF_8))
  (loop [frames []]
    (when (= 16 (count frames))
      (throw (err/fault ::pg-query "PostgreSQL query did not reach ReadyForQuery")))
    (let [frame (pg-frame in)
          frames (conj frames frame)]
      (if (= "Z" (:tag frame))
        {:sql sql, :frames frames}
        (recur frames)))))

(defn- pg-observations [port {:keys [statement_mismatch unwanted_token follow_up]}]
  (with-open [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 30000)
    (.setSoTimeout socket 30000)
    (let [in (DataInputStream. (.getInputStream socket))
          out (DataOutputStream. (.getOutputStream socket))]
      (pg-startup! in out)
      (let [mismatch (pg-query! in out statement_mismatch)
            unwanted (pg-query! in out unwanted_token)
            follow-up (pg-query! in out follow_up)]
        (pg-send! out (int \X) (byte-array 0))
        {:errors {:statement_mismatch mismatch, :unwanted_token unwanted}, :follow_up follow-up}))))

(defn- receive-session-cookie! [state cookies]
  (when (seq cookies)
    (swap! state
           (fn [{:keys [cookie] :as session}]
             (let [[value & extra] cookies]
               (if (and (nil? cookie) (empty? extra) (string? value)
                        (re-matches #"arrow_flight_session_id=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" value))
                 (assoc session :cookie value)
                 (assoc session :cookie-changed true)))))))

(defn- session-middleware ^FlightClientMiddleware$Factory [state]
  (reify FlightClientMiddleware$Factory
    (onCallStarted [_ _]
      (reify FlightClientMiddleware
        (onBeforeSendingHeaders [_ ^CallHeaders headers]
          (when-let [^String cookie (:cookie @state)]
            (when (seq (.getAll headers "cookie"))
              (swap! state assoc :cookie-changed true))
            (.insert headers "cookie" cookie)
            (swap! state update :sent inc)))
        (onHeadersReceived [_ ^CallHeaders headers]
          (receive-session-cookie! state (.getAll headers "set-cookie")))
        (onCallCompleted [_ ^CallStatus status]
          (when (and (pos? (:sent @state)) status)
            (let [^ErrorFlightMetadata trailers (.metadata status)]
              (when trailers
                (receive-session-cookie! state (.getAll trailers "set-cookie"))))))))))

(defn- checked-flight-call! [state id call]
  (let [{:keys [cookie sent]} @state
        result (call)
        after @state]
    (when-not (and cookie
                   (= cookie (:cookie after))
                   (= (inc sent) (:sent after))
                   (not (:cookie-changed after)))
      (throw (err/fault ::flight-session "Flight call did not keep the established session" {:call id})))
    (swap! state update :calls conj id)
    result))

(defn- flight-error [^FlightSqlClient client ^String sql]
  (try
    (.execute client sql call-options)
    (throw (err/fault ::flight-query "Malformed Flight statement returned FlightInfo"))
    (catch FlightRuntimeException error
      (let [^CallStatus status (.status error)
            ^ErrorFlightMetadata metadata (.metadata status)
            ^bytes anomaly (when metadata (.getByte metadata "xtdb-anomaly-bin"))]
        (when (and metadata (seq (.getAll metadata "set-cookie")))
          (throw (err/fault ::flight-session "Flight parser rejection returned a session cookie")))
        (when-not (and anomaly (= "INVALID_ARGUMENT" (.name (.code status))))
          (throw (err/fault ::flight-query "Flight parser rejection omitted its invalid-argument status or anomaly metadata")))
        {:sql sql, :status (.name (.code status)), :message (.description status),
         :metadata_bytes (unsigned-bytes anomaly)}))))

(defn- flight-schema [^Schema schema]
  (let [[^Field answer & extra] (.getFields schema)]
    (when-not (and answer (empty? extra)
                   (= "answer" (.getName answer))
                   (= (ArrowType$Int. 64 true) (.getType answer)))
      (throw (err/fault ::flight-follow-up "Flight follow-up schema omitted bigint answer")))
    {:fields [{:name (.getName answer), :type "i64", :nullable (.isNullable answer),
               :metadata (into (sorted-map) (.getMetadata answer))}],
     :metadata (into (sorted-map) (.getCustomMetadata schema))}))

(defn- flight-stream [^FlightSqlClient client ^FlightInfo info]
  (let [endpoints (.getEndpoints info)
        ^FlightEndpoint endpoint (first endpoints)]
    (when-not (= 1 (count endpoints))
      (throw (err/fault ::flight-follow-up "Flight follow-up did not return one endpoint")))
    (with-open [^FlightStream stream (.getStream client (.getTicket endpoint) call-options)]
      (let [^VectorSchemaRoot root (.getRoot stream)
            stream-schema (flight-schema (.getSchema root))]
        (when-not (and (.next stream) (= 1 (.getRowCount root)))
          (throw (err/fault ::flight-follow-up "Flight follow-up did not return one described row")))
        (let [answer (.getVector root "answer")
              batch-schema (flight-schema (.getSchema root))]
          (when-not (and (instance? BigIntVector answer) (not (.isNull ^BigIntVector answer 0)))
            (throw (err/fault ::flight-follow-up "Flight follow-up did not return a bigint answer")))
          (let [value (.get ^BigIntVector answer 0)]
            (when (.next stream)
              (throw (err/fault ::flight-follow-up "Flight follow-up returned another batch")))
            {:stream_schema stream-schema,
             :batches [{:schema batch-schema, :rows [[{:kind "i64", :value value}]]}]}))))))

(defn- flight-observations [port {:keys [statement_mismatch unwanted_token follow_up]}]
  (let [state (atom {:sent 0, :calls []})]
    (with-open [allocator (RootAllocator.)
                flight (-> (FlightClient/builder allocator (Location/forGrpcInsecure "127.0.0.1" (int port)))
                           (.intercept (session-middleware state))
                           (.build))
                client (FlightSqlClient. flight)]
      (let [^SetSessionOptionsResult result
            (.setSessionOptions client
                                (SetSessionOptionsRequest. {"catalog" (SessionOptionValueFactory/makeSessionOptionValue "xtdb")})
                                call-options)]
        (when-not (and (not (.hasErrors result)) (:cookie @state) (zero? (:sent @state))
                       (not (:cookie-changed @state)))
          (throw (err/fault ::flight-session "Flight session options did not establish one session"))))
      (let [mismatch (checked-flight-call! state "statement_mismatch" #(flight-error client statement_mismatch))
            unwanted (checked-flight-call! state "unwanted_token" #(flight-error client unwanted_token))
            ^FlightInfo info (checked-flight-call! state "follow_up_info" #(.execute client ^String follow_up call-options))
            streamed (checked-flight-call! state "follow_up_stream" #(flight-stream client info))]
        {:errors {:statement_mismatch mismatch, :unwanted_token unwanted},
         :session {:options_applied true, :cookie_established true, :calls (:calls @state)},
         :follow_up (assoc streamed :sql follow_up, :declared_schema (flight-schema (.getSchema info)))}))))

(defn- canonical-path [^String path]
  (str (.toRealPath (Path/of path (make-array String 0)) resolve-links)))

(defn- runtime-closure []
  (let [java-home (canonical-path (System/getProperty "java.home"))
        ^ProcessHandle$Info process (.info (ProcessHandle/current))
        executable (.command process)
        classes [Xtdb Sql SqlParser PgType XtdbProducer FlightClient FlightSqlClient FlightStream BigIntVector]
        runtime (embedded/runtime-closure classes nil)]
    (when (.isEmpty executable)
      (throw (err/fault ::runtime "Parser wire JVM process did not identify its Java executable")))
    (let [java-executable (canonical-path (.get executable))
          java-modules (canonical-path (str java-home "/lib/modules"))]
      (when-not (and (= java-executable (canonical-path (str java-home "/bin/java")))
                     (= [java-executable java-modules] (:jdk runtime)))
        (throw (err/fault ::runtime "Parser wire JVM runtime differs from the process executable")))
      (assoc runtime :java_home java-home, :java_executable java-executable, :java_modules java-modules))))

(defn- write-new! [^Path path ^bytes bytes]
  (Files/write path bytes write-options))

(defn -main [^String fixture-path ^String output-path]
  (let [^bytes fixture-bytes (with-open [^InputStream in (Files/newInputStream (Path/of fixture-path (make-array String 0)) read-options)]
                               (.readNBytes in 4097))
        _ (when (> (alength fixture-bytes) 4096)
            (throw (err/fault ::fixture "Parser wire fixture exceeds its size bound")))
        fixture (json/read-value fixture-bytes json/keyword-keys-object-mapper)
        ^Path output (Path/of output-path (make-array String 0))]
    (when-not (= expected-fixture fixture)
      (throw (err/fault ::fixture "Invalid parser wire fixture")))
    (let [runtime (runtime-closure)
          observations (with-open [^Xtdb node (xtn/start-node {:server {:host "127.0.0.1", :port 0}
                                                               :flight-sql {:host "127.0.0.1", :port 0}})]
                         {:pgwire (pg-observations (.getServerPort node) fixture),
                          :flight_sql (flight-observations (.getFlightSqlPort node) fixture)})
          ^bytes artifacts (json/write-value-as-bytes runtime)
          execution {:schema_version 1, :engine "jvm", :fixture_sha256 (sha256 fixture-bytes),
                     :binary_sha256 (sha256 artifacts), :runtime_unchanged (= runtime (runtime-closure)),
                     :observations observations}]
      (write-new! (.resolve output "runtime-artifacts.json") artifacts)
      (write-new! (.resolve output "execution.json") (json/write-value-as-bytes execution)))))
