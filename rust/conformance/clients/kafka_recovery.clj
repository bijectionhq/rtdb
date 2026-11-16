(require '[clojure.java.io :as io]
         '[jsonista.core :as json])
(import '(java.nio.charset StandardCharsets)
        '(java.nio.file Files Path OpenOption StandardOpenOption)
        '(java.security MessageDigest)
        '(java.time Duration)
        '(java.util HexFormat Properties)
        '(java.util.concurrent TimeUnit)
        '(org.apache.kafka.clients.admin AdminClient NewTopic OffsetSpec)
        '(org.apache.kafka.clients.producer KafkaProducer ProducerRecord RecordMetadata)
        '(org.apache.kafka.common TopicPartition)
        '(org.apache.kafka.common.serialization ByteArraySerializer))

(set! *warn-on-reflection* true)

(defn- sha [^bytes value]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") value)))

(defn- properties ^Properties [request]
  (let [result (Properties.)]
    (.setProperty result "bootstrap.servers" (get-in request [:kafka :bootstrap_servers]))
    (doseq [[key environment] (get-in request [:kafka :properties_environment])]
      (let [value (System/getenv (name environment))]
        (when-not value (throw (ex-info "Kafka credential environment is absent" {:name environment})))
        (.setProperty result (name key) value)))
    (.setProperty result "request.timeout.ms" "30000")
    (.setProperty result "default.api.timeout.ms" "60000")
    result))

(defn- execute [request]
  (let [namespace (:namespace request)
        topics (:topics request)]
    (when-not (and (= 1 (:schema_version request))
                   (re-matches #"rtdb-recovery-[a-z0-9-]{12,65}" namespace)
                   (seq topics) (= (count topics) (count (set topics)))
                   (every? #(and (.startsWith ^String % (str namespace "-")) (re-matches #"[a-z0-9_-]+" %)) topics))
      (throw (ex-info "Invalid scoped Kafka recovery request" {})))
    (let [config (properties request)]
      (case (:action request)
        "assert_absent"
        (with-open [admin (AdminClient/create config)]
          (let [existing (.get (.names (.listTopics admin)) 60 TimeUnit/SECONDS)]
            (when (some #(.contains ^java.util.Set existing %) topics)
              (throw (ex-info "Recovery topic namespace is already occupied" {}))))
          {:absent topics})

        "create"
        (with-open [admin (AdminClient/create config)]
          (let [existing (.get (.names (.listTopics admin)) 60 TimeUnit/SECONDS)]
            (when (some #(.contains ^java.util.Set existing %) topics)
              (throw (ex-info "Recovery topic namespace is already occupied" {}))))
          (.get (.all (.createTopics admin (mapv #(.configs (NewTopic. ^String % (int 1) (short 1))
                                                           {"cleanup.policy" "delete", "retention.ms" "-1", "retention.bytes" "-1"}) topics))) 60 TimeUnit/SECONDS)
          {:created topics})

        "produce"
        (do
          (when-not (and (= 1 (count topics)) (<= (count (:records request)) 10000))
            (throw (ex-info "Recovery producer requires one topic and bounded records" {})))
          (.setProperty config "acks" "all")
          (.setProperty config "enable.idempotence" "true")
          (.setProperty config "delivery.timeout.ms" "60000")
          (with-open [producer (KafkaProducer. config (ByteArraySerializer.) (ByteArraySerializer.))]
            {:records (mapv (fn [{:keys [key value]}]
                              (let [key-bytes (.getBytes ^String key StandardCharsets/UTF_8)
                                    value-bytes (when value (.getBytes ^String value StandardCharsets/UTF_8))
                                    record (ProducerRecord. ^String (first topics) (Integer/valueOf 0) key-bytes value-bytes)
                                    ^RecordMetadata metadata (.get (.send producer record) 60 TimeUnit/SECONDS)]
                                {:topic (.topic metadata), :partition (.partition metadata), :offset (.offset metadata),
                                 :key_sha256 (sha key-bytes), :value_sha256 (when value-bytes (sha value-bytes)), :tombstone (nil? value)}))
                            (:records request))}))

        "positions"
        (with-open [admin (AdminClient/create config)]
          (let [requested (into {} (map #(vector (TopicPartition. ^String % 0) (OffsetSpec/latest)) topics))
                result (.get (.all (.listOffsets admin requested)) 60 TimeUnit/SECONDS)]
            {:positions (mapv (fn [topic]
                                (let [^org.apache.kafka.clients.admin.ListOffsetsResult$ListOffsetsResultInfo value (.get ^java.util.Map result (TopicPartition. ^String topic 0))]
                                  {:topic topic, :partition 0, :end_offset (.offset value)})) topics)}))

        "delete"
        (with-open [admin (AdminClient/create config)]
          (.get (.all (.deleteTopics admin topics)) 60 TimeUnit/SECONDS)
          (let [deadline (+ (System/nanoTime) 60000000000)]
            (loop []
              (let [existing (.get (.names (.listTopics admin)) 60 TimeUnit/SECONDS)]
                (when (some #(.contains ^java.util.Set existing %) topics)
                  (when (> (System/nanoTime) deadline) (throw (ex-info "Recovery topics did not delete" {})))
                  (Thread/sleep 50)
                  (recur)))))
          {:deleted topics})

        (throw (ex-info "Unknown Kafka recovery action" {}))))))

(let [[input-name output-name] *command-line-args*
      input (.toPath (io/file input-name))
      output (.toPath (io/file output-name))]
  (when (> (Files/size input) 1048576) (throw (ex-info "Kafka recovery request exceeds one MiB" {})))
  (let [bytes (Files/readAllBytes input)
        request (json/read-value bytes json/keyword-keys-object-mapper)
        result (execute request)
        receipt {:schema_version 1, :request_sha256 (sha bytes), :action (:action request),
                 :execution_complete true, :clients_closed true, :result result}]
    (Files/write output (json/write-value-as-bytes receipt)
                 (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))))
