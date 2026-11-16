(require '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jsonista.core :as json])
(import '(java.nio.file Files OpenOption StandardOpenOption)
        '(java.security MessageDigest)
        '(java.time Duration Instant ZoneOffset)
        '(java.time.format DateTimeFormatter)
        '(java.util HexFormat)
        '(java.util.concurrent TimeUnit)
        '(software.amazon.awssdk.auth.credentials DefaultCredentialsProvider)
        '(software.amazon.awssdk.core ResponseBytes)
        '(software.amazon.awssdk.core.async AsyncResponseTransformer)
        '(software.amazon.awssdk.core.client.config ClientOverrideConfiguration)
        '(software.amazon.awssdk.regions Region)
        '(software.amazon.awssdk.services.s3 S3AsyncClient)
        '(software.amazon.awssdk.services.s3.model GetObjectRequest ListObjectsV2Request ListObjectsV2Response S3Object))

(set! *warn-on-reflection* true)

(defn- sha [^bytes value]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") value)))

(defn- prefixes [{:keys [prefix started_at ended_at]}]
  (let [start (Instant/parse started_at), end (Instant/parse ended_at)
        formatter (.withZone (DateTimeFormatter/ofPattern "yyyy/MM/dd") ZoneOffset/UTC)]
    (when (or (.isAfter start end) (> (.getSeconds (Duration/between start end)) 3600))
      (throw (ex-info "CloudTrail worker interval is invalid" {})))
    (distinct [(str prefix "/" (.format formatter start) "/") (str prefix "/" (.format formatter end) "/")
               (str prefix "/" (.format formatter (.plusSeconds end 600)) "/")])))

(defn- execute [request output]
  (let [{:keys [namespace bucket prefix region]} request]
    (when-not (and (= 1 (:schema_version request))
                   (re-matches #"rtdb-recovery-[a-z0-9-]{12,65}" namespace)
                   (re-matches #"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]" bucket)
                   (seq prefix) (not-any? #{"" "." ".."} (str/split prefix #"/"))
                   (not (re-find #"[\\%?#]" prefix)))
      (throw (ex-info "Invalid CloudTrail S3 evidence location" {})))
    (with-open [^DefaultCredentialsProvider credentials (.build (DefaultCredentialsProvider/builder))
                ^S3AsyncClient client (.build (doto (S3AsyncClient/builder)
                                 (.credentialsProvider credentials)
                                 (.region (Region/of region))
                                 (.overrideConfiguration (.build (doto (ClientOverrideConfiguration/builder)
                                                                  (.apiCallTimeout (Duration/ofSeconds 60))
                                                                  (.apiCallAttemptTimeout (Duration/ofSeconds 30)))))))]
      (loop [remaining (seq (prefixes request)), continuation nil, pages 0, total 0, retained []]
        (if-let [selected (first remaining)]
          (let [_ (when (= pages 20) (throw (ex-info "CloudTrail listing exceeds twenty pages" {})))
                ^ListObjectsV2Request listing (.build (cond-> (doto (ListObjectsV2Request/builder)
                                         (.bucket bucket) (.prefix selected) (.maxKeys (Integer/valueOf 100)))
                                  continuation (.continuationToken continuation)))
                ^ListObjectsV2Response response (.get (.listObjectsV2 client listing) 60 TimeUnit/SECONDS)
                objects (filter #(str/ends-with? (.key ^S3Object %) ".json.gz") (.contents response))
                result (reduce (fn [{:keys [total retained]} ^S3Object object]
                                 (let [size (long (.size object))
                                       _ (when (or (neg? size) (> size 8388608) (> (+ total size) 67108864) (= 1000 (count retained)))
                                           (throw (ex-info "CloudTrail compressed evidence exceeds its bound" {})))
                                       ^GetObjectRequest get (.build (doto (GetObjectRequest/builder)
                                                     (.bucket bucket) (.key (.key object)) (.ifMatch (.eTag object))
                                                     (.range "bytes=0-8388608")))
                                       ^ResponseBytes bytes (.get (.getObject client get (AsyncResponseTransformer/toBytes)) 60 TimeUnit/SECONDS)
                                       data (.asByteArray bytes)
                                       _ (when-not (= size (alength ^bytes data)) (throw (ex-info "CloudTrail object changed during capture" {})))
                                       file (.resolveSibling ^java.nio.file.Path output (str (.getFileName ^java.nio.file.Path output) ".object-" (count retained) ".json.gz"))]
                                   (Files/write file data (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))
                                   {:total (+ total size), :retained (conj retained {:bucket bucket, :key (.key object), :etag (.eTag object),
                                                                                :file (str (.getFileName file)), :bytes size, :sha256 (sha data)})}))
                               {:total total, :retained retained} objects)]
            (if (.isTruncated response)
              (recur remaining (.nextContinuationToken response) (inc pages) (long (:total result)) (:retained result))
              (recur (next remaining) nil (inc pages) (long (:total result)) (:retained result))))
          {:prefixes (vec (prefixes request)), :objects retained, :compressed_bytes total})))))

(let [[input-name output-name] *command-line-args*
      input (.toPath (io/file input-name)), output (.toPath (io/file output-name))]
  (when (> (Files/size input) 1048576) (throw (ex-info "CloudTrail request exceeds one MiB" {})))
  (let [bytes (Files/readAllBytes input), request (json/read-value bytes json/keyword-keys-object-mapper)
        result (execute request output)
        receipt {:schema_version 1, :request_sha256 (sha bytes), :execution_complete true, :clients_closed true, :result result}]
    (Files/write output (json/write-value-as-bytes receipt)
                 (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))))
