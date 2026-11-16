(require '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jsonista.core :as json])
(import '(com.google.auth.oauth2 GoogleCredentials)
        '(java.io InputStream)
        '(java.net URI)
        '(java.net.http HttpClient HttpRequest HttpResponse HttpResponse$BodyHandlers HttpRequest$BodyPublishers)
        '(java.nio.file Files OpenOption StandardOpenOption)
        '(java.security MessageDigest)
        '(java.time Duration Instant)
        '(java.util HexFormat))

(set! *warn-on-reflection* true)

(defn- sha [^bytes value]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") value)))

(defn- quoted [value] (json/write-value-as-string value))

(defn- audit-filter [{:keys [bucket object principal_email started_at ended_at]}]
  (str/join " AND " [(str "resource.type=" (quoted "gcs_bucket"))
                     (str "resource.labels.bucket_name=" (quoted bucket))
                     (str "protoPayload.serviceName=" (quoted "storage.googleapis.com"))
                     (str "protoPayload.methodName=" (quoted "storage.objects.create"))
                     (str "protoPayload.resourceName=" (quoted (str "projects/_/buckets/" bucket "/objects/" object)))
                     (str "protoPayload.authenticationInfo.principalEmail=" (quoted principal_email))
                     "protoPayload.status.code=7"
                     (str "timestamp>=" (quoted started_at))
                     (str "timestamp<=" (quoted ended_at))]))

(defn- execute [request output]
  (let [{:keys [namespace project bucket object started_at ended_at]} request
        start (Instant/parse started_at)
        end (Instant/parse ended_at)]
    (when-not (and (= 1 (:schema_version request))
                   (re-matches #"rtdb-recovery-[a-z0-9-]{12,65}" namespace)
                   (re-matches #"[a-z][a-z0-9-]{4,61}[a-z0-9]" project)
                   (re-matches #"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]" bucket)
                   (= 1 (count (filter #{namespace} (str/split object #"/"))))
                   (not-any? #{"" "." ".."} (str/split object #"/"))
                   (re-matches #"[A-Za-z0-9_.+%-]+@[A-Za-z0-9.-]+" (:principal_email request))
                   (not (.isAfter start end))
                   (<= (.getSeconds (Duration/between start end)) 3600))
      (throw (ex-info "Invalid exact GCS audit request" {})))
    (let [uri (URI/create "https://logging.googleapis.com/v2/entries:list")
          ^GoogleCredentials credentials (.createScoped (GoogleCredentials/getApplicationDefault)
                                     ^java.util.Collection ["https://www.googleapis.com/auth/logging.read"])
          base {:resourceNames [(str "projects/" project)], :filter (audit-filter request), :orderBy "timestamp desc", :pageSize 100}]
      (with-open [^HttpClient client (.build (.connectTimeout (HttpClient/newBuilder) (Duration/ofSeconds 30)))]
        (loop [page 0, token nil, pages []]
          (when (= page 10) (throw (ex-info "GCS audit search exceeded ten response pages" {})))
          (let [body (cond-> base token (assoc :pageToken token))
                builder (doto (HttpRequest/newBuilder uri)
                          (.timeout (Duration/ofSeconds 60))
                          (.header "Content-Type" "application/json")
                          (.POST (HttpRequest$BodyPublishers/ofByteArray (json/write-value-as-bytes body))))]
            (doseq [[key values] (.getRequestMetadata credentials uri), value values]
              (.header builder ^String key ^String value))
            (let [^HttpResponse response (.send client (.build builder) (HttpResponse$BodyHandlers/ofInputStream))
                  bytes (with-open [^InputStream stream (.body response)] (.readNBytes stream (int 8388609)))
                  _ (when (> (alength ^bytes bytes) 8388608) (throw (ex-info "GCS audit response exceeds eight MiB" {})))
                  file (.resolveSibling ^java.nio.file.Path output (str (.getFileName ^java.nio.file.Path output) ".page-" page ".json"))
                  _ (Files/write file bytes (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))
                  _ (when-not (= 200 (.statusCode response)) (throw (ex-info "GCS audit logging is unavailable to the selected credentials" {:status (.statusCode response)})))
                  result (json/read-value bytes json/keyword-keys-object-mapper)
                  pages (conj pages {:file (str (.getFileName file)), :sha256 (sha bytes), :bytes (alength ^bytes bytes), :status 200})]
              (if-let [next-token (not-empty (:nextPageToken result))]
                (recur (inc page) next-token pages)
                {:query base, :pages pages}))))))))

(let [[input-name output-name] *command-line-args*
      input (.toPath (io/file input-name))
      output (.toPath (io/file output-name))]
  (when (> (Files/size input) 1048576) (throw (ex-info "GCS audit request exceeds one MiB" {})))
  (let [bytes (Files/readAllBytes input)
        request (json/read-value bytes json/keyword-keys-object-mapper)
        result (execute request output)
        receipt {:schema_version 1, :request_sha256 (sha bytes), :execution_complete true, :client_closed true, :result result}]
    (Files/write output (json/write-value-as-bytes receipt)
                 (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))))
