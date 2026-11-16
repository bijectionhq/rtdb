(ns rtdb.conformance.sql-logic
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.error :as err]
            [xtdb.sql.logic-test.runner :as slt]
            [xtdb.sql.logic-test.xtdb-engine :as engine]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.io BufferedReader BufferedWriter File InputStream Writer)
           (java.nio.file Files LinkOption OpenOption Path StandardOpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.security MessageDigest)
           (java.util HexFormat)))

(set! *warn-on-reflection* true)

(def ^:private no-links (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private mapper json/keyword-keys-object-mapper)
(def ^:private maximum-line (* 64 1024 1024))
(def ^:private maximum-plan (* 16 1024 1024))

(defn- read-plan [^Path path]
  (util/with-open [^InputStream stream (Files/newInputStream path (make-array OpenOption 0))]
    (let [bytes (.readNBytes stream (int (inc maximum-plan)))]
      (when (> (alength bytes) maximum-plan)
        (throw (err/incorrect ::plan-limit "SLT plan exceeds bounded size")))
      (json/read-value bytes mapper))))

(defn- validate-options [{:keys [direct_sql decorrelate query_limit max_rows max_result_bytes]}]
  (when-not (and (boolean? direct_sql) (boolean? decorrelate)
                 (or (nil? query_limit) (and (integer? query_limit) (<= 0 query_limit Long/MAX_VALUE)))
                 (integer? max_rows) (<= 1 max_rows Long/MAX_VALUE)
                 (integer? max_result_bytes) (<= 1 max_result_bytes (* 512 1024 1024)))
    (throw (err/incorrect ::options "Invalid SQL logic execution limit"))))

(defn- file-digest [^Path path]
  (util/with-open [^InputStream stream (Files/newInputStream path (make-array OpenOption 0))]
    (let [digest (MessageDigest/getInstance "SHA-256") buffer (byte-array 65536)]
      (loop []
        (let [count (.read stream buffer)]
          (when (pos? count) (.update digest buffer 0 count) (recur))))
      (.formatHex (HexFormat/of) (.digest digest)))))

(defn- directory-files [^Path root]
  (util/with-open [stream (Files/walk root (make-array java.nio.file.FileVisitOption 0))]
    (->> (iterator-seq (.iterator ^java.util.stream.Stream stream))
         (remove #(= root %))
         (keep (fn [^Path path]
                 (cond
                   (Files/isSymbolicLink path) (throw (err/incorrect ::artifact-symlink "Runtime artifact is a symbolic link"))
                   (Files/isDirectory path no-links) nil
                   (Files/isRegularFile path no-links) {:path (str (.relativize root path)), :bytes (Files/size path), :sha256 (file-digest path)}
                   :else (throw (err/incorrect ::artifact-type "Runtime artifact is not a regular file")))))
         (sort-by :path) vec)))

(defn- runtime-artifacts []
  (mapv (fn [^File file]
          (let [path (.toPath file)]
            (if (.isDirectory file)
              {:path (.getCanonicalPath file), :kind "directory", :files (directory-files path)}
              {:path (.getCanonicalPath file), :kind "file", :bytes (Files/size path), :sha256 (file-digest path)})))
        (map io/file (concat (str/split (System/getProperty "java.class.path")
                                      (re-pattern (java.util.regex.Pattern/quote File/pathSeparator)))
                             [(str (System/getProperty "java.home") "/bin/java")
                              (str (System/getProperty "java.home") "/lib/modules")]))))

(defn- write-new! [^Path path value]
  (let [bytes (json/write-value-as-bytes value)]
    (util/with-open [channel (java.nio.channels.FileChannel/open path
                             (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))]
      (let [buffer (java.nio.ByteBuffer/wrap bytes)]
        (while (.hasRemaining buffer) (.write ^java.nio.channels.FileChannel channel buffer)))
      (.force ^java.nio.channels.FileChannel channel true))))

(defn- read-entry [^BufferedReader reader]
  (let [line (StringBuilder.)]
    (loop []
      (let [character (.read reader)]
        (cond
          (= -1 character) (when (pos? (.length line)) (throw (err/incorrect ::incomplete-line "SLT artifact lacks final newline")))
          (= 10 character) (json/read-value (str line) mapper)
          (>= (.length line) maximum-line) (throw (err/incorrect ::oversized-line "SLT record artifact exceeds reader limit"))
          :else (do (.append line (char character)) (recur)))))))

(defn canonical-record [{:keys [type mode statement query type-string sort-mode label result-set-size result-set-md5sum result-set
                                 max-result-set-size variable value skipif onlyif]}]
  (cond-> (case type
            :statement {:type "statement", :mode (name mode), :sql statement}
            :query {:type "query", :sql query, :type_string type-string, :sort (name sort-mode), :label label,
                    :expected (if result-set-md5sum {:count result-set-size, :md5 result-set-md5sum}
                                  {:count result-set-size, :text result-set})}
            :hash-threshold {:type "hash-threshold", :threshold max-result-set-size}
            :variable {:type "variable", :variable variable, :value value}
            :halt {:type "halt"})
    skipif (assoc :skipif skipif)
    onlyif (assoc :onlyif onlyif)))

(defrecord ObservedEngine [delegate observation options]
  slt/DbEngine
  (get-engine-name [_] (slt/get-engine-name delegate))
  (execute-statement [_ statement variables]
    (vswap! observation assoc :engine_executed true)
    (->ObservedEngine (slt/execute-statement delegate statement variables) observation options))
  (execute-query [_ query variables]
    (vswap! observation assoc :engine_executed true)
    (let [rows (slt/execute-query delegate query variables)]
      (when (> (count rows) (:max_rows options))
        (throw (err/unsupported ::result-limit "SLT result exceeds retained row limit")))
      (vswap! observation assoc :raw-result rows)
      rows)))

(defn- exception-value [^Throwable error]
  {:class (.getName (class error)), :message (ex-message error),
   :suppressed (mapv (fn [^Throwable error] {:class (.getName (class error)), :message (ex-message error)}) (.getSuppressed error))})

(defn execute-one
  "Applies the pinned validation record method and its surrounding update/catch semantics.
  Returns the next transient runner context and an evidence value; it does not publish either."
  [ctx record identity options]
  (let [observation (volatile! {:identity identity, :executed true, :engine_executed false, :result nil, :error nil,
                           :kind (name (:type record)), :label (:label record), :query_count_before (:queries-run ctx),
                           :hash_threshold (:max-result-set-size ctx)})
        reports (volatile! [])
        engine (:db-engine ctx)
        ctx (assoc ctx :db-engine (if (instance? ObservedEngine engine)
                                   (assoc engine :observation observation)
                                   (->ObservedEngine engine observation options)))
        query-count (if (= :query (:type record)) 1 0)
        [next-ctx failure] (try
                             [(with-redefs [t/do-report (fn [report] (vswap! reports conj report))]
                                (-> (slt/execute-record ctx record)
                                    (update :queries-run + query-count))) nil]
                             (catch Throwable error
                               (let [suppressed? (or (contains? (set (:skipif record)) "postgresql")
                                                     (str/includes? (or (ex-message error) "") "Duplicate column projection"))]
                                 (when-not suppressed?
                                   (vswap! reports conj {:type :error, :actual error, :file (:file record), :line (:line record)}))
                                 [(cond-> (update ctx :queries-run + query-count)
                                    (not suppressed?) (update-in [:results :error] (fnil inc 0))) error])))
        raw-result (:raw-result @observation)
        result (when (and (nil? failure) (contains? @observation :raw-result))
                 (let [text (#'slt/format-result-str (:sort-mode record) (:type-string record) raw-result)]
                   (when (> (count (.getBytes ^String text java.nio.charset.StandardCharsets/UTF_8)) (:max_result_bytes options))
                     (throw (err/unsupported ::result-limit "SLT result exceeds retained byte limit")))
                   {:text text, :md5 (#'slt/md5 text), :rows (count raw-result), :values (reduce + (map count raw-result))}))
        disposition (cond
                      (and failure (= ::result-limit (:xtdb.error/code (ex-data failure)))) "harness_gap"
                      (and failure (= :halt (:type record))) "halt_error"
                      failure (cond
                                (contains? (set (:skipif record)) "postgresql") "suppressed_postgresql_error"
                                (str/includes? (or (ex-message failure) "") "Duplicate column projection") "suppressed_duplicate_projection"
                                :else "error")
                      (contains? #{:variable :hash-threshold} (:type record)) "directive"
                      (not (:engine_executed @observation)) "engine_condition"
                      (= :statement (:type record)) (if (= :error (:mode record))
                                                     (if (some #(= :pass (:type %)) @reports) "statement_expected_error" "statement_expected_error_missing")
                                                     "statement_ok")
                      (= :query (:type record)) (if (some #(= :fail (:type %)) @reports) "query_mismatch" "query_ok")
                      :else "unknown")]
    [next-ctx (-> @observation (dissoc :raw-result)
                  (assoc :result result, :error (some-> (or failure (some #(when (instance? Throwable (:actual %)) (:actual %)) @reports)) exception-value), :disposition disposition,
                         :query_count_after (:queries-run next-ctx),
                         :expected (:expected (canonical-record record)),
                         :reports (mapv #(select-keys % [:type :file :line]) @reports)))]))

(defn- validate-entry [plan script entry ordinal]
  (let [identity (:identity entry)]
    (when-not (and (= ordinal (:ordinal entry) (:ordinal identity)) (pos? (:line entry))
                   (= (:line entry) (:line identity)) (= (:assertion_ordinal entry) (:assertion_ordinal identity))
                   (= (:reference plan) (:reference identity)) (= (:source script) (:source identity))
                   (= (:source_sha256 script) (:source_sha256 identity)) (= (:selection script) (:selection identity))
                   (= (:options script) (:options identity)))
      (throw (err/incorrect ::identity "SLT record identity differs from retained source selection")))))

(defn- execute-script [plan script ^Path input ^Path output]
  (let [options (:options script)
        _ (validate-options options)
        run (fn []
              (util/with-open [reader (io/reader (.toFile input))
                               writer (Files/newBufferedWriter output (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))]
                (loop [ctx (merge {:db-engine slt/*db-engine*, :queries-run 0, :results {}} slt/*opts*), count 0, assertions 0, dispositions {}]
                  (if-let [entry (read-entry reader)]
                    (let [ordinal (inc count)
                          _ (validate-entry plan script entry ordinal)
                          assertions (if-let [assertion (:assertion_ordinal entry)]
                                       (let [expected (inc assertions)]
                                         (when-not (= expected assertion)
                                           (throw (err/incorrect ::assertion-ordinal "SLT assertion ordinal differs from original record order")))
                                         expected)
                                       assertions)
                          unavailable (cond (pos? (:parse_errors script)) "script_parse_error"
                                            (= (:queries-run ctx) (:query_limit options)) "query_limit")
                          [ctx receipt] (if unavailable
                                          [ctx {:identity (:identity entry), :executed false, :engine_executed false, :disposition unavailable,
                                                :query_count_before (:queries-run ctx), :query_count_after (:queries-run ctx)}]
                                          (let [record (slt/parse-record (:lines entry))]
                                            (when-not (= (:record entry) (canonical-record record))
                                              (throw (err/incorrect ::record-parser "Streaming parser differs from pinned parse-record")))
                                            (execute-one ctx (assoc record :file (:source script), :line (:line entry)) (:identity entry) options)))]
                      (.write ^BufferedWriter writer ^String (json/write-value-as-string receipt)) (.newLine ^BufferedWriter writer)
                      (recur ctx ordinal assertions (update dispositions (:disposition receipt) (fnil inc 0))))
                    (do
                      (when-not (and (= count (:records script)) (= assertions (:assertions script)))
                        (throw (err/incorrect ::record-count "SLT record or assertion count differs from plan")))
                      (.flush ^BufferedWriter writer)
                      {:source (:source script), :selection (:selection script), :records count, :queries_run (:queries-run ctx),
                       :dispositions dispositions, :pinned_results (:results ctx), :execution_complete true})))))]
    (binding [slt/*opts* {:script-mode :validation, :direct-sql (:direct_sql options), :decorrelate? (:decorrelate options), :query-limit (:query_limit options)}
              *out* (Writer/nullWriter)]
      (tu/with-mock-clock (fn [] (tu/with-node (fn [] (slt/with-xtdb run))))))))

(defn -main [& arguments]
  (when-not (= 2 (count arguments)) (throw (err/incorrect ::arguments "Expected PLAN.json OUTPUT")))
  (let [plan-path (.normalize (.toAbsolutePath (.toPath (io/file (first arguments)))))
        output (.normalize (.toAbsolutePath (.toPath (io/file (second arguments)))))
        plan-sha (file-digest plan-path)
        plan (read-plan plan-path)
        artifacts (runtime-artifacts)]
    (when-not (and (= 1 (:schema_version plan)) (= "c84f9334eb060ceb92d28ecdd4a06e6b625932fd" (:reference plan)) (seq (:scripts plan)))
      (throw (err/incorrect ::plan "Invalid SQL logic selection plan")))
    (Files/createDirectory output (make-array FileAttribute 0))
    (write-new! (.resolve output "runtime-artifacts.json") artifacts)
    (let [scripts (mapv (fn [index script]
                          (let [relative (Path/of (:records_file script) (make-array String 0))
                                _ (when (or (.isAbsolute relative) (some #{"." ".."} (map str (iterator-seq (.iterator relative)))))
                                    (throw (err/incorrect ::records-path "SLT record path escapes plan directory")))
                                input (.resolve (.getParent plan-path) relative)
                                filename (format "script-%05d.jsonl" index)
                                receipt-path (.resolve output filename)]
                            (when-not (= (:records_sha256 script) (file-digest input)) (throw (err/incorrect ::input-changed "SLT record artifact changed")))
                            (let [receipt (try (execute-script plan script input receipt-path)
                                               (catch Throwable error {:source (:source script), :selection (:selection script), :execution_complete false, :error (exception-value error)}))]
                              (when-not (= (:records_sha256 script) (file-digest input)) (throw (err/incorrect ::input-changed "SLT record artifact changed during execution")))
                              (cond-> receipt
                                (Files/exists receipt-path no-links) (assoc :observations {:path filename, :sha256 (file-digest receipt-path)})))))
                        (range) (:scripts plan))
          execution {:schema_version 1, :engine "jvm", :selection (:selection plan), :plan_sha256 plan-sha,
                     :artifact_revision (:reference plan), :binary_sha256 (file-digest (.resolve output "runtime-artifacts.json")),
                     :execution_complete (every? :execution_complete scripts), :complete_parity false, :scripts scripts, :artifacts artifacts,
                     :environment {:timezone "UTC", :locale "en_US", :source_clock "2020-01-01T00:00:00Z plus one day per submitted transaction",
                                   :query_clock "wall_clock_or_CURRENT_TIMESTAMP_variable"}}]
      (when-not (= artifacts (runtime-artifacts)) (throw (err/incorrect ::runtime-changed "JVM artifacts changed during SQL logic execution")))
      (when-not (= plan-sha (file-digest plan-path)) (throw (err/incorrect ::plan-changed "SLT plan changed during execution")))
      (write-new! (.resolve output "execution.json") execution)
      (when-not (:execution_complete execution) (throw (err/fault ::execution-failed "SQL logic script did not complete; inspect retained receipts"))))))
