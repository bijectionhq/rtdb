(ns rtdb.conformance.embedded-parser-native
  (:require [rtdb.conformance.embedded-parser :as observer]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.rust])
  (:import (java.io File)
           (org.apache.arrow.adbc.core AdbcStatement$QueryResult)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowReader)
           (xtdb.api.error Incorrect)
           (xtdb.rust NativeConnection NativeDatabase NativeException NativeStatement)))

(set! *warn-on-reflection* true)

(defn- prepare [^NativeConnection connection ^String sql]
  (with-open [^NativeStatement statement (.createStatement connection sql)]
    (try
      (.prepare statement)
      {:outcome (observer/returned nil)}
      (catch NativeException error
        {:outcome (observer/failure "statement_prepare" (.category error) (.code error)
                                    {:cause (observer/cause-class (ex-cause error))} error)
         :diagnostics {:status (.name (.getStatus error)), :message (ex-message error),
                       :transit_bytes (alength (.transit error))}}))))

(defn- query [^NativeConnection connection ^String sql]
  (try
    {:outcome (observer/returned (xt/q connection sql))}
    (catch Incorrect error
      (let [data (ex-data error)
            category (:cognitect.anomalies/category data)
            code (:xtdb.error/code data)
            cause (ex-cause error)]
        {:outcome (observer/failure "clojure_query" (when (keyword? category) (name category))
                                    (when (keyword? code) (subs (str code) 1))
                                    {:cause (when cause {:class (observer/throwable-class cause)
                                                         :cause (observer/cause-class (ex-cause cause))})}
                                    error)
         :message (ex-message error), :sql (:sql data)}))))

(defn- follow-up [^NativeConnection connection ^String sql]
  (with-open [^NativeStatement statement (.createStatement connection sql)
              ^AdbcStatement$QueryResult result (.executeQuery statement)]
    (let [^ArrowReader reader (.getReader result)
          present (.loadNextBatch reader)
          ^VectorSchemaRoot root (.getVectorSchemaRoot reader)
          rows (if present (.getRowCount root) 0)
          ^FieldVector column (when (pos? rows) (.getVector root "answer"))
          answer (when column (.getObject column 0))]
      {:outcome (observer/returned {:first_batch present, :rows rows, :answer answer,
                                    :additional_batch (if present (.loadNextBatch reader) false)})})))

(defn- observe [{:keys [sql native]}]
  (let [{:keys [statement_mismatch unwanted_token follow_up]} sql
        {:keys [yaml memory_bytes legacy_merge_sort]} native]
    (with-open [^RootAllocator allocator (RootAllocator.)
                ^NativeDatabase database (NativeDatabase/open allocator ^String yaml (long memory_bytes) (boolean legacy_merge_sort))
                ^NativeConnection connection (.connect database)]
      (let [mismatch (prepare connection statement_mismatch)
            wrapped-mismatch (query connection statement_mismatch)]
        {:observations [(assoc (select-keys mismatch [:outcome]) :id "statement_mismatch" :executed true)]
         :supplemental {:statement_mismatch (dissoc mismatch :outcome)
                        :wrapped_mismatch wrapped-mismatch
                        :unwanted_token (prepare connection unwanted_token)
                        :wrapped_unwanted (query connection unwanted_token)
                        :follow_up (follow-up connection follow_up)}}))))

(defn -main [fixture-path output-path]
  (let [path (System/getProperty "xtdb.native.library")]
    (when-not (and path (.isAbsolute (File. path)))
      (throw (err/incorrect ::native-library "An absolute packaged native library path is required")))
    (observer/observe-fixture! "native" [NativeDatabase NativeConnection NativeStatement NativeException Incorrect]
                   path observe fixture-path output-path)))
