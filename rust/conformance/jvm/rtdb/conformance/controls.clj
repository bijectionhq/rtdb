(ns rtdb.conformance.controls
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jsonista.core :as json])
  (:import (com.carrotsearch.hppc LongArrayList)
           (java.io File)
           (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.security MessageDigest)
           (java.util HexFormat)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector.types.pojo ArrowType$Struct)
           (xtdb.api.log ReplicaMessage ReplicaMessage$Codec)
           (xtdb.arrow Encoding$Plain Relation Vector)
           (xtdb.bitemporal Polygon PolygonCalculator)
           (xtdb.trie EventRowPointer Trie)
           (xtdb.util TemporalBounds TemporalDimension)))

(set! *warn-on-reflection* true)

(defn- sha256 [^bytes bytes]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)))

(defn- file-digest [^File file]
  (with-open [stream (io/input-stream file)]
    (let [digest (MessageDigest/getInstance "SHA-256")
          buffer (byte-array 65536)]
      (loop []
        (let [n (.read stream buffer)]
          (when (pos? n)
            (.update digest buffer 0 n)
            (recur))))
      (.formatHex (HexFormat/of) (.digest digest)))))

(defn- artifact [^File path]
  (if (.isDirectory path)
    {:path (.getCanonicalPath path)
     :kind "directory"
     :files (mapv (fn [^File f]
                    {:path (str (.relativize (.toPath path) (.toPath f)))
                     :sha256 (file-digest f)})
                  (sort-by #(.getPath ^File %) (filter #(.isFile ^File %) (file-seq path))))}
    {:path (.getCanonicalPath path), :kind "file", :sha256 (file-digest path)}))

(defn- artifacts []
  (mapv (comp artifact io/file)
        (concat (str/split (System/getProperty "java.class.path")
                          (re-pattern (java.util.regex.Pattern/quote File/pathSeparator)))
                [(str (System/getProperty "java.home") "/bin/java")
                 (str (System/getProperty "java.home") "/lib/modules")])))

(defn- bytes-from-hex ^bytes [^String text]
  (.parseHex (HexFormat/of) text))

(defn- returned [id value]
  {:id id, :executed true, :outcome {:kind "returned", :typed_value value}})

(defn- write-event [^Relation relation event]
  (.writeBytes ^Vector (.get relation "_iid") (ByteBuffer/wrap (bytes-from-hex (:iid event))))
  (.writeLong ^Vector (.get relation "_system_from") (long (:system_from event)))
  (.writeLong ^Vector (.get relation "_valid_from") (long (:valid_from event)))
  (.writeLong ^Vector (.get relation "_valid_to") (long (:valid_to event)))
  (let [^Vector op (.get relation "op")]
    (case (:operation event)
      "put" (.writeObject ^Vector (.vectorFor op "put" ArrowType$Struct/INSTANCE false) {"x" 0})
      "delete" (.writeNull ^Vector (.get op "delete"))
      "erase" (.writeNull ^Vector (.get op "erase"))))
  (.endRows relation 1))

(defn- bounds ^TemporalBounds [value]
  (when value
    (let [dimension (fn [value]
                      (TemporalDimension. (long (:lower value)) (long (:upper value))))]
      (TemporalBounds. (dimension (:valid_time value)) (dimension (:system_time value))))))

(defn- polygon-values [^Polygon polygon]
  (when polygon
    {"valid_times" (vec (.toArray ^LongArrayList (.getValidTimes polygon)))
     "system_time_ceilings" (vec (.toArray ^LongArrayList (.getSysTimeCeilings polygon)))}))

(defn- temporal-observations [{:keys [events observations query_bounds]}]
  (with-open [allocator (RootAllocator.)
              relation (Trie/openLogDataWriter allocator (.dataRelSchema Trie/INSTANCE nil Encoding$Plain/INSTANCE))]
    (doseq [event events] (write-event relation event))
    (let [calculator (PolygonCalculator. (bounds query_bounds))
          pointer (EventRowPointer. relation (byte-array 0))
          polygons (loop [result []]
                     (if (.isValid pointer)
                       (let [value (polygon-values (.calculate calculator pointer))]
                         (.nextIndex pointer)
                         (recur (conj result value)))
                       result))]
      (mapv (fn [{:keys [id event_index field]}]
              (when-not (contains? #{"valid_times" "system_time_ceilings"} field)
                (throw (ex-info "Unknown polygon observation field" {:field field})))
              (returned id (get (nth polygons event_index) field)))
            observations))))

(defn- replica-observations [{:keys [envelopes]}]
  (mapv (fn [{:keys [name bytes_hex]}]
          (let [^ReplicaMessage message (.decode ^ReplicaMessage$Codec ReplicaMessage/Codec (bytes-from-hex bytes_hex))
                ^ReplicaMessage round-trip (.decode ^ReplicaMessage$Codec ReplicaMessage/Codec (.encode message))]
            (returned (str name "/decoded") {:term (.getTermId round-trip), :term_sequence (.getTermSeq round-trip)})))
        envelopes))

(defn -main [fixture-path output-path]
  (let [input (Files/readAllBytes (.toPath (io/file fixture-path)))
        fixture (json/read-value input json/keyword-keys-object-mapper)
        observations (case (:kind fixture)
                       "temporal_ceilings" (temporal-observations fixture)
                       "replica_term_sequence" (replica-observations fixture))
        ids (mapv :id observations)
        artifacts (artifacts)]
    (when (or (empty? ids) (some str/blank? ids) (not= (count ids) (count (distinct ids))))
      (throw (ex-info "Fixture must produce distinct nonempty observations" {})))
    (spit output-path
          (json/write-value-as-string
            {:schema_version 1, :fixture_sha256 (sha256 input)
             :binary_sha256 (sha256 (json/write-value-as-bytes artifacts))
             :artifacts artifacts, :observations observations}))))
