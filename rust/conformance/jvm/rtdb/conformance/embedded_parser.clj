(ns rtdb.conformance.embedded-parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jsonista.core :as json]
            [xtdb.error :as err])
  (:import (java.io File InputStream)
           (java.nio.file Files LinkOption OpenOption Path StandardOpenOption)
           (java.security MessageDigest)
           (java.util HexFormat)
           (java.util.regex Pattern)
           (java.util.stream Stream)))

(set! *warn-on-reflection* true)

(def ^:private no-links (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))
(def ^:private read-options (make-array OpenOption 0))
(def ^:private write-options (into-array OpenOption [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE]))
(def ^:private sql-ids #{:statement_mismatch :unwanted_token :follow_up})

(defn- sha256 [^bytes bytes]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)))

(defn- fixture-bytes [^Path path]
  (with-open [^InputStream stream (Files/newInputStream path read-options)]
    (let [bytes (.readNBytes stream 65537)]
      (when (> (alength bytes) 65536)
        (throw (err/incorrect ::fixture-size "Embedded parser fixture exceeds 64 KiB")))
      bytes)))

(defn- read-fixture [^bytes bytes]
  (let [{:keys [schema_version sql native] :as fixture} (json/read-value bytes json/keyword-keys-object-mapper)
        {:keys [yaml memory_bytes legacy_merge_sort]} native]
    (when-not (and (= #{:schema_version :sql :native} (set (keys fixture)))
                   (= 1 schema_version)
                   (= sql-ids (set (keys sql))) (every? #(and (string? %) (not (str/blank? %))) (vals sql))
                   (= #{:yaml :memory_bytes :legacy_merge_sort} (set (keys native)))
                   (string? yaml) (not (str/blank? yaml))
                   (integer? memory_bytes) (<= 1 memory_bytes Long/MAX_VALUE)
                   (boolean? legacy_merge_sort))
      (throw (err/incorrect ::fixture "Invalid embedded parser observation inputs")))
    fixture))

(defn- file-digest [^Path path]
  (with-open [^InputStream stream (Files/newInputStream path read-options)]
    (let [digest (MessageDigest/getInstance "SHA-256")
          buffer (byte-array 65536)]
      (loop []
        (let [count (.read stream buffer)]
          (when (pos? count)
            (.update digest buffer 0 count)
            (recur))))
      (.formatHex (HexFormat/of) (.digest digest)))))

(defn- file-record [^Path path]
  {:bytes (Files/size path), :sha256 (file-digest path)})

(defn- directory-files [^Path root]
  (with-open [^Stream stream (Files/walk root (make-array java.nio.file.FileVisitOption 0))]
    (->> (iterator-seq (.iterator stream))
         (remove #(= root %))
         (keep (fn [^Path path]
                 (cond
                   (Files/isDirectory path no-links) nil
                   (Files/isRegularFile path no-links) (assoc (file-record path) :path (str (.relativize root path)))
                   :else (throw (err/incorrect ::artifact-type "Runtime directory contains a redirected or non-regular file")))))
         (sort-by :path)
         vec)))

(defn- artifact-record [^File file]
  (let [path (.toPath file)
        name (.getPath file)]
    (cond
      (Files/isDirectory path no-links) {:path name, :kind "directory", :files (directory-files path)}
      (Files/isRegularFile path no-links) (assoc (file-record path) :path name, :kind "file")
      :else (throw (err/incorrect ::artifact-type "Runtime artifact is missing or non-regular")))))

(defn- canonical-file ^File [path]
  (.getCanonicalFile (io/file path)))

(defn- class-location [^Class clazz]
  (let [^java.net.URL location (.. clazz getProtectionDomain getCodeSource getLocation)]
    (.getPath (canonical-file (io/file (.toURI location))))))

(defn runtime-closure
  "Returns this JVM's classpath and JDK inventory and the locations of the supplied Class values.
  Pass nil for library-path when the observer has no native library."
  [classes library-path]
  (let [classpath (mapv canonical-file (str/split (System/getProperty "java.class.path")
                                                (Pattern/compile (Pattern/quote File/pathSeparator)) -1))
        {:keys [present absent]} (group-by #(if (.exists ^File %) :present :absent) classpath)
        java-home (System/getProperty "java.home")
        jdk (mapv canonical-file [(str java-home "/bin/java") (str java-home "/lib/modules")])
        ^File library (some-> library-path canonical-file)
        files (distinct (concat present jdk (when library [library])))]
    {:classpath (mapv #(.getPath ^File %) classpath)
     :jdk (mapv #(.getPath ^File %) jdk)
     :absent_classpath (mapv #(.getPath ^File %) absent)
     :class_locations (into (sorted-map) (map (fn [^Class clazz] [(.getName clazz) (class-location clazz)])) classes)
     :library (some-> library .getPath)
     :artifacts (mapv artifact-record files)}))

(defn- write-bytes! [^Path output ^String name ^bytes bytes]
  (Files/write (.resolve output name) bytes write-options))

(defn throwable-class [^Throwable cause]
  (some-> cause class .getName))

(defn cause-class [^Throwable cause]
  (when cause {:class (throwable-class cause)}))

(defn failure [stage category code signature ^Throwable error]
  (if (and (string? category) (not (str/blank? category)) (string? code) (not (str/blank? code)))
    {:kind "expected_failure", :stage stage, :category category, :code code, :signature signature}
    {:kind "unexpected_failure", :stage stage, :code (throwable-class error), :detail (or (ex-message error) "")}))

(defn returned [value]
  {:kind "returned", :typed_value value})

(defn observe-fixture! [side classes library-path observe fixture-path output-path]
  (let [path (.toPath (io/file fixture-path))
        output (.toPath (io/file output-path))
        bytes (fixture-bytes path)
        fixture (read-fixture bytes)
        fixture-digest (sha256 bytes)
        runtime (runtime-closure classes library-path)
        result (observe fixture)]
    (when-not (= fixture-digest (sha256 (fixture-bytes path)))
      (throw (err/incorrect ::fixture-changed "Embedded parser fixture changed during observation")))
    (when-not (= runtime (runtime-closure classes library-path))
      (throw (err/incorrect ::artifacts-changed "Embedded parser runtime artifacts changed during observation")))
    (let [runtime-bytes (json/write-value-as-bytes runtime)]
      (write-bytes! output "runtime-artifacts.json" runtime-bytes)
      (write-bytes! output "execution.json"
                    (json/write-value-as-bytes
                     (merge result {:schema_version 1, :side side, :fixture_sha256 fixture-digest,
                                    :binary_sha256 (sha256 runtime-bytes)}))))))
