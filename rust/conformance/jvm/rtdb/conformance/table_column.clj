(ns rtdb.conformance.table-column
  (:require [xtdb.error :as err]
            [xtdb.test-util :as tu])
  (:import (java.math BigInteger)
           (org.apache.arrow.vector.types FloatingPointPrecision)
           (org.apache.arrow.vector.types.pojo ArrowType$FloatingPoint ArrowType$Int ArrowType$Null)
           (xtdb.arrow VectorType)))

(set! *warn-on-reflection* true)

(defn- decode-scalar [value]
  (cond
    (= "Null" value) nil
    (and (map? value) (= 1 (count value)) (contains? value :I64)
         (integer? (:I64 value)) (<= Long/MIN_VALUE (:I64 value) Long/MAX_VALUE))
    (long (:I64 value))
    (and (map? value) (= 1 (count value)) (contains? value :F64)
         (integer? (:F64 value)) (<= 0 (:F64 value) 18446744073709551615N))
    (Double/longBitsToDouble (.longValue ^Number (:F64 value)))
    :else (throw (err/unsupported ::fixture-type "Column-table fixtures require null, I64 or F64 scalar values"))))

(defn- decode-input [value]
  (if (and (map? value) (= 1 (count value)) (vector? (:List value)))
    (mapv decode-scalar (:List value))
    (decode-scalar value)))

(defn- encode-cell [value]
  (cond
    (nil? value) "Null"
    (instance? Long value) {:I64 value}
    (instance? Double value) {:F64 (BigInteger. (Long/toUnsignedString (Double/doubleToRawLongBits (double value))))}
    :else (throw (err/unsupported ::result-type "Column-table result has an unsupported scalar class"))))

(defn- encode-leg [^VectorType type]
  (let [arrow (.getArrowType type)]
    (cond
      (instance? ArrowType$Null arrow) [0 "Null"]
      (and (instance? ArrowType$Int arrow) (= 64 (.getBitWidth ^ArrowType$Int arrow)) (.getIsSigned ^ArrowType$Int arrow)) [1 {:Scalar "I64"}]
      (and (instance? ArrowType$FloatingPoint arrow) (= FloatingPointPrecision/DOUBLE (.getPrecision ^ArrowType$FloatingPoint arrow))) [2 {:Scalar "F64"}]
      :else (throw (err/unsupported ::result-type "Column-table result has an unsupported vector type")))))

(defn- encode-type [^VectorType type]
  (let [legs (mapv second (sort-by first (map encode-leg (.getLegs type))))]
    (case (count legs)
      0 "Nothing"
      1 (first legs)
      {:Union legs})))

(defn observe
  "Executes null, I64, F64 and flat-list fixture inputs through the production Table-column operator."
  [cases]
  (tu/with-allocator
    (fn []
      (mapv (fn [{:keys [id column parameter input]}]
              (let [input (decode-input input)
                    plan [:table {:column {(symbol column) (if parameter '?coll input)}}]
                    result (tu/query-ra plan (cond-> {:with-types? true, :key-fn :snake-case-string}
                                               parameter (assoc :args {:coll input})))
                    [name type] (first (:types result))
                    name (str name)]
                (when-not (= 1 (count (:types result)))
                  (throw (err/fault ::result-width "Column-table fixture returned an unexpected schema width")))
                {:id id, :executed true,
                 :outcome {:kind "returned", :typed_value {name {:data_type (encode-type type),
                                                                :rows (mapv (fn [row] [(encode-cell (get row name))]) (:res result))}}}}))
            cases))))
