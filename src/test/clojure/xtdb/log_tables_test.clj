(ns xtdb.log-tables-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.log-tables :as log-tables]
            [xtdb.test-util :as tu])
  (:import (com.google.protobuf ByteString)
           (java.nio ByteOrder)
           (xtdb.arrow Relation)))

(t/use-fixtures :each tu/with-mock-clock tu/with-allocator tu/with-node)

(deftest test-source-log-msgs-basic
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 2, :v "world"}]])

  (t/testing "can read source log messages with BETWEEN bounds"
    (let [rows (xt/q tu/*node*
                     "SELECT msg_id, log_offset, log_timestamp, msg, tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")]
      (t/is (pos? (count rows)) "should return at least one row")

      (t/testing "msg column contains readable EDN with :type key"
        (t/is (string? (:msg (first rows))))
        (t/is (re-find #":type" (:msg (first rows)))))

      (t/testing "tx_ops column is present for tx messages"
        (let [rows-with-tx-ops (filter :tx-ops rows)]
          (when (seq rows-with-tx-ops)
            (t/is (string? (:tx-ops (first rows-with-tx-ops)))))))))

  (t/testing "msg_id and log_offset are present and numeric"
    (let [rows (xt/q tu/*node*
                     "SELECT msg_id, log_offset FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")]
      (doseq [row rows]
        (t/is (number? (:msg-id row)))
        (t/is (number? (:log-offset row))))))

  (t/testing "empty range returns no rows"
    (let [rows (xt/q tu/*node*
                     "SELECT * FROM xt.source_log_msgs WHERE msg_id BETWEEN 99999 AND 99999")]
      (t/is (empty? rows)))))

(deftest test-source-log-msgs-tx-ops-decoded
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])

  (let [rows (xt/q tu/*node*
                   "SELECT msg, tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")
        tx-rows (filter :tx-ops rows)]
    (t/is (seq tx-rows) "should have at least one tx message")

    (t/testing "tx_ops contains decoded operations"
      (t/is (re-find #"put-docs" (:tx-ops (first tx-rows)))))))

(deftest test-source-log-msgs-tx-ops-framing
  (let [value (apply str (repeat 4097 "a"))
        ^ByteString ipc (with-open [rel (Relation/openFromCols tu/*allocator* {"value" [value]})]
                          (.toArrowStream rel 0 1))
        schema-end (+ 8 (-> (.substring ipc 4 8) (.asReadOnlyByteBuffer) (.order ByteOrder/LITTLE_ENDIAN) (.getInt)))
        schema-only (.substring ipc 0 (int schema-end))
        eos (ByteString/copyFrom (byte-array 4))
        decode #(#'log-tables/decode-tx-ops tu/*allocator* %)]
    (t/is (> (count (.asReadOnlyByteBufferList ipc)) 1))
    (t/is (= (pr-str [{:value value}]) (decode ipc)))
    (t/is (= (pr-str [{:value value}]) (decode (.substring ipc 0 (- (.size ipc) 4)))))
    (t/is (= "[]" (decode (.concat schema-only eos))))
    (t/is (nil? (decode nil)))
    (doseq [truncated [ByteString/EMPTY schema-only (.concat schema-only (.substring eos 0 2))
                       (.substring ipc 0 (- (.size ipc) 5))]]
      (t/is (= "<error decoding tx-ops>" (decode truncated))))))
