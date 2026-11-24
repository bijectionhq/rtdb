(ns rtdb.conformance.as-of-erase
  (:require [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import (java.time Duration Instant ZoneId)
           (xtdb.api TransactionKey)
           (xtdb.catalog TableCatalog$State)
           (xtdb.database Database)
           (xtdb.indexer LiveIndex$State)))

(set! *warn-on-reflection* true)

(def ^:private docs #xt/table docs)

(defn- block-index ^long [^Database database]
  (let [^TableCatalog$State catalog (.snap (.getTableCatalog database))]
    (long (or (.getBlockIdx catalog) -1))))

(defn- flush! [node ^Database database]
  (let [previous (block-index database)
        started (System/nanoTime)]
    (tu/flush-block! node (Duration/ofSeconds 30))
    (loop []
      (when-let [error (.getIngestionError database)] (throw error))
      (when (<= (block-index database) previous)
        (when (> (- (System/nanoTime) started) 30000000000)
          (throw (ex-info "Timed out awaiting an adopted erase fixture block" {})))
        (Thread/sleep 10)
        (recur)))))

(defn- same-tx? [^TransactionKey actual ^TransactionKey expected]
  (and (some? actual)
       (= (.getTxId actual) (.getTxId expected))
       (= (.getSystemTime actual) (.getSystemTime expected))))

(defn- require-layout! [^Database database ^TransactionKey transaction live?]
  (let [^LiveIndex$State live (.getState (.getLiveIndex database))
        ^TableCatalog$State catalog (.snap (.getTableCatalog database))
        index (.getBlockIdx catalog)
        tries (trie-cat/current-tries (.tableState (.getState (.getTrieCatalog database)) docs))]
    (when-not (and (some? index)
                   (same-tx? (.getLatestCompletedTx live) transaction)
                   (= (contains? (.getTables live) docs) live?)
                   (some #(and (= 0 (:level %)) (= index (:block-idx %))) tries)
                   (or live? (same-tx? (.getLatestCompletedTx catalog) transaction)))
      (throw (ex-info "Erase fixture is missing its applied transaction or required docs layout" {})))
    index))

(defn- observe-query [node query current-time live?]
  (let [rows (xt/q node (:sql query) {:current-time current-time})
        ids (mapv (fn [row]
                    (let [id (:xt/id row)]
                      (when-not (and (= [:xt/id] (vec (keys row))) (keyword? id))
                        (throw (ex-info "Erase fixture query must project a keyword _id" {})))
                      id))
                  rows)
        value (mapv (fn [id] {:kind "keyword", :namespace (namespace id), :name (name id)})
                    (sort-by str (set ids)))]
    {:id (if live? (:live_observation query) (:flushed_observation query))
     :executed true
     :outcome {:kind "returned", :typed_value value}}))

(defn observe [{:keys [schema_version timezone put_system_time put_keyword_ids erase_system_time erase_keyword_id query_time queries]}]
  (let [put-time (Instant/parse put_system_time)
        erase-time (Instant/parse erase_system_time)
        query-time (Instant/parse query_time)
        clock (atom put-time)]
    (when-not (and (= schema_version 1) (= timezone "UTC") (= (count queries) 2)
                   (= (count put_keyword_ids) 2) (= (count (set put_keyword_ids)) 2)
                   (contains? (set put_keyword_ids) erase_keyword_id)
                   (.isBefore put-time erase-time) (= query-time erase-time))
      (throw (ex-info "Invalid AS OF erase fixture" {})))
    (tu/with-tmp-dirs [directory]
      (util/with-open [node (tu/->local-node {:node-dir directory, :buffers-dir "storage",
                                             :instant-src (tu/->settable-clock clock),
                                             :compactor-threads 0, :gc? false, :default-tz (ZoneId/of "UTC")})]
        (let [^Database database (db/primary-db node)
              put-op (into [:put-docs :docs] (map (fn [id] {:xt/id (keyword id)})) put_keyword_ids)
              ^TransactionKey put (xt/execute-tx node [put-op])]
          (when-not (= put-time (.getSystemTime put))
            (throw (ex-info "Erase fixture put did not commit at its fixture clock reading" {})))
          (flush! node database)
          (let [put-block (require-layout! database put false)
                _ (reset! clock erase-time)
                ^TransactionKey erase (xt/execute-tx node [[:erase-docs :docs (keyword erase_keyword_id)]])]
            (when-not (= erase-time (.getSystemTime erase))
              (throw (ex-info "Erase fixture erase did not commit at its fixture clock reading" {})))
            (when-not (= put-block (require-layout! database erase true))
              (throw (ex-info "Erase fixture moved to a later block before its live queries" {})))
            (let [live (mapv #(observe-query node % query-time true) queries)]
              (flush! node database)
              (when-not (> (long (require-layout! database erase false)) (long put-block))
                (throw (ex-info "Erase fixture did not adopt a block after flushing the erase" {})))
              (into live (map #(observe-query node % query-time false)) queries))))))))
