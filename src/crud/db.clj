(ns crud.db
  (:require [honeysql.core :as sql]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [crud.utils :as utils]
            [com.netflix.hystrix.core :as hystrix]
            [bf.data.lazy-map :as lazy]))

(hystrix/defcommand fetch
  "Fetch from a table using the table name (made singular) as a namespace on the
  resulset, fallback to (). `keys` are in honeysql syntax.
  Eg: :users {:where [:= :id 1]} => ({:user/id 1, …}, …). You can pass a
  full-blown honeysql {:select … :from … :where …} map in `keys`."
  {:hystrix/fallback-fn (constantly ())}
  [db table keys & {:keys [opts]}]
  (try
    (when (:where keys)
      (as-> {:select (utils/pullq->select (:pull opts '[*]))
             :from   [(utils/table-name table)]} <>
        (merge <> keys)
        (sql/format <>)
        (jdbc/query db <> (dissoc opts :pull))
        (utils/namespacize table <>)
        (map #(lazy/->?LazyMap %) <>)))
    (catch Throwable t
      (log/error t (str "Unable to fetch " table " with params " keys))
      (throw t))))

(defn- insert* [db t table coll opts]
  (as-> (utils/generate-insert db table coll) <>
    (sql/format <>) (jdbc/query t <> opts) (utils/namespacize table <>)))

(hystrix/defcommand save!
  "Save a map or a coll of maps in the given table, only save keys matching existing columns.
   If keys are namespaced, only keep keys namespaced by the table name.
   Eg: :user {:user/id 1, :project/id 2} will save {:id 1}"
  {:hystrix/fallback-fn (constantly nil)}
  [db table keys-or-coll-keys & {:keys [opts]}]
  (try
    (let [table (utils/table-name table)]
      (jdbc/with-db-transaction [t db]
        (if-let [update-q (utils/generate-update db table keys-or-coll-keys)]
          (if-let [updated (as-> update-q <> (sql/format <>) (jdbc/query t <> opts) (first <>))]
            (utils/namespacize table updated)
            (insert* db t table keys-or-coll-keys opts))
          (insert* db t table keys-or-coll-keys opts))))
    (catch Throwable t
      (log/error t (.getMessage t))
      (throw t))))

(hystrix/defcommand update!
  "Perform an update with the given query"
  {:hystrix/fallback-fn (constantly nil)}
  [db table query & {:keys [opts]}]
  (try
    (as-> {:returning (utils/pullq->select (:pull opts '[*]))} <>
      (merge <> query)
      (sql/format <>)
      (jdbc/query db <> opts)
      (utils/namespacize table <>))
    (catch Throwable t
      (log/error t (.getMessage t))
      (throw t))))

(hystrix/defcommand delete!
  "Perform a delete, return how many rows (int) where deleted. Even if deleting
  data is possible (and sometime mandatory) it is recommended to 'retract' your
  facts using a boolean field like `retracted_at` or `archived_at`. Never delete
  anything if you can, storage is cheap nowadays and it will make you system
  more reliable and change-friendly."
  {:hystrix/fallback-fn (constantly 0)}
  [db table query & {:keys [opts]}]
  (try
    (let [q (->> (merge {:delete-from table} query))]
      (if-not (utils/secure-where? q)
        (utils/where-error! q)
        (as-> q $
          (sql/format $)
          (jdbc/execute! db $ opts)
          (first $))))
    (catch Throwable t
      (log/error t (.getMessage t))
      (throw t))))
