(ns bf.crud.db
  (:require [bf.crud.core :as crud]
            [bf.crud.utils :as utils]
            [camel-snake-kebab.core :as kebab]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as sql]))

(defn row-fn [row]
  (into {} (comp
            (filter (fn [[k v]] (some? v)))
            (filter (fn [[k v]] (not (str/starts-with? k ":_"))))
            (map (fn [[k v]] [(kebab/->kebab-case-keyword k) v])))
        row))

(defn- fetch*
  "Fetch from a table. `keys` are in honeysql syntax.
  Eg: :users {:where [:= :id 1]} => ({:id 1, …}, …). You can pass a
  full-blown honeysql {:select … :from … :where …} map in `keys`."
  [db table keys & {:keys [opts]}]
  (let [opts (merge {:row-fn row-fn} opts)]
    (when (or (:where keys)
              (:union keys)
              (:intersect keys))
      (as-> (cond
              (or (:union keys)
                  (:intersect keys)) {}
              :else                  {:select (utils/pullq->select (or (:pull opts) '[*]))
                                      :from   [(utils/table-name table)]}) <>
        (merge <> keys)
        (sql/format <>)
        (jdbc/query db <> (dissoc opts :pull))))))

(def standard-genkeys [:generated-key
                       (keyword "last-insert-rowid()")
                       (keyword "scope-identity()")])

(defn generated-pk
  "Return the generated primary key of resultset item"
  [rs-item]
  (when (map? rs-item)
    (some-> (select-keys rs-item standard-genkeys)
            (first)
            (val))))

(defn- insert* [db table entity opts]
  (let [opts (merge {:row-fn row-fn} opts)]
    (jdbc/insert-multi! db table (utils/maps->table-rows db table [entity]) opts)))

(defn- update* [db table entity]
  (let [nb-affected (some->> (utils/generate-update db table entity)
                             (sql/format)
                             (jdbc/execute! db)
                             (first))]
    (when (and (number? nb-affected)
               (pos? nb-affected))
      [{:generated-key (crud/identity entity)}])))

(defn upsert! [db table entity & {:keys [opts]}]
  (try
    (let [table (utils/table-name table)]
      (if-let [updated (update* db table entity)]
        updated
        (insert* db table entity opts)))))

(defn- delete*
  "Perform a delete, return how many rows (int) where deleted. Even if deleting
  data is possible (and sometime mandatory) it is recommended to 'retract' your
  facts using a boolean field like `retracted_at` or `archived_at`. Never delete
  anything if you can, storage is cheap nowadays and it will make you system
  more reliable and change-friendly."
  [db table query & {:keys [opts]}]
  (as-> (merge {:delete-from (utils/table-name table)} query)$
    (sql/format $)
    (jdbc/execute! db $ opts)
    (first $)))

(defn query-raw [db query-vec & {:keys [opts]}]
  (jdbc/query db query-vec (merge {:row-fn row-fn} opts)))

(defn prepare-multi-rows [db table rows]
  (->> rows
       (utils/maps->table-rows db table)
       (map (fn [value]
              (into {}
                    (map (fn [[k v]] [(keyword k) v]) value))))))

;;;;;;;;;;;;;;;;;;;;;;;;
;; DEFAULT BEHAVIOURS ;;
;;;;;;;;;;;;;;;;;;;;;;;;

(defn renew [entity map]
  (into (crud/empty-record entity) map))

(defn recover-entities
  "Take a db-spec, entity, a resultset and will re-fetch all resultset items from
  db. If no primary key are available, return the resultset."
  [db entity resultset]
  (if (some? (generated-pk (first resultset)))
    (fetch* db (crud/store entity)
            {:where [:in (crud/primary-key entity) (map generated-pk resultset)]})
    resultset))

(defn query! [db entity query-vec & {:keys [opts]}]
  (map (partial renew entity) (query-raw db query-vec :opts opts)))

(defn fetch! [db entity where-clause & {:keys [opts]}]
  (map (partial renew entity)
       (fetch* db
               (crud/store entity)
               (or where-clause
                   {:where [:= (crud/primary-key entity) (crud/identity entity)]})
               :opts opts)))

(defn save!
  "Save a map or a coll of maps in the given table, only save keys matching
  existing columns."
  [db entity & {:keys [opts]}]
  (some->> (upsert! db (crud/store entity) entity :keys [opts])
           (recover-entities db entity)
           (first)
           (renew entity)))

(defn delete! [db entity]
  (delete* db (crud/store entity) {:where [:= (crud/primary-key entity) (crud/identity entity)]}))

(defn insert-multi!
  [db entities & {:keys [opts]}]
  (when (seq entities)
    (let [table (-> (first entities)
                    (crud/store)
                    (utils/table-name))]
      (->> (jdbc/insert-multi! db
                               table
                               (prepare-multi-rows db table entities)
                               (merge {:row-fn row-fn} opts))
           (recover-entities db (first entities))
           (map (partial renew (first entities)))))))

(extend-type Object

  crud/Fetchable
  (crud/fetch!
    ([this db]
     (crud/fetch! this db nil))
    ([this db where-clause]
     (first (fetch! db this where-clause))))

  crud/Savable
  (crud/save! [this db]
    (save! db this))

  crud/Deletable
  (crud/delete! [this db]
    (delete! db this)))

