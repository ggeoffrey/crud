(ns bf.crud.db
  (:require [bf.crud.core :as crud]
            [bf.crud.utils :as utils]
            [camel-snake-kebab.core :as kebab]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as sql]
            [clojure.data :as diff]))

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

(defn remove-nils [amap]
  (->> amap
       (filter val)
       (into {})))

(defn- insert* [db table entity opts]
  (let [opts (merge {:row-fn row-fn} opts)]
    (jdbc/insert-multi! db table (map remove-nils (utils/maps->table-rows db table [entity])) opts)))

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

(defn update! [db entity query-clause & {:keys [opts]}]
  (as-> (merge {:update (keyword (crud/store entity))} query-clause) $
    (sql/format $)
    (doto $ prn)
    (jdbc/execute! db $ opts)))

(defn- delete*
  "Perform a delete, return how many rows (int) where deleted. Even if deleting
  data is possible (and sometime mandatory) it is recommended to 'retract' your
  facts using a boolean field like `retracted_at` or `archived_at`. Never delete
  anything if you can, storage is cheap nowadays and it will make you system
  more reliable and change-friendly."
  [db table query & {:keys [opts]}]
  (as-> (merge {:delete-from (utils/table-name table)} query) $
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

;;;;;;;;;;;;;;;;;
;; REFLEXIVITY ;;
;;;;;;;;;;;;;;;;;

(defn invoke-record-constructor
  "For a record instance, invoke the auto-generated map->Record factory function."
  [this amap]
  (clojure.lang.Reflector/invokeStaticMethod (.getName (class this)) "create" (object-array [amap])))

(defn empty-record
  "Same a c.c/empty, but work on records. An empty record is a record with all
  fields initialized to `nil`"
  [this]
  (invoke-record-constructor this {}))

;;;;;;;;;;;;;;;;;;;;;;;;
;; DEFAULT BEHAVIOURS ;;
;;;;;;;;;;;;;;;;;;;;;;;;

(defn renew [entity map]
  (into (empty-record entity) map))

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

(defn generate-where-from-entity
  "Generate a where clause from an entity, ignore nil values for primary keys."
  [entity]
  (let [pk (crud/primary-key entity)]
    (if (coll? pk)
      (->> (select-keys entity pk) ;; we keep all the pks entries in the map/record
           (filter second) ;; we drop the ones the null ones …
           (into {}) ;; we rebuild the entity map
           (utils/generate-where))
      (utils/generate-where (select-keys entity #{pk})))))

(defn generate-strict-where-from-entity
  "Generate a where clause from entity, preserving nils value for primary-keys."
  [entity]
  (let [pk (crud/primary-key entity)]
    (if (coll? pk)
      (utils/generate-where (select-keys entity pk))
      (utils/generate-where (select-keys entity #{pk})))))

(defn fetch!
  ([db entity]
   (fetch! db entity {:where (generate-where-from-entity entity)}))
  ([db entity where-clause & {:keys [opts]}]
   (map (partial renew entity)
        (fetch* db (crud/store entity) (or where-clause {:where (generate-where-from-entity entity)}) :opts opts))))

(defn save!
  "Save a map or a coll of maps in the given table, only save keys matching
  existing columns."
  [db entity & {:keys [opts]}]
  {:pre [(satisfies? crud/Storable entity)
         (record? entity)]}
  (some->> (upsert! db (crud/store entity) entity :keys [opts])
           (recover-entities db entity)
           (first)
           (renew entity)))

(defn save-subentities!
  "Upsert and/or delete the designated `subentities` in `db`, implementing the
  save operation on a one-to-many relationship. Take a `db` spec, an `entity`, a
  key designating a slot of related `subentities`, a `relation-key` keyword
  naming the reference from the subentity to the entity, and a 2 arity `getterf`
  function. Will use `getterf`, passing it `db` and `entity` to find the actual
  subentities in `db`, diff them agains the given `subentities` and perform 2
  actions in this order: save in `db` all given `subentities` setting their
  `relation-key` to the entity's primary-key, then delete from `db` the ones
  present in `db` but not in `subentities`.
  Example: (save-subentities! db user :posts :user-id posts/find-by-user)"
  [db entity subentities relation-key getterf]
  {:pre [(keyword? subentities)
         (keyword? relation-key)
         (satisfies? crud/Identified entity)
         (or (nil? (get entity subentities))
             (vector? (get entity subentities)))
         (ifn? getterf)]}
  (when-let [xs (get entity subentities)]
    (let [actual-xs             (getterf db entity)                 ;; we fetch subentities from db
          actual-pks            (set (map crud/identity actual-xs)) ;; we get the actual subentities identities
          pks-to-save           (set (map crud/identity xs))        ;; we get the subentities to save identities'
          [pks-to-delete _ _]   (diff/diff actual-pks pks-to-save)  ;; diff them with the actual
          subentities-to-delete (filter #(contains? pks-to-delete (crud/identity %)) actual-xs)]
      ;; we save all subentities
      (doseq [x xs]
        (crud/save! (assoc x relation-key (crud/identity entity)) db))
      ;; Then we delete the removed ones
      (doseq [x subentities-to-delete]
        (crud/delete! x db)))))

(defn delete!
  ([db entity]
   (delete! db entity {:where (generate-strict-where-from-entity entity)}))
  ([db entity where-clause]
   (delete* db (crud/store entity) where-clause)))

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
     (first (crud/fetch! this db nil)))
    ([this db where-clause]
     (crud/fetch! this db where-clause nil))
    ([this db where-clause opts]
     (fetch! db this where-clause :opts opts)))

  crud/Savable
  (crud/save! [this db]
    (save! db this))

  crud/Deletable
  (crud/delete! [this db]
    (delete! db this)))

