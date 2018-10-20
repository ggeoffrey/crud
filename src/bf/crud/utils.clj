(ns bf.crud.utils
  (:require [bf.crud.postgres :as pg]
            [camel-snake-kebab.core :as camel]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]))

(defn sql-values [row]
  (into {} (map (fn [[k v]]
                  [k (cond
                       (keyword? v) (str v)
                       (string? v)  v
                       (map? v)     (pg/json v)
                       (seq? v)     (pg/json (into [] v))
                       (set? v)     (pg/json (into [] v))
                       :else        v)])
                row)))

(defn table-name [named]
  (camel/->snake_case_keyword named))

(defn condition? [vec]
  (and (vector? vec)
       (keyword? (first vec))
       (= (count vec) 2)))

(declare generate-where)
(defn generate-condition [key value]
  (let [key (keyword key)]
    (cond
      (map? value)       (generate-where value)
      (condition? value) (let [[operator value] value]
                           [operator key value])
      (or (sequential? value)
          (set? value))  [:in key (seq value)]
      :else              [:= key value])))

(defn generate-where [amap]
  (let [conds (map #(apply generate-condition %) amap)]
    (if (> (count conds) 1)
      (into [:and] conds)
      (first conds))))

(defn pure-db
  "Return a db-spec on which we can extract metadata, be it a simple db map or a
  transaction map."
  [db-or-t]
  (if (:datasource db-or-t)
    (select-keys db-or-t #{:datasource})
    db-or-t))

(defn get-columns
  "Return a list of column names from the given table.
  If you call it too much it might be slow, if you memoize it your code can
  become out of sync with the db. Use it wisely."
  [db table]
  (map :column_name
       (jdbc/with-db-metadata [md (pure-db db)]
         (jdbc/metadata-result (.getColumns md "" "" (name table) "%")))))

(defn get-primary-keys [db table]
  (map :column_name
       (jdbc/with-db-metadata [md (pure-db db)]
         (jdbc/metadata-result (.getPrimaryKeys md nil nil (name table))))))

(defn stringify-keys
  "Transform keys of a map to string, drop namespaces. Since namespace is droped,
  later occurence of a key name will take precedence.
  Eg: {:foo/a 1, :bar/a 2} -> {\"a\" 2}"
  [amap]
  (into {} (map (fn [[k v]] [(name k) v]) amap)))

(defn restrict-map-to-columns
  "Keep only keys in the map that appears in the table in database"
  [columns amap]
  (select-keys (stringify-keys amap)
               columns))

(defn table-restrictor [db table]
  (map (partial restrict-map-to-columns (get-columns db table))))

(defn maps->table-rows
  "Coerce a coll of map to a coll of rows. A map is an open set of key/values
  where keys can be namespaced keywords. A row is a closed set of key/values
  where keys are unnamespaced strings matching column names of the given `table`
  in `db`. Given a `Users` table of (id, age) in database, a given `table` of
  value :users and a coll of [{:user/id 1, :user/age 42, :project/id 2}], will
  produce ({'id' 1, 'age' 42})."
  [db table coll]
  (sequence (comp (table-restrictor db table)
                  (map sql-values))
            coll))

(defn keywordize [amap]
  (into {} (map (fn [[k v]] [(keyword k) v]) amap)))

(defn include-keys?
  "State if all the keys in a set are present in the keyset of a map"
  [aset amap]
  (and (seq aset)
       (set/subset? (set aset)
                    (set (keys amap)))))

(defn generate-update
  "Generate a hugsql map for an update statement by primary key. Return nil if the
  primary key is not present or if all keys that forms a primary key are not
  present in the map."
  [db table map]
  (when (map? map)
    (let [prims (get-primary-keys db table)
          row   (first (maps->table-rows db table [map]))]
      (cond
        (not (seq prims))         (throw (ex-info "Unable to generate an update clause on a table missing a primary key"
                                                  {:table              table
                                                   :found-primary-keys prims
                                                   :data               map}))
        (include-keys? prims row) (->> {:update table
                                        :set    (keywordize row)
                                        :where  (generate-where (select-keys row prims))
                                        ;; :returning [:*]
                                        })
        :else                     nil))))

(defn pullq->select [pullq]
  (if (some #{'*} pullq)
    [:*]
    (->> pullq
         (filter keyword?)
         (map name)
         (map keyword)
         (into []))))

