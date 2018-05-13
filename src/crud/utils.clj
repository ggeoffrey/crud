(ns crud.utils
  (:require [clojure.string :as str]
            [camel-snake-kebab.core :as camel]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [clojure.set :as set]
            [inflections.core :as inflex])
  (:import org.postgresql.jdbc.PgDatabaseMetaData))

(defn table-name [named]
  (camel/->snake_case_keyword named))

(defn unplural [str]
  (inflex/singular str))

(defn to-keyword
  ([kw]
   (to-keyword nil kw))
  ([ns kw]
   (let [[ns' nme] (str/split (name kw) #"\$")]
     (cond
       nme   (keyword ns' nme)
       ns    (keyword (name ns) ns')
       :else (keyword ns')))))

(defn namespacize [ns coll]
  (let [singular (-> ns name unplural keyword)]
    (map #(into {} (map (fn [[k v]]
                          [(to-keyword singular k) v])
                       %))
         (if (map? coll)
           [coll]
           coll))))

(defn hyphenify [str]
  (camel/->kebab-case str))

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

;; WIP : check all static preds for a dangerous truthy one
;; (def operator->fn {:=    =
;;                    :>    >
;;                    :<    <
;;                    :not= not=})

;; (defn eval-pred [[op left right]]
;;   (when-let [f (operator->fn op)]
;;     (apply (operator op) [left right])))

;; (defn eval-all-preds [preds]
;;   (loop [acc            []
;;          [pred & preds] preds]
;;     (cond
;;       (nil? pred)        acc
;;       (#{:and :or} pred) ……
;;       (vector? pred) ……)))

;; TODO: improve security by checking for alaways true predicates.
(defn secure-where?
  "False when the query has no WHERE or when 'WHERE TRUE'. In this case the query
  could update or delete the whole table and may makes you shout a
  'Oh… oh shit!' when you finaly understand where the problem comes from."
  [amap]
  (boolean
   (when-let [where (:where amap)]
     (and (not (true? where))))))

(defn where-error! [query]
  (throw (ex-info "Query aborted for security reasons. The given query has no
  `:where` condition or one of the conditions always evaluate to `true`, this
  could make a mess in the target table." {:where-condition (:where query)
                                           :query           query})))

(defn pure-db
  "Return a db-spec on which we can extract metadata, be it a simple db map or a
  transaction map."
  [db-or-t]
  (select-keys db-or-t #{:datasource}))

(defn get-columns
  "Return a list of column names from the given table.
  If you call it too much it might be slow, if you memoize it your code can
  become out of sync with the db. Use it wisely."
  [db table]
  (map :column_name
       (jdbc/with-db-metadata [^PgDatabaseMetaData md (pure-db db)]
         (jdbc/metadata-result (.getColumns md "" "" (name table) "%")))))

(defn get-primary-keys [db table]
  (map :column_name
       (jdbc/with-db-metadata [^PgDatabaseMetaData md (pure-db db)]
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

(defn select-ns
  "Filter a map, keeping only keys namespaced by `ns-name`"
  [ns-name amap]
  (into {} (filter (fn [[k _]] (= (name ns-name) (namespace k)))
                   amap)))

(defn restrict-coll-to-table [db table coll]
  (let [columns (get-columns db table)]
    (map (partial restrict-map-to-columns columns) coll)))

(defn maps->table-rows
  "Coerce a coll of map to a coll of rows. A map is an open set of key/values
  where keys can be namespaced keywords. A row is a closed set of key/values
  where keys are unnamespaced strings matching column names of the given `table`
  in `db`. Given a `Users` table of (id, age) in database, a given `table` of
  value :users and a coll of [{:user/id 1, :user/age 42, :project/id 2}], will
  produce ({'id' 1, 'age' 42})."
  [db table coll]
  (as-> (name table) <>
    (unplural <>)
    (map (partial select-ns <>) coll)
    (restrict-coll-to-table db table <>)))

(defn coll->values
  "Transform a map or a seq of maps to a seq of rows that can fit the given table
  in an insert statement."
  [db table coll]
  (maps->table-rows db table
       (cond
         (map? coll) [coll]
         :else       coll)))

(defn keywordize [amap]
  (into {} (map (fn [[k v]] [(keyword k) v]) amap)))

(defn include-keys?
  "State if all the keys in a set are present in the keyset of a map"
  [aset amap]
  (set/subset? (set aset)
               (set (keys amap))))

(defn generate-update
  "Generate a hugsql map for an update statement by primary key. Return nil if the
  primary key is not present or if all keys that forms a primary key are not
  present in the map."
  [db table map]
  (when (map? map)
    (let [prims (get-primary-keys db table)
          row   (first (maps->table-rows db table [map]))]
      (when (include-keys? prims row)
        (->> {:update    table
              :set       (keywordize row)
              :where     (generate-where (select-keys row prims))
              :returning [:*]})))))

(defn generate-insert
  "Generate a hugsql map for an insert statement. Do not remove primary keys and thus will fail if you try to insert already existing primary keys values."
  [db table map-or-maps]
  (->> {:insert-into table
        :values      (->> (if (map? map-or-maps)
                            [map-or-maps]
                            map-or-maps)
                          (maps->table-rows db table)
                          (map keywordize))
        :returning   [:*]}))

;; (generate-update bf.data.db.core/*db* :users {:user/id 1, :user/fuck 2})
#_(generate-update bf.data.db.core/*db* :has_role {:has_role/user_id    nil,
                                                   :has_role/role_id    2
                                                   :has_role/created_at "now"})

(defn pullq->select [pullq]
  (if (some #{'*} pullq)
    [:*]
    (->> pullq
         (filter keyword?)
         (map name)
         (map keyword)
         (into []))))

