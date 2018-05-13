(ns crud.core)

(defmulti fetch
  "Fetch an entity from `db` and table `type` using `query`. Default behaviour is
  to fetch table `type` by formating `query` to sql like SELECT * FROM type
  WHERE <query>. Query need to be in the honeysql format (see example). This
  multimethod can be extended to provide custom behaviour for a given type, be
  it a table or something else. Opts are clojure.java.jdbc options. `pull` is a
  datomic pull query to extract a subset of columns or to traverse relations.
  Will return a sequence of maps: if `type` is :users then you'll get
  ({:user/id …, :user/name …}). Note that the table name is plural whereas the
  returned seq contains singular keys.
  E.g.:
  (fetch *db* :users {:where [:and [:= :user_id 42] [:= :banned false]]})
  (fetch *db* :users {:where [:and [:= :user_id 42]]]}
                     :pull [:user/id :user/name])"
  (fn [db type query & {:keys [opts pull]}] type))

(defmulti fetch-by
  "Same as fetch but higher level, you don't have to use honeysql syntax, but you
  can only specify a where close:

    (fetch-by *db* :users {:id 42, :banned false} :pull [:user/id :user/name])
  You can pass vectors as values in the where-map:
    (fetch-by *db* :users {:score [:> 50]} :pull [:user/id :user/name])

  Will fetch all users id and name if their score is greater than 50. The
  default behaviour is to fetch the table `type` and return the whole rows. You
  can customize this behaviour by Note that the table name is plural whereas the
  returned seq contains maps with singular keys."
  (fn [db type keys & {:keys [opts pull]}] type))

(defmulti save!
  "Update a row if this row exists, otherwise insert it. It's an upsert.
  The default behaviour is to detect the primary keys on the target table. Will
  fail and throw if you don't pass it the primary key(s) in the entity. If you
  want to save-by some other criteria than primary key(s), you must extend the
  behaviour using defmethod. Will run in a transaction. Return the upserted
  entity."
  (fn [db type entity] type))

(defmulti update!
  "The good old SQL UPDATE, except it uses honeysql syntax.
  Will fail and throw if you don't provide a WHERE clause or provide one that
  statically evaluate to a truthy value. In classic SQL this would update the
  whole table and can lead to desastre and backup recoveries. Will return the
  whole row if the backend supports it. You can provide a pull query to extract
  only the required fields or traverse relations on the updated value(s)."
  (fn [db type query & {:keys [opts pull]}] type))

(defmulti delete!
  "Delete rows permanently. Will fail and throw if you don't provide a WHERE
  clause or provide one that statically evaluate to a truthy value, otherwise
  you might simply whipe the whole table. Side note: storage is cheap nowadays
  and data are valuable. It might be wiser to flag your data as :archived
  or :retracted rather than erasing them."
  (fn [db type query & {:keys [opts]}] type))

(defn- subpulls
  "Collect all nested pull queries in a pull query."
  [pullq]
  (->> pullq (filter map?) (apply merge)))

(defn pull
  "Given a mapping map of {:entity/subentity delay-fn} a pull query and the entity
  itself, will reduce over the mapping map and call delay-fn with [db entity
  pullq], thus recursively pulling subentities. It is advised that delay-fn
  should return a delay object (see clojure.core/delay)."
  ([db mapping entity]
   (pull db mapping '[*] entity))
  ([db mapping pullq entity]
   (reduce (fn [entity [key mapper]]
             (assoc entity key (delay (when-let [subpull (get (subpulls pullq) key)]
                                        (mapper db entity subpull)))))
           entity
           mapping)))


;;;;;;;;;;;;;;;;;;;;;;;;
;; DEFAULT BEHAVIOURS ;;
;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod fetch :default [db type query & {:keys [opts pull]}]
  (db/fetch db type query :opts opts :pull pull))

(defmethod fetch-by :default [db kw keys & {:keys [opts pull]}]
  (fetch db kw {:where (utils/generate-where keys)} :opts opts :pull pull))

(defmethod save! :default [db table map-or-maps & {:keys [opts pull]}]
  (db/save! db table map-or-maps :opts opts :pull pull))

(defmethod update! :default [db table query & {:keys [opts pull]}]
  (db/update! db table (merge {:update table} query) :opts opts :pull pull))

(defmethod e/delete! :default [db table query & {:keys [opts pull]}]
  (db/delete! db table query :opts opts :pull pull))

