(ns bf.crud.core
  (:refer-clojure :exclude [identity])
  (:require
   [camel-snake-kebab.core :as camel]
   [clojure.string :as str]
   [inflections.core :as inflex]
   )
  #?(:clj (:import (java.util UUID))))

(defprotocol Identified
  (primary-key [this] "Return the primary key")
  (identity [this] "Return the primary key's value"))

(defmulti generate-id (fn [x] x))

(defmethod generate-id ::non-predictable-id [_]
  #?(:clj (UUID/randomUUID)
     :cljs (random-uuid)))

(defmethod generate-id :default [_] nil)

(defprotocol Initializable
  (init [this] "Act as constructor. Given a basis record (built with
  constructor functions or static constructor) will set the primary key to a
  newly generated id. If the entity has no specific instrution on how to
  generate an id, will leave it nil for the database to apply it's DEFAULT. If
  entitie's primary key is composite (tuple of ids) you need to either overload
  the default implementation and set them by yourself. If the record already
  have an id and you want to preserve it, then use `bf.crud.core/save!`
  instead."))

(defprotocol Storable
  (store [this] "Typically return the table name where this entity should be stored."))

(defprotocol Fetchable
  (fetch!
    [this db]
    [this db where-clause]
    [this db where-clause opts]
    ))

(defprotocol Savable
  (save! [this db] [this db opts] "Save an entity by its primary key."))

(defprotocol Updatable
  (update! [this db where-clause] [this db where-clause opts] "Usefull to update
  multiple entities of the same kind by an arbitrary predicate. Use
  Savable/save! if your predicate is to update by primary key."))

(defprotocol Deletable
  (delete! [this db]))

(defprotocol Pullable
  (pull! [this db ids pullq] [this db ids pullq opts]))

;;;;;;;;;;;;;;;;;;
;; DEFAULT IMPL ;;
;;;;;;;;;;;;;;;;;;

(extend-protocol Identified
  #?(:clj Object
     :cljs object)
  (primary-key [this] :id)
  (identity [this] (get this (primary-key this))))

(defn super-init
  "Base behaviour of Initializable/init that will generate a primary key for an
  entity, you can call this function like you would call super(…) in OOP if you
  overload it."
  ([this]
   (super-init this :default))
  ([this id-type]
   (let [id (or (get this (primary-key this)) (generate-id id-type))]
     (assoc this (primary-key this) id))))

(extend-protocol Initializable
  #?(:clj Object
     :cljs object)
  (init [this] (super-init this)))

(extend-protocol Storable
  #?(:clj Object
     :cljs object)
  (store [this]
    (let [class-name (-> (type this)
                         (str)
                         (str/split #"\s")
                         (last)
                         (str/split #"\.")
                         (last))]
      (-> (str/lower-case class-name)
          (inflex/plural)
          (keyword)))))


;;;;;;;;;
;; DSL ;;
;;;;;;;;;

(defmacro defentity [entity-name {:keys [primary-key id-type store]}]
  (let [base `(defrecord ~entity-name ~(mapv (comp symbol name) (or primary-key [:id])))]
    (cond-> base
      store                        (concat `(Storable (store [this#] ~store)))
      (= :non-predictable id-type) (concat `(Initializable (init [this#] (super-init this# ::non-predictable-id)))))))

(defmacro attach-lifecycle [entity-name {:keys [on-fetch on-save on-delete]}]
  (let [base `(extend-type ~entity-name)]
    (cond-> base
      on-fetch  (concat `(Fetchable (fetch! ~@(rest on-fetch))))
      on-save   (concat `(Savable (save! ~@(rest on-save))))
      on-delete (concat `(Deletable (delete! ~@(rest on-delete)))))))

(defn fetch-by-id [this db]
  (fetch! this db {:where [:= (primary-key this) (identity this)]}))

;;;;;;;;;;;;;;;;;
;; BATCH LAYER ;;
;;;;;;;;;;;;;;;;;

(defn- is-one-to-one-join? [id-entity-group]
  {:pre [(map? id-entity-group)
         (every? vector? id-entity-group)]}
  (let [values (vals (dissoc id-entity-group nil))]
    (->> (map count values)
         (reduce + 0)
         (= (count values)))))

(defn- join-entity
  "Assoc in `entity` at key `slot` the subentity found in `id-entity-group` or nil."
  [slot is-one-to-one? id-entity-group entity]
  (assoc entity slot (cond-> (get id-entity-group (identity entity))
                       is-one-to-one? (first))))

(defn join
  "Take a list of `entities` that can be Identified, and a list of arbitrary
  `subentities` maps. Will zip the two collections and join each subentities
  with their respective parents by `join-key` and store the result at the `slot`
  key in each entity. If at least one entity match with multiple
  subentities (one-to-many), then the `slot` will be a list of subentities, a
  single value otherwise.
  E.g: (fold [(User {:id 1}) (User {:id 2})]
             [(Profile {:user-id 1}) (Profile {:user-id 2})]
             :profile
             :user-id)"
  ([entities subentities slot join-key]
   (join entities subentities slot join-key nil))
  ([entities subentities slot join-key one-to-one-join?]
   {:pre [(every? #(satisfies? Identified %) entities)
          (keyword? slot)
          (keyword? join-key)]}
   (if-not (seq entities)
     entities
     (let [by-id       (dissoc (group-by join-key subentities) nil)
           one-to-one? (if (nil? one-to-one-join?)
                         (is-one-to-one-join? by-id)
                         one-to-one-join?)
           folder      (partial join-entity slot one-to-one? by-id)]
       (map folder entities)))))

(defmacro ?->>
  "Like `clojure.core/some->>` but short-circuit when the value do not satisfy the
  predicate `pred?`. (?->> nil? ...) is equivalent to `clojure.core/some->>`."
  [pred? expr & forms]
  (let [g     (gensym)
        steps (map (fn [step] `(if-not (~pred? ~g) ~g (->> ~g ~step)))
                   forms)]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps)
          g
          (last steps)))))

(defmacro seq->> [& body]
  `(?->> seq ~@body))

(comment
  (?->> seq [] (map inc) (map inc))
  (join [{:id 1} {:id 2}]
        [{:user-id 1} {:user-id 2}]
        :profile
        :user-id)
  )
