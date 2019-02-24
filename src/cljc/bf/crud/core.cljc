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
