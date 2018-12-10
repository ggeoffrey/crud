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

(defprotocol NonPredictableId)

(defmulti generate-id type)

(defmethod generate-id bf.crud.core.NonPredictableId [_]
  #?(:clj (UUID/randomUUID)
     :cljs (random-uuid)))

(defmethod generate-id :default [_] :default)

(defprotocol Creatable
  (create [this] "Act as constructor. Given a basis record (built with
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

;;;;;;;;;;;;;;;;;;
;; DEFAULT IMPL ;;
;;;;;;;;;;;;;;;;;;

(extend-protocol Identified
  Object
  (primary-key [this] :id)
  (identity [this] (get this (primary-key this))))

(extend-protocol Creatable
  Object
  (create [this] (assoc this (primary-key this) (generate-id this))))

(extend-protocol Storable
  Object
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
