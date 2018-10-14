(ns bf.crud.core
  (:refer-clojure :exclude [identity])
  (:require
   [camel-snake-kebab.core :as camel]
   [clojure.string :as str]
   ))

(defprotocol Identified
  (primary-key [this] "Return the primary key")
  (identity [this] "Return the primary key's value"))

(defprotocol Storable
  (store [this] "Typically return the table name where this entity should be stored."))

(defprotocol Fetchable
  (fetch! [this db] [this db where-clause]))

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

(defn get-constructor*
  "For a record instance, return the auto-generated map->Record factory function."
  [this]
  (let [segments (-> (class this)
                     (str)
                     (str/split #"\s")
                     (last)
                     (str/split #"\."))]
    (-> (str/join "." (butlast segments))
        (str "/map->")
        (str (last segments))
        (symbol)
        (resolve))))

;; TODO activate once stabilized
(def get-constructor (memoize get-constructor*))

(defn empty-record
  "Same a c.c/empty, but work on records. An empty record is a record with all
  fields initialized to `nil`"
  [this]
  ((get-constructor* this) {}))


;; (empty-record (crud.test.entity/map->TestPass {:id 1}))

;;;;;;;;;;;;;;;;;;
;; DEFAULT IMPL ;;
;;;;;;;;;;;;;;;;;;


(extend-protocol Identified
  Object
  (primary-key [this] :id)
  (identity [this] (get this (primary-key this))))

(extend-protocol Storable
  Object
  (store [this] (-> this class str (str/split #"\.") last camel/->snake_case_string)))
