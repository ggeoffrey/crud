(ns bf.crud.postgres-test
  (:require [bf.crud.core :as crud]
            [bf.crud.db :as db]
            [bf.crud.postgres :as pg]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all])
  (:import [java.util Date UUID]))

(def db {:classname   "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname     (str "//localhost:5432/bf")
         :user        "bf"
         :password    "bf"})

(defrecord Test [id name json jsonb time keyword]
  crud/Savable
  (crud/save! [this db]
    (db/save! db (-> this
                     (update :json pg/json)
                     (update :jsonb pg/json)))))

(def entity (map->Test {}))

(def schema [[:id :uuid "primary key not null default uuid_generate_v4()"]
             [:name :text "not null default ''"]
             [:json :json "not null default '[]'::json"]
             [:jsonb :jsonb "not null default '{}'::jsonb"]
             [:time :timestamp "not null default now()"]
             [:keyword :text "not null default ''"]])

;;;;;;;;;;;;;;
;; FIXTURES ;;
;;;;;;;;;;;;;;

(defn around-all! [next]
  (jdbc/db-do-commands db
                       (jdbc/create-table-ddl (crud/store entity)
                                              schema))
  (next)
  (jdbc/db-do-commands db
                       (jdbc/drop-table-ddl (crud/store entity))))

(use-fixtures :once around-all!)
(use-fixtures :each (fn [next]
                        (jdbc/delete! db (crud/store entity) ["true"])
                        (next)))

;;;;;;;;;;
;; TEST ;;
;;;;;;;;;;

(deftest pg-types
  (testing "About Postgres"
    (testing "we should be able to save an entity leveraging default values"
      (let [e (crud/save! (map->Test {:id (UUID/randomUUID)}) db)]
        (are [pred? value] (pred? value)
          uuid? (:id e)
          string? (:name e)
          vector? (:json e)
          map? (:jsonb e)
          inst? (:time e)
          string? (:keyword e) ;; yes kewords are string until being prepended with `:`
          )))
    (testing "we should be able to save an entity with complex types"
      (let [e     (map->Test {:id      (UUID/randomUUID)
                              :name    "foo"
                              :json    ["some" "vector" 1 2]
                              :jsonb   {:foo "bar"}
                              :time    (Date.)
                              :keyword :bar})
            saved (crud/save! e db)]
        (is (= (:time e) (:time saved)))
        (is (= e saved))))))
