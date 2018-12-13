(ns bf.crud.db-test
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.test :as t]
   [bf.crud.core :as crud]
   [bf.crud.db :as db]
   ))

;;;;;;;;;;;;;;;;;;;;;
;; SCHEMA & ENTITY ;;
;;;;;;;;;;;;;;;;;;;;;

(def schema [[:id :integer "primary key"]
             [:name :text]
             [:extra_slot :text]])

(defrecord Test [id name extra-slot])
(def entity (map->Test {}))

;;;;;;;;;;;;;;
;; DATABASE ;;
;;;;;;;;;;;;;;

(def ^:dynamic *db* nil)

(def specs {:sqlite   {:classname   "org.sqlite.JDBC"
                       :subprotocol "sqlite"
                       :subname     "target/database.db"}
            :postgres {:classname   "org.postgresql.Driver"
                       :subprotocol "postgresql"
                       :subname     (str "//localhost:5432/bf")
                       :user        "bf"
                       :password    "bf"}})

;;;;;;;;;;;;;;
;; FIXTURES ;;
;;;;;;;;;;;;;;

(defn around-all! [next]
  (jdbc/db-do-commands *db*
                       (jdbc/create-table-ddl (crud/store entity)
                                              schema))
  (next)
  (jdbc/db-do-commands *db*
                       (jdbc/drop-table-ddl (crud/store entity))))

(defn use-spec [spec-name]
  (fn [f]
    (binding [*db* (get specs spec-name)]
      (around-all! f))))

(defn around-fixtures! [f]
  ((juxt (use-spec :sqlite) (use-spec :postgres)) f))

(t/use-fixtures :once around-fixtures!)
(t/use-fixtures :each (fn [next]
                        (jdbc/delete! *db* (crud/store entity) ["true"])
                        (next)))
;;;;;;;;;;;
;; TESTS ;;
;;;;;;;;;;;

(t/deftest core-features
  (t/testing "About Records"
    (t/testing "an empty one should be equal to itself"
      (let [e (map->Test {})]
        (t/is (= e (db/empty-record e)))))
    (t/testing "if we create one, empty it, then use `into` on it, they should be equal."
      (let [e (map->Test {:id 1})]
        (t/is (= e (-> (db/empty-record e)
                       (into {:id 1}))))))))

(t/deftest basic-entity
  (t/testing "An entity"
    (t/testing "should have a primary key"
      (let [e (map->Test {:id 1})]
        (t/is (= :id (crud/primary-key e)))
        (t/is (= 1 (crud/identity e)))))))

(t/deftest saving
  (t/testing "An entity should be savable"
    (let [e (map->Test {:id 42 :name "saving"})]
      (t/is (= e (crud/save! e *db*))))))

(t/deftest fetching
  (t/testing "An entity should be fetchable"
    (let [e (crud/save! (map->Test {:id 1 :name "fetching"}) *db*)]
      (t/testing "by it's entity primary key"
        (t/is (= e (crud/fetch! e *db*))))
      (t/testing "by arbitrary :where criteria"
        (t/is (= e (first (crud/fetch! e *db* {:where [:= :name "fetching"]}))))))))

(t/deftest updating
  (t/testing "An entity"
    (let [e (crud/save! (map->Test {:id 42, :name "updating"}) *db*)]
      (t/testing "should be updatable by it's primary key"
        (let [updated (crud/save! (assoc e :name "updated") *db*)]
          (t/is (= (:id e) (:id updated)))
          (t/is (= "updated" (:name updated)))
          (t/testing "and should preserve it's updated state in the database, thus should be fetchable again."
            (let [fetched (crud/fetch! e *db*)]
              (t/is (= fetched updated)))))))))

(t/deftest deleting
  (t/testing "An entity"
    (let [e (crud/save! (map->Test {:id 42}) *db*)]
      (t/testing "should be deletable"
        ;; We ensure first this entity exists
        (t/is (= e (crud/fetch! e *db*)))
        ;; We then try to delete it
        (t/is (= 1 (crud/delete! e *db*)))
        ;; We then try to fetch it again
        (t/is (nil? (crud/fetch! e *db*)))
        ))))

(t/deftest batch-insert
  (t/testing "Batch inserts"
    (let [entities (->> ["foo" "bar" "baz"]
                        (map-indexed (fn [idx name] (map->Test {:id idx :name name})))
                        (sort-by :id))]
      (t/testing "should allow us to insert multiple entities"
        (t/is (= entities (db/insert-multi! *db* entities)))))))

(t/deftest complex-queries
  (t/testing "Amongs complex queries"
    (let [names    ["foo" "bar" "baz"]
          entities (->> names
                        (map-indexed (fn [idx name] (map->Test {:id idx :name name})))
                        (db/insert-multi! *db*)
                        (sort-by :id))]
      (t/testing "we can use raw `:where` queries"
        (t/is (= entities (db/fetch! *db* (first entities) {:where [:in :name names]}))))
      (t/testing "we can use `:union` queries"
        (t/is (= entities (->> {:union (map (fn [e]
                                              {:select [:*]
                                               :from   [(crud/store e)]
                                               :where  [:= :name (:name e)]})
                                            entities)}
                               (db/fetch! *db* (first entities))
                               (sort-by :id)))))
      (t/testing "we can use `:intersect` queries"
        (t/is (= entities (->> {:intersect (map (fn [e]
                                                  {:select [:*]
                                                   :from   [(crud/store e)]
                                                   :where  [:in :name names]})
                                                entities)}
                               (db/fetch! *db* (first entities))
                               (sort-by :id)))))
      (t/testing "we can use raw sql queries"
        (t/is (= entities (db/query! *db* (first entities)
                                     ["SELECT * FROM test WHERE name IN (?, ?, ?);"
                                      (first names)
                                      (second names)
                                      (last names)]))))

      (t/testing "we can use a datomic pull expression to restrict selected columns"
        (t/is (= (assoc (first entities) :name nil)
                 (first (crud/fetch! (first entities) *db* nil {:pull '[:id]}))))))))

(comment
  (deleting)
  )
