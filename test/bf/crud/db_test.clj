(ns bf.crud.db-test
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.test :as t]
   [bf.crud.core :as crud]
   [bf.crud.db :as db]
   [bf.crud.test.entity :as entity]
   ))

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "target/database.db"})

(defn around-all! [next]
  (jdbc/db-do-commands db
                       (jdbc/create-table-ddl entity/table
                                              entity/schema))
  (next)
  (jdbc/db-do-commands db
                       (jdbc/drop-table-ddl entity/table)))

(t/use-fixtures :once around-all!)

(t/deftest core-features
  (t/testing "About Records"
    (t/testing "an empty one should be equal to itself"
      (let [e (entity/map->TestPass {})]
        (t/is (= e (crud/empty-record e)))))
    (t/testing "if we create one, empty it, then use `into` on it, they should be equal."
      (let [e (entity/map->TestPass {:id 1})]
        (t/is (= e (-> (crud/empty-record e)
                       (into {:id 1}))))))))

(t/deftest basic-entity
  (t/testing "An entity"
    (t/testing "should have a primary key"
      (let [e (entity/map->TestPass {:id 1})]
        (t/is (= :id (crud/primary-key e)))
        (t/is (= 1 (crud/identity e)))))))

(t/deftest saving
  (t/testing "An entity should be savable"
    (let [e (entity/map->TestPass {:id 42 :name "saving"})]
      (t/is (= e (crud/save! e db))))))

(t/deftest fetching
  (t/testing "An entity should be fetchable"
    (let [e (crud/save! (entity/map->TestPass {:id 1 :name "fetching"}) db)]
      (t/testing "by it's entity primary key"
        (t/is (= e (crud/fetch! e db))))
      (t/testing "by arbitrary :where criteria"
        (t/is (= e (crud/fetch! e db {:where [:= :name "fetching"]})))))))

(t/deftest updating
  (t/testing "An entity"
    (let [e (crud/save! (entity/map->TestPass {:id 42, :name "updating"}) db)]
      (t/testing "should be updatable by it's primary key"
        (let [updated (crud/save! (assoc e :name "updated") db)]
          (t/is (= (:id e) (:id updated)))
          (t/is (= "updated" (:name updated)))
          (t/testing "and should preserve it's updated state in the database, thus should be fetchable again."
            (let [fetched (crud/fetch! e db)]
              (t/is (= fetched updated)))))))))

(t/deftest deleting
  (t/testing "An entity"
    (let [e (crud/save! (entity/map->TestPass {:id 42}) db)]
      (t/testing "should be deletable"
        ;; We ensure first this entity exists
        (t/is (= e (crud/fetch! e db)))
        ;; We then try to delete it
        (t/is (= 1 (crud/delete! e db)))
        ;; We then try to fetch it again
        (t/is (nil? (crud/fetch! e db)))
        ))))

(comment
  (deleting)
  )
