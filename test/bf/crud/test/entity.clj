(ns bf.crud.test.entity)

(def table "test_pass")
(def schema [[:id :integer "primary key"]
             [:name :text]
             [:keyword :text]
             [:json :text]])

(defrecord TestPass [id name json])

