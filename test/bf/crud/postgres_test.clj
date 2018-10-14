(ns bf.crud.postgres-test
  (:require
   [bf.crud.postgres :as sut]
   [bf.crud.core :as crud]
   [clojure.test :as t]
   ))

(def db {:classname   "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname     (str "//localhost:5432/bf")
         :user        "bf"
         :password    "bf"})

(defrecord Test [id name json time keyword])
