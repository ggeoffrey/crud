(defproject crud "0.1.0-SNAPSHOT"
  :description "Easy and extensible sql CRUD operations in Clojure"
  :url "https://github.com/ggeoffrey/crud"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.netflix.hystrix/hystrix-clj "1.5.12"]
                 [org.postgresql/postgresql "42.1.4"]
                 [conman "0.7.1"]
                 [honeysql "0.9.1"]
                 [camel-snake-kebab "0.4.0"]
                 [nilenso/honeysql-postgres "0.2.3"]
                 [org.clojure/tools.logging "0.4.0"]
                 [cheshire "5.8.0"]
                 [inflections "0.13.0"]])
