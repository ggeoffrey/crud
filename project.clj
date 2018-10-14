(defproject bf.crud "0.1.0-SNAPSHOT"
  :description "Easy and extensible sql CRUD operations in Clojure"
  :url "https://github.com/ggeoffrey/bf.crud"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[cider/cider-nrepl "0.18.0-SNAPSHOT"]
            [refactor-nrepl "2.4.0-SNAPSHOT"]
            [jonase/eastwood "0.3.1"]
            [lein-kibit "0.1.6"]
            [lein-cloverage "1.0.13"]]

  :dependencies [[org.clojure/clojure "1.10.0-RC1"]
                 [org.postgresql/postgresql "42.2.5"]
                 [com.impossibl.pgjdbc-ng/pgjdbc-ng "0.7.1"]
                 [org.clojure/java.jdbc "0.7.8"]
                 [conman "0.8.2"]
                 [honeysql "0.9.4"]
                 [camel-snake-kebab "0.4.0"]
                 [cheshire "5.8.1"]
                 [nilenso/honeysql-postgres "0.2.4"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.xerial/sqlite-jdbc "3.25.2"]])
