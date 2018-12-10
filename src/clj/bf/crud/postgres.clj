(ns bf.crud.postgres
  "When loaded, this namespace extends postgres types and clojure types to make
  them match."
  (:require
   [clojure.java.jdbc :as jdbc]
   [cheshire.core :as json]
   [clojure.string :as str])
  (:import org.postgresql.util.PGobject
           java.sql.Array
           clojure.lang.IPersistentMap
           [java.sql
            Date
            Timestamp
            PreparedStatement]
           (org.postgresql.geometric PGpoint)
           (clojure.lang IPersistentVector)
           (com.impossibl.postgres.jdbc PGDataSource)
           (com.impossibl.postgres.api.jdbc PGNotificationListener)))

(defn to-date [^Date sql-date]
  (-> sql-date (.getTime) (java.util.Date.)))

(defn to-point [^PGpoint p]
  {:lng (.x p)
   :lat (.y p)})

(extend-protocol jdbc/IResultSetReadColumn
  Date
  (result-set-read-column [v _ _] (to-date v))

  Timestamp
  (result-set-read-column [v _ _] (to-date v))

  Array
  (result-set-read-column [v _ _] (vec (.getArray v)))

  PGpoint
  (result-set-read-column [value metadata index]
    (to-point value))

  PGobject
  (result-set-read-column [pgobj _metadata _index]
    (let [type (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json" (json/parse-string-strict value true)
        "jsonb" (json/parse-string-strict value true)
        "citext" (str value)
        value)))
  String
  (result-set-read-column [value _ _]
    (if (re-matches #"^:[a-zA-Z].*" value)
      (keyword (subs value 1))
      value)))

(extend-type java.util.Date
  jdbc/ISQLParameter
  (set-parameter [v ^PreparedStatement stmt idx]
    (.setTimestamp stmt idx (Timestamp. (.getTime v)))))

(defn json [value]
  (when value
    (doto (PGobject.)
      (.setType "jsonb")
      (.setValue (json/generate-string value)))))

(extend-type IPersistentVector
  jdbc/ISQLParameter
  (set-parameter [v ^PreparedStatement stmt ^long idx]
    (let [conn (.getConnection stmt)
          meta (.getParameterMetaData stmt)
          type-name (.getParameterTypeName meta idx)]
      (if-let [elem-type (when (= (first type-name) \_) (str/join (rest type-name)))]
        (.setObject stmt idx (.createArrayOf conn elem-type (to-array v)))
        (.setObject stmt idx (json v))))))

(extend-protocol jdbc/ISQLValue
  IPersistentMap
  (sql-value [value] (json value))
  IPersistentVector
  (sql-value [value] (json value))
  clojure.lang.Keyword
  (sql-value [value] (str value)))

