(ns raft.migration
  (:require [ragtime.jdbc :as jdbc]
            [ragtime.strategy :as strategy]))

(defn migration-config
  "Get migration config for ragtime, given the db spec and the location on all the migration files."
  [connection location]
  {:datastore  (jdbc/sql-database connection)
   :migrations (jdbc/load-resources location)
   :strategy strategy/apply-new})
