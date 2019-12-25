(ns raft.persistence
  (:require [com.stuartsierra.component :as component]
            [raft.config :as config]
            [raft.database :as database]
            [ragtime.repl :as repl]
            [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as l]
            [raft.migration :as migration])
  (:gen-class))

(def system (component/system-map
             :database
             (database/make-db-connection (:db-spec config/config))))

(alter-var-root #'system component/start)

(defn- db-connection
  "Get a connection from the connection pool."
  []
  @(:connection (:database system)))

(defn add-log-entry
  "Add a new log entry to the local DB."
  [sequence value]
  (sql/execute! (db-connection)
                ["INSERT INTO raftlog (sequence, value) VALUES ((?), (?))" sequence value]))

(defn migrate-db
  "Migrate the database."
  []
  (l/info "=================================About to start DB migration=================================")
  ;; (l/info (str "Migrations are: \n" (:migrations (migration/migration-config (:database system) "migrations"))))
  (repl/migrate
   (migration/migration-config
    @(:connection (:database system))
    "migrations"))
  (l/info "==================================DB migration done.================================"))
