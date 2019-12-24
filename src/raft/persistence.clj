(ns raft.persistence
  (:require [com.stuartsierra.component :as component]
            [raft.config :as config]
            [raft.database :as database]
            [ragtime.repl :as repl]
            [clojure.tools.logging :as l]
            [raft.migration :as migration])
  (:gen-class))

(def system (component/system-map
             :database
             (database/make-db-connection (:db-spec config/config))))

(alter-var-root #'system component/start)

(defn migrate-db
  "Migrate the database."
  []
  (l/info "=================================About to start DB migration=================================")
  ;; (l/info (str "Migrations are: \n" (:migrations (migration/migration-config (:database system) "resources/migrations"))))
  (repl/migrate (migration/migration-config @(:connection (:database system)) "resources/migrations"))
  (l/info "==================================DB migration done.================================"))
