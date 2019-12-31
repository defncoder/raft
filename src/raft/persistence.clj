(ns raft.persistence
  (:require [com.stuartsierra.component :as component]
            [raft.config :as config]
            [raft.database :as database]
            [ragtime.repl :as repl]
            [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as l]
            [raft.migration :as migration])
  (:gen-class))

(def system nil)

(defn- make-db-spec
  "Make the map with the component info."
  [server-name]
  (assoc (:db-spec config/config) :dbname (str server-name ".db")))

(defn- start-db-component
  "Start the DB component."
  [server-name]
  (let [db-spec (make-db-spec server-name)]
    (component/start (component/system-map
                      :database
                      (database/make-db-connection db-spec)))))

(defn- db-connection
  "Get a connection from the connection pool."
  []
  @(:connection (:database system)))

(defn init-db-connection
  "Initialize DB connection and set up the system component."
  [server-name]
  (alter-var-root #'system (fn [_] (start-db-component server-name))))

(defn get-current-term
  "Read the current term from persistent storage."
  []
  (l/info "Current term query result is: " (sql/query (db-connection) ["SELECT current_term FROM terminfo WHERE recnum=1"]))
  (-> (sql/query (db-connection) ["SELECT current_term FROM terminfo WHERE recnum=1"])
      (first)
      (:current_term 0)))

(defn get-voted-for
  "Get the candidateId of the candidate this server voted for."
  []
  (-> (sql/query (db-connection) ["SELECT voted_for FROM terminfo WHERE recnum=1"])
      (first)
      (:voted_for nil)))

(defn save-current-term-and-voted-for
  "Conditionally update to new term if it is greater than existing term. If term is udpated, then voted_for is reset to NULL."
  [new-term voted-for]
  (sql/execute! (db-connection)
                ["INSERT OR REPLACE INTO terminfo VALUES (1, (?), (?))" new-term, voted-for]))

(defn add-log-entry
  "Add a new log entry to the local DB."
  [sequence value]
  (sql/execute! (db-connection)
                ["INSERT INTO raftlog (sequence, value) VALUES ((?), (?))" sequence, value]))

(defn get-last-log-entry
  "Get the last log entry from persistent storage."
  []
  (-> (sql/query
       (db-connection)
       ["SELECT * FROM raftlog ORDER BY log_index DESC LIMIT 1"])
      (first)))

(defn has-log-at-index-with-term?
  "Check if saved log entries has an entry at index whose term matches input."
  [index term]
  (-> (sql/query
       (db-connection)
       ["select log_index from raftlog where log_index = ? and term_number = ?" index term])
      (first)
      (:log_index false)))

(defn delete-conflicting-log-entries
  "If an existing log entry conflicts with a new one(same index but different terms),
  delete the existing entry and all that follow it. Section §5.3 in http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [new-log-index new-log-term]
  (sql/execute! (db-connection)
                ["DELETE FROM raftlog where log_index >= 
                    (SELECT log_index FROM raftlog WHERE log_index = ? AND term_number != ?)" new-log-index new-log-term]))

(defn log-entry-as-vec
  "Make a vector of field values from a LogEntry. Useful for DB manipulation."
  [log-entry]
  [(.getLogIndex log-entry) (.getTermNumber log-entry) (.getCommand log-entry)])

(defn append-new-log-entries
  "Append a list of new log entries into the raftlog table."
  [log-entries]
  (let [log-values-vec (map #(log-entry-as-vec %1) log-entries)]
    (sql/execute! (db-connection)
                  ["INSERT INTO raftlog (log_index, term_number, command) VALUES ((?), (?), (?)) ON CONFLICT DO NOTHING" log-values-vec] {:multi? true})))

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
