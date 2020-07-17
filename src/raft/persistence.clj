(ns raft.persistence
  (:require [com.stuartsierra.component :as component]
            [raft.config :as config]
            [raft.database :as database]
            [ragtime.repl :as repl]
            [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as l]
            [raft.migration :as migration]
            [raft.util :as util])
  (:gen-class))

(def system nil)

(defn- make-db-spec
  "Make the map with the component info."
  [server-info]
  (assoc (:db-spec config/config) :dbname  (util/db-filename-for-server server-info)))

(defn- start-db-component
  "Start the DB component."
  [server-info]
  (let [db-spec (make-db-spec server-info)]
    (component/start (component/system-map
                      :database
                      (database/make-db-connection db-spec)))))

(defn- db-connection
  "Get a connection from the connection pool."
  []
  @(:connection (:database system)))

(defn init-db-connection
  "Initialize DB connection and set up the system component."
  [server-info]
  (alter-var-root #'system (fn [_] (start-db-component server-info))))

(defn get-current-term
  "Read the current term from persistent storage."
  []
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

(defn add-new-log-entry
  "Add a new log entry to the local DB."
  [term command]
  (sql/execute! (db-connection)
                ["INSERT INTO raftlog (log_index, term, command) VALUES ((SELECT count(*) FROM raftlog)+1, (?), (?))" term, command]))

(defn get-log-entries
  "Get log entries starting from an index and up to limit number of items."
  [start-index limit]
  (sql/query (db-connection) ["SELECT * FROM raftlog WHERE log_index >= ? ORDER BY log_index LIMIT ?" start-index limit]))

(defn get-last-log-entry
  "Get the last log entry from persistent storage."
  []
  (-> (sql/query
       (db-connection)
       ["SELECT * FROM raftlog ORDER BY log_index DESC LIMIT 1"])
      (first)))

(defn get-last-log-index
  "Get the log_index of the last log entry in local storage."
  []
  (-> (sql/query
       (db-connection)
       ["SELECT log_index FROM raftlog ORDER BY log_index DESC LIMIT 1"])
      (first)
      (:log_index 0)))

(defn has-log-at-term-and-index?
  "Check if log has an entry with term and index that match the input."
  [term index]
  (if (and (zero? index) (zero? term))
    true  ;; When index and term are zero it is the start of the log
    (-> (sql/query
         (db-connection)
         ["select log_index from raftlog where term = ? and log_index = ?" term index])
        (first)
        (:log_index false))))

(defn get-term-for-log-index
  "Get the term corresponding to the log entry at the given index. Zero if it doesn't exist."
  [log-index]
  (-> (sql/query
       (db-connection)
       ["SELECT term FROM raftlog WHERE log_index=?" log-index])
      (first)
      (:term 0)))

(defn get-prev-log-entry
  "Get the log entry that precedes the log at the given index."
  [log-index]
  (-> (sql/query
       (db-connection)
       ["SELECT * FROM raftlog WHERE log_index < ? ORDER BY log_index DESC LIMIT 1" log-index])
      (first)))

(defn delete-conflicting-log-entries
  "If an existing log entry conflicts with a new one(same index but different terms),
  delete the existing entry and all that follow it. Section §5.3 in http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [new-log-index new-log-term]
  (sql/execute! (db-connection)
                ["DELETE FROM raftlog where log_index >= 
                    (SELECT log_index FROM raftlog WHERE log_index = ? AND term != ?)" new-log-index new-log-term]))

(defn- log-entry-as-vec
  "Make a vector of field values from a LogEntry. Useful for DB manipulation."
  [log-entry]
  [(:index log-entry) (:term log-entry) (:command log-entry)])

(defn add-missing-log-entries
  "Add only those log entries that are missing from local storage."
  [log-entries]
  (let [log-values-vec (vec (map #(log-entry-as-vec %1) log-entries))
        sql-statement ["INSERT INTO raftlog (log_index, term, command) VALUES ((?), (?), (?)) ON CONFLICT DO NOTHING"]
        stmt-with-args (vec (concat sql-statement log-values-vec))]
    ;; (l/debug "Log values vector is: " log-values-vec)
    (sql/execute! (db-connection) stmt-with-args {:multi? true})))

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
