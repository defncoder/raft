(ns raft.persistence
  (:require [com.stuartsierra.component :as component]
            [raft.config :as config]
            [raft.database :as database]
            [ragtime.repl :as repl]
            [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as l]
            [raft.migration :as migration]
            [raft.util :as util]))

(def system nil)

(defn- make-db-spec
  "Make the map with the component info."
  [server-info]
  (assoc (:db-spec (config/config)) :dbname  (util/db-filename-for-server server-info)))

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
  (sql/with-db-transaction [t-conn (db-connection)]
    (-> (sql/query t-conn ["SELECT current_term FROM terminfo WHERE recnum=1"])
        (first)
        (:current_term 0))))

(defn get-voted-for
  "Get the candidateId of the candidate this server voted for."
  []
  (sql/with-db-transaction [t-conn (db-connection)]
    (-> (sql/query t-conn ["SELECT voted_for FROM terminfo WHERE recnum=1"])
        (first)
        (:voted_for nil))))

(defn save-current-term-and-voted-for
  "Conditionally update to new term if it is greater than existing term. If term is udpated, then voted_for is reset to NULL."
  [new-term voted-for]
  (sql/execute! (db-connection)
                ["INSERT OR REPLACE INTO terminfo VALUES (1, (?), (?))" new-term, voted-for]))

(defn get-log-entries
  "Get log entries starting from an index and up to limit number of items."
  [start-index limit]
  (sql/query (db-connection) ["SELECT * FROM raftlog WHERE idx >= ? ORDER BY idx LIMIT ?" start-index limit]))

(defn get-log-with-index
  "Get a specific logentry."
  [idx]
  (->
   (sql/query (db-connection) ["SELECT * FROM raftlog WHERE idx = ?" idx])
   first))

(defn get-last-log-entry
  "Get the last log entry from persistent storage."
  []
  (-> (sql/query
       (db-connection)
       ["SELECT * FROM raftlog ORDER BY idx DESC LIMIT 1"])
      (first)))

(defn get-last-log-index
  "Get the idx of the last log entry in local storage."
  [ & [t-conn] ]
  (-> (sql/query
       (or t-conn (db-connection))
       ["SELECT idx FROM raftlog ORDER BY idx DESC LIMIT 1"])
      (first)
      (:idx 0)))

(defn has-log-at-term-and-index?
  "Check if log has an entry with term and index that match the input."
  [term index]
  (if (and (zero? index) (zero? term))
    true  ;; When index and term are zero it is the start of the log
    (-> (sql/query
         (db-connection)
         ["SELECT idx FROM raftlog WHERE term = ? and idx = ?" term index])
        (first)
        (:idx false))))

(defn get-term-for-log-index
  "Get the term corresponding to the log entry at the given index. Zero if it doesn't exist."
  [log-index]
  (-> (sql/query
       (db-connection)
       ["SELECT term FROM raftlog WHERE idx=?" log-index])
      (first)
      (:term 0)))

(defn get-first-index-for-term
  "Get the first index for a given term or nil."
  [term]
  (-> (sql/query (db-connection) ["SELECT min(idx) as idx FROM raftlog WHERE term = ?" term])
      first
      (:idx 0)))

(defn get-first-log-entry-for-term<=
  "Get the first entry for the most recent term that is less than or equal to a given term."
  [term]
  (-> (sql/query
       (db-connection)
       ["SELECT min(idx) AS idx, term, requestid, command FROM raftlog WHERE term = (SELECT max(term) FROM raftlog WHERE term <= ?)" term])
      first))

(defn get-most-recent-term
  "Get the most recent term in the log."
  []
  (-> (sql/query (db-connection) ["SELECT max(term) as term FROM raftlog"])
      (first)
      (:term 0)))

(defn get-min-index-for-most-recent-term
  "Get the min index value for the most recent term. Helpful in optimizing log synchronization when there are conflicts.
  Returns a map with two keys :idx and :term"
  []
  (-> (sql/query (db-connection) ["SELECT min(idx) AS idx, term FROM raftlog WHERE term = (SELECT max(term) FROM raftlog)"])
      first))

(defn get-prev-log-entry
  "Get the log entry that precedes the log at the given index."
  [log-index]
  (-> (sql/query
       (db-connection)
       ["SELECT * FROM raftlog WHERE idx < ? ORDER BY idx DESC LIMIT 1" log-index])
      (first)))

(defn delete-log-entries-beyond-index
  "Delete all log entries whose index value is strictly greater than the given index value."
  [index]
  (sql/execute! (db-connection)
                ["DELETE FROM raftlog where idx > ?" index]))

(defn delete-conflicting-log-entries
  "If an existing log entry conflicts with a new one(same index but different terms),
  delete the existing entry and all that follow it. Section §5.3 in http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [new-log-index new-log-term]
  (sql/execute! (db-connection)
                ["DELETE FROM raftlog where idx >= 
                    (SELECT idx FROM raftlog WHERE idx = ? AND term != ? LIMIT 1)" new-log-index new-log-term]))

(defn- log-entry-as-vec
  "Make a vector of field values from a LogEntry. Useful for DB manipulation."
  [log-entry]
  [(:idx log-entry) (:term log-entry) (:requestid log-entry) (:command log-entry)])

(defn save-log-entries
  "Add only those log entries that are missing from local storage."
  [log-entries & [t-conn]]
  (let [log-values-vec (vec (map log-entry-as-vec log-entries))
        sql-statement ["INSERT INTO raftlog (idx, term, requestid, command) VALUES ((?), (?), (?), (?)) ON CONFLICT DO NOTHING"]
        stmt-with-args (vec (concat sql-statement log-values-vec))]
    ;; (l/debug "Log values vector is: " log-values-vec)
    (sql/execute! (or t-conn (db-connection)) stmt-with-args {:multi? true})))

(defn append-new-log-entries-from-client
  "Append log entries from a client. This function will be called ONLY when the current
  server is the leader.
  Performs the operation in the scope of a DB transaction so there's no inconsistency because
  of other DB operations interleaving between when the last-log-index is read vs when the new
  log entries are appended.
  Returns the last log index AND the index of the first new log index. This can be used to delete these logs if required."
  [log-entries current-term requestid]
  (l/trace "Beginning to store new log entries...")
  (sql/with-db-transaction [t-conn (db-connection)]
    (let [last-log-index (get-last-log-index t-conn)
          next-log-index (inc last-log-index)
          ;; Set :idx for the new log entries to start from next-log-index
          indexed-entries (map #(assoc %1 :idx %2 :term current-term :requestid requestid)
                               log-entries
                               (range next-log-index Integer/MAX_VALUE))]
      ;; Save 20 log entries at a time.
      (doall (map #(save-log-entries %1 t-conn) (partition 20 20 [] indexed-entries)))
      (l/trace "Done saving new log entries...")
      ;; Return the range of indices of the new log entries: [start-index...end-index] inclusive.
      [next-log-index (+ next-log-index (count log-entries) -1)])))

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
