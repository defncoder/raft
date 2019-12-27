(ns raft.state)

;;; Volatile state on all servers
;; index of highest log entry known to becommitted (initialized to 0, increases monotonically)
(def commit-index (atom 0))
;; index of highest log entry applied to statemachine (initialized to 0, increases monotonically)
(def last-applied (atom 0))

;;; Volatile state on leaders. Reinitialized after election.
;; for each server, index of the next log entry to send to that server(initialized to leaderlast log index + 1)
(def next-index (atom {}))
;; for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
(def match-index (atom {}))

(defn- initial-index-map
  "Make a next-index map for known servers."
  [num-servers leader-last-log-index]
  (reduce #(assoc %1 %2 leader-last-log-index) {} (take num-servers (range 1 (inc num-servers)))))

(defn init-with-num-servers
  "Initialize volatile state for a given number of servers in the cluster."
  [num-servers leader-last-log-index]
  (swap! next-index #(initial-index-map num-servers leader-last-log-index))
  (swap! match-index #(initial-index-map num-servers 0)))

(defn- set-index-value
  "Set the association for key to value in a map wrapped inside an atom."
  [atom-map key value]
  (swap! atom-map #(assoc %1 key value)))

(defn set-next-index-for-server
  "Set the next log entry index to send to a particular server."
  [server next-log-index]
  (set-index-value next-index server next-log-index))

(defn set-match-index-for-server
  "Set the next log entry index to send to a particular server."
  [server server-match-index]
  (set-index-value match-index server server-match-index))
