(ns raft.state)

;;; Volatile state on all servers
;; index of highest log entry known to becommitted (initialized to 0, increasesmonotonically)
(def commit-index (atom 0))
;; index of highest log entry applied to statemachine (initialized to 0, increasesmonotonically)
(def last-applied (atom 0))

;;; Volatile state on leaders. Reinitialized after election.
;; for each server, index of the next log entry to send to that server(initialized to leaderlast log index + 1)
(def next-index (atom (int-array 5)))
;; for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
(def match-index (atom (int-array 5)))

(defn init-with-num-servers
  "Initialize volatile state for a given number of servers in the cluster."
  [num-servers]
  (reset! next-index (int-array num-servers))
  (reset! match-index (int-array num-servers)))
