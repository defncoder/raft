(ns raft.state
  (:require [raft.persistence :as persistence]
            [clojure.tools.logging :as l]))

;;; Volatile state on all servers
;; index of highest log entry known to becommitted (initialized to 0, increases monotonically)
(def commit-index (atom 0))
;; index of highest log entry applied to statemachine (initialized to 0, increases monotonically)
(def last-applied (atom 0))

;; volatile index of AppendEntries call sequence number from current leader.
;; Used to detect liveness. If election timeout happens without call from current
;; leader OR voting for someone then follower can become a candidate.
(def append-entries-call-sequence (atom 0))

;; volatile sequence to remember number of times voted so far.
;; Used to detect liveness. If election timeout happens without an AppendEntries
;; call OR voting for someone, then a follower can become a candidate.
(def voted-sequence (atom 0))

;;; Volatile state on leaders. Reinitialized after election.
;; for each server, index of the next log entry to send to that server(initialized to leaderlast log index + 1)
(def next-index (atom {}))
;; for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
(def match-index (atom {}))

;; A synchronized set of in-memory values that mirrors the persistent values of current_term and voted_for within
;; the terminfo DB table.
;; ALL updates to these persistent values will only be done within a coordinated transaction of these related refs.
(def current-term (ref 0))
(def voted-for (ref nil))

(defn- initial-index-map
  "Make a next-index map for known servers."
  [num-servers leader-last-log-index]
  (reduce #(assoc %1 %2 leader-last-log-index) {} (take num-servers (range 1 (inc num-servers)))))

(defn init-with-num-servers
  "Initialize volatile state for a given number of servers in the cluster."
  [num-servers leader-last-log-index]
  (swap! next-index (fn [_] (initial-index-map num-servers leader-last-log-index)))
  (swap! match-index (fn [_] (initial-index-map num-servers 0))))

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

(defn inc-append-entries-call-sequence
  "Increment the AppendEntries call sequence number."
  []
  (swap! append-entries-call-sequence inc))

(defn inc-voted-sequence
  "Increment the voted-sequence number."
  []
  (swap! voted-sequence inc))

(defn get-current-term
  "Get the current term value."
  []
  @current-term)

(defn read-term-and-last-voted-for
  "Initialize current term and last voted for values from persistent storage."
  []
  (l/info "Current term is: " (persistence/get-current-term))
  (dosync
   (ref-set current-term (persistence/get-current-term))
   (ref-set voted-for (persistence/get-voted-for))))

(defn update-current-term-and-voted-for
  "Synchronously update the current-term and voted-for values and their corresponding persistent state."
  [new-term new-voted-for]
  (dosync
   (if (>= new-term @current-term)
     (do 
       (ref-set current-term new-term)
       (ref-set voted-for new-voted-for)
       (persistence/save-current-term-and-voted-for new-term new-voted-for)))))
