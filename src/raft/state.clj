(ns raft.state
  (:require [raft.persistence :as persistence]
            [clojure.tools.logging :as l]
            [raft.util :as util]))

;;; Volatile state on all servers

;; total number of servers
(def num-servers (atom 0))
;; index of highest log entry known to be committed (initialized to 0, increases monotonically)
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
;; For each server, index of the next log entry to send to that server
;; (initialized to leaderlast log index + 1)
(def next-index (atom {}))
;; For each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)
(def match-index (atom {}))

;; A synchronized set of in-memory values that mirrors the persistent values of current_term and voted_for within
;; the terminfo DB table.
;; ALL updates to these persistent values will only be done within a coordinated transaction of these related refs.
(def current-term-and-vote (atom {:current-term 0 :voted-for nil}))

;; Info about this/current server.
(def this-server (atom {}))
;; Vector of other servers.
(def other-servers (atom []))

;; State of this server: follower OR candidate OR leader
(def server-state (atom :follower))

(defn- initial-index-map
  "Make a next-index map for known servers."
  [servers leader-last-log-index]
  (reduce #(assoc %1 %2 leader-last-log-index) {} servers))

(defn- server-names
  "Get an array of server names from server deployment info."
  [servers]
  (map #(util/qualified-server-name %1) servers))

(defn- get-server-state
  "Get the current server state."
  []
  @server-state)

(defn- set-server-state
  "Change server state."
  [new-state]
  (swap! server-state (fn [_] new-state)))

(defn get-commit-index
  "Get the commit index for this server."
  []
  @commit-index)

(defn get-num-servers
  "Return total number of servers in this cluster."
  []
  @num-servers)

(defn get-append-entries-call-sequence
  "Get value of AppendEntries call sequence number."
  []
  @append-entries-call-sequence)

(defn get-voted-sequence
  "Get value of VotedFor sequence."
  []
  @voted-sequence)

(defn reinit-next-and-match-indices
  "Reinitialize relevant state after an election."
  []
  (let [names (server-names @other-servers)
        last-log-entry (persistence/get-last-log-entry)
        last-log-index (if (not-empty last-log-entry) (:log-index last-log-entry 0) 0)]
    (swap! next-index (fn [_] (initial-index-map names last-log-index)))
    (swap! match-index (fn [_] (initial-index-map names 0)))))

(defn init-with-servers
  "Initialize volatile state for a given number of servers in the cluster."
  [servers current-server]
  (swap! num-servers (fn [_] (count servers)))
  (swap! this-server (fn [_] current-server))
  (swap! other-servers (fn [_] (filterv #(not= current-server %1) servers)))
  (reinit-next-and-match-indices))

(defn- set-index-value
  "Set the association for key to value in a map wrapped inside an atom."
  [atom-map key value]
  (swap! atom-map #(assoc %1 key value)))

(defn set-next-index-for-server
  "Set the next log entry index to send to a particular server."
  [server-info next-log-index]
  (set-index-value next-index (util/qualified-server-name server-info) next-log-index))

(defn get-next-index-for-server
  "Get the next-index value for a server."
  [server-info]
  (get @next-index (util/qualified-server-name server-info) 0))

(defn set-match-index-for-server
  "Set the next log entry index to send to a particular server."
  [server-info server-match-index]
  (set-index-value match-index (util/qualified-server-name server-info) server-match-index))

(defn get-match-index-for-server
  "Get the match-index value for a server."
  [server-info]
  (get @match-index (util/qualified-server-name server-info) 0))

(defn set-indices-for-server
  "Set next and match indices for server."
  [server-info index]
  (let [server-name (util/qualified-server-name server-info)]
    (set-index-value next-index server-name index)
    (set-index-value match-index server-name index)))

(defn inc-append-entries-call-sequence
  "Increment the AppendEntries call sequence number."
  []
  (swap! append-entries-call-sequence inc))

(defn inc-voted-sequence
  "Increment the voted-sequence number."
  []
  (swap! voted-sequence inc))

(defn get-other-servers
  "Get a list of other servers info."
  []
  @other-servers)

(defn get-this-server
  "Get info about this server."
  []
  @this-server)

(defn get-this-server-name
  "Get the qualified name of the current server."
  []
  (util/qualified-server-name @this-server))

(defn get-current-term
  "Get the current term value."
  []
  (:current-term @current-term-and-vote))

(defn get-voted-for
  "Get name of server this server instance voted for."
  []
  (:voted-for @current-term-and-vote))

(defn init-term-and-last-voted-for
  "Initialize current term and last voted for values from persistent storage."
  []
  (l/debug "Current term is: " (persistence/get-current-term))
  (swap! current-term-and-vote (fn [_]
                                 {:current-term (persistence/get-current-term)
                                  :voted-for (persistence/get-voted-for)})))

(defn- swap-term-and-voted-for-info
  "Helper to swap current term and voted for info."
  [old-info new-term new-voted-for]
  (if (>= new-term (:current-term old-info 0))
    (let [new-info {:current-term new-term
                    :voted-for new-voted-for}]
      (persistence/save-current-term-and-voted-for new-term new-voted-for)
      new-info)
    old-info))

(defn update-current-term-and-voted-for
  "Synchronously update the current-term and voted-for values and their corresponding persistent state."
  [new-term new-voted-for]
  (swap! current-term-and-vote (fn [old-info]
                                 (swap-term-and-voted-for-info old-info new-term new-voted-for))))

(defn inc-current-term-and-vote-for-self
  "Synchronously increment the current-term and set the voted-for value
  to the current server and persist this info."
  []
  (swap! current-term-and-vote (fn [old-info]
                                 (swap-term-and-voted-for-info
                                  old-info
                                  (inc (:current-term old-info))
                                  (util/qualified-server-name @this-server)))))

(defn is-candidate?
  "Is this server a candidate at this time?"
  []
  (= @server-state :candidate))

(defn is-leader?
  "Is this sever a leader at this time?"
  []
  (= @server-state :leader))

(defn is-follower?
  "Is this server a follower at this time?"
  []
  (= @server-state :follower))

(defn become-leader
  "Become a leader."
  []
  (set-server-state :leader)
  ;; next-index and match-index values must be reinitialized after election
  (reinit-next-and-match-indices))

(defn become-follower
  "Become a follower."
  []
  (set-server-state :follower))

(defn become-candidate
  "Become a candidate."
  []
  (set-server-state :candidate))

