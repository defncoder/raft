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
(def append-entries-request-sequence (atom 0))

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

;; Current leader's qualified name. Only valid when current server is a follower
;; AND if the current leader has sent at least one heartbeat message successfully.
(def current-leader-name (atom ""))

(defn majority-number
  "Number of servers that would make up a majority."
  []
  (inc (quot (count @other-servers) 2)))

(defn is-majority?
  "Is this number a majority?"
  [num]
  (>= num (majority-number)))

(defn- initial-index-map
  "Make a next-index map for known servers."
  [servers leader-last-log-index]
  (reduce #(assoc %1 %2 leader-last-log-index) {} servers))

(defn- server-names
  "Get an array of server names from server deployment info."
  [servers]
  (map util/qualified-server-name servers))

(defn- get-server-state
  "Get the current server state."
  []
  @server-state)

(defn- set-server-state
  "Change server state."
  [new-state]
  (reset! server-state new-state ))

(defn get-commit-index
  "Get the commit index for this server."
  []
  @commit-index)

(defn set-commit-index
  "Set the new commit index for this server."
  [index]
  (swap! commit-index (fn [old-index] (if (> index old-index)
                                        index
                                        old-index))))

(defn get-num-servers
  "Return total number of servers in this cluster."
  []
  @num-servers)

(defn get-append-entries-request-sequence
  "Get value of AppendEntries call sequence number."
  []
  @append-entries-request-sequence)

(defn get-voted-sequence
  "Get value of VotedFor sequence."
  []
  @voted-sequence)

(defn inc-append-entries-request-sequence
  "Increment the AppendEntries call sequence number."
  []
  (swap! append-entries-request-sequence inc))

(defn inc-voted-sequence
  "Increment the voted-sequence number."
  []
  (swap! voted-sequence inc))

(defn reinit-next-and-match-indices
  "Reinitialize relevant state after an election."
  []
  (let [names (server-names @other-servers)
        last-log-entry (persistence/get-last-log-entry)
        last-log-index (if (not-empty last-log-entry) (:idx last-log-entry 0) 0)]
    ;; Initialize next-index array entries to last-log-index+1
    (swap! next-index (fn [_] (initial-index-map names (inc last-log-index))))
    ;; Initialize match-index array entries to 0
    (swap! match-index (fn [_] (initial-index-map names 0)))))

(defn init-with-servers
  "Initialize volatile state for a given number of servers in the cluster."
  [servers current-server]
  (reset! num-servers (count servers))
  (reset! this-server current-server)
  (reset! other-servers (filterv #(not= current-server %1) servers))
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
  (get @next-index (util/qualified-server-name server-info) 1))

(defn add-next-index-for-server
  "Add the given offset to the next index value for a server."
  [server offset]
  (let [server-name (util/qualified-server-name server)]
    (swap! next-index #(assoc %1 server-name (+ offset (get %1 server-name 1))))))

(defn get-next-index-map-for-servers
  "Get the next index values map for servers."
  []
  @next-index)

(defn set-match-index-for-server
  "Set the next log entry index to send to a particular server."
  [server-info server-match-index]
  (set-index-value match-index (util/qualified-server-name server-info) server-match-index))

(defn get-match-index-for-server
  "Get the match-index value for a server."
  [server-info]
  (get @match-index (util/qualified-server-name server-info) 0))

(defn get-match-indices
  "Get all the match indices."
  []
  @match-index)

(defn set-indices-for-server
  "Set next and match indices for server."
  [server-info index]
  (let [server-name (util/qualified-server-name server-info)]
    (set-index-value next-index server-name index)
    (set-index-value match-index server-name index)))

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

(defn get-current-term-and-voted-for
  "Get the current term and who this server voted for in that term."
  []
  @current-term-and-vote)

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

(defn- save-current-term-and-voted-for-info
  "Save current term and voted for info."
  [new-info]
  (persistence/save-current-term-and-voted-for (:current-term new-info) (:voted-for new-info))
  new-info)

(defn- update-if-newer
  "Update the term if it's newer."
  [old-info new-term new-voted-for]
  (if (> new-term (:current-term old-info))
    (save-current-term-and-voted-for-info {:current-term new-term
                                           :voted-for new-voted-for})
    old-info))

(defn update-current-term-and-voted-for
  "Synchronously update the current-term and voted-for values and their corresponding persistent state."
  [new-term new-voted-for]
  (swap! current-term-and-vote (fn [old-info]
                                 (update-if-newer old-info new-term new-voted-for))))

(defn inc-current-term-and-vote-for-self
  "Synchronously increment the current-term and set the voted-for value
  to the current server and persist this info.
  Returns true if able to vote for self. false otherwise."
  []
  (let [self (util/qualified-server-name @this-server)
        new-term (inc (get-current-term))
        new-info (swap! current-term-and-vote (fn [old-info]
                                                (update-if-newer old-info new-term self)))]
    (= self (:voted-for new-info))))

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

(defn get-current-leader
  "Get the fully qualified name of the current leader."
  []
  @current-leader-name)

(defn set-current-leader
  "Set the fully qualified name of the current leader."
  [leader]
  (reset! current-leader-name leader))
