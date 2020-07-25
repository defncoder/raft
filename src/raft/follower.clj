(ns raft.follower
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.state :as state]))

(defn- can-append-logs?
  "Check if logs from AppendRequest can be used:
  1. Logs in request cannot be used if its term < currentTerm (§5.1)
  2. Logs in request cannot be used if local log doesn’t contain
     an entry at index request.prevLogIndex whose term matches request.prevLogTerm (§5.3).
  See http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf for details."
  [request]
  (when (not-empty (:entries request))
    (l/trace "Got append entries request with a non-empty logs list."))
  (let [request-term (:term request)
        current-term (state/get-current-term)]
    (cond
      (< request-term current-term) (do
                                      (l/debug "Non-empty list but request term " request-term "is less than current term:" current-term)
                                      false)
      (empty? (:entries request)) false
      (not
       (persistence/has-log-at-term-and-index?
        (:prev-log-term request)
        (:prev-log-index request)))  (do
                                       (l/trace "Non-empty list but has-log-at-index-with-term? with prevLogIndex:"
                                                (:prev-log-index request)
                                                "and prevLogTerm: " (:prev-log-term request) "returned false.")
                                       false)
      :else true)))

(defn- new-commit-index-for-request
  "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry in request)"
  [request prev-commit-index]
  (if (> (:leader-commit request) prev-commit-index)
    (min (:leader-commit request) (:idx (last (:entries request))))
    prev-commit-index))

(defn- delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  ;; TODO: See if this can be done using a single delete statement instead of 1 for each log entry in the input.
  (map #(persistence/delete-conflicting-log-entries
         (:idx %1)
         (:term %1))
       (:entries request)))

(defn- append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf
  NOTE: This function is only for a follower. Look at <> for how clients of the leader ask it to add new log entries."
  [request]
  (delete-conflicting-entries-for-request request)
  (persistence/save-log-entries (:entries request))
  (reset! state/commit-index (new-commit-index-for-request request (state/get-commit-index))))

(defn become-a-follower
  "Become a follower."
  [& [new-term]]
  (l/trace "Changing state to be a follower...")
  (state/update-current-term-and-voted-for (or new-term (state/get-current-term)) nil)
  (state/become-follower))

(defn process-server-response
  "Process response to a vote request."
  [response]
  (let [term (:term response 0)]
    (when (> term (state/get-current-term))
      (l/debug "Response from servers had a higher term than current term. Becoming a follower..." term (state/get-current-term))
      (become-a-follower term))))

(defn handle-append-request
  "Handle an AppendEntries request."
  [request]
  ;; Increment the sequence that's maintained for the number of times an AppendRequest call is seen.
  (state/inc-append-entries-request-sequence)
  ;; Remember current leader. Will be useful to redirect client requests to this server.
  (state/set-current-leader (:leader-id request))
  (let [log-entries (not-empty (:entries request))
        can-append?  (can-append-logs? request)]
    (when can-append?
      (append-log-entries request))
    (when (and log-entries (not can-append?))
      (l/trace "Has log entries but can't append."))
    ;; (If request term > current term then update to new term and become a follower.)
    ;;           OR
    ;; (If this server is a candidate OR a leader AND an AppendEntries RPC
    ;; came in from new leader then convert to a follower.)
    (when (or (> (:term request) (state/get-current-term))
              (not (state/is-follower?)))
      (become-a-follower (max (:term request) (state/get-current-term))))
    ;; Return the response for the request.
    {:term (state/get-current-term) :success (if log-entries can-append? true)}))
