(ns raft.grpcservice
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.state :as state]
   )
  (:import
   [io.grpc.stub StreamObserver]
   [io.grpc
    Server
    ServerBuilder]
   [raft.rpc
    AppendRequest
    AppendResponse
    VoteRequest
    VoteResponse
    RaftRPCGrpc$RaftRPCImplBase
    ])
  
  (:gen-class
   :name raft.grpcservice.RaftRPCImpl
   :extends
   raft.rpc.RaftRPCGrpc$RaftRPCImplBase)  
  )

(defn start [port]
  (l/info "About to start gRPC service")
  (let [raft-service (new raft.grpcservice.RaftRPCImpl)
        server (-> (ServerBuilder/forPort port)
                   (.addService raft-service)
                   (.build)
                   (.start))]
    (-> (Runtime/getRuntime)
        (.addShutdownHook
         (Thread. (fn []
                    (l/info "Shutdown hook invoked")
                    (if (not (nil? server))
                      (.shutdown server))))))
    server))

(defn is-heartbeat-request?
  "Is this a heartbeat request."
  [request]
  (empty? (.getLogEntryList request)))

(defn is-unacceptable-append-request?
  "Check if req should NOT be accepted.
  1. Request is unacceptable if its term < currentTerm (§5.1)
  2. Request is unacceptable if log doesn’t contain an entry at index request.prevLogIndex whose
     term matches request.prevLogTerm (§5.3).
  See http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf for details."
  [req]
  (or (< (.getTerm req) (persistence/get-current-term))
      (not (persistence/has-log-at-index-with-term? (.getPrevLogIndex req) (.getPrevLogTerm req)))))

(defn new-commit-index-for-request
  "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry in request)"
  [request prev-commit-index]
  (if (> (.getLeaderCommitIndex request) prev-commit-index)
    (min (.getLeaderCommitIndex request) (.getLogIndex (last (.getLogEntryList request))))
    prev-commit-index))

(defn make-response
  "Make a response with the given term and success values."
  [term success?]
  (-> (AppendResponse/newBuilder)
      (.setTerm term)
      (.setSuccess success?)
      (.build)))

(defn heartbeat-response
  "Make a heartbeat response."
  []
  (make-response (persistence/get-current-term) true))

(defn unsuccessful-response
  "Make a heartbeat response."
  []
  (make-response (persistence/get-current-term) false))

(defn delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  (map #(persistence/delete-conflicting-log-entries (.getLogIndex %1) (.getTerm %1)) (.getLogEntryList request)))

(defn append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [request]
  (persistence/cond-update-term-voted-for (.getTerm request))
  (delete-conflicting-entries-for-request request)
  (persistence/append-new-log-entries (.getLogEntryList request))
  (reset! state/commit-index (new-commit-index-for-request request @state/commit-index))
  (make-response (persistence/get-current-term) true))

(defn handle-append-request
  "Handle an AppendEntries request."
  [request]
  (cond
    ;; Handle heartbeat requests.
    (is-heartbeat-request? request) (do
                                      ;; Update current term if necessary.
                                      (persistence/cond-update-term-voted-for (.getTerm request))
                                      ;; Respond to heartbeat.
                                      (heartbeat-response))
    ;; Request is not acceptable. See is-unacceptable-append-request? for details.
    (is-unacceptable-append-request? request) (unsuccessful-response)
    ;; Handle request with log entries.
    :else (append-log-entries request)))

(defn make-vote-response
  "Make a VoteResponse"
  [term vote-granted?]
  (-> (VoteResponse/newBuilder)
      (.setTerm term)
      (.setVoteGranted vote-granted?)
      (.build)))


;; Raft determines which of two logs is more up-to-dateby comparing the index and term of the last entries in thelogs. If the logs have last entries with different terms, thenthe log with the later term is more up-to-date. If the logsend with the same term, then whichever log is longer ismore up-to-date.

(defn is-candidate-up-to-date?
  "Are a candidate's log entries up to date?"
  [candidate-last-log-index candidate-last-log-term]
  (if-let [last-log-entry (persistence/get-last-log-entry)]
    (if (not= candidate-last-log-term (:term_number last-log-entry))
      (> candidate-last-log-term (:term_number last-log-entry))
      (>= candidate-last-log-index (:log_index last-log-entry)))
    true))

(defn can-vote-for-candidate?
  "Can this server vote for a candidate?"
  [candidate-term candidate-id candidate-last-log-index candidate-last-log-term]
  (let [current-term (persistence/get-current-term)]
    (if (< candidate-term current-term)
      false
      (let [voted-for (persistence/get-candidate-voted-for)]
        (or (nil? voted-for) (and (= candidate-id voted-for) (is-candidate-up-to-date? candidate-id)))))))

(defn handle-vote-request
  "Handle a VoteRequest message."
  [request]
  (let [current-term (persistence/get-current-term)
        voted-for    (persistence/get-candidate-voted-for)]
    (if (< (.getTerm request) current-term)
      (make-vote-response current-term false)
      (make-vote-response current-term (or (nil? voted-for) (= (.getCandidateId request) voted-for) ())))))

(defn -appendEntries [this request response]
  (doto response
    (.onNext (handle-append-request request))
    (.onCompleted)))

(defn -requestVote [this req res]
  (doto res
    (.onNext (handle-vote-request req))
    (.onCompleted)))
