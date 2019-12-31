(ns raft.grpcservice
  (:require
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.election :as election]
   [raft.util :as util]
   [raft.grpcclient :as client]
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
   raft.rpc.RaftRPCGrpc$RaftRPCImplBase))

(defn candidate-operations
  "Work to do as a candidate."
  [timeout]
  ;; (l/info "Candidate operations...")
  (state/vote-for-self)
  (client/make-vote-requests (state/get-other-servers) timeout))

(defn service-thread
  "Main loop for service."
  [server-name]
  (async/thread
    (loop []
      (let [append-sequence (state/get-append-entries-call-sequence)
            voted-sequence (state/get-voted-sequence)
            election-timeout (election/choose-election-timeout)]
        (Thread/sleep election-timeout)
        ;; (l/info "Woke up from election timeout of " election-timeout "milliseconds.")

        ;; If election timeout elapses without receiving AppendEntriesRPC from current leader
        ;; or granting vote to candidate then convert to candidate.
        (if (and (= append-sequence (state/get-append-entries-call-sequence))
                 (= voted-sequence (state/get-voted-sequence)))
          (do
            ;; (l/info "That's it. I'm becoming a candidate!!!!!")
            (state/set-server-state :candidate)))
        
        (if (= (state/get-server-state) :candidate)
          (candidate-operations election-timeout))

        (recur)))))

(defn start-raft-service [server-info]
  (l/info "About to start gRPC service")
  (let [port (:port server-info)
        server-name (util/qualified-server-name server-info)
        raft-service (new raft.grpcservice.RaftRPCImpl)
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
    (service-thread server-name)
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
  (or (< (.getTerm req) (state/get-current-term))
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
  (make-response (state/get-current-term) true))

(defn unsuccessful-response
  "Make a heartbeat response."
  []
  (make-response (state/get-current-term) false))

(defn delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  (map #(persistence/delete-conflicting-log-entries (.getLogIndex %1) (.getTerm %1)) (.getLogEntryList request)))

(defn append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [request]
  (state/update-current-term-and-voted-for (.getTerm request) nil)
  (delete-conflicting-entries-for-request request)
  (persistence/append-new-log-entries (.getLogEntryList request))
  (reset! state/commit-index (new-commit-index-for-request request @state/commit-index))
  (make-response (state/get-current-term) true))

(defn handle-append-request
  "Handle an AppendEntries request."
  [request]
  (state/inc-append-entries-call-sequence)
  (cond
    ;; Handle heartbeat requests.
    (is-heartbeat-request? request) (do
                                      ;; Update current term if necessary.
                                      (state/update-current-term-and-voted-for (.getTerm request) nil)
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

;; Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
;; If the logs have last entries with different terms, then the log with the later term is more up-to-date.
;; If the logs end with the same term, then whichever log is longer is more up-to-date.
(defn is-candidate-up-to-date?
  "Are a candidate's log entries up to date?"
  [candidate-last-log-index candidate-last-log-term]
  (if-let [last-log-entry (persistence/get-last-log-entry)]
    (let [last-log-term (:term_number last-log-entry)
          last-log-index (:log_index last-log-entry)]
      (if (= candidate-last-log-term last-log-term)
        (>= candidate-last-log-index last-log-index)
        (> candidate-last-log-term last-log-term)))
    true))

(defn should-vote-for-candidate?
  "Can this server vote for a candidate?"
  [request]

  (let [candidate-term (.getTerm request)
        candidate-id (.getCandidateId request)
        candidate-last-log-term (.getLastLogTerm request)
        candidate-last-log-index (.getLastLogIndex request)
        current-term (state/get-current-term)]
    ;; (l/info "VoteRequest info: " candidate-term candidate-id candidate-last-log-term candidate-last-log-index current-term)
    (if (< candidate-term current-term)
      false
      (let [voted-for (state/get-voted-for)]
        ;; (l/info "Voted for is: " voted-for)
        (and (or (nil? voted-for) (= candidate-id voted-for))
             (is-candidate-up-to-date? candidate-last-log-index candidate-last-log-term))))))

(defn remember-vote-granted
  "Bookkeeping mechanism once vote is granted to someone."
  [request]
  (state/inc-voted-sequence)
  ;; (l/info "Updating current term and voted for to: " (.getTerm request) (.getCandidateId request))
  (state/update-current-term-and-voted-for (.getTerm request) (.getCandidateId request))
  ;; (l/info "Reading current term and voted for: " (state/get-current-term) (state/get-voted-for))
  )

(defn handle-vote-request
  "Handle a VoteRequest message."
  [request]
  (let [current-term (state/get-current-term)
        last-voted-for (state/get-voted-for)]
    (if (> (.getTerm request) current-term)
      (do
        (l/info "VoteRequest received with term > current-term. Changing to a follower************" (.getTerm request) current-term)
        (state/update-current-term-and-voted-for (.getTerm request) nil)
        (state/set-server-state :follower)))
    
    (if (should-vote-for-candidate? request)
      (do
        (remember-vote-granted request)
        (make-vote-response current-term true))
      (make-vote-response current-term false))))

(defn -appendEntries [this request response]
  (doto response
    (.onNext (handle-append-request request))
    (.onCompleted)))

(defn -requestVote [this req res]
  (doto res
    (.onNext (handle-vote-request req))
    (.onCompleted)))
