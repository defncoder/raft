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
   [io.grpc Server ServerBuilder]
   [raft.rpc AppendRequest AppendResponse VoteRequest VoteResponse RaftRPCGrpc$RaftRPCImplBase])
  
  (:gen-class
   :name raft.grpcservice.RaftRPCImpl
   :extends
   raft.rpc.RaftRPCGrpc$RaftRPCImplBase))

(defn- count-votes-received
  "Count the total number of votes received from all responses plus one for self vote."
  [responses]
  (inc (count (filter #(some-> %1 :response (.getVoteGranted)) responses))))

(defn- won-election?
  "Did this server win the election?"
  [responses]
  ;; Is total votes in favor > floor(total-number-of-servers/2)
  (> (count-votes-received responses) (quot (state/get-num-servers) 2)))

(defn- term-from-response
  "Get term value from a response."
  [response]
  (if-let [r (:response response)]
    (.getTerm r)
    0))

(defn- max-term-from-responses
  "Get max term from a collection of responses."
  [responses]
  (if-let [terms (seq (map term-from-response responses))]
    (apply max terms)
    0))

(defn- valid-responses
  "Filter for valid responses."
  [responses]
  (filter some? (map #(:response %1) responses)))

(defn- max-term-response
  "Max term index from responses. Assume responses are valid and is an array of objects with a
  .getTerm function implemented."
  [responses]
  (reduce #(if (> (.getTerm %1) (.getTerm %2)) %1 %2) responses))

(defn- process-terms-in-responses
  "Process term fields in responses. This is in case one of the other servers
  has a higher current-term. If so, this server must become a follower."
  [responses]
  (let [max-term (max-term-from-responses responses)]
    (l/trace "Max term from responses: " max-term)
    (when (> max-term (state/get-current-term))
      (l/debug "Response from servers had a higher term than current term. Becoming a follower..." max-term (state/get-current-term))
      (state/update-current-term-and-voted-for max-term nil)
      (state/become-follower))))

(defn- send-heartbeat-to-servers
  "Send heartbeat requests to other servers."
  [timeout]
  (async/thread
    (do
      (l/trace "Sending heartbeat requests to other servers...")
      (let [responses (client/make-heartbeat-requests (state/get-other-servers) timeout)]
        (process-terms-in-responses responses)))))

(defn- become-a-leader
  "Become a leader and initiate appropriate activities."
  [timeout]
  (l/info "Won election. Becoming a leader!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  (state/become-leader)
  (send-heartbeat-to-servers timeout))

(defn- start-new-election
  "Work to do as a candidate."
  [timeout]
  (l/debug "Starting new election...")
  (state/inc-current-term-and-vote-for-self)
  (let [other-servers (state/get-other-servers)
        responses (client/make-vote-requests other-servers timeout)]
    (process-terms-in-responses responses)
    (when (and (state/is-candidate?)
               (won-election? responses))
      (become-a-leader timeout))))

(defn- become-a-candidate
  "Operations to do just as this server became a candidate."
  [timeout]
  (async/thread
    (do
      (state/become-candidate)
      (start-new-election timeout))))

(defn- got-new-rpc-requests?
  "Did this server get either AppendEntries or VoteRequest RPC requests?"
  [prev-append-sequence prev-voted-sequence]
  (or
   (not= prev-append-sequence (state/get-append-entries-call-sequence))
   (not= prev-voted-sequence (state/get-voted-sequence))))

(defn- service-thread
  "Main loop for service."
  [server-name]
  (async/thread
    (loop []
      (let [append-sequence (state/get-append-entries-call-sequence)
            voted-sequence (state/get-voted-sequence)
            election-timeout (election/choose-election-timeout)
            idle-timeout (if (state/is-leader?) 100 election-timeout)
            grpc-timeout 80]
        ;; Sleep for idle-timeout to see if some other server might send requests.
        (Thread/sleep idle-timeout)
        (l/trace "Woke up from idle timeout of" idle-timeout "milliseconds.")

        (cond
          (state/is-leader?) (send-heartbeat-to-servers grpc-timeout)
          ;; If this server didn't receive new RPC requests that might've
          ;; changed it to a follower, then become a candidate.
          ;; If idle timeout elapses without receiving AppendEntriesRPC from current leader
          ;; OR granting vote to a candidate then convert to candidate.
          (not (got-new-rpc-requests? append-sequence voted-sequence)) (become-a-candidate grpc-timeout))

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

(defn- can-append-logs?
  "Check if logs from AppendRequest can be used:
  1. Logs in request cannot be used if its term < currentTerm (§5.1)
  2. Logs in request cannot be used if local log doesn’t contain
     an entry at index request.prevLogIndex whose term matches request.prevLogTerm (§5.3).
  See http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf for details."
  [request]
  (and (not-empty (.getLogEntryList request))
       (>= (.getTerm request) (state/get-current-term))
       (persistence/has-log-at-index-with-term? (.getPrevLogIndex request) (.getPrevLogTerm request))))

(defn- new-commit-index-for-request
  "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry in request)"
  [request prev-commit-index]
  (if (> (.getLeaderCommitIndex request) prev-commit-index)
    (min (.getLeaderCommitIndex request) (.getLogIndex (last (.getLogEntryList request))))
    prev-commit-index))

(defn- make-response
  "Make a response with the given term and success values."
  [term success?]
  (-> (AppendResponse/newBuilder)
      (.setTerm term)
      (.setSuccess (true? success?))
      (.build)))

(defn- delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  ;; TODO: See if this can be done using a single delete statement instead of 1 for each log entry in the input.
  (map #(persistence/delete-conflicting-log-entries (.getLogIndex %1) (.getTerm %1)) (.getLogEntryList request)))

(defn- append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [request]
  (delete-conflicting-entries-for-request request)
  (persistence/append-new-log-entries (.getLogEntryList request))
  (reset! state/commit-index (new-commit-index-for-request request @state/commit-index)))

(defn- handle-append-request
  "Handle an AppendEntries request."
  [request]
  ;; Increment the sequence that's maintained for the number of times an AppendRequest call is seen.
  (state/inc-append-entries-call-sequence)

  (let [can-append?  (can-append-logs? request)
        current-term (state/get-current-term)
        response     (make-response current-term can-append?)]
    (if can-append?
      (append-log-entries request))
    ;; Additional bookkeeping based on current server state.
    ;; If request term > current term then update current term to the request term and reset
    ;; candidate voted for.
    (when (> (.getTerm request) current-term)
      (state/update-current-term-and-voted-for (.getTerm request) nil))
    
    ;; If this server is a candidate in an election AND an AppendEntries RPC
    ;; came in from new leader then convert to a follower.
    (when (state/is-candidate?)
      (l/debug "Got AppendEntries RPC from" (.getCandidateId request) "while current server was a candidate. Becoming a follower..." )
      (state/become-follower))
    ;; Return the response for the request.
    response))

(defn- make-vote-response
  "Make a VoteResponse"
  [term vote-granted?]
  (-> (VoteResponse/newBuilder)
      (.setTerm term)
      (.setVoteGranted vote-granted?)
      (.build)))

;; Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
;; If the logs have last entries with different terms, then the log with the later term is more up-to-date.
;; If the logs end with the same term, then whichever log is longer is more up-to-date.
(defn- is-candidate-up-to-date?
  "Are a candidate's log entries up to date?"
  [candidate-last-log-index candidate-last-log-term]
  (if-let [last-log-entry (persistence/get-last-log-entry)]
    (let [last-log-term (:term_number last-log-entry)
          last-log-index (:log_index last-log-entry)]
      (if (= candidate-last-log-term last-log-term)
        (>= candidate-last-log-index last-log-index)
        (> candidate-last-log-term last-log-term)))
    true))

(defn- should-vote-for-candidate?
  "Can this server vote for a candidate?"
  [request]

  (let [candidate-term (.getTerm request)
        candidate-id (.getCandidateId request)
        candidate-last-log-term (.getLastLogTerm request)
        candidate-last-log-index (.getLastLogIndex request)
        current-term (state/get-current-term)]
    (l/trace "VoteRequest info: " candidate-term candidate-id candidate-last-log-term candidate-last-log-index current-term)
    (if (< candidate-term current-term)
      false
      (let [voted-for (state/get-voted-for)]
        (and (or (nil? voted-for) (= candidate-id voted-for))
             (is-candidate-up-to-date? candidate-last-log-index candidate-last-log-term))))))

(defn- remember-vote-granted
  "Bookkeeping mechanism once vote is granted to someone."
  [request]
  (state/inc-voted-sequence)
  (l/trace "Updating current term and voted for to: " (.getTerm request) (.getCandidateId request))
  (state/update-current-term-and-voted-for (.getTerm request) (.getCandidateId request))
  (l/trace "Reading current term and voted for: " (state/get-current-term) (state/get-voted-for)))

(defn handle-vote-request
  "Handle a VoteRequest message."
  [request]
  (let [current-term (state/get-current-term)
        last-voted-for (state/get-voted-for)]
    (if (> (.getTerm request) current-term)
      (do
        (l/debug "VoteRequest received with term > current-term. Changing to a follower************" (.getTerm request) current-term)
        (state/update-current-term-and-voted-for (.getTerm request) nil)
        (state/become-follower)))
    
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
