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
   [java.util.concurrent TimeUnit]
   [io.grpc.stub StreamObserver]
   [io.grpc Server ServerBuilder ManagedChannelBuilder StatusRuntimeException]
   [raft.rpc
    RaftContainer RaftRPCGrpc LogEntry AppendRequest
    AppendResponse VoteRequest VoteResponse RaftRPCGrpc$RaftRPCImplBase])
  
  (:gen-class
   :name raft.grpcservice.RaftRPCImpl
   :extends
   raft.rpc.RaftRPCGrpc$RaftRPCImplBase))

(declare become-a-follower)
(declare become-a-leader)

(defn- make-new-grpc-channel
  "Get a channel for a host."
  [server-info]
  (-> (ManagedChannelBuilder/forAddress (:host server-info) (:port server-info))
      (.usePlaintext)
      .build))

(defn- make-new-grpc-client
  "Construct a gRPC client for host and port."
  [server-info timeout]
  (-> (make-new-grpc-channel server-info)
      (raft.rpc.RaftRPCGrpc/newBlockingStub)
      ))

(defn- client-for-server
  "Get a gRPC client to communicate with a particular server."
  [server-info timeout]
  ;; (l/info @grpc-clients)

  (if-let [client (get @grpc-clients server-info)]
    (do
      ;; (l/info "Channel state for server" (:port server-info) "is"  (-> client (.getChannel) (.getState false)))
      (.withDeadlineAfter client timeout TimeUnit/MILLISECONDS))
    (let [new-client (make-new-grpc-client server-info timeout)]
      (l/trace "Created new client: " new-client)
      (swap! grpc-clients #(assoc %1 server-info new-client))
      (.withDeadlineAfter new-client timeout TimeUnit/MILLISECONDS))))

(defn- construct-log-entry
  "Create a new LogEntry object."
  [log-entry]
  (l/trace "Log entry is: " log-entry)
  (-> (raft.rpc.LogEntry/newBuilder)
      (.setLogIndex (:log_index log-entry 0))
      (.setTerm (:term log-entry 0))
      (.setCommand (:command log-entry "Empty"))
      (.build)))

(defn- construct-append-request
  "Create a new AppendRequest object for a term."
  [data]
  (let [request (raft.rpc.AppendRequest/newBuilder)
        log-entries (doall (map construct-log-entry (:log-entries data)))]

    (doto request
      (.setTerm (:leader-term data))
      (.setLeaderId (:leader-id data "localhost"))
      (.setPrevLogIndex (:prev-log-index data 0))
      (.setPrevLogTerm (:prev-log-term data 0))
      (.setLeaderCommitIndex (:leader-commit-index data 0)))

    (doseq [e log-entries]
      (.addLogEntry request e))

    (.build request)))

(defn- send-append-entries-request
  "Make an AppendEntries gRPC call to a server an wait up to timeout for a response.
  Returns response in a :response key.
  If an error happened it is returned in a :error key."
  [server-info request timeout]
  (try
    (let [grpc-client (client-for-server server-info timeout)
          response (.appendEntries grpc-client request)]
      {:response response})
    (catch StatusRuntimeException e
      (do
        {:error e}))
    (finally)))

(defn- construct-heartbeat-request
  "Construct a new heartbeat request. Heartbeat requests are just AppendEntries requests
  with an empty log-entries list."
  []
  (construct-append-request {:leader-term (state/get-current-term)
                             :leader-id (state/get-this-server-name)}))

(defn make-heartbeat-requests
  "Send a heartbeat request to all other servers."
  [servers timeout]
  (let [heartbeat-request (construct-heartbeat-request)]
    ;; Heartbeat requests are sent out as AppendEntries request with an empty log-entries list.
    (doall (pmap (fn [server-info]
                   (send-append-entries-request server-info heartbeat-request timeout)) servers))))

(defn- construct-vote-request
  "Construct a vote request message to be sent to all other servers."
  []
  (let [last-log-entry (persistence/get-last-log-entry)]
    (-> (raft.rpc.VoteRequest/newBuilder)
        (.setTerm (state/get-current-term))
        (.setCandidateId (state/get-this-server-name))
        (.setLastLogIndex (:log_index last-log-entry 0))
        (.setLastLogTerm (:term last-log-entry 0))
        (.build))))

(defn- make-vote-request
  "Make a vote request to a server."
  [server-info vote-request timeout]
  ;; (l/info "Making vote request to: " server-info)

  (try
    (let [grpc-client (client-for-server server-info timeout)
          response (.requestVote grpc-client vote-request)]
      (if (.getVoteGranted response)
        (l/trace "Received vote from server: " server-info "Term: "(.getTerm response) "Current term: "(state/get-current-term)))
      {:response response})
    (catch StatusRuntimeException e
      (do
        ;; (if (= 10020 (:port server-info))
        ;;   (l/info "Exception: " e))
        ;; (l/info "Exception: " e)
        {:error e}))
    (finally)))

(defn make-vote-requests
  "Make a request for vote to a server."
  [servers timeout]
  (let [vote-request (construct-vote-request)]
    (doall (pmap (fn [server-info] (make-vote-request server-info vote-request timeout)) servers))))

(defn- send-logs-entries-to-server
  "Send num-entries log entries starting at given index to server."
  [log-entries prev-log-entry server-info timeout]
  (let [prev-log-index (if prev-log-entry (:log_index prev-log-entry 0) 0)
        prev-log-term  (if prev-log-entry (:term prev-log-entry 0) 0)
        data {:leader-term (persistence/get-current-term)
              :leader-id (state/get-this-server-name)
              :prev-log-index prev-log-index
              :prev-log-term prev-log-term
              :leader-commit-index (state/get-commit-index)
              :log-entries log-entries}
        request (construct-append-request data)]
    ;; Send data to server and return the response map.
    (l/trace "Sending this data: " data "To server: " server-info)
    (l/debug "Sending " (count log-entries) "records to server: " (util/qualified-server-name server-info))
    (send-append-entries-request server-info request timeout)))

(defn send-logs-to-server
  "Send log entries to server."
  [server-info timeout]

  (loop [index (state/get-next-index-for-server server-info)]
    (let [log-entries (persistence/get-log-entries index 20)] ;; Read up to 20 log entries at a time.
      (if (not-empty log-entries)
        (let [prev-log-entry (persistence/get-prev-log-entry index)
              response-map (send-logs-entries-to-server log-entries prev-log-entry server-info timeout)
              response (:response response-map)]
          (cond
            ;; Encountered an error in sending data to server.
            (:error response-map) response-map
            ;; Term on receiving server is > current-term. Current server will become a follower.
            (> (.getTerm response) (state/get-current-term)) response-map
            ;; Receiving server couldn't accept log-entries we sent because
            ;; it would create a gap in its log.
            ;; If AppendEntries fails because of log inconsistency then decrement nextIndex and retry (§5.3)
            (not (.getSuccess response)) (if (> index 0)
                                           (do
                                             (l/debug "Got a log-inconsistency result. Retrying with previous index.")
                                             (recur (dec index)))
                                           response-map)
            ;; Successfully sent log entries to server. Try next set of entries, if any.
            :else (let [next-index (+ index (count log-entries) 1)]
                    (state/set-indices-for-server server-info next-index)
                    (recur next-index))))
        ;; Log entries exhausted. Return a success response.
        (:response (util/make-append-logs-response (state/get-current-term) true))))))

(defn- become-a-follower
  "Become a follower."
  [new-term]
  (l/debug "Changing state to be a follower...")
  (state/update-current-term-and-voted-for new-term nil)
  (state/become-follower))

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

(defn- process-terms-in-responses
  "Process term fields in responses. This is in case one of the other servers
  has a higher current-term. If so, this server must become a follower."
  [responses]
  (let [max-term (max-term-from-responses responses)]
    (l/trace "Max term from responses: " max-term)
    (when (> max-term (state/get-current-term))
      (l/trace "Response from servers had a higher term than current term. Becoming a follower..." max-term (state/get-current-term))
      (become-a-follower max-term))))

(defn- send-heartbeat-to-servers
  "Send heartbeat requests to other servers."
  [timeout]
  (async/thread
    (do
      (l/trace "Sending heartbeat requests to other servers...")
      (let [responses (client/make-heartbeat-requests (state/get-other-servers) timeout)]
        (process-terms-in-responses responses)))))

(defn- async-heartbeat-loop
  "A separate thread to check and send heartbeat requests to other servers whenever
  this server is the leader."
  []
  (async/thread
    (loop []
      (when (state/is-leader?)
        (send-heartbeat-to-servers 100)
        ;; Sleep for 100 milliseconds.
        (Thread/sleep 100)
        (recur)))))

(defn- become-a-leader
  "Become a leader and initiate appropriate activities."
  []
  (l/info "Won election. Becoming a leader!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  (state/become-leader)
  (async-heartbeat-loop))

(defn- count-votes-received
  "Count the total number of votes received from all responses plus one for self vote."
  [responses]
  (inc (count (filter #(some-> %1 :response (.getVoteGranted)) responses))))

(defn- won-election?
  "Did this server win the election?"
  [responses]
  ;; Is total votes in favor > floor(total-number-of-servers/2)
  (> (count-votes-received responses) (quot (state/get-num-servers) 2)))

(defn- valid-responses
  "Filter for valid responses."
  [responses]
  (filter some? (map #(:response %1) responses)))

(defn- max-term-response
  "Max term index from responses. Assume responses are valid and is an array of objects with a
  .getTerm function implemented."
  [responses]
  (reduce #(if (> (.getTerm %1) (.getTerm %2)) %1 %2) responses))

(defn- start-new-election
  "Work to do as a candidate."
  [timeout]
  (l/trace "Starting new election...")
  (state/inc-current-term-and-vote-for-self)
  (let [other-servers (state/get-other-servers)
        responses (client/make-vote-requests other-servers timeout)]
    (process-terms-in-responses responses)
    (when (and (state/is-candidate?)
               (won-election? responses))
      (become-a-leader))))

(defn- got-new-rpc-requests?
  "Did this server get either AppendEntries or VoteRequest RPC requests?"
  [prev-append-sequence prev-voted-sequence]
  (or
   (not= prev-append-sequence (state/get-append-entries-call-sequence))
   (not= prev-voted-sequence (state/get-voted-sequence))))

(defn- async-election-loop
  "Main loop for service."
  []
  (async/thread
    (loop []
      (let [append-sequence (state/get-append-entries-call-sequence)
            voted-sequence (state/get-voted-sequence)
            election-timeout (election/choose-election-timeout)]
        ;; Sleep for election-timeout to see if some other server might send requests.
        (Thread/sleep election-timeout)
        (l/trace "Woke up from election timeout of" election-timeout "milliseconds.")

        ;; If this server didn't receive new RPC requests that might've
        ;; changed it to a follower, then become a candidate.
        ;; If idle timeout elapses without receiving AppendEntriesRPC from current leader
        ;; OR granting vote to a candidate then convert to candidate.
        (when (and (not (state/is-leader?))
                   (not (got-new-rpc-requests? append-sequence voted-sequence)))
          (state/become-candidate)
          (start-new-election 100)))
      
      (recur))))

(defn start-raft-service [server-info]
  (l/info "Starting gRPC service...")
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
    (async-election-loop)
    (l/info "gRPC service started.")
    server))

(defn- can-append-logs?
  "Check if logs from AppendRequest can be used:
  1. Logs in request cannot be used if its term < currentTerm (§5.1)
  2. Logs in request cannot be used if local log doesn’t contain
     an entry at index request.prevLogIndex whose term matches request.prevLogTerm (§5.3).
  See http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf for details."
  [request]

  (if (not-empty (.getLogEntryList request))
    (l/debug "Got append entries request with a non-empty logs list."))
  
  (cond
    (empty? (.getLogEntryList request)) false
    (< (.getTerm request) (state/get-current-term)) (do
                                                      (l/debug "Non-empty list but request term " (.getTerm request) "is less than current term:" (state/get-current-term))
                                                      false)
    (not
     (persistence/has-log-at-index-with-term?
      (.getPrevLogIndex request)
      (.getPrevLogTerm request)))  (do
                                     (l/debug "Non-empty list but has-log-at-index-with-term? with prevLogIndex:" (.getPrevLogIndex request) "and prevLogTerm: " (.getPrevLogTerm request) "returned false."))
    :else true)
  
  ;; (and (not-empty (.getLogEntryList request))
  ;;      (>= (.getTerm request) (state/get-current-term))
  ;;      (persistence/has-log-at-index-with-term? (.getPrevLogIndex request) (.getPrevLogTerm request)))
  )

(defn- new-commit-index-for-request
  "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry in request)"
  [request prev-commit-index]
  (if (> (.getLeaderCommitIndex request) prev-commit-index)
    (min (.getLeaderCommitIndex request) (.getLogIndex (last (.getLogEntryList request))))
    prev-commit-index))

(defn- delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  ;; TODO: See if this can be done using a single delete statement instead of 1 for each log entry in the input.
  (map #(persistence/delete-conflicting-log-entries
         (.getLogIndex %1)
         (.getTerm %1))
       (.getLogEntryList request)))

(defn- is-server-trailing?
  "Is a server trailing the current server in logs synced?"
  [server-info cur-log-index]
  (>= cur-log-index (state/get-next-index-for-server server-info)))

(defn- servers-with-trailing-logs
  "Get a list of servers whose local storage may be trailing this server."
  []
  (let [last-log-index (persistence/get-last-log-index)]
    (filter #(>= last-log-index (state/get-next-index-for-server %1))
            (state/get-other-servers))))

(defn- group-servers-by-trailing-logs
  "Return a map of servers grouped by whether they have trailing logs or not."
  []
  (let [last-index (persistence/get-last-log-index)]
    (group-by #(is-server-trailing? %1 last-index) (state/get-other-servers))))

(defn propagate-logs
  "Propagate logs to other servers."
  []
  (doall (pmap #(client/send-logs-to-server %1 100) (servers-with-trailing-logs))))

(defn add-new-log-entry
  "Add a new log entry to local storage for the current term."
  [command]
  (persistence/add-new-log-entry (state/get-current-term) command)
  (propagate-logs))

(defn- append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [request]
  (delete-conflicting-entries-for-request request)
  (persistence/add-missing-log-entries (.getLogEntryList request))
  (reset! state/commit-index (new-commit-index-for-request request @state/commit-index)))

(defn- handle-append-request
  "Handle an AppendEntries request."
  [request]
  ;; Increment the sequence that's maintained for the number of times an AppendRequest call is seen.
  (state/inc-append-entries-call-sequence)

  (let [has-log-entries? (not-empty (.getLogEntryList request))
        can-append?  (can-append-logs? request)]
    (if (> (count (.getLogEntryList request)) 0)
      (l/debug "Log entries is non-zero..."))
    
    (if can-append?
      (append-log-entries request))

    (if (and has-log-entries? (not can-append?) )
      (l/debug "Has log entries but can't append."))

    ;; (If request term > current term then update to new term and become a follower.)
    ;;           OR
    ;; (If this server is a candidate OR a leader AND an AppendEntries RPC
    ;; came in from new leader then convert to a follower.)
    (when (or (> (.getTerm request) (state/get-current-term))
              (not (state/is-follower?)))
      (become-a-follower (max (.getTerm request) (state/get-current-term))))
    
    ;; Return the response for the request.
    (util/make-append-logs-response (state/get-current-term) can-append?)))

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
    (let [last-log-term (:term last-log-entry)
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
  (if (> (.getTerm request) (state/get-current-term))
    (do
      (l/trace "VoteRequest received with term > current-term. Changing to a follower************" (.getTerm request) (state/get-current-term))
      (become-a-follower (.getTerm request))))

  (if (should-vote-for-candidate? request)
    (do
      (l/trace "Vote granted for request: " request "Current term: " (state/get-current-term))
      (remember-vote-granted request)
      (make-vote-response (state/get-current-term) true))
    (make-vote-response (state/get-current-term) false)))

(defn -appendEntries [this request response]
  (doto response
    (.onNext (handle-append-request request))
    (.onCompleted)))

(defn -requestVote [this req res]
  (doto res
    (.onNext (handle-vote-request req))
    (.onCompleted)))
