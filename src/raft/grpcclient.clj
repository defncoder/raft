(ns raft.grpcclient
  (:require [clojure.tools.logging :as l]
            [raft.persistence :as persistence]
            [raft.state :as state])
  (:import
   [java.util.concurrent TimeUnit]
   [raft.rpc RaftContainer RaftRPCGrpc AppendRequest AppendResponse LogEntry]
   [io.grpc ManagedChannelBuilder StatusRuntimeException]))

;; Cached gRPC clients so they can be reused.
(def grpc-clients (atom {}))

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
      (l/info "Created new client: " new-client)
      (swap! grpc-clients #(assoc %1 server-info new-client))
      (.withDeadlineAfter new-client timeout TimeUnit/MILLISECONDS))))

(defn loop-index
  "Loop with index on a sequable collection."
  [col f]
  (if-let [s (seq col)]
    (loop [index 0
           elems s]
      (if elems
        (do
          (f index (first elems))
          (recur (inc index) (next elems)))))))

(defn construct-append-request
  "Create a new AppendRequest object for a term."
  [data]
  (let [request (raft.rpc.AppendRequest/newBuilder)]
    (doto request
      (.setTerm (:leader-term data))
      (.setLeaderId (:leader-id data "localhost"))
      (.setPrevLogIndex (:prev-log-index data 0))
      (.setPrevLogTerm (:prev-log-term data 0))
      (.setLeaderCommitIndex (:leader-commit-index data 0)))

    ;; (l/info "Log entries is: " (:log-entries data))

    (loop-index (:log-entries data) #(.addLogEntry request %2))

    ;; (for [e (seq (:log-entries data))]
    ;;   (do 
    ;;     (l/info "*****************************Adding log entry:" e)
    ;;     (.addLogEntry request e)))
    
    (.build request)))

(defn construct-log-entry
  "Create a new LogEntry object."
  [log-index term-number command]
  (-> (raft.rpc.LogEntry/newBuilder)
      (.setLogIndex log-index)
      (.setTermNumber term-number)
      (.setCommand command)
      (.build)))

(defn construct-test-log-entries
  "Construct some number of test log entries."
  [num]
  (take num 
        (repeatedly #(construct-log-entry (rand-int 100) 10 "Hickey"))))

(defn make-append-request
  "Make an AppendRequest GRPC call."
  [server-info term timeout]
  (let [request (construct-append-request {:leader-term term
                                           :log-entries (construct-test-log-entries 2)})
        response (.appendEntries (client-for-server server-info timeout) request)
        current-term (state/get-current-term)
        new-term (.getTerm response)]
    (if (< current-term new-term)
      (state/update-current-term-and-voted-for new-term nil))
    (if (.getSuccess response)
      (l/info "Successful response with term result:" (.getTerm response) (.getSuccess response))
      (l/info "Not successful response." (.getTerm response) (.getSuccess response)))))

(defn append-request [server-info term timeout]
  (make-append-request server-info term timeout))

(defn- construct-heartbeat-request
  "Construct a new heartbeat request."
  []
  (construct-append-request {:leader-term (state/get-current-term)
                             :leader-id (state/get-this-server-name)}))

(defn- make-heartbeat-request
  "Make a heartbeat request to a server."
  [server-info heartbeat-request timeout]
  (try
    (let [grpc-client (client-for-server server-info timeout)
          response (.appendEntries grpc-client heartbeat-request)]
      {:response response})
    (catch StatusRuntimeException e
      (do
        {:error e}))
    (finally)))

(defn make-heartbeat-requests
  "Send a heartbeat request to all other servers."
  [servers timeout]
  (let [heartbeat-request (construct-heartbeat-request)]
    (doall (pmap (fn [server-info] (make-heartbeat-request server-info heartbeat-request timeout)) servers))))

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
        (l/info "Received vote from server: " server-info (.getTerm response) (state/get-current-term)))
      ;; (l/info "Received response. Term: " (.getTerm response) "Vote granted: " (.getVoteGranted response))
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
