(ns raft.grpcclient
  (:require [clojure.tools.logging :as l]
            [raft.persistence :as persistence]
            [raft.state :as state]
            [raft.util :as util])
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
      (l/trace "Created new client: " new-client)
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

(defn construct-log-entry
  "Create a new LogEntry object."
  [log-entry]
  (l/trace "Log entry is: " log-entry)
  (-> (raft.rpc.LogEntry/newBuilder)
      (.setLogIndex (:log_index log-entry 0))
      (.setTerm (:term log-entry 0))
      (.setCommand (:command log-entry "Empty"))
      (.build)))

(defn construct-append-request
  "Create a new AppendRequest object for a term."
  [data]
  (let [request (raft.rpc.AppendRequest/newBuilder)
        log-entries (doall (map construct-log-entry (:log-entries data)))]

    (if (> (count log-entries) 0)
      (l/debug "Sending non-empty log entries..."))
    
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

(defn send-logs-to-server
  "Send log entries to server."
  [server-info timeout]
  (let [next-index  (state/get-next-index-for-server server-info)
        log-entries (persistence/get-log-entries next-index 10)
        prev-log-entry (persistence/get-prev-log-entry next-index)
        prev-log-index (if prev-log-entry (:log_index prev-log-entry 0) 0)
        prev-log-term  (if prev-log-entry (:term prev-log-entry 0) 0)
        data {:leader-term (persistence/get-current-term)
              :leader-id (state/get-this-server-name)
              :prev-log-index prev-log-index
              :prev-log-term prev-log-term
              :leader-commit-index (state/get-commit-index)
              :log-entries log-entries}
        request (construct-append-request data)
        ]
    (l/trace "Sending this data: " data "To server: " server-info)
    (l/debug "Sending " (count log-entries) "records to server: " (util/qualified-server-name server-info))
    (l/debug "*****************Num log entries is: " (count (.getLogEntryList request)))
    (let [response (send-append-entries-request server-info request timeout)]
      (when (:error response)
        (l/warn "Log entries transmission error: " (:error response))))))
