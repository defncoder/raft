(ns raft.grpcclient
  (:require [clojure.tools.logging :as l]
            [raft.persistence :as persistence]
            [raft.state :as state])
  (:import [raft.rpc
            RaftContainer
            RaftRPCGrpc
            AppendRequest
            AppendResponse
            LogEntry
            ]))

(defn client-for
  "Construct a gRPC client for host and port."
  [hostname port]
  (raft.rpc.RaftRPCGrpc/newBlockingStub
   (-> (io.grpc.ManagedChannelBuilder/forAddress hostname port)
       (.usePlaintext)
       .build)))

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
  [host port term]
  (let [request (construct-append-request {:leader-term term
                                           :log-entries (construct-test-log-entries 2)})
        response (.appendEntries (client-for host port) request)
        current-term (state/get-current-term)
        new-term (.getTerm response)]
    (if (< current-term new-term)
      (state/update-current-term-and-voted-for new-term nil))
    (if (.getSuccess response)
      (l/info "Successful response with term result:" (.getTerm response) (.getSuccess response))
      (l/info "Not successful response." (.getTerm response) (.getSuccess response)))))

(defn append-request [host port term]
  (make-append-request host port term))
