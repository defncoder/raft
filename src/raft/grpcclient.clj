(ns raft.grpcclient
  (:require [clojure.tools.logging :as l])
  (:import [raft.rpc
            RaftContainer
            RaftRPCGrpc
            AppendRequest
            AppendResponse
            ]))

;;; client
(def client (raft.rpc.RaftRPCGrpc/newBlockingStub
             (-> (io.grpc.ManagedChannelBuilder/forAddress "localhost" (int 50051))
                 (.usePlaintext)
                 .build)))

(defn create-append-request
  "Create a new AppendRequest object for a term."
  [term]
  (-> (raft.rpc.AppendRequest/newBuilder)
      (.setTerm term)
      (.setLeaderId "0")
      (.setPrevLogIndex 0)
      (.setPrevLogTerm 0)
      (.setLeaderCommitIndex 0)                          
      .build))

(defn append-request [term]
  (l/info (.getTerm (.appendEntries client (create-append-request term)))))
