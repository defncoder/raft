(ns raft.grpcservice
  (:import
   [io.grpc.stub StreamObserver]
   [raft.rpc
    AppendRequest
    AppendResponse
    VoteRequest
    VoteResponse
    RaftRPCGrpc$RaftRPCImplBase])
  
  (:gen-class
   :name raft.grpcservice.RaftRPCImpl
   :extends
   raft.rpc.RaftRPCGrpc$RaftRPCImplBase)  
  )

(defn -appendEntries [this req res]
  (let [term (.getTerm req)]
    (doto res
      (.onNext (-> (AppendResponse/newBuilder)
                   (.setTerm (+ term 55))
                   (.setSuccess true)
                   (.build)))
      (.onCompleted))))


(defn -requestVote [this req res]
  (let [term (.getTerm req)]
    (doto res
      (.onNext (-> (AppendResponse/newBuilder)
                   (.setTerm (+ term 10))
                   (.setVoteGranted true)
                   (.build)))
      (.onCompleted))))
