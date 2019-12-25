(ns raft.core
  (:gen-class)
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.grpcservice :as service]
   [raft.grpcclient :as client]
   )
  (:import
   [io.grpc
    Server
    ServerBuilder]
   [io.grpc.stub StreamObserver]
   [raft.grpcservice RaftRPCImpl]))

(def SERVER_PORT 50051)

(defn start []
  (l/info "About to start gRPC service")
  (let [raft-service (new RaftRPCImpl)
        server (-> (ServerBuilder/forPort SERVER_PORT)
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

(defn make-test-client-calls
  "docstring"
  []
  (l/debug "??????????????????????About to make a client requests...?????????????????")
  (let [values (take 5 (repeatedly #(rand-int 10000)))]
    (doseq [v values]
      (l/info "Sending request with value:" v)
      (client/append-request v)))
  (l/info "Client requests completed."))

(defn -main
  "docstring"
  [& args]
  (persistence/migrate-db)
  ;; (persistence/add-log-entry 1 "{name: 'Blah1', address: 'Main St.'}")
  ;; (persistence/add-log-entry 2 "{name: 'Blah2', address: 'Wall St.'}")
  (l/info "Now listening for gRPC requests on port" SERVER_PORT)
  (if-let [server (start)]
    (do
      (make-test-client-calls)
      (.awaitTermination server))))
