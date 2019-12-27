(ns raft.core
  (:gen-class)
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.grpcservice :as service]
   [raft.grpcclient :as client]
   ))

(def SERVER_PORT 50051)

(defn make-test-client-calls
  "docstring"
  []
  (l/debug "??????????????????????About to make a client requests...?????????????????")
  (let [values (take 150 (repeatedly #(rand-int 10000)))]
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
  (if-let [server (service/start SERVER_PORT)]
    (do
      (make-test-client-calls)
      (.awaitTermination server))))
