(ns raft.core
  (:gen-class)
  (:require
   [clojure.tools.cli :as cli]
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.grpcservice :as service]
   [raft.grpcclient :as client]
   [raft.state :as state]
   ))

(def cli-options
  ;; An option with a required argument
  [["-p" "--port PORT" "Port number"
    :default (+ 10000 (rand-int 30000))
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]

   ["-c" "--count COUNT" "Count of servers"
    :default 5
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 3 % 7) "Must be a number between 3 and 7"]]
   
   ;; Server name
   ["-n" "--name NAME" "Name of server"
    ;; :id :name
    :default "localhost"
    ]])

(defn make-test-client-calls
  "docstring"
  [hostname port]
  (l/debug "??????????????????????About to make a client requests...?????????????????")
  (let [values (take 5 (repeatedly #(rand-int 10000)))]
    (doseq [v values]
      (l/info "Sending request with value:" v)
      (client/append-request hostname port v)))
  (l/info "Client requests completed."))

(defn -main
  "docstring"
  [& args]
  (l/info "Command line args is:" *command-line-args*)
  (l/info "Parsed args: " (cli/parse-opts args cli-options))
  (let [options (:options (cli/parse-opts args cli-options))
        num-servers (:count options)
        hostname (:name options)
        port (:port options)]
    (persistence/migrate-db)
    (state/read-term-and-last-voted-for)
    (state/init-with-num-servers (:count options) 0)
    ;; (persistence/add-log-entry 1 "{name: 'Blah1', address: 'Main St.'}")
    ;; (persistence/add-log-entry 2 "{name: 'Blah2', address: 'Wall St.'}")
    (l/info "Now listening for gRPC requests on port" port)
    (if-let [server (service/start port)]
      (do
        (make-test-client-calls hostname port)
        (.awaitTermination server)))))
