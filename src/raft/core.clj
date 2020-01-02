(ns raft.core
  (:gen-class)
  (:require
   [clojure.tools.cli :as cli]
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.grpcservice :as service]
   [raft.grpcclient :as client]
   [raft.state :as state]
   [raft.util :as util]
   [raft.config :as config]
   [ring.adapter.jetty :as jetty]
   [clojure.core.async :as async]
   [raft.routes :as routes]
   ))

(def cli-options
  ;; An option with a required argument
  [
   ;; ["-p" "--port PORT" "Port number"
   ;;  :default (+ 10000 (rand-int 30000))
   ;;  :parse-fn #(Integer/parseInt %)
   ;;  :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]

   ;; ["-c" "--count COUNT" "Count of servers"
   ;;  :default 5
   ;;  :parse-fn #(Integer/parseInt %)
   ;;  :validate [#(<= 3 % 7) "Must be a number between 3 and 7"]]
   
   ;; This server name
   ["-i" "--index INDEX" "Index of this server in the deployment list."
    :id :index
    :default 0
    :parse-fn #(Integer/parseInt %)
    ]
   ])

(defn -main
  "docstring"
  [& args]
  (l/info "Command line args is:" *command-line-args*)
  (l/info "Parsed args: " (cli/parse-opts args cli-options))
  (l/info "Deployment details: " (config/read-deployment-details "deployment.edn"))

  (let [parsed-info (cli/parse-opts args cli-options)
        options (:options parsed-info)
        deployment-file (first (:arguments parsed-info))
        deployment (config/read-deployment-details deployment-file)
        servers (:servers deployment)
        this-server (nth servers (:index options))]
    (persistence/init-db-connection this-server)
    (persistence/migrate-db)
    (state/init-term-and-last-voted-for)
    (state/init-with-servers servers this-server)
    (if-let [server (service/start-raft-service this-server)]
      (do
        (async/thread
          (do
            (jetty/run-jetty routes/app {
                                         :ssl? false
                                         :http? true
                                         :port (:client-port this-server 11010)
                                         })))
        (.awaitTermination server)))))
