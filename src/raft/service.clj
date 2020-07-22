(ns raft.service
  (:require
   [clojure.tools.cli :as cli]
   [clojure.tools.logging :as l]
   [raft.election :as election]
   [raft.follower :as follower]
   [raft.http :as http]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.config :as config]
   [ring.adapter.jetty :as jetty]
   [raft.routes :as routes])
  (:gen-class))

(defn- cli-options
  []
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

(defn- prepare-to-start-raft-service
  "Preparation before the raft service can be initialized and started."
  [this-server]
  (l/trace "Initializing DB connection...")
  (persistence/init-db-connection this-server)
  (l/trace "Migrating DB...")
  (persistence/migrate-db)
  (http/init-http-client))

(defn- start-raft-service
  "Do the raft service initialization."
  [all-servers this-server]
  (state/init-with-servers all-servers this-server)
  (follower/become-a-follower)
  (election/async-election-loop))

(defn- startup-services
  "A convenient startup function that can be used by lein ring plugins."
  [ & [parsed-cli]]
  (let [options (:options parsed-cli)
        deployment-file (first (:arguments parsed-cli ["deployment.edn"]))
        deployment (config/read-deployment-details deployment-file)
        all-raft-servers (:servers deployment)
        this-raft-server (nth all-raft-servers (:index options 0))]
    (prepare-to-start-raft-service this-raft-server)
    (let [http-service-options {:ssl? false
                                :http? true
                                :join? false
                                :port (:port this-raft-server 11010)}
          http-service (jetty/run-jetty (routes/app) http-service-options)]
      (start-raft-service all-raft-servers this-raft-server)
      (l/info "raft service started on port: " (:port http-service-options))
      (.join http-service))))

(defn -main
  "The Main function for JVM to start."
  [& args]
  (l/trace "Command line args is:" *command-line-args*)
  (l/trace "Parsed args: " (cli/parse-opts args (cli-options)))
  (l/trace "Deployment details: " (config/read-deployment-details "deployment.edn"))
  (startup-services (cli/parse-opts args (cli-options))))
