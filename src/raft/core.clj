(ns raft.core
  (:gen-class)
  (:require
   [clojure.tools.cli :as cli]
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.config :as config]
   [ring.adapter.jetty :as jetty]
   [raft.routes :as routes]
   [raft.service :as service]))

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

(defn init-application
  "docstring"
  []
  )

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
    (l/info "Initializing DB...")
    (persistence/init-db-connection this-server)
    (l/info "Migrating DB...")
    (persistence/migrate-db)
    (l/info "Init term and last voted for...")
    (state/init-term-and-last-voted-for)
    (l/info "Init with servers")
    (state/init-with-servers servers this-server)
    (let [server-options {:ssl? false
                          :http? true
                          :join? false
                          :port (:port this-server 11010)}
          server (jetty/run-jetty routes/app server-options)]
      (l/info "raft service started on port: " (:port server-options))
      (service/after-startup-work)
      (.join server))))
