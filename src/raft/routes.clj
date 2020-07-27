(ns raft.routes
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [compojure.coercions :refer :all]
            [clj-http.util :as http-util]
            [raft.election :as election]
            [raft.follower :as follower]
            [raft.leader :as leader]
            [raft.persistence :as persistence]
            [raft.state :as state]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.middleware.anti-forgery :refer :all]
            [ring.middleware.json :as json]
            [ring.middleware.session :refer :all]
            [clojure.tools.logging :as l]
            [ring.util.response :as resp])
  (:import [java.util UUID]))

(defn- make-test-payload
  "docstring"
  [start n]
  {:requestid (str (UUID/randomUUID))
   :entries (mapv #(assoc {} :command (str "Command " %)) (range start (+ start n)))})

(defn health-check-handler
  "Handle health check request."
  []
  (l/info "In health check...")
  (resp/response {:name "Raft consensus service"
                  :version "1.0.0"
                  :status "Healthy!"
                  :is-leader? (state/is-leader?)
                  :clojure-version (clojure-version)}))

(defn add-logentry-handler
  "Create a new visitor object."
  [req]
  (if (state/is-leader?)
    (let [command (:body req)]
      (l/trace "new log entry from client: " command)
      (->>
       command
       leader/handle-append
       resp/response))
    (let [leader-url (str "http://" (state/get-current-leader) "/logs")]
      (l/info "Redirecting to leader:" leader-url)
      (resp/redirect leader-url :temporary-redirect))))

(defn add-test-logs-handler
  "Generate some test log entries to try out the functionality."
  [num-logs]
  (if (state/is-leader?)
    (do
      (l/trace "Adding new test log entries.")
      (->>
       (make-test-payload (:idx (persistence/get-last-log-entry) 1) num-logs)
       leader/handle-append
       resp/response))
    (let [leader-url (str "http://" (state/get-current-leader) "/test-logs?num-logs=" num-logs)]
      (l/info "Redirecting to leader:" leader-url)
      (resp/redirect leader-url :temporary-redirect))))

(defn replicate-handler
  "Handle a replication request."
  [req]
  (let [args (:body req)]
    (l/trace "Replicate request: " args)
    (->>
     args
     follower/handle-append-request
     resp/response)))

(defn vote-handler
  "Handle a request for vote."
  [req]
  (let [args (:body req)]
    (l/trace "Vote request: " args)
    (->>
     args
     election/handle-vote-request
     resp/response)))

(defn app
  "The application's route definition for Ring."
  []
  (->
   (defroutes app-routes
     (POST "/vote" req (vote-handler req))
     (POST "/replicate" req (replicate-handler req))
     (POST "/logs" req (add-logentry-handler req))
     (GET "/test-logs" req
          (do
            (l/trace "test-logs request is:" req)
            (add-test-logs-handler (Integer. (get (:query-params req) "num-logs" "100")))))
     (GET "/health/full" [] (health-check-handler))
     (route/not-found
      (do
        (l/info "Route Not Found")
        "Route Not Found")))
   (json/wrap-json-body {:keywords? true})
   (json/wrap-json-response {:pretty true})
   (wrap-defaults (merge site-defaults {:security {:anti-forgery false}}))))
