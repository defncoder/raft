(ns raft.routes
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [compojure.coercions :refer :all]
            [raft.service :as service]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.middleware.anti-forgery :refer :all]
            [ring.middleware.json :as json]
            [ring.middleware.session :refer :all]
            [clojure.tools.logging :as l]
            [ring.util.response :as resp]))

(defn health-check-handler
  "Handle health check request."
  []
  (l/info "In health check...")
  (resp/response {:name "Raft consensus service"
                  :version "1.0.0"
                  :status "Healthy!"
                  :clojure-version (clojure-version)}))

(defn add-logentry-handler
  "Create a new visitor object."
  [req]
  (l/info "********************************************************************")
  (let [command (:body req)]
    ;; (service/add-new-log-entry command)
    (l/info "new log entry: " command)
    (resp/response {:status "Ok"})))

(defn replicate-handler
  "Handle a replication request."
  [req]
  (let [args (:body req)]
    (l/trace "Replicate request: " args)
    (service/handle-append-request args)
    (resp/response args)))

(defn vote-handler
  "Handle a request for vote."
  [req]
  (let [args (:body req)]
    (l/trace "Vote request: " args)
    (resp/response (service/handle-vote-request args))))

(defroutes app-routes
  (POST "/replicate" req (replicate-handler req))
  (POST "/vote" req (vote-handler req))
  (GET "/health/full" [] (health-check-handler))
  (POST "/logs" req (add-logentry-handler req))
  (route/not-found
   (do
     (l/info "Route Not Found")
     "Route Not Found")))

(def app
  (->
   app-routes
   (json/wrap-json-body {:keywords? true})
   (json/wrap-json-response {:pretty true})
   (wrap-defaults (merge site-defaults {:security {:anti-forgery false}}))))
