(ns raft.routes
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [compojure.coercions :refer :all]
            [ring.util.response :refer [response content-type set-cookie]]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.middleware.anti-forgery :refer :all]
            [ring.middleware.session :refer :all]
            [clojure.tools.logging :as l]
            [cheshire.core :refer :all]
            ))

(defn- json-response
  "Return a json response."
  [body]
  (->
   (response body)
   (content-type "application/json")))

(defn add-logentry-handler
  "Create a new visitor object."
  [req]
  (l/info "********************************************************************")
  (let [req-obj (parse-string (slurp (:body req)))]
    (json-response (generate-string (assoc req-obj :time "now")))))

(defroutes app-routes
  (GET "/health/full" [] "Ok")
  ;; (GET "/cache" [ & query-params ] (get-cached-value-handler (or (:key query-params) "foo")))
  ;; (GET "/visitors" [ & query-params :as r ] (do
  ;;                                             (l/info (str "Limit is: " (:limit query-params)))
  ;;                                             (l/info (str "Request is: " r))
  ;;                                             (visitors-get-handler query-params)))
  ;; (GET "/users" [] (users-get-handler 10))
  (POST "/logs" req (add-logentry-handler req))
  (route/not-found "Route Not Found"))

(def app (wrap-defaults app-routes (assoc-in site-defaults [:security :anti-forgery] false)))
