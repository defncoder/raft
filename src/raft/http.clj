(ns raft.http
  (:require
   [cheshire.core :as json]
   [clj-http.client :as client]
   [clj-http.conn-mgr :as conn-mgr]
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.util :as util]))

(declare make-async-server-request)

(def sync-cm nil)
(def async-cm nil)

(defn init-http
  "Do connection manager initialization for HTTP clients."
  []
  (alter-var-root #'sync-cm (fn [_] (conn-mgr/make-reusable-conn-manager {:timeout 10 :default-per-route 4})))
  (alter-var-root #'async-cm (fn [_] (conn-mgr/make-reusable-async-conn-manager {:timeout 10 :default-per-route 4}))))

(defn- json-response
  "Get json response from a web response."
  [res]
  (l/trace "Response body is: " (:body res))
  (or (->
       res
       :body
       (json/parse-string true)) {}))

(defn- make-request-map
  "Make a request body from raw request payload."
  [payload timeout & [async?]]
  {:body (json/generate-string payload)
   :content-type :json
   :socket-timeout timeout
   :connection-timeout timeout
   :accept :json
   :async? async?
   :connection-manager (if async? async-cm sync-cm)})

(defn send-data-to-servers
  "Send a piece of data to all servers to a specified endpoint.
  Return a collection of responses from them."
  [data servers endpoint timeout]
  (let [num (count servers)
        channel (async/chan num)]
    ;; Issue async requests to other servers.
    (doseq [server-info servers]
      (make-async-server-request server-info endpoint data timeout #(async/go (async/>! channel %))))

    ;; Read results from channel
    (loop [i num
           result []]
      (if (= 0 i)
        (do
          (async/close! channel)
          result)
        (recur (dec i) (conj result (async/<!! channel)))))))

(defn make-async-server-request
  "Make an async server request and send the result to a channel."
  [server-info endpoint data timeout callback]
  (let [url (util/url-for-server-endpoint server-info endpoint)]
    (l/trace "Request URL is: " url)
    (client/post url
                 (make-request-map data timeout true)
                 (fn [resp]
                   (l/trace "Got response for request...")
                   (callback (json-response resp)))
                 (fn [exception]
                   (l/trace "Exception in network call..." (.getMessage exception))
                   (callback {:error exception})))))

(defn make-server-request
  "Make a request to a server endpoint."
  [server-info endpoint data timeout]
  (let [url (util/url-for-server-endpoint server-info endpoint)]
    (try
      (l/trace "Request URL is: " url)
      (->>
       (make-request-map data timeout)
       (client/post url)
       json-response)
      (catch Exception e
        (l/trace "Caught exception: " (.getMessage e))
        {:error e})
      (finally ))))
