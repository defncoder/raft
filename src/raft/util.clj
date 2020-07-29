(ns raft.util
  (:require
   [clojure.core.async :as async]
   [clojure.string :as str]
   ))

(defn qualified-server-name
  "Make a qualified server name to be used within the service code when referring to a server at a hostname and port."
  [server-info]
  (str (:host server-info) ":" (:port server-info)))

(defn server-info-from-qualified-name
  "Get server info from qualified name."
  [qname]
  (let [parts (seq (str/split qname #":"))
        host  (first parts)
        port  (if (second parts) (Integer/parseInt (second parts)) 10000)]
    {:host host
     :port port}))

(defn db-filename-for-server
  "Get the DB filename to save local persistent info based on server info."
  [server-info]
  (str (:host server-info) "_" (:port server-info) ".db"))

(defn url-for-server-endpoint
  "Get the base URL for server."
  [server-info endpoint]
  (str "http://"
       (get server-info :host "localhost")
       (or (and (:port server-info) (str ":" (:port server-info))) "")
       endpoint))

(defn close-and-drain-channel
  "Close an async channel and drain out any unconsumed values in it so the
  runtime can reclaim the channel."
  [channel]
  (async/close! channel)
  ;; Any unconsumed values would still be there in the channel.
  ;; Once they are taken out, a closed channel will return nil and this
  ;; while loop will exist at that time.
  (while (async/<!! channel)))
