(ns raft.util
  (:require [clojure.string :as str]))

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

(defn make-append-logs-response
  "Make a response with the given term and success values."
  [term success?]
  {:type :AppendResponse
   :term term
   :success success?})
