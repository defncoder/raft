(ns raft.util)

(defn qualified-server-name
  "Make a qualified server name to be used within the service code when referring to a server at a hostname and port."
  [server-info]
  (str (:host server-info) ":" (:port server-info)))

(defn db-filename-for-server
  "Get the DB filename to save local persistent info based on server info."
  [server-info]
  (str (:host server-info) "_" (:port server-info) ".db"))
