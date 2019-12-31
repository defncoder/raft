(ns raft.util)

(defn qualified-server-name
  "Make a qualified server name to be used within the service code when referring to a server at a hostname and port."
  [server-info]
  (str (:host server-info) "_" (:port server-info)))
