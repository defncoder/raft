(ns raft.resource)

(defn get-resource-full-path
  "docstring"
  [resource]
  (str (.getResource raft.resource resource)))
