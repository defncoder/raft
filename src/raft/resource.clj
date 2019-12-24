(ns raft.resource
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as l])
  (:gen-class))

(defn get-resource-full-path
  "docstring"
  [resource]
  (println (str "*************************************" (.getPath (io/resource resource))))
  (.getPath (io/resource resource)))
