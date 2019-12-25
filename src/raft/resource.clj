(ns raft.resource
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as l])
  (:gen-class))

(defn get-resource-full-path
  "Get a resource instance from the local dir in dev mode or from the JAR bundle."
  [resource]
  (io/resource resource))
