(ns raft.core
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]))

(defn -main
  "docstring"
  [& args]
  (persistence/migrate-db))
