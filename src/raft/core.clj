(ns raft.core
  (:require
   [clojure.tools.logging :as l]
   [raft.persistence :as persistence]))

(defn -main
  "docstring"
  [& args]
  (persistence/migrate-db)
  ;; (persistence/add-log-entry 1 "{name: 'Blah1', address: 'Main St.'}")
  ;; (persistence/add-log-entry 2 "{name: 'Blah2', address: 'Wall St.'}")
  )
