(ns raft.election
  (:require [clojure.tools.logging :as l]
            [raft.state :as state]))

(defn choose-election-timeout
  "Choose a new timeout value for the next election. A random number between 150-300ms."
  []
  (+ 150 (rand-int 150)))

(defn should-convert-to-candidate?
  "Should this server convert to being a candidate?"
  [prev-append-entries-seq prev-vote-seq]
  (and (= prev-append-entries-seq @state/append-entries-call-sequence) (= prev-vote-seq @state/voted-sequence)))
