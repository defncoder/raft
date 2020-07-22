(ns raft.leader
  (:require
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.follower :as follower]
   [raft.http :as http]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.util :as util]))


(defn- is-server-trailing?
  "Is a server trailing the current server in logs synced?"
  [server-info]
  (>= (persistence/get-last-log-index) (state/get-next-index-for-server server-info)))

(defn- get-other-up-to-date-servers
  "Get a list of other servers that are up to date with the current server (which is the leader)."
  []
  (remove is-server-trailing? (state/get-other-servers)))

(defn- prepare-heartbeat-payload
  "Prepare the heartbeat payload for followers."
  []
  (let [prev-log-entry (persistence/get-last-log-entry)
        prev-log-index (:idx prev-log-entry 0)
        prev-log-term  (:term prev-log-entry 0)]
    {:term (persistence/get-current-term)
     :leader-id (state/get-this-server-name)
     :prev-log-index prev-log-index
     :prev-log-term prev-log-term
     :leader-commit (state/get-commit-index)}))

(defn- send-heartbeat-to-servers
  "Send heartbeat requests to other servers."
  [timeout]
  (l/trace "Sending heartbeat requests to other servers...")
  (let [heartbeat-request (prepare-heartbeat-payload)
        servers (state/get-other-servers)]
    (when-let [responses (and (not-empty servers)
                              (http/send-data-to-servers heartbeat-request servers "/replicate" timeout))]
      ;; Process all heartbeat responses. This is done for the side-effect. The return value
      ;; from this function is not relevant.
      (doall (map follower/process-server-response responses)))))

(defn- async-heartbeat-loop
  "A separate thread to check and send heartbeat requests to other servers whenever
  this server is the leader."
  []
  (async/thread
    (loop []
      (when (state/is-leader?)
        (do
          (send-heartbeat-to-servers 100)
          ;; Sleep for 100 milliseconds.
          (Thread/sleep 100)
          (recur))))))

(defn- prepare-append-payload-for-follower
  "Get the AppendEntries data to send to a particular follower.
  Refer to page 4 of https://raft.github.io/raft.pdf and to §5.3 and §5.2
  of that document for more details.
  Returns a map that is the data."
  [server]
  (when (state/is-leader?)
    (let [index (state/get-next-index-for-server server)
          log-entries (persistence/get-log-entries index 20)
          prev-log-entry (persistence/get-prev-log-entry index)
          prev-log-index (:idx prev-log-entry 0)
          prev-log-term  (:term prev-log-entry 0)]
      {:term (persistence/get-current-term)
       :leader-id (state/get-this-server-name)
       :prev-log-index prev-log-index
       :prev-log-term prev-log-term
       :leader-commit (state/get-commit-index)
       :entries log-entries})))

(defn- process-append-logs-response
  "Process the response from a follower to the append log entries request."
  [server-info data response]
  (l/trace "Received this response: " response " from server: " (util/qualified-server-name server-info))
  (cond
    ;; Encountered an error in sending data to server.
    (:error response) (do
                        (l/trace "Error when sending logs to server: " (util/qualified-server-name server-info) " Error is: " (.getMessage (:error response)))
                        ;; Sleep for a tiny bit.
                        (Thread/sleep 2))
    
    ;; Term on receiving server is > current-term.
    ;; The source server/current leader should become a follower. Indicate the result so this server
    ;; can become a follower right away.
    ;; NOTE: This case stops the loop and exits this thread.
    (> (:term response) (state/get-current-term)) (do
                                                    (l/debug "Received a newer term when replicating logs: "
                                                             (:term response)
                                                             " from follower: "
                                                             (util/qualified-server-name server-info))
                                                    (follower/become-a-follower (:term response)))

    ;; Receiving server couldn't accept log-entries we sent because
    ;; it would create a gap in its log.
    ;; If AppendEntries fails because of log inconsistency then decrement nextIndex and retry (§5.3)
    (not (:success response)) (do
                                (l/debug "Got a log-inconsistency result. Retrying with previous index.")
                                (state/set-next-index-for-server server-info (:prev-log-index data 1)))
    
    ;; Successfully sent log entries to server. Try next set of entries, if any.
    :else (do
            (l/debug "Successfully sent log entries to follower: " (util/qualified-server-name server-info))
            (when (> (count (:entries data)) 0)
              (state/add-next-index-for-server server-info (count (:entries data)))
              (state/set-match-index-for-server server-info (:idx (last (:entries data))))))))

(defn- send-log-entries-to-follower
  "Send log entries to a server. Use control-channel to notify caller about completion."
  [server control-channel]
  (async/thread
    (loop []
      (let [payload (prepare-append-payload-for-follower server)]
        (if (empty? (:entries payload))
          (async/>!! control-channel server)
          (do
            ;; Send data to server
            (l/trace "Sending this data: " payload "To server: " server)
            (l/trace "Sending " (count (:entries payload)) "records to server: " (util/qualified-server-name server))
            (->>
             (http/make-server-request server "/replicate" payload 100)
             (process-append-logs-response server payload))
            (recur)))))))

(defn- synchronize-logs-with-followers
  "Send log entries to other servers."
  []
  (let [other-servers (state/get-other-servers)
        control-channel (async/chan (count other-servers))]
    (doseq [server other-servers]
      (send-log-entries-to-follower server control-channel))
    (loop []
      ;; Wait for a notification from one of the threads working on sending data to other servers.
      (async/<!! control-channel)
      ;; When this server is still the leader AND when less than a majority of all servers are
      ;; up-to-date, continue... (Include self in the number of servers that are up to date.
      (when (and (state/is-leader?)
                 (< (inc (count (get-other-up-to-date-servers))) (state/majority-number)))
        (recur)))
    (async/close! control-channel)))

(defn become-a-leader
  "Become a leader and initiate appropriate activities."
  []
  (l/info "Won election. Becoming a leader!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  (state/become-leader)
  (async-heartbeat-loop)
  (synchronize-logs-with-followers))

(defn- update-commit-index
  "Update commit index based on what log index a majority of servers have."
  []
  (let [sorted-match-indices (->
                              (state/get-match-indices)
                              vals
                              sort)
        majority-replicated-index (nth sorted-match-indices (- (state/get-num-servers) (state/majority-number)))]
    (when (> majority-replicated-index (state/get-commit-index))
      (l/debug "Setting commit-index to: " majority-replicated-index)
      (state/set-commit-index majority-replicated-index))))

(defn handle-append
  "Handle and append logs request from a client when this server is the leader."
  [request]
  (if-let [log-entries (not-empty (:entries request))]
    (let [log-index (persistence/append-new-log-entries-from-client log-entries
                                                                    (state/get-current-term))]
      (l/info "Saved the following data to the DB:\n" (persistence/get-log-entries log-index (count log-entries)))
      (synchronize-logs-with-followers)
      (update-commit-index)
      (persistence/get-log-entries log-index (count log-entries)))
    []))
