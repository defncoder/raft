(ns raft.leader
  (:require
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [clojure.set :as cset]
   [raft.follower :as follower]
   [raft.http :as http]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.util :as util]))

(def request-channel-for-sync (async/chan 100))
(def response-channel-for-sync (async/chan 100))
(def sync-in-progress-servers (atom #{}))
(defn- is-server-trailing?
  "Is a server trailing the current server in logs synced?"
  [server-info]
  (>= (persistence/get-last-log-index) (state/get-next-index-for-server server-info)))

(defn- get-other-up-to-date-servers
  "Get a list of other servers that are up to date with the current server (which is the leader)."
  []
  (remove is-server-trailing? (state/get-other-servers)))

(defn- get-trailing-servers
  "Get a list of servers that are trailing logs when compared to the leader."
  []
  (filter is-server-trailing? (state/get-other-servers)))

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
        servers (get-other-up-to-date-servers)]
    ;; Process all heartbeat responses. This is done for the side-effect. The return value
    ;; from this function is not relevant.
    (doseq [server servers]
      (http/make-async-server-request server "/replicate" heartbeat-request timeout #(follower/process-server-response %)))))

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
          (async/<!! (async/timeout 100))
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
    (:error response) (l/trace "Error when sending logs to server: " (util/qualified-server-name server-info) " Error is: " (.getMessage (:error response)))
    
    ;; Term on receiving server is > current-term.
    ;; The source server/current leader should become a follower. Indicate the result so this server
    ;; can become a follower right away.
    ;; NOTE: This case stops the loop and exits this thread.
    (> (:term response) (state/get-current-term)) (do
                                                    (l/debug "Received a newer term when replicating logs: "
                                                             (:term response)
                                                             " from follower: "
                                                             (util/qualified-server-name server-info))
                                                    (follower/become-a-follower (:term response))
                                                    true)

    ;; Receiving server couldn't accept log-entries we sent because
    ;; it would create a gap in its log.
    ;; If AppendEntries fails because of log inconsistency then decrement nextIndex and retry (§5.3)
    (not (:success response)) (do
                                (l/debug "Got a log-inconsistency result. Retrying with previous index.")
                                (state/set-next-index-for-server server-info (:prev-log-index data 1))
                                true)
    
    ;; Successfully sent log entries to server. Try next set of entries, if any.
    :else (do
            (l/debug "Successfully sent log entries to follower: " (util/qualified-server-name server-info))
            (when (> (count (:entries data)) 0)
              (l/trace "Next index value before: " (state/get-next-index-for-server server-info))
              (state/add-next-index-for-server server-info (count (:entries data)))
              (l/trace "Next index value after: " (state/get-next-index-for-server server-info))
              (state/set-match-index-for-server server-info (:idx (last (:entries data)))))
            true)))

(defn- send-log-entries-to-follower
  "Send log entries to a server. For now, retries indefinitely on communication errors."
  [server channel]
  (async/thread
    ;; Add lagging server to the set of servers with sync.
    (swap! sync-in-progress-servers #(conj % server))
    (loop []
      (let [payload (and (state/is-leader?)
                         (prepare-append-payload-for-follower server))]
        (when (not-empty (:entries payload))
          (l/trace "Making http request to: " (util/qualified-server-name server) "with payload:" payload)
          (let [response (http/make-server-request server "/replicate" payload 100)]
            (l/trace "Response from follower:" (util/qualified-server-name server) "is:" response)
            (if (:error response)
              (Thread/sleep 100)
              (process-append-logs-response server payload response))
            (recur)))))
    (swap! sync-in-progress-servers #(disj %1 server))
    ;; Notify orchestrator that sync to this server is complete.
    (l/trace "Done sending logs to server: " (util/qualified-server-name server))
    (async/>!! channel server)))

(defn- is-majority-in-sync?
  "Are the logs in a majority of servers, including the leader, in sync?"
  []
  (>= (inc (count (get-other-up-to-date-servers)))
      (state/majority-number)))

(defn- handle-sync-progress
  "Handle progress updates from the currently ongoing syncs to trailing servers."
  [channel]
  (async/go-loop [do-dispatch true]
    ;; Listen for log sync completion message from any of the sync attempts going on in parallel.
    (when-let [s (async/<! channel)]
      (l/trace "Completed sending log to: " (util/qualified-server-name s))
      (if (and do-dispatch (is-majority-in-sync?))
        (do
          ;; Since a majority of servers are in sync, it's time to notify the sync originator.
          (async/close! channel)
          (l/trace "Notifying response channel that majority replication is complete")
          ;; When a majority of servers, counting self, are up-to-date notify original log append requestor in response channel.
          (async/>! response-channel-for-sync true)
          ;; This recur to the go-loop is to drain out any remaining data on the internal-channel
          ;; Note that this data won't be used to notify the response-channel-for-sync because it was
          ;; just notified in the above call.
          (recur false))
        (recur do-dispatch)))))

(defn- synchronize-logs-with-followers
  "Send log entries to other servers."
  []
  (async/thread
    (loop [internal-channel (async/chan (count (state/get-other-servers)))]
      ;; Get a list of servers that are trailing AND with whom a log sync is not in progress already.
      (when-let [trailing-servers (and (state/is-leader?)
                                       (cset/difference (set (get-trailing-servers)) @sync-in-progress-servers))]
        (l/debug "Sending logs to trailing servers:" (doall (map util/qualified-server-name trailing-servers)))
        (doseq [server trailing-servers]
          (l/debug "Sending log entries to: " (util/qualified-server-name server))
          (send-log-entries-to-follower server internal-channel))
        (handle-sync-progress internal-channel))
      
      (if (state/is-leader?)
        (do
          ;; Wait for a new sync request.
          (async/<!! request-channel-for-sync)
          (l/debug "Received a request on channel...")
          ;; loop
          (recur (async/chan (count (state/get-other-servers)))))
        (async/close! internal-channel)))))

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
      (async/>!! request-channel-for-sync true)
      (async/<!! response-channel-for-sync)
      (update-commit-index)
      (persistence/get-log-entries log-index (count log-entries)))
    []))
