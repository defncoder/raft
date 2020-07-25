(ns raft.leader
  (:require
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.follower :as follower]
   [raft.http :as http]
   [raft.persistence :as persistence]
   [raft.state :as state]
   [raft.util :as util]))

(def request-channel-for-sync (async/chan 100))
(def response-channel-for-sync (async/chan 100))

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
                                (l/trace "Got a log-inconsistency result. Retrying with previous index.")
                                (state/set-next-index-for-server server-info (:prev-log-index data 1))
                                true)
    
    ;; Successfully sent log entries to server. Try next set of entries, if any.
    :else (do
            (l/trace "Successfully sent log entries to follower: " (util/qualified-server-name server-info))
            (when (> (count (:entries data)) 0)
              (l/trace "Next index value before: " (state/get-next-index-for-server server-info))
              (state/add-next-index-for-server server-info (count (:entries data)))
              (l/trace "Next index value after: " (state/get-next-index-for-server server-info))
              (state/set-match-index-for-server server-info (:idx (last (:entries data)))))
            true)))

(defn- send-log-entries-to-follower
  "Send log entries to a server. For now, retries indefinitely on communication errors."
  [server request-channel response-channel]
  (async/thread
    (while (async/<!! request-channel)
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
      ;; Notify orchestrator that sync to this server is complete.
      (l/trace "Done sending logs to server: " (util/qualified-server-name server))
      (async/>!! response-channel server))))

(defn- is-majority-in-sync?
  "Are the logs in a majority of servers, including the leader, in sync?"
  []
  (>= (inc (count (get-other-up-to-date-servers)))
      (state/majority-number)))

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

(defn- handle-sync-progress
  "Handle progress updates from the currently ongoing syncs to trailing servers."
  [channel]
  (async/go-loop [do-dispatch true]
    ;; Listen for log sync completion message from any of the sync attempts going on in parallel.
    (let [server (async/<! channel)]
      (l/trace "Completed sending log to: " (util/qualified-server-name server))
      (if (and do-dispatch (is-majority-in-sync?))
        (do
          (update-commit-index)
          (l/trace "Notifying response channel that majority replication is complete")
          ;; When a majority of servers, counting self, are up-to-date notify original log append requestor in response channel.
          (async/>! response-channel-for-sync true)
          ;; This recur to the go-loop is to drain out any remaining data on the internal-channel
          ;; Note that this data won't be used to notify the response-channel-for-sync because it was
          ;; just notified in the above call.
          (recur false))
        (recur do-dispatch)))))

(defn- make-comm-channel
  "Make a channel to use for communicating in one direction to another thread.
  Use a default buffer size of 100."
  [ & [buffer-size] ]
  (async/chan (async/sliding-buffer (or buffer-size 100))))

(defn async-log-replication-thread
  "Orchestration thread for managing sending logs to all other servers.
  This function is called as soon as this server becomes the leader.
  Function completes as soon as this server becomes a follower."
  []
  (async/thread
    (let [servers (vec (state/get-other-servers))
          num-servers (count servers)
          ;; Create num-other-servers number of channels used to initiate work on worker threads.
          worker-channels (->> (repeatedly make-comm-channel)
                               (take num-servers)
                               vec)
          ;; A single channel for listening to sync completions. Note: Channel's buffer size is 100.
          completion-channel (make-comm-channel)]
      (doseq [n (range num-servers)]
        ;; Kickoff async threads to do the syncing with the other servers.
        ;; Each of the thread is going to wait for a request through its work channel and will
        ;; respond to work completion through the single completion channel that's common for all worker threads.
        (send-log-entries-to-follower (nth servers n) (nth worker-channels n) completion-channel))
      ;; Loop that waits for a new message on the request-channel-for-sync.
      (loop []
        ;; Wait for a new sync request with a true in it. A false indicates this server
        ;; became a follower while this thread was waiting on a new event in the request-channel-for-sync
        (async/<!! request-channel-for-sync)
        ;; Send a message to all worker channels to notify them to start syncing with their corresponding server
        (doall (map #(async/>!! %1 %2) worker-channels servers))
        ;; A common sync progress handler to deal with work completion responses as and when they appear.
        (handle-sync-progress completion-channel)
        (recur)))))

(defn become-a-leader
  "Become a leader and initiate appropriate activities."
  []
  (l/info "Won election. Becoming a leader!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  (state/become-leader)
  (async-heartbeat-loop))

(defn handle-append
  "Handle and append logs request from a client when this server is the leader."
  [request]
  (if-let [log-entries (not-empty (:entries request))]
    (let [log-index (persistence/append-new-log-entries-from-client log-entries
                                                                    (state/get-current-term))]
      (l/trace "Saved the following data to the DB:\n" (persistence/get-log-entries log-index (count log-entries)))
      (async/>!! request-channel-for-sync true)
      (async/<!! response-channel-for-sync)
      (persistence/get-log-entries log-index (count log-entries)))
    []))
