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

(defn- is-server-trailing?
  "Is a server trailing the current server in logs when only considering those logs up to the given index?"
  [server-info & [up-to-index]]
  (>= (or up-to-index (persistence/get-last-log-index)) (state/get-next-index-for-server server-info)))

(defn- get-other-up-to-date-servers
  "Get a list of other servers that are in sync with the current server(the leader), up to the given index.
  If no up-to-index is given, then check up to the last index of this server."
  [ & [up-to-index] ]
  (remove #(is-server-trailing? %1 up-to-index) (state/get-other-servers)))

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
          log-entries (persistence/get-log-entries index 50)
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
    ;; Note that if a follower responds with data that includes the :recent-term-min-index key, then
    ;; it is used here to skip over entries that can be avoided. Refer to §5.3 pages 7 and 8 for details
    ;; on this optimization.
    (not (:success response)) (do
                                (l/debug "Got a log-inconsistency result. Retrying with recent-term-min-index or previous index." response)
                                (state/set-next-index-for-server server-info (or (:recent-term-min-index response)
                                                                                 (:prev-log-index data 1)))
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
  [server request-channel]
  (async/thread
    ;; Wait for an incoming request from the orchestrator to sync with this server.
    (while true
      (let [response-channel (async/<!! request-channel)]
        (l/trace "Starting to send logs to server:" (util/qualified-server-name server))
        (loop []
          (let [payload (and (state/is-leader?)
                             (prepare-append-payload-for-follower server))]
            (when (not-empty (:entries payload))
              (l/trace "Making http request to: " (util/qualified-server-name server) "with payload:" payload)
              (let [response (http/make-server-request server "/replicate" payload 100)]
                (l/trace "Got response from follower:" (util/qualified-server-name server))
                (l/trace "Response from follower:" (util/qualified-server-name server) "is:" response)
                (if (:error response)
                  (Thread/sleep 100)
                  (process-append-logs-response server payload response))
                (recur)))))
        ;; Notify orchestrator that sync to this server is complete.
        (l/trace "Done sending logs to server:" (util/qualified-server-name server))
        (async/>!! response-channel true)))))

(defn- is-majority-in-sync?
  "Are the logs in a majority of servers, including the leader, in sync?"
  [ & [up-to-index] ]
  (>= (inc (count (get-other-up-to-date-servers up-to-index)))
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
      (l/trace "Setting commit-index to: " majority-replicated-index)
      (state/set-commit-index majority-replicated-index))))

(defn- make-comm-channel
  "Make a channel to use for communicating in one direction to another thread.
  Use a default buffer size of 100."
  [ & [buffer-size] ]
  (async/chan (async/sliding-buffer (or buffer-size 500))))

(defn async-log-replication-thread
  "Orchestration thread for managing sending logs to all other servers.
  This function is called as soon as this server becomes the leader.
  Function completes as soon as this server becomes a follower."
  []
  (async/thread
    (let [servers (state/get-other-servers)
          ;; Create one channel for each server used to initiate synch on worker threads.
          worker-channels  (take (count servers) (repeatedly make-comm-channel))]
      ;; Kickoff async threads to do the syncing with the other servers.
      ;; Each of the thread is going to wait for a request through its work channel and will
      ;; respond to work completion through the single response channel that's common for all worker threads.
      (doall (map send-log-entries-to-follower servers worker-channels))
      ;; Loop that waits for a new message on the request-channel-for-sync that will be invoked when
      ;; clients append new log entries.
      (loop []
        ;; Wait for a new sync request from client
        (let [response-channel (async/<!! request-channel-for-sync)]
          ;; Send a message on worker channels for worker threads to start syncing with their corresponding servers
          (doall (map #(async/>!! %1 response-channel) worker-channels)))
        (recur)))))

(defn become-a-leader
  "Become a leader and initiate appropriate activities."
  []
  (l/debug (util/qualified-server-name (state/get-this-server)) "is the new leader!!!!!!!!!!!!!!!!!!!!!!.")
  (state/become-leader)
  (async-heartbeat-loop))

(defn handle-append
  "Handle and append logs request from a client when this server is the leader."
  [request]
  (if-let [log-entries (not-empty (:entries request))]
    (let [[start-log-index end-log-index] (persistence/append-new-log-entries-from-client log-entries (state/get-current-term))
          response-channel (async/chan (state/get-num-servers))]
      (l/trace "Saved the following data to the DB:\n" (persistence/get-log-entries start-log-index (count log-entries)))
      (l/trace "Start log index:" start-log-index "End log index:" end-log-index)
      (async/>!! request-channel-for-sync response-channel)
      (while (not (is-majority-in-sync? end-log-index))
        ;; Wait for a notification from any of the synch worker threads.
        (async/<!! response-channel)
        (l/trace "Got notification from worker thread..."))
      (async/close! response-channel)
      ;; Drain any unused values from the response-channel so it can be reclaimed by the runtime.
      (while (async/<!! response-channel))
      (update-commit-index)
      (l/trace "Successfully added and synchronized new logs")
      (persistence/get-log-entries start-log-index (count log-entries)))
    []))
