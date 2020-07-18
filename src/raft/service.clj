(ns raft.service
  (:require
   [cheshire.core :as json]
   [clj-http.client :as client]
   [clj-http.conn-mgr :as conn-mgr]
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.persistence :as persistence]
   [raft.state :as state]
   ;; [raft.election :as election]
   [raft.util :as util]))

;;;;; Forward declarations of internal functions.
(declare become-a-follower become-a-leader propagate-logs async-heartbeat-loop is-response-valid?)
(declare async-election-loop handle-append-request handle-vote-request can-append-logs? append-log-entries)
(declare make-vote-response remember-vote-granted make-server-request url-for-server-endpoint make-async-server-request)
(declare send-data-to-servers become-a-leader)

;; total number of active voting requests that are ongoing to various servers
(def num-active-voting-requests (atom 0))
(def sync-cm (conn-mgr/make-reusable-conn-manager {:timeout 10 :default-per-route 4}))
(def async-cm (conn-mgr/make-reusable-async-conn-manager {:timeout 10 :default-per-route 4}))

(defn after-startup-work
  "Start the raft service on this machine."
  []
  (become-a-follower 0)
  (async-election-loop))

(defn add-new-log-entry
  "Add a new log entry to local storage for the current term."
  [command]
  (persistence/add-new-log-entry (state/get-current-term) command)
  (propagate-logs))

(defn handle-vote-request
  "Handle a VoteRequest message."
  [request]
  (let [request-term (:term request)
        current-term (state/get-current-term)
        response (make-vote-response request)]
    (l/trace "Request term: " request-term " Current term: " current-term " Vote granted?: " (:vote-granted response))
    ;; Remember vote granted in our persistent store.
    (when (:vote-granted response)
      (remember-vote-granted request))
    ;; If this server sees an incoming voting request that has a term > this server's term
    ;; then this server must become a follower.
    (when (> request-term current-term)
      (do
        (l/trace "Request for vote received with term > current-term. Changing to a follower************" request-term current-term)
        (become-a-follower request-term)))
    response))

(defn handle-append-request
  "Handle an AppendEntries request."
  [request]
  ;; Increment the sequence that's maintained for the number of times an AppendRequest call is seen.
  (state/inc-append-entries-call-sequence)
  (let [log-entries (not-empty (:entries request))
        can-append?  (can-append-logs? request)]
    (when log-entries
      (l/debug "Log entries is non-zero..."))
    (when can-append?
      (append-log-entries request))
    (when (and log-entries (not can-append?))
      (l/debug "Has log entries but can't append."))
    ;; (If request term > current term then update to new term and become a follower.)
    ;;           OR
    ;; (If this server is a candidate OR a leader AND an AppendEntries RPC
    ;; came in from new leader then convert to a follower.)
    (when (or (> (:term request) (state/get-current-term))
              (not (state/is-follower?)))
      (become-a-follower (max (:term request) (state/get-current-term))))
    ;; Return the response for the request.
    {:term (state/get-current-term) :success (if log-entries can-append? true)}))

;;;;; Private functions.

(defn- construct-vote-request
  "Construct a vote request message to be sent to all other servers."
  []
  (let [last-log-entry (persistence/get-last-log-entry)]
    {:term (state/get-current-term)
     :candidate-id (state/get-this-server-name)
     :last-log-index (:log_index last-log-entry 0)
     :last-log-term (:term last-log-entry 0)}))

(defn- send-append-entries-request
  "Make an AppendEntries call to a server and wait up to timeout for a response.
  Returns response map.
  If an error happened it is returned in a :error key."
  [server-info data timeout]
  (l/trace "Append request URL is: " (url-for-server-endpoint server-info "/replicate"))
  (make-server-request server-info "/replicate" data timeout))

(defn- send-logs-entries-to-server
  "Send num-entries log entries starting at given index to server."
  [log-entries prev-log-entry server-info timeout]
  (let [prev-log-index (if prev-log-entry (:log_index prev-log-entry 0) 0)
        prev-log-term  (if prev-log-entry (:term prev-log-entry 0) 0)
        data {:term (persistence/get-current-term)
              :leader-id (state/get-this-server-name)
              :prev-log-index prev-log-index
              :prev-log-term prev-log-term
              :leader-commit (state/get-commit-index)
              :entries log-entries}]
    ;; Send data to server and return the response map.
    (l/trace "Sending this data: " data "To server: " server-info)
    (l/debug "Sending " (count log-entries) "records to server: " (util/qualified-server-name server-info))
    (send-append-entries-request server-info data timeout)))

(defn- send-logs-to-server
  "Send log entries to server."
  [server-info timeout]
  (loop [index (state/get-next-index-for-server server-info)]
    (let [log-entries (persistence/get-log-entries index 20)] ;; Read up to 20 log entries at a time.
      (if (not-empty log-entries)
        (let [prev-log-entry (persistence/get-prev-log-entry index)
              response (send-logs-entries-to-server log-entries prev-log-entry server-info timeout)]
          (cond
            ;; Encountered an error in sending data to server.
            (:error response) response
            ;; Term on receiving server is > current-term. Current server will become a follower.
            (> (:term response) (state/get-current-term)) response
            ;; Receiving server couldn't accept log-entries we sent because
            ;; it would create a gap in its log.
            ;; If AppendEntries fails because of log inconsistency then decrement nextIndex and retry (§5.3)
            (not (:success response)) (if (> index 0)
                                        (do
                                          (l/debug "Got a log-inconsistency result. Retrying with previous index.")
                                          (recur (dec index)))
                                        response)
            ;; Successfully sent log entries to server. Try next set of entries, if any.
            :else (let [next-index (+ index (count log-entries) 1)]
                    (state/set-indices-for-server server-info next-index)
                    (recur next-index))))
        ;; Log entries exhausted. Return a success response.
        {:term (state/get-current-term) :success true}))))

(defn- become-a-follower
  "Become a follower."
  [new-term]
  (l/debug "Changing state to be a follower...")
  (state/update-current-term-and-voted-for new-term nil)
  (state/become-follower))

(defn- become-a-leader
  "Become a leader and initiate appropriate activities."
  []
  (l/info "Won election. Becoming a leader!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
  (state/become-leader)
  (async-heartbeat-loop))

(defn- process-server-response
  "Process response to a vote request."
  [response]
  (let [term (:term response 0)]
    (when (> term (state/get-current-term))
      (l/trace "Response from servers had a higher term than current term. Becoming a follower..." term (state/get-current-term))
      (become-a-follower term))))

(defn- send-heartbeat-to-servers
  "Send heartbeat requests to other servers."
  [timeout]
  (l/trace "Sending heartbeat requests to other servers...")
  (let [heartbeat-request {:term (state/get-current-term)
                           :leader-id (state/get-this-server-name)}
        servers (state/get-other-servers)
        responses (send-data-to-servers heartbeat-request servers "/replicate" timeout)]
    ;; Process all heartbeat responses.
    (doall (map process-server-response responses))))

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

(defn- count-votes-received
  "Count the total number of votes received from all responses plus one for self vote."
  [responses]
  (->>
   responses
   (filter :vote-granted)
   count
   inc))

(defn- won-election?
  "Did this server win the election?"
  [responses]
  ;; Is total votes in favor > floor(total-number-of-servers/2)
  (> (count-votes-received responses) (quot (state/get-num-servers) 2)))

(defn- is-response-valid?
  "Check if a response is valid."
  [response]
  (not (:error response)))

(defn- conduct-a-new-election
  "Work to do as a candidate."
  [timeout]
  (when (state/inc-current-term-and-vote-for-self)
    (l/debug "Starting new election...")
    (state/become-candidate)
    (let [other-servers (state/get-other-servers)
          vote-request (construct-vote-request)
          responses (send-data-to-servers vote-request other-servers "/vote" timeout)]
      ;; Process all vote responses.
      (doall (map process-server-response responses))
      (when (and (state/is-candidate?)
                 (won-election? responses))
        ;; !!!
        (become-a-leader)))))

(defn- got-new-rpc-requests?
  "Did this server get either AppendEntries or VoteRequest RPC requests?"
  [prev-append-sequence prev-voted-sequence]
  (or
   (not= prev-append-sequence (state/get-append-entries-call-sequence))
   (not= prev-voted-sequence (state/get-voted-sequence))))

(defn random-sleep-timeout
  "Choose a new timeout value for the next election. A random number between 150-300ms."
  []
  (+ 150 (rand-int 150)))

(defn- async-election-loop
  "Main loop for service."
  []
  (async/thread
    (loop []
      (let [append-sequence (state/get-append-entries-call-sequence)
            voted-sequence (state/get-voted-sequence)
            timeout (random-sleep-timeout)]
        ;; Sleep for timeout to see if some other server might send requests.
        (Thread/sleep timeout)
        (l/trace "Woke up from election timeout of" timeout "milliseconds.")
        ;; If this server didn't receive new RPC requests that might've
        ;; changed it to a follower, then become a candidate.
        ;; If idle timeout elapses without receiving AppendEntriesRPC from current leader
        ;; OR granting vote to a candidate then convert to candidate.
        (when (and (not (state/is-leader?))
                   (not (got-new-rpc-requests? append-sequence voted-sequence)))
          (conduct-a-new-election 100)))
      (recur))))

(defn- can-append-logs?
  "Check if logs from AppendRequest can be used:
  1. Logs in request cannot be used if its term < currentTerm (§5.1)
  2. Logs in request cannot be used if local log doesn’t contain
     an entry at index request.prevLogIndex whose term matches request.prevLogTerm (§5.3).
  See http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf for details."
  [request]
  (when (not-empty (:entries request))
    (l/debug "Got append entries request with a non-empty logs list."))
  (let [request-term (:term request)
        current-term (state/get-current-term)]
    (cond
      (< request-term current-term) (do
                                      (l/debug "Non-empty list but request term " request-term "is less than current term:" current-term)
                                      false)
      (empty? (:entries request)) false
      (not
       (persistence/has-log-at-term-and-index?
        (:prev-log-term request)
        (:prev-log-index request)))  (do
                                       (l/debug "Non-empty list but has-log-at-index-with-term? with prevLogIndex:"
                                                (:prev-log-index request)
                                                "and prevLogTerm: " (:prev-log-term request) "returned false.")
                                       false)
      :else true)))

(defn- new-commit-index-for-request
  "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry in request)"
  [request prev-commit-index]
  (if (> (:leader-commit request) prev-commit-index)
    (min (:leader-commit request) (:log-index (last (:entries request))))
    prev-commit-index))

(defn- delete-conflicting-entries-for-request
  "Delete all existing but conflicting log entries for this request."
  [request]
  ;; TODO: See if this can be done using a single delete statement instead of 1 for each log entry in the input.
  (map #(persistence/delete-conflicting-log-entries
         (:log-index %1)
         (:term %1))
       (:entries request)))

(defn- is-server-trailing?
  "Is a server trailing the current server in logs synced?"
  [server-info cur-log-index]
  (>= cur-log-index (state/get-next-index-for-server server-info)))

(defn- servers-with-trailing-logs
  "Get a list of servers whose local storage may be trailing this server."
  []
  (let [last-log-index (persistence/get-last-log-index)]
    (filter #(>= last-log-index (state/get-next-index-for-server %1))
            (state/get-other-servers))))

(defn- group-servers-by-trailing-logs
  "Return a map of servers grouped by whether they have trailing logs or not."
  []
  (let [last-index (persistence/get-last-log-index)]
    (group-by #(is-server-trailing? %1 last-index) (state/get-other-servers))))

(defn group-servers-by-response
  "Group servers by whether an error was encountered when communicating with them."
  [servers responses]
  (->>
   (zipmap servers responses)
   (group-by #(is-response-valid? (second %1)))))

(defn errored-servers-from-responses
  "Get a vector of servers that errored out, based on responses."
  [servers responses]
  (get (group-servers-by-response servers responses) false []))

(defn- propagate-logs
  "Propagate logs to other servers."
  []
  (let [servers (state/get-other-servers)
        responses (doall (pmap #(send-logs-to-server %1 100) servers))
        errored-servers (errored-servers-from-responses servers responses)]
    
    ))

(defn- append-log-entries
  "Append log entries from request based on rules listed in the AppendEntries RPC section of http://nil.csail.mit.edu/6.824/2017/papers/raft-extended.pdf"
  [request]
  (delete-conflicting-entries-for-request request)
  (persistence/add-missing-log-entries (:entries request))
  (reset! state/commit-index (new-commit-index-for-request request @state/commit-index)))

;; Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
;; If the logs have last entries with different terms, then the log with the later term is more up-to-date.
;; If the logs end with the same term, then whichever log is longer is more up-to-date.
(defn- is-candidate-up-to-date?
  "Are a candidate's log entries up to date?"
  [candidate-last-log-index candidate-last-log-term]
  (let [last-log-entry (persistence/get-last-log-entry)
        last-log-term (:term last-log-entry 0)
        last-log-index (:log_index last-log-entry 0)]
    (if (= candidate-last-log-term last-log-term)
      (>= candidate-last-log-index last-log-index)
      (> candidate-last-log-term last-log-term))))

(defn- make-vote-response
  "Can this server vote for a candidate?"
  [request]
  (let [candidate-term (:term request)
        candidate-id (:candidate-id request)
        candidate-last-log-term (:last-log-term request)
        candidate-last-log-index (:last-log-index request)
        current-term-and-voted-for (state/get-current-term-and-voted-for)
        current-term (:current-term current-term-and-voted-for)
        voted-for (:voted-for current-term-and-voted-for)]
    (l/trace "VoteRequest info: " candidate-term candidate-id candidate-last-log-term candidate-last-log-index current-term)
    {:term current-term
     :vote-granted (and (>= candidate-term current-term)
                        (or (nil? voted-for) (= candidate-id voted-for))
                        (is-candidate-up-to-date? candidate-last-log-index candidate-last-log-term))}))

(defn- remember-vote-granted
  "Bookkeeping mechanism once vote is granted to someone."
  [request]
  (state/inc-voted-sequence)
  (l/trace "Updating current term and voted for to: " (:term request) (:candidate-id request))
  (state/update-current-term-and-voted-for (:term request) (:candidate-id request))
  (l/trace "Reading current term and voted for: " (state/get-current-term) (state/get-voted-for)))

(defn- url-for-server-endpoint
  "Get the base URL for server."
  [server-info endpoint]
  (str "http://"
       (get server-info :host "localhost")
       (or (and (:port server-info) (str ":" (:port server-info))) "")
       endpoint))

(defn- json-response
  "Get json response from a web response."
  [res]
  (l/trace "Response body is: " (:body res))
  (or (->
       res
       :body
       (json/parse-string true)) {}))

(defn- make-request-map
  "Make a request body from raw request payload."
  [payload timeout & [async?]]
  {:body (json/generate-string payload)
   :content-type :json
   :socket-timeout timeout
   :connection-timeout timeout
   :accept :json
   :async? async?
   :connection-manager (if async? async-cm sync-cm)})

(defn- send-data-to-servers
  "Send a piece of data to all servers to a specified endpoint.
  Return a collection of responses from them."
  [data servers endpoint timeout]
  (let [num (count servers)
        channel (async/chan num)]
    ;; Issue async requests to other servers.
    (doseq [s servers]
      (make-async-server-request s endpoint data timeout channel))
    ;; Read results from channel
    (loop [i num
           result []]
      (if (= 0 i)
        (do
          (async/close! channel)
          result)
        (recur (dec i) (conj result (async/<!! channel)))))))

(defn make-async-server-request
  "Make an async server request and send the result to a channel."
  [server-info endpoint data timeout c]
  (let [url (url-for-server-endpoint server-info endpoint)]
    (l/trace "Request URL is: " url)
    (client/post url
                 (make-request-map data timeout true)
                 (fn [resp]
                   (l/trace "Got response for request...")
                   (async/go (async/>! c (json-response resp))))
                 (fn [exception]
                   (l/trace "Exception in network call..." (.getMessage exception))
                   (async/go (async/>! c {:error exception}))))))

(defn- make-server-request
  "Make a request to a server endpoint."
  [server-info endpoint data timeout]
  (let [url (url-for-server-endpoint server-info endpoint)]
    (try
      (l/trace "Request URL is: " url)
      (->>
       (make-request-map data timeout)
       (client/post url)
       json-response)
      (catch Exception e
        (l/trace "Caught exception: " (.getMessage e))
        {:error e})
      (finally ))))
