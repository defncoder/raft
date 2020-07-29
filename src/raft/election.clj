(ns raft.election
  (:require
   [clojure.tools.logging :as l]
   [clojure.core.async :as async]
   [raft.follower :as follower]
   [raft.http :as http]
   [raft.leader :as leader]
   [raft.persistence :as persistence]
   [raft.state :as state]))

(defn- construct-vote-request
  "Construct a vote request message to be sent to all other servers."
  []
  (let [last-log-entry (persistence/get-last-log-entry)]
    {:term (state/get-current-term)
     :candidate-id (state/get-this-server-name)
     :last-log-index (:idx last-log-entry 0)
     :last-log-term (:term last-log-entry 0)}))

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

(defn- conduct-new-election
  "Declare this server to be a candidate and ask for votes 8-:"
  [timeout]
  (when (state/inc-current-term-and-vote-for-self)
    (l/trace "Starting new election...")
    (state/become-candidate)
    (let [other-servers (state/get-other-servers)
          vote-request (construct-vote-request)
          responses (http/send-data-to-servers vote-request other-servers "/vote" timeout)]
      ;; Process all vote responses.
      (doall (map follower/process-server-response responses))
      (when (and (state/is-candidate?)
                 (won-election? responses))
        ;; !!!
        (leader/become-a-leader)))))

(defn- got-new-requests-since?
  "Did this server get log append or vote requests since the last time (based on previous sequence numbers)?"
  [prev-append-req-sequence prev-voted-sequence]
  (or
   (not= prev-append-req-sequence (state/get-append-entries-request-sequence))
   (not= prev-voted-sequence (state/get-voted-sequence))))

(defn- random-sleep-timeout
  "Choose a new timeout value for the next election. A random number between 150-300ms."
  []
  (+ 150 (rand-int 150)))

(defn async-election-loop
  "Main loop for service."
  []
  (async/thread
    (loop []
      (let [prev-append-req-sequence (state/get-append-entries-request-sequence)
            prev-voted-sequence (state/get-voted-sequence)
            timeout (random-sleep-timeout)]
        ;; Sleep for timeout to see if some other server might send requests.
        (Thread/sleep timeout)
        (l/trace "Woke up from election timeout of" timeout "milliseconds.")
        ;; If this server didn't receive new RPC requests that might've
        ;; changed it to a follower, then become a candidate.
        ;; If idle timeout elapses without receiving AppendEntriesRPC from current leader
        ;; OR granting vote to a candidate then convert to candidate.
        (when (and (not (state/is-leader?))
                   (not (got-new-requests-since? prev-append-req-sequence prev-voted-sequence)))
          (conduct-new-election 100)))
      (recur))))

;; Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
;; If the logs have last entries with different terms, then the log with the later term is more up-to-date.
;; If the logs end with the same term, then whichever log is longer is more up-to-date.
(defn- is-candidate-up-to-date?
  "Are a candidate's log entries up to date?"
  [candidate-last-log-index candidate-last-log-term]
  (let [last-log-entry (persistence/get-last-log-entry)
        last-log-term (:term last-log-entry 0)
        last-log-index (:idx last-log-entry 0)]
    (or (> candidate-last-log-term last-log-term)
        (and (= candidate-last-log-term last-log-term) (>= candidate-last-log-index last-log-index)))))

(defn- make-vote-response
  "Can this server vote for a candidate?"
  [request]
  (let [candidate-id (:candidate-id request)
        candidate-term (:term request)
        candidate-last-log-term (:last-log-term request)
        candidate-last-log-index (:last-log-index request)
        current-term-and-voted-for (state/get-current-term-and-voted-for)
        current-term (:current-term current-term-and-voted-for)
        last-voted-for (:voted-for current-term-and-voted-for)
        candidate-up-to-date? (is-candidate-up-to-date? candidate-last-log-index candidate-last-log-term)
        vote-granted? (and (>= candidate-term current-term)
                           (or (nil? last-voted-for) (= candidate-id last-voted-for))
                           candidate-up-to-date?)]
    (l/trace "Vote request. candidate-id:" candidate-id
             "last-voted-for:" last-voted-for
             "candidate-term:" candidate-term
             "current-term:" current-term
             "candidate-last-log-term:" candidate-last-log-term
             "candidate-last-log-index:"
             candidate-last-log-index)
    (l/trace "last-voted-for:" last-voted-for ". Is candidate up-to-date?" candidate-up-to-date? ". Vote granted?" vote-granted?)
    {:term current-term
     :vote-granted vote-granted?}))

(defn- remember-vote-granted
  "Bookkeeping mechanism once vote is granted to someone."
  [request]
  (state/inc-voted-sequence)
  (l/trace "Updating current term and voted for to: " (:term request) (:candidate-id request))
  (state/update-current-term-and-voted-for (:term request) (:candidate-id request))
  (l/trace "Reading current term and voted for: " (state/get-current-term) (state/get-voted-for)))

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
      (l/trace "Request for vote received with term > current-term. Changing to a follower************" request-term current-term)
      (follower/become-a-follower request-term))
    response))
