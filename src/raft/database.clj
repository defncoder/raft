(ns raft.database
  (:require
   [[clojure.tools.logging :as l]])
  (:import
   [com.zaxxer.hikari HikariConfig HikariDataSource]))

(defn hikari-connection-pool
  [spec]
  (let [hds (doto (HikariDataSource.)
              (.setProperty "dataSourceClassName" (:classname spec))
              (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
              ;; (.setUsername (:user spec))
              ;; (.setPassword (:password spec))
              (.setConnectionTimeout 10000)
              ;; expire excess connections after 30 minutes of inactivity:
              ;; (.setMaxIdleTimeExcessConnections (* 30 60))
              ;; expire connections after 3 hours of inactivity:
              ;; (.setMaxIdleTime (* 3 60 60))
              )]
    {:datasource hds}))

(defrecord Database [db-spec connection]
  ;; Implement the Lifecycle protocol
  component/Lifecycle

  (start [component]
    (l/info ";; Starting database")
    ;; In the 'start' method, initialize this component
    ;; and start it running. For example, connect to a
    ;; database, create thread pools, or initialize shared
    ;; state.
    (let [conn-pool (delay (hikari-connection-pool (:db-spec component)))]
      ;; Return an updated version of the component with
      ;; the run-time state assoc'd in.
      (assoc component :connection conn-pool)))

  (stop [component]
    (l/info ";; Stopping database")
    ;; In the 'stop' method, shut down the running
    ;; component and release any external resources it has
    ;; acquired.
    (.close @connection)
    ;; Return the component, optionally modified. Remember that if you
    ;; dissoc one of a record's base fields, you get a plain map.
    (assoc component :connection nil)))

(defn make-db-connection
  "Make a new DB connection pool."
  [db-spec]
  (map->Database {:db-spec db-spec}))
