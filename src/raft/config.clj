(ns raft.config
  (:require [raft.resource :as resource]
            [clojure.tools.logging :as l]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [clojure.java.io :as io]))

(defn- read-config
  "Read an edn config from a resource file."
  [config-resource-name]
  ;; (println (resource/get-resource-full-path "app.default.edn"))
  ;; (println (resource/get-resource-full-path "app.default.edn"))
  (println (str "***************************" (resource/get-resource-full-path "app.default.edn") "*****************************"))
  (try
    (->
     (io/resource config-resource-name)
     (slurp)
     (edn/read-string))
    (catch Exception e
      (do
        (l/warn e)
        {}))
    (finally )))

(defn get-current-env-config
  "Read and return the config."
  []
  (let [default-config (read-config "app.default.edn")]
    (if-let [envname (-> (System/getProperties)
                         (get "app.env"))]
      (merge default-config (read-config (str "app." envname ".edn")))
      default-config)))

(defonce config (get-current-env-config))

(l/info "Config is: " config)
