(ns raft.config
  (:require [raft.resource :as resource]
            [clojure.tools.logging :as l]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]))

(defn- read-config
  "Read an edn config from a resource file."
  [config-resource-name]
  (try
    (-> (resource/get-resource-full-path config-resource-name)
        (slurp)
        (edn/read-string))
    (catch Exception e {})
    (finally )))

(defn get-current-env-config
  "Read and return the config."
  []
  ;; (l/info "Environment is: " (System/getProperties))
  ;; (l/info "Env is: " (-> (System/getProperties) (get "app.env")))
  
  (let [default-config (read-config "app.default.edn")]
    (if-let [envname (-> (System/getProperties)
                         (get "app.env"))]
      (merge default-config (read-config (str "/src/resources/app." envname ".edn")))
      default-config)))

(defonce config (get-current-env-config))

(l/info "Config is: " config)
