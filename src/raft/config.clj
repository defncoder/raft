(ns raft.config
  (:require [clojure.tools.logging :as l]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn- read-config
  "Read an edn config from a resource file."
  [config-resource-name]
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

;; (defonce internal-config (delay (get-current-env-config)))

(defn config
  "Get the configuration."
  []
  (get-current-env-config))

(defn read-deployment-details
  "Read deployment details from a supplied file."
  [f]
  (->
   (io/file f)
   (slurp)
   (edn/read-string)))
