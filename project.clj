(defproject com.dnrtech/raft "0.0.1"
  :description "A basic implementation of the RAFT consensus protocol in Clojure."
  :url "https://github.com/defncoder/raft"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :managed-dependencies [
                         [org.clojure/clojure "1.10.1"]
                         ]
  :dependencies [[org.xerial/sqlite-jdbc "3.32.3.2"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/java.jdbc "0.7.11"]
                 ;; logging
                 [org.slf4j/slf4j-api "1.7.30"]
                 [ch.qos.logback/logback-core "1.2.3"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 
                 ;; database
                 [com.zaxxer/HikariCP "3.4.5"] ;; for connection pool
                 [ragtime "0.8.0"] ;; for migrations
                 ;; JSON
                 [cheshire/cheshire "5.10.0"]

                 ;; ring, ring middleware and compojure
                 [ring "1.8.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-anti-forgery "1.3.0"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.5.0"]
                 [compojure "1.6.1" :exclusions [ring/ring-codec commons-codec]]

                 ;; HTTP client
                 [clj-http "3.10.1"]

                 ;; general component facility
                 [com.stuartsierra/component "1.0.0"]

                 [org.clojure/tools.cli "1.0.194"]
                 [org.clojure/core.async "1.3.610"]]

  :plugins [
            [lein-ancient "0.6.15"]
            ;; [lein-ring "0.12.5"]
            ;; [refactor-nrepl "2.5.0"]
            ;; [lein-cloverage "1.0.13"]
            ;; [lein-shell "0.5.0"]
            ;; [lein-ancient "0.6.15"]
            ;; [lein-changelog "0.3.2"]
            ]

  ;; :ring {:handler raft.routes/app
  ;;        :init  raft.service/startup-services}
  
  :main raft.service
  ;; :aot [raft.service raft.resource raft.config raft.state]
  :manifest {"Main-Class" "raft.service"}
  :resource-paths ["resources"]
  :target-path "target/%s"
  :profiles {
             :repl {
                    :plugins [
                              [refactor-nrepl "2.5.0"]
                              ;; [lein-ring "0.12.5"]
                              ;; [lein-cloverage "1.0.13"]
                              ;; [lein-shell "0.5.0"]
                              ;; [lein-changelog "0.3.2"]
                              ;; [lein-protoc "0.5.0"]
                              ]}
             :uberjar {:aot :all}}
  :deploy-repositories [["releases" :clojars]]
  :aliases {"update-readme-version" ["shell" "sed" "-i" "s/\\\\[com\\.dnrtech\\\\/raft \"[0-9.]*\"\\\\]/[com\\.dnrtech\\\\/raft \"${:version}\"]/" "README.md"]}
  :release-tasks [["shell" "git" "diff" "--exit-code"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["changelog" "release"]
                  ["update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["deploy"]
                  ["vcs" "push"]])
