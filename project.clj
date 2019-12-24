(defproject com.dnrtech/raft "0.0.1"
  :description "A basic implementation of the RAFT consensus protocol in Clojure."
  :url "https://github.com/defncoder/raft"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.xerial/sqlite-jdbc "3.28.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/java.jdbc "0.7.9"]
                 ;; logging
                 ;; https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
                 [org.apache.logging.log4j/log4j-core "2.13.0"]
                 ;; https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
                 [org.apache.logging.log4j/log4j-api "2.13.0"]
                 ;; https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-slf4j-impl
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.13.0"]
                 ;; database
                 [com.zaxxer/HikariCP "3.3.1"] ;; for connection pool
                 [ragtime "0.8.0"] ;; for migrations
                 ;; JSON
                 [cheshire/cheshire "5.8.0"]
                 ;; general component facility
                 [com.stuartsierra/component "0.4.0"]]
  :plugins [[lein-cloverage "1.0.13"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]]
  :main raft.core
  :aot [raft.core raft.resource raft.config]
  :manifest {"Main-Class" "raft.core"}
  ;; :resource-paths ["resources"]
  :resource-paths ["src/resources"]
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]]}}
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
