(defproject com.dnrtech/raft "0.0.1"
  :description "A basic implementation of the RAFT consensus protocol in Clojure."
  :url "https://github.com/defncoder/raft"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :managed-dependencies [
                         [org.clojure/clojure "1.10.1"]
                         ]
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
                 [com.stuartsierra/component "0.4.0"]
                 ;; https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java
                 [com.google.protobuf/protobuf-java "3.11.1"]

                 [javax.annotation/javax.annotation-api "1.2"]
                 [io.netty/netty-codec-http2 "4.1.25.Final"]
                 [io.grpc/grpc-core "1.26.0" :exclusions [io.grpc/grpc-api]]
                 [io.grpc/grpc-netty "1.26.0"
                  :exclusions [io.grpc/grpc-core
                               io.netty/netty-codec-http2]]
                 [io.grpc/grpc-protobuf "1.26.0"]
                 [io.grpc/grpc-stub "1.26.0"]

                 [org.clojure/tools.cli "0.4.2"]                 
                 ]
  :plugins [
            ;; [lein-cloverage "1.0.13"]
            ;; [lein-shell "0.5.0"]
            ;; [lein-ancient "0.6.15"]
            ;; [lein-changelog "0.3.2"]
            [lein-protoc "0.5.0"]
            ]

  :protoc-version "3.6.0"
  :proto-source-paths ["resources"]
  :protoc-grpc {:version "1.26.0"}

  :proto-target-path "src/generated"
  :java-source-paths ["src/generated"]
  
  :main raft.core
  ;; :aot [raft.core raft.resource raft.config raft.state]
  :manifest {"Main-Class" "raft.core"}
  :resource-paths ["resources"]
  :target-path "target/%s"
  :profiles {:dev {:aot [raft.grpcservice]
                   :dependencies []}
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
