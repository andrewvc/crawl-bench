(defproject crawl-bench "0.1.0-SNAPSHOT"
  :description "Crawl an endpoint reporting speed"
  :url "http://blog.andrewvc.com"
  :main crawl-bench.core
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [itsy "0.1.2-SNAPSHOT"]
                 [org.clojure/tools.logging "0.2.6"]]
  :java-source-paths ["java-src"])

