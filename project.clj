(defproject bjitcask "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ^:replace ["-XX:-OmitStackTraceInFastThrow" "-server" "-Xmx2g"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [byte-streams "0.1.7"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [gloss "0.2.2"]]
  :profiles {:dev {:dependencies [[criterium "0.4.2"]
                                  [org.clojure/java.jdbc "0.3.2"]
                                  ;[h2database/h2 "1.3.175"]
                                  [factual/clj-leveldb "0.1.0"]]
                   :main bjitcask.benchmark
                   :aliases {"bench" "run" "-m" "bjitcask.benchmark"}}})

