(defproject bjitcask "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [byte-streams "0.1.7-SNAPSHOT"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [gloss "0.2.2"]])
