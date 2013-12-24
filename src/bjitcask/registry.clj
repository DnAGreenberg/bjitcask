(ns bjitcask.registry
  (:require [bjitcask core keydir io merge]
            [clojure.core.async :as async]
            [clojure.java.io]))

(def registry-atom (atom {}))

(defn open
  "Opens a bitcask in the given directory"
  [dir]
  (let [dir (clojure.java.io/file dir)]
    (when-not (.exists dir)
      (.mkdirs dir))
    (assert (.isDirectory dir) (str dir " must refer to a directory"))
    (if-let [bc (get @registry-atom dir)]
      bc
      (let [fs (bjitcask.io/open dir)
            init-dir (bjitcask.keydir/init fs)
            keydir (bjitcask.keydir/KeyDir fs init-dir)
            bc {:fs fs :keydir keydir :dir dir}]
        (async/go
          (while true
            (async/<! (async/timeout (* 5 60 1000)))
            (bjitcask.merge/process-bitcask bc)))
        (swap! registry-atom assoc dir bc)
        bc))))
