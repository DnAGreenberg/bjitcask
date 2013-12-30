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
            keydir (bjitcask.keydir/create-keydir fs init-dir)
            stop-merge-chan (async/chan)
            bc {:fs fs :keydir keydir :dir dir :stop-merge stop-merge-chan}]
        (async/go
          (loop [merge-chan (async/timeout (* 5 60 1000))]
            (async/alt!
              stop-merge-chan ([_] nil)
              merge-chan ([_]
                          (bjitcask.merge/process-bitcask bc)
                         (recur (async/timeout (* 5 60 1000)))))))
        (swap! registry-atom assoc dir bc)
        bc))))

(defn close
  [bc]
  (swap! registry-atom dissoc (:dir bc))
  (async/close! (:stop-merge bc))
  (bjitcask.core/close! (:keydir bc)))
