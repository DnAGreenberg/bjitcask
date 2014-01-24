(ns bjitcask.registry
  (:require [bjitcask core keydir io merge]
            [clojure.core.async :as async]
            [clojure.java.io]))

(def registry-atom (atom {}))

(defn open
  "Opens a bitcask in the given directory"
  [dir {:as config
        :keys [max-data-file-size
               merge-frequency
               merge-fragmentation-threshold]
        :or {max-data-file-size 1000000000
             merge-frequency 300
             merge-fragmentation-threshold 0.7}}]
  (let [dir (clojure.java.io/file dir)]
    (when-not (.exists dir)
      (.mkdirs dir))
    (assert (.isDirectory dir) (str dir " must refer to a directory"))
    (if-let [bc (get @registry-atom dir)]
      bc
      (let [fs (bjitcask.io/open dir config)
            init-dir (bjitcask.keydir/init fs config)
            keydir (bjitcask.keydir/create-keydir fs init-dir config)
            stop-merge-chan (async/chan)
            bc {:fs fs :keydir keydir :dir dir :stop-merge stop-merge-chan}]
        (async/go
          (loop [merge-chan (async/timeout (* merge-frequency 1000))]
            (async/alt!
              stop-merge-chan ([_] nil)
              merge-chan ([_]
                          (bjitcask.merge/process-bitcask bc config)
                         (recur (async/timeout (* merge-frequency 1000)))))))
        (swap! registry-atom assoc dir bc)
        bc))))

(defn close
  [bc]
  (swap! registry-atom dissoc (:dir bc))
  (async/close! (:stop-merge bc))
  (bjitcask.core/close! (:keydir bc)))
