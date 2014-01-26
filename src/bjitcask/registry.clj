(ns bjitcask.registry
  (:require [bjitcask core keydir io merge]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clojure.java.io]))

(def registry-atom (atom {}))

(defn open
  "Opens a bitcask in the given directory"
  [dir config]
  (let [{:keys [max-data-file-size
                merge-frequency
                merge-fragmentation-threshold]
         :as config}
        ;;NB: keep in sync with bjitcask.clj
        (merge {:max-data-file-size 1000000000
                :map-values? false
                :merge-frequency 300
                :merge-fragmentation-threshold 0.7}
               config)  
        dir (.getAbsoluteFile (clojure.java.io/file dir))]
    (if-not (.exists dir)
      (do (log/debug (format "mkdir %s" (.getPath dir)))
          (.mkdirs dir)))
    (assert (.isDirectory dir) (str dir " must refer to a directory"))
    (if-let [bc (get @registry-atom dir)]
      (do (log/info (format "Returning previously opened Bjitcask %s"
                            (.getPath dir)))
          bc)
      (let [_ (log/info (format "Opening Bjitcask %s" (.getPath dir)))
            fs (bjitcask.io/open dir config)
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
  (log/info (format "Closing Bjitcask %s" (-> bc :dir .getPath)))
  (swap! registry-atom dissoc (:dir bc))
  (async/close! (:stop-merge bc))
  (bjitcask.core/close! (:keydir bc)))
