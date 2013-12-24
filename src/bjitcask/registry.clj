(ns bjitcask.registry
  (:require [bjitcask core keydir io]
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
            keydir (bjitcask.keydir/KeyDir fs)
            bc {:fs fs :keydir keydir :dir dir}]
        (swap! registry-atom assoc dir bc)
        bc))))
