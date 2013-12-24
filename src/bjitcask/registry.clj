(ns bjitcask.registry
  (:require [bjitcask core keydir io]
            [clojure.java.io]))

(def registry-atom (atom {}))

(defrecord Bitcask [fs keydir dir]
  bjitcask.core/Bitcask
  (get
    ([_ k]
     (bjitcask.core/get keydir k))
    ([_ k not-found]
     (bjitcask.core/get keydir k not-found)))
  (put [_ k v]
    (bjitcask.core/get keydir k v))
  (alter [_ fun]
    (bjitcask.core/alter keydir fun)))

(defn open
  "Opens a bitcask in the given directory"
  [dir]
  (let [dir (clojure.java.io/file dir)]
    (assert (.isDirectory dir) (str dir " must refer to a directory"))
    (if-let [bc (get @registry-atom dir)]
      bc
      (let [fs (bjitcask.io/open dir)
            init-dir (init fs)
            keydir (bjitcask.keydir/KeyDir fs init-dir)
            bc (->Bitcask fs keydir dir)]
        (swap! registry-atom assoc dir bc)
        bc))))
