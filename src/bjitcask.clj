(ns bjitcask
  "Public API for bjitcask"
  (:refer-clojure :exclude [get alter])
  (:require [bjitcask registry core]))

(defn open
  "Opens a bjitcask if it's not already opened in this process. Returns
   the bjitcask.
   
   - `max-data-file-size` determines when the data files roll over
   - `merge-frequency` determines how often old logs are garbage collected
   - `merge-fragmentation-threshold` determines which logs are garbage collected
   "
  [directory & {:keys [max-data-file-size
                       merge-frequency
                       merge-fragmentation-threshold]
                :or {max-data-file-size 1000000000
                     merge-frequency 300
                     merge-fragmentation-threshold 0.7}}]
  (bjitcask.registry/open directory))

(defn close
  "Gracefully closes the given bjitcask."
  [bjitcask]
  (bjitcask.registry/close bjitcask))

(defn get
  "Given a key, retrieves the value from the bjitcask."
  ([bjitcask key]
   (bjitcask.core/get (:keydir bjitcask) key)) 
  ([bjitcask key not-found]
   (bjitcask.core/get (:keydir bjitcask) key not-found)))

(defn put
  "Store the value at the key in the bjitcask."
  [bjitcask key value]
  (bjitcask.core/put (:keydir bjitcask) key value))

(defn delete
  "Deletes the given key."
  [bjitcask key]
  (bjitcask.core/put (:keydir bjitcask) key "bitcask_tombstone"))

(defn alter
  "Invokes `f` on the write path of the bjitcask. `f` returns
   a `[key value]` pair which will be `put`. This allows for
   `f` to transactionally read any number of keys before choosing
   which key to write."
  [bjitcask f]
  (bjitcask.core/alter (:keydir bjitcask) f))
