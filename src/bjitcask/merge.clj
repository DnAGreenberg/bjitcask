(ns bjitcask.merge
  (:require [bjitcask core io keydir]
            [bjitcask.codecs :as codecs]
            [clojure.tools.logging :as log]
            byte-streams)
  (:import java.io.File))


;;We'll merge every n milliseconds
(defn calculate-yield-per-file
  "Returns a map from files to the number of bytes in use in
   that file given a keydir snapshot."
  [kd-snapshot]
  (->> (vals kd-snapshot)
       (group-by :file)
       (map (fn [[file entries]]
              ;;TODO: should look at absolute size and yield %
              [file [(reduce (fn [total entry]
                               (+ total
                                  bjitcask.core/header-size
                                  (codecs/byte-count
                                    (codecs/to-bytes (:key entry)))
                                  (:value-len entry)))
                             0
                             entries)
                     entries]]))
       (into {})))

(defn stale-files
  "Returns the set of files that have no data at all in them.
   They're safe to delete immediately."
  [kd-files files]
  (remove (set kd-files) files))

(defn get-data-buf-from-keydir-entry
  [fs kd-entry]
  (let  [{:keys [key file value-offset value-len tstamp]} kd-entry
         value-buf (bjitcask.core/scan fs
                                       file
                                       value-offset
                                       value-len)
         key-buf (codecs/to-bytes key)
         data-buf (codecs/encode-entry (bjitcask.core/->Entry
                                         key-buf
                                         value-buf
                                         tstamp))]
    data-buf))

(defn get-hint-buf-from-keydir-entry
  [fs kd-entry offset]
  (let [{:keys [key value-len tstamp]} kd-entry
        key-buf (codecs/to-bytes key)
        key-len (codecs/byte-count key-buf)
        hint-buf (codecs/encode-hint (bjitcask.core/->HintEntry
                                       key-buf
                                       offset
                                       (+ key-len
                                          value-len
                                          bjitcask.core/header-size)
                                       tstamp))]
    hint-buf))

(defn process-bitcask
  [bc config]
  (let [files (sort-by
                (fn [file]
                  (let [file-name (.getName file)]
                    (->> (.indexOf file-name ".")
                         (.substring file-name 0)
                         (Integer/parseInt))))
                (bjitcask.core/data-files (:fs bc)))
        active-file (last files)
        files (drop-last files)
        kd-yield (dissoc (calculate-yield-per-file (bjitcask.core/keydir (:keydir bc)))
                         active-file)
        stale-files (stale-files (keys kd-yield) files)
        entries (->> kd-yield
                     (sort-by (fn [[file [size entries]]] size))
                     (mapcat (fn [[file [size entries]]]
                               entries)))]
    (doseq [file stale-files]
      (log/info (format "Deleting %s due to no active data remaining" (.getPath file)))
      (.delete file))
    (loop [[entry & entries] entries
           file (bjitcask.core/create (:fs bc))
           curr-offset 0]
      (when entry
        (log/debug (format "Preparing to copy entry %s" (pr-str entry)))
        (let [data-buf (get-data-buf-from-keydir-entry (:fs bc) entry)
              ;; This creates a new data file segment if the old one was full
              [file curr-offset]
              (bjitcask.io/get-file-offset-or-rollover file
                                                       curr-offset
                                                       (codecs/byte-count data-buf)
                                                       (:fs bc))
              hint-buf (get-hint-buf-from-keydir-entry (:fs bc) entry curr-offset) 
              key-len (codecs/byte-count
                        (codecs/to-bytes (:key entry)))
              value-offset (+ curr-offset bjitcask.core/header-size key-len)
              kde (bjitcask.core/->KeyDirEntry (:key entry)
                                               (:data-file file)
                                               value-offset
                                               (:value-len entry)
                                               (:tstamp entry)
                                               (:lock entry))] 
          (bjitcask.core/append-data file data-buf)
          (bjitcask.core/append-hint file hint-buf)
          (bjitcask.core/inject (:keydir bc) (:key kde) kde)
          (recur entries file (+ value-offset
                                 (:value-len entry))))))
    (doseq [[file [size entries]] kd-yield]
      (log/info (format "Deleting %s due to kd-yield of %f"
                        (.getPath file)
                        (float (/ size (.length file)))))
      (doseq [{:keys [lock]} entries]
        (.. lock writeLock lock))
      (.delete file)
      (doseq [{:keys [lock]} entries]
        (.. lock writeLock unlock)))
    (doseq [file (->> (bjitcask.core/hint-files (:fs bc))
                      (remove (->> (bjitcask.core/data-files (:fs bc))
                                   (map #(bjitcask.core/hint-file (:fs bc) %))
                                   (set))))]
      (log/info (format "Deleting unused hint file %s" file))
      (.delete file))
    (log/info "Compacted" (count entries) "entries")))

(comment
  (def my-bc  (bjitcask.registry/open "/Users/dgrnbrg/test-bc"))

  (def sample-set  (map #(str "test" %)  (range 1000)))
  (time (dotimes [i 10000]
          (bjitcask.core/put (:keydir my-bc)
                             (rand-nth sample-set)
                             (byte-array  (rand-int 200)))))
  (time (dotimes [i 10000]
          (bjitcask.core/get (:keydir my-bc)
                             (rand-nth sample-set))))

  (bjitcask.core/get (:keydir my-bc) "test177")
  (get (bjitcask.core/keydir (:keydir my-bc)) "test177")

  (map (partial bjitcask.core/hint-file (:fs my-bc)) (bjitcask.core/data-files (:fs my-bc)))

  (time (process-bitcask my-bc))

  )
