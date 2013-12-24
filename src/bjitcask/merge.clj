(ns bjitcask.merge
  (:require [bjitcask core io keydir]
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
                                  14
                                  (gloss.data.bytes.core/byte-count
                                    (gloss.data.bytes.core/create-buf-seq
                                      (byte-streams/to-byte-buffers (:key entry))))
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

(defn get-bufs-from-keydir-entry
  [fs kd-entry]
  (let [{:keys [key file value-offset value-len tstamp]} kd-entry
        value-bufs (bjitcask.core/scan fs
                                       file
                                       value-offset
                                       value-len)
        key (gloss.data.bytes.core/create-buf-seq
              (byte-streams/to-byte-buffers key))
        key-len (gloss.data.bytes.core/byte-count key)
        data-buf (bjitcask.io/encode-entry {:key key
                                            :value value-bufs
                                            :tstamp tstamp})
        hint-buf (bjitcask.io/encode-hint {:key key
                                           :file file
                                           :offset (- value-offset
                                                      14
                                                      key-len)
                                           :total-len (+ key-len value-len 14)
                                           :tstamp tstamp})]
    [data-buf hint-buf]))

(defn process-bitcask
  [bc]
  (let [files (->> (bjitcask.core/data-files (:fs bc))
                   (sort-by #(.lastModified ^File %)))
        active-file (last files)
        files (drop-last files)
        kd-yield (dissoc (calculate-yield-per-file (bjitcask.core/keydir (:keydir bc)))
                         active-file)
        stale-files (stale-files (keys kd-yield) files)
        entries (->> kd-yield
                     (sort-by (fn [[file [size entries]]] size))
                     (mapcat (fn [[file [size entries]]]
                               entries)))]
    (println "Deleting data files with no active data:" stale-files)
    (doseq [file stale-files]
      (.delete file))
    (loop [[entry & entries] entries
           file (bjitcask.core/create (:fs bc))
           curr-offset 0]
      (when entry
        (let [[data-buf hint-buf] (get-bufs-from-keydir-entry (:fs bc) entry)
              ;; This creates a new data file segment if the old one was full
              [file curr-offset]
              (if (> (+ (gloss.data.bytes.core/byte-count data-buf)
                        (bjitcask.core/data-size file))
                     10000)
                (do (bjitcask.core/close file)
                    (println "Rollover")
                    [(bjitcask.core/create (:fs bc)) 0])
                [file curr-offset])
              key-len (gloss.data.bytes.core/byte-count
                        (gloss.io/to-buf-seq
                          (byte-streams/to-byte-buffers
                            (:entry key))))
              value-offset (+ curr-offset 14 key-len)
              kde (bjitcask.core/->KeyDirEntry (:key entry)
                                               (:data-file file)
                                               value-offset
                                               (:value-len entry)
                                               (:tstamp entry))] 
          (bjitcask.core/append-data file data-buf)
          (bjitcask.core/append-hint file hint-buf)
          (bjitcask.core/inject (:keydir bc) (:key kde) kde)
          (print ".")
          (recur entries file (+ curr-offset 14 key-len (:value-len entry))))))
    (doseq [[file] kd-yield]
      (when-let [hint (bjitcask.core/hint-file (:fs bc) file)]
        (.delete hint))
      (.delete file))
    (println "Compacting" (count entries) "entries")))

(comment
  (def my-bc  (bjitcask.registry/open "/Users/dgrnbrg/test-bc"))

  (def sample-set  (map #(str "test" %)  (range 1000)))
  (time (dotimes [i 10000]
          (bjitcask.core/put (:keydir my-bc)
                             (rand-nth sample-set)
                             (byte-array  (rand-int 200)))))

  (bjitcask.core/get (:keydir my-bc) "test177")
  (get (bjitcask.core/keydir (:keydir my-bc)) "test177")

  (map (partial bjitcask.core/hint-file (:fs my-bc)) (bjitcask.core/data-files (:fs my-bc)))

  (time (process-bitcask my-bc))

  )
