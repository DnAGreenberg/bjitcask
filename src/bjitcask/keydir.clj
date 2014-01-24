(ns bjitcask.keydir
  (:require [bjitcask.core :as core]
            byte-streams
            [bjitcask.io :as io]
            [bjitcask.codecs :as codecs]
            [clojure.core.async :as async]))

(defn create-keydir
  "Creates a keydir." 
  [fs init-dir config]
  (let [chm (java.util.concurrent.ConcurrentHashMap. init-dir)
        put-chan (async/chan)
        stop-chan (async/chan)]
    (async/go
      (loop [file (core/create fs)
             curr-offset 0]
        (async/alt!
          stop-chan ([_]
                     (async/close! put-chan)
                     (recur file curr-offset))
          put-chan ([{:keys [op ack-chan] :as command}]
                    (if command
                      (let [[key value] (case op
                                          :put [(:key command) (:value command)]
                                          :alter ((:fun command)))
                            key-buf (codecs/to-bytes key)
                            val-buf (codecs/to-bytes value)
                            key-len (codecs/byte-count key-buf) 
                            value-len (codecs/byte-count val-buf)
                            total-len (+ key-len value-len core/header-size)
                            ; Unix time
                            now (quot (System/currentTimeMillis) 1000)
                            ; data entry
                            data-entry (core/->Entry key-buf val-buf now)
                            data-buf (codecs/encode-entry data-entry)
                            ;; This creates a new data file segment if the old one was full
                            [file curr-offset]
                            (io/get-file-offset-or-rollover file
                                                            curr-offset
                                                            (codecs/byte-count data-buf)
                                                            fs)
                            value-offset (+ curr-offset core/header-size key-len)
                            keydir-entry (core/->KeyDirEntry key
                                                             (:data-file file)
                                                             value-offset
                                                             value-len
                                                             now)
                            hint-entry (core/->HintEntry key-buf curr-offset total-len now)
                            hint-buf (codecs/encode-hint hint-entry)]
                        (core/append-data file data-buf)
                        (core/append-hint file hint-buf)   
                        (.put chm key keydir-entry)
                        (async/close! ack-chan)
                        (recur file (+ curr-offset total-len))) 
                      (core/close! file))))))
    (reify
      bjitcask.core.Bitcask
      (keydir [kd]
        (into {} chm))
      (inject [kd k kde]
        (.put chm k kde))
      (get [kd key]
        (core/get kd key nil))
      (get [_ key not-found]
        (let [keydir-value (.get chm key)
              data-file (:file keydir-value)
              value-offset (:value-offset keydir-value)
              value-len (:value-len keydir-value)
              value-bytes (core/scan fs
                                     data-file
                                     value-offset
                                     value-len)]

          (if (and keydir-value
                   ; TODO aysylu: refactor out the hard-coded tombstone value into config
                   (not (byte-streams/bytes= "bitcask_tombstone" value-bytes)))
            value-bytes 
            not-found)))
      (put [_ key value] (let [ack-chan (async/chan)]
                           (async/>!! put-chan {:op :put
                                                :key key
                                                :value value
                                                :ack-chan ack-chan})
                           (async/<!! ack-chan)))
      (alter [_ fun] (let [ack-chan (async/chan)]
                       (async/>!! put-chan {:op :alter
                                            :fun fun
                                            :ack-chan ack-chan})
                       (async/<!! ack-chan)))
      core/IClose
      (close! [_] (async/close! stop-chan)))))

(defn hint->keydir-entry
  "Convert hints in the hint file to KeyDirEntries."
  [fs data-file hint-file]
  (map (fn [{:keys [key offset total-len tstamp]}]
         (let [key-len (codecs/byte-count key)
               value-len (- total-len core/header-size key-len)
               value-offset (+ offset core/header-size key-len)]
           (core/->KeyDirEntry key data-file value-offset value-len tstamp)))
       (codecs/decode-all-hints (core/scan fs hint-file))))

(defn list-keydir-entries
  "Returns keydir entries for the data or hint file, if present."
  [fs data-file]
  (let [hint-file (core/hint-file fs data-file)]
    (if hint-file
      (do (log/info "Loading hint %s into keydir" (.getPath hint-file))
          (hint->keydir-entry fs data-file hint-file))
      (do (log/info "Loading data %s into keydir" (.getPath hint-file))
          (codecs/decode-all-keydir-entries data-file)))))

(defn init
  "Initializes the KeyDir's chm from files."
  [fs config]
  (let  [chm (java.util.HashMap.)
         ; data files in order from oldest first
         data-files (sort-by
                      (fn [file]
                        (let [file-name (.getName file)]
                          (->> (.indexOf file-name ".")
                               (.substring file-name 0)
                               (Integer/parseInt))))
                      (core/data-files fs))]
    (->> data-files
         (mapcat (partial list-keydir-entries fs))
         (reduce (fn [chm entry] (doto chm
                                   (.put (byte-streams/to-string (:key entry)) entry)))
                 chm))))

(comment
  (def kd (KeyDir (io/open (java.io.File. "/Users/aysylu/bjitcask/bctest"))))

  (codecs/encode-entry 
    (core/->Entry (codecs/to-bytes (byte-array 10))
                  (codecs/to-bytes (byte-array 22))
                  (quot (System/currentTimeMillis) 1000)))

  (codecs/encode-hint
    (core/->HintEntry (codecs/to-bytes (byte-array 10))
                      1345
                      1512
                      (quot (System/currentTimeMillis) 1000)))

  (core/put kd
            (byte-streams/to-byte-buffers "hello")
            (byte-streams/to-byte-buffers "world"))

  (core/put kd "what's up" "pussycat")
  (byte-streams/to-string (core/get kd "what's up"))
  (core/get kd "whats up")

  (core/put kd "kv" "uno")
  (core/put kd "kv2" "does")
  (core/put kd "kv3" "whazzzzaaaaa")
  (core/put kd "kv" "rewrote")

  (core/put kd "dead" "bitcask_tombstone")
  (byte-streams/to-string (core/get kd "dead" "not-found"))

  (for [k ["kv" "kv2" "kv3"]] (byte-streams/to-string (core/get kd k)))

  (byte-streams/to-string (core/get kd (byte-streams/to-byte-buffers "hello")))

  )

(comment
  (def my-bc  (bjitcask.registry/open "test-bc"))

  (def sample-set  (map #(str "test" %)  (range 1000)))
  (time (dotimes [i 10000]
          (bjitcask.core/put (:keydir my-bc)
                             (rand-nth sample-set)
                             (byte-array  (rand-int 200)))))

  (time (dotimes [i 10000]
          (bjitcask.core/get (:keydir my-bc)
                             (rand-nth sample-set))))

  (sort-by first (core/keydir (:keydir my-bc)))
  (core/get (:keydir my-bc) "test79")

  (bjitcask.core/keydir (:keydir my-bc))

  (time (bjitcask.merge/process-bitcask my-bc))

  )
