(ns bjitcask.keydir
  (:require [bjitcask.core :as core]
            byte-streams
            [bjitcask.io :as io]
            gloss.io
            [clojure.core.async :as async]))

(defn KeyDir
  ""
  [fs init-dir]
  (let [chm (java.util.concurrent.ConcurrentHashMap. init-dir)
        put-chan (async/chan)]
    (async/go-loop [files (core/create fs)
                    curr-offset 0]
                   (let [{:keys [op ack-chan] :as command} (async/<! put-chan)
                         [key value] (case op
                                       :put [(:key command) (:value command)]
                                       :alter ((:fun command)))
                         key-buf (-> key
                                     (byte-streams/to-byte-buffers)
                                     (gloss.io/to-buf-seq))
                         val-buf (-> value
                                     (byte-streams/to-byte-buffers)
                                     (gloss.io/to-buf-seq))
                         key-len (gloss.data.bytes.core/byte-count key-buf) 
                         value-len (gloss.data.bytes.core/byte-count val-buf)
                         ; TODO aysylu: refactor the hardcoded 14 bytes into
                         ; global header length variable
                         value-offset (+ curr-offset 14 key-len)
                         total-len (+ key-len value-len 14)
                         ; TODO aysylu: unix time
                         now (quot (System/currentTimeMillis) 1000)
                         keydir-value (core/->KeyDirEntry key
                                                          (:data-file files)
                                                          value-offset
                                                          value-len
                                                          now)
                         data-entry (core/->Entry key-buf val-buf now)
                         hint-entry (core/->HintEntry key-buf value-offset total-len now)
                         data-buf (io/encode-entry data-entry)
                         hint-buf (io/encode-hint hint-entry)
                         ;; This creates a new data file segment if the old one was full
                         [files curr-offset]
                         (if (> (+ (gloss.data.bytes.core/byte-count data-buf)
                                         (core/data-size files))
                                      10000)
                                 (do (core/close files)
                                     [(core/create fs) 0])
                                 [files curr-offset])]
                     (core/append-data files data-buf)
                     (core/append-hint files hint-buf)   
                     (.put chm key keydir-value)
                     (async/close! ack-chan)
                     (recur files (+ curr-offset total-len))))
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
                       (async/<!! ack-chan))))))

(defn hint->keydir-entry
  "Convert hints in the hint file to KeyDirEntries."
  [fs data-file hint-file]
  (map (fn [{:keys [key offset total-len tstamp]}]
         (let [value-len (- total-len 14 (gloss.data.bytes.core/byte-count key))]
           (core/->KeyDirEntry key data-file offset value-len tstamp)))
       (io/decode-all-hints (core/scan fs hint-file))))

(defn list-keydir-entries
  "Returns keydir entries for the data or hint file, if present."
  [fs data-file]
  (let [hint-file (core/hint-file fs data-file)]
    (if hint-file
      (hint->keydir-entry fs data-file hint-file)
      (io/decode-all-keydir-entries data-file))))

(defn init
  ""
  [fs]
   (let  [chm (java.util.HashMap.)
          ; data files in order from oldest first
          data-files (sort-by #(.lastModified %) (core/data-files fs))]
     (->> data-files
          (mapcat (partial list-keydir-entries fs))
          (reduce (fn [chm entry] (doto chm
                                    (.put (byte-streams/to-string (:key entry)) entry)))
                  chm))))

(comment
  (def kd (KeyDir (io/open (java.io.File. "/Users/aysylu/bjitcask/bctest"))))

  (io/encode-entry 
    (core/->Entry (-> (byte-array 10)
                      (byte-streams/to-byte-buffers)
                      (gloss.io/to-buf-seq))
                  (-> (byte-array 22)
                      (byte-streams/to-byte-buffers)
                      (gloss.io/to-buf-seq))
                  (quot (System/currentTimeMillis) 1000)))

  (io/encode-hint
    (core/->HintEntry (-> (byte-array 10)
                          (byte-streams/to-byte-buffers)
                          (gloss.io/to-buf-seq))
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
  (def my-bc  (bjitcask.registry/open "/Users/aysylu/test-bc"))

  (def sample-set  (map #(str "test" %)  (range 1000)))
  (time (dotimes [i 10000]
          (bjitcask.core/put (:keydir my-bc)
                             (rand-nth sample-set)
                             (byte-array  (rand-int 200)))))

  (core/get (:keydir my-bc) "test22")

  (bjitcask.core/keydir (:keydir my-bc))

  (time (bjitcask.merge/process-bitcask my-bc))

  )
