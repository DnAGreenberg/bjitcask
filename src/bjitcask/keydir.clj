(ns bjitcask.keydir
  (:require [bjitcask.core :as core]
            byte-streams
            [bjitcask.io :as io]
            gloss.io
            [clojure.core.async :as async]))

(defn KeyDir
  ""
  [fs]
  (let [chm (java.util.concurrent.ConcurrentHashMap.)
        put-chan (async/chan)]
    (async/go-loop [{:keys [data-file data hint] :as files} (core/create fs)
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
                         keydir-value (core/->KeyDirValue data-file
                                                          value-offset
                                                          value-len
                                                          now)
                         data-entry (core/->Entry key-buf val-buf now)
                         hint-entry (core/->HintEntry key-buf value-offset total-len now)
                         data-buf (io/encode-entry data-entry)
                         hint-buf (io/encode-hint hint-entry)]
                     (byte-streams/transfer data-buf data)
                     (byte-streams/transfer hint-buf hint)
                     (.put chm key keydir-value)
                     (async/close! ack-chan)
                     (recur files (+ curr-offset total-len))))
    (reify
      bjitcask.core.Bitcask
      (get [_ key]
        (let [keydir-value (.get chm key)
              data-file (:file keydir-value)
              value-offset (:value-offset keydir-value)
              value-len (:value-len keydir-value)
              value-bytes (core/scan fs
                                     data-file
                                     value-offset
                                     value-len)]
          value-bytes))
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

(comment
  (def kd (KeyDir (io/open (java.io.File. "/Users/dgrnbrg/bjitcask/bctest"))))

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

  (byte-streams/to-string (core/get kd (byte-streams/to-byte-buffers "hello")))

  (byte-streams/print-bytes (java.io.File ))
  )
