(ns bjitcask.keydir
  (:require [bjitcask.core :as core]
            [clojure.core.async :as async]))

(defn KeyDir
  ""
  [fs serdes]
  (let [chm (java.util.concurrent.ConcurrentHashMap.)
        put-chan (async/chan)]
    (async/go-loop [{data-file :data hint-file :hint :as files} (core/create fs)
                    curr-offset 0]
                   (let [{:keys [op ack-chan] :as command} (async/<! put-chan)
                         [key value] (case op
                                       :put [(:key command) (:value command)]
                                       :alter ((:fun command)))
                         key-len (.length key)
                         value-len (.length value)
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
                         data-entry (core/->Entry key value now)
                         hint-entry (core/->HintEntry key value-offset total-len now)
                         data-buf (core/encode-entry serdes data-entry)
                         hint-buf (core/encode-hint serdes hint-entry)]
                     (core/write-bufs data-file data-buf) 
                     (core/write-bufs hint-file hint-buf) 
                     (.put chm key keydir-value)
                     (async/close! ack-chan)
                     (recur files (+ curr-offset total-len))))
    (reify
      bjitcask.core.Bitcask
      (get [_ key]
        (let [keydir-value (.get chm key)
              data-file (:file keydir-value)
              value-offset (:value-offset keydir-value)
              value-len (:value-len  keydir-value)
              value-bytes (core/scan fs
                                     data-file
                                     value-offset
                                     value-len)
              entry (core/decode-entry serdes value-bytes)]
          (:value entry)))
      (put [_ key value] (let [ack-chan (async/chan)]
                         (async/>!! put-chan {:op :put
                                              :key key
                                              :value value
                                              :ack-chan ack-chan})))
      (alter [_ fun] (let [ack-chan (async/chan)]
                       (async/>!! put-chan {:op :alter
                                            :fun fun
                                            :ack-chan ack-chan}))))))
