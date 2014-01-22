(ns bjitcask.io
  (:require [bjitcask.core :as core]
            [bjitcask.codecs :as codecs]
            [gloss.io :as gio]
            [clojure.core.async :as async]
            byte-streams)
  (:import [java.io RandomAccessFile File FilenameFilter]
           [java.nio ByteBuffer]
           [java.util Arrays]))

(defrecord DataFile [data data-file hint active-size]
  core/IDataWriter
  (data-size [this]
    @active-size)
  (append-data [this bufs]
    (let [size (codecs/byte-count bufs)]
      (swap! active-size + size)
      (doseq [buf bufs]
        (.write data buf))))
  (append-hint [this bufs]
    (doseq [buf bufs]
      (.write hint buf)))
  core/IClose
  (close! [this]
    (.close data)
    (.close hint)))

(defn get-file-offset-or-rollover
  [file curr-offset data-size fs]
  (if (> (+ data-size 
            (core/data-size file))
         10000)
    (do (core/close! file)
        ;; TODO: log rollover to INFO here
        [(core/create fs) 0])
    [file curr-offset]))

(comment
  (mapv (fn [{:keys [key value tstamp]}]
          (byte-streams/print-bytes key)
          (byte-streams/print-bytes value)
          (println (java.util.Date. (* 1000 tstamp))))
        (decode-all-entries
          (-> (File. "/tmp/bc.iterator.test.fold/1.bitcask.data")
              (RandomAccessFile. "r")
              (.getChannel)
              (.position 0)
              ;; TODO: limit length here
              (byte-streams/convert (byte-streams/seq-of ByteBuffer))
              )
          ))

  (mapv (fn [{:keys [key value tstamp]}]
          (byte-streams/print-bytes key)
          (byte-streams/print-bytes value)
          (println (java.util.Date. (* 1000 tstamp))))
        (-> (File. "/Users/dgrnbrg/bjitcask/bctest/2.bitcask.data")
            (RandomAccessFile. "r")
            (.getChannel)
            (.position 0)
            ;; TODO: limit length here
            (byte-streams/convert (byte-streams/seq-of ByteBuffer))
            (decode-all-entries)
            ))

  (-> (File. "/tmp/bc.iterator.test.fold/1.bitcask.hint")
      (RandomAccessFile. "r")
      (.getChannel)
      (.position 0)
      ;; TODO: limit length here
      (byte-streams/convert (byte-streams/seq-of ByteBuffer))
      (decode-all-hints)
      clojure.pprint/pprint
      )

  

)

(comment
  (def e1 {:key (byte-array 10) :value (byte-array 22) :tstamp 7})

  (decode-entry (encode-entry core/->Entry
                              (byte-array 10)
                              (byte-array 22)
                              7))

  (decode-entry (encode-entry e1))

  ;; roundtrip
  (let [{:keys [key value tstamp]} (decode-entry (encode-entry e1))]
    (assert (= key (gloss.data.bytes.core/create-buf-seq
                     (ByteBuffer/wrap (:key e1)))))
    (assert (= value (gloss.data.bytes.core/create-buf-seq
                       (ByteBuffer/wrap (:value e1)))))
    (assert (= tstamp (:tstamp e1))))

  ;; check
  (let [[b] (encode-entry e1)
        _ (.put b 0 (byte (inc (.get b 0))))]
    (decode-entry [b]))

  (def buf (gio/encode bitcask-entry (assoc e1 :crc32 22)))
  (bitcask-crc32 buf)
  (gio/decode bitcask-entry buf)

  (byte-str/k)

  )

(defn data-files
  [^File dir]
  (.listFiles dir (reify FilenameFilter
                    (accept [_ _ name]
                      (.endsWith name ".bitcask.data")))))

(defn hint-files
  [^File dir]
  (.listFiles dir (reify FilenameFilter
                    (accept [_ _ name]
                      (.endsWith name ".bitcask.hint")))))

(defn open
  "Takes a directory and opens the bitcask inside."
  [^File dir]
  (assert (.isDirectory dir) "Bitcask must be a directory")
  (let [suffix-len (count ".bitcask.data")
        largest-int (->> (data-files dir)
                         (map (fn [^File f]
                                (let [n (.getName f)]
                                  (Long. (.substring n 0 (- (count n) suffix-len))))))
                         (reduce max 0)
                         atom)]
    (println "largest-int is starting at" largest-int)
    (reify core/FileSystem
      (data-files [_] (data-files dir))
      (hint-files [_] (hint-files dir))
      (hint-file [_ data-file]
        (let [data-file ^File data-file
              path (.getPath data-file)]
          (assert (.endsWith path ".bitcask.data"))
          (let [hint (File. (str (.substring path 0 (- (count path) 4)) "hint"))]
            (when (.exists hint)
              hint))))
      (lock [_] "todo")
      (unlock [_ forec?] "todo")
      (scan [fs file]
        (core/scan fs file 0 (.length file)))
      (scan [fs file offset length]
        (let [channel
              (-> file
                  (RandomAccessFile. "r")
                  (.getChannel))
              bytes
              (-> channel
                  (.position offset)
                  (byte-streams/convert (byte-streams/seq-of ByteBuffer))
                  (gio/to-buf-seq)
                  (gloss.data.bytes/take-bytes length))]
          (.close channel)
          bytes))
      (create [_]
        (let [id (swap! largest-int inc)
              data-file (File. dir (str id ".bitcask.data"))
              hint-file (File. dir (str id ".bitcask.hint"))
              data (.getChannel (RandomAccessFile. data-file "rw"))
              hint (.getChannel (RandomAccessFile. hint-file "rw"))]
          (->DataFile data data-file hint (atom 0)))))))
