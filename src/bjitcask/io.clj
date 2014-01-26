(ns bjitcask.io
  (:require [bjitcask.core :as core]
            [bjitcask.codecs :as codecs]
            [gloss.io :as gio]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            byte-streams)
  (:import [java.io RandomAccessFile File FilenameFilter]
           [java.nio ByteBuffer]
           [java.util Arrays]))

(defrecord DataFile [data data-file hint active-size config]
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
         ;TODO: got NPE here?
         (get-in file [:config :max-data-file-size]))
    (do 
      ;(core/append-hint file (codecs/encode-hint (core/->HintEntry (byte-array 0) (long 0x7fffffffffffffff) (.getValue hint-crc32) 0)))
      (core/close! file) 
      (log/info (format "Roll over data file. Size was %d, overflower was %d" curr-offset data-size))
      [(core/create fs) 0 (java.util.zip.CRC32.)])
    [file curr-offset]))

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

(defn open
  "Takes a directory and opens the bitcask inside."
  [^File dir config]
  (assert (.isDirectory dir) "Bitcask must be a directory")
  (let [suffix-len (count ".bitcask.data")
        largest-int (->> (data-files dir)
                         (map (fn [^File f]
                                (let [n (.getName f)]
                                  (Long. (.substring n 0 (- (count n) suffix-len))))))
                         (reduce max 0)
                         atom)]
    (log/info "Largest number of preexisting data file is" largest-int)
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
        (log/debug (format "Scanning %s from %d for %d bytes" file offset length))
        ;;TODO: determine if map-values is safe, because it's a ~10-15us
        ;;penalty to have the feature flag, bringing reads from 60us to 75us
        ;;when in map mode
        (let [map-values? (:map-values? config)
              channel
              (-> file
                  (RandomAccessFile. "r")
                  (.getChannel))
              buf (if map-values?
                    (.map channel
                          java.nio.channels.FileChannel$MapMode/READ_ONLY
                          offset
                          length)
                    (java.nio.ByteBuffer/allocate length))
              _ (when-not map-values?
                  (.. channel
                      (position offset)
                      (read buf))
                  (.flip buf))
              bytes (gio/to-buf-seq buf)]
          (.close channel)
          (when-not map-values?
            (assert (= length (.limit buf)) (format "Read corrupted! Expected %d bytes, got %d bytes" length (.limit buf))))
          bytes))
      (create [_]
        (let [id (swap! largest-int inc)
              _ (log/debug "Creating a new hint/data file pair with id" id)
              data-file (File. dir (str id ".bitcask.data"))
              hint-file (File. dir (str id ".bitcask.hint"))
              data (.getChannel (RandomAccessFile. data-file "rw"))
              hint (.getChannel (RandomAccessFile. hint-file "rw"))]
          (->DataFile data data-file hint (atom 0) config))))))
