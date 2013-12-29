(ns bjitcask.io
  (:require [bjitcask.core :as core]
            [gloss.core :as gloss]
            [gloss.io :as gio]
            [clojure.core.async :as async]
            byte-streams)
  (:import [java.io RandomAccessFile File FilenameFilter]
           [java.nio ByteBuffer]
           [java.util Arrays]))


(def ^:const page-size 4096)

;; TODO: should these be uint?
(gloss/defcodec bitcask-entry-header
  (gloss/ordered-map :crc32 :int32
                     :tstamp :int32
                     :keysz :int16
                     :valsz :int32))

(gloss/defcodec bitcask-entry
  (gloss/header
    bitcask-entry-header
    (fn [{:keys [crc32 tstamp keysz valsz]}]
      (gloss/ordered-map
        :crc32 crc32
        :tstamp tstamp
        :key (gloss/finite-block keysz)
        :value (gloss/finite-block valsz)))
    (fn [{:keys [crc32 tstamp key value]}]
      {:crc32 crc32
       :tstamp tstamp
       :keysz (gloss.data.bytes.core/byte-count key)
       :valsz (gloss.data.bytes.core/byte-count value)})))

(defn bitcask-crc32
  "Takes the sequence of bitcask bytebuffers, and updates it with the proper crc32"
  [bufs]
  (when bufs
    (let [{:keys [keysz valsz]} (gio/decode bitcask-entry-header bufs false)
          b (byte-array page-size)
          crc32 (java.util.zip.CRC32.)
          final-total (+ 10 keysz valsz) ; 10 instead of 14 because we leave off the crc32
          bufs (-> bufs
                   (gloss.data.bytes/drop-bytes 4) ; skip crc32 
                   (gloss.data.bytes/take-bytes final-total)
                   (byte-streams/to-byte-arrays))]
      (doseq [b (if (seq? bufs)
                  bufs
                  [bufs])]
        (.update crc32 b))
      (.getValue crc32))))

(defn encode-entry
  [entry]
  (let [buf (gio/encode bitcask-entry (assoc entry
                                             :crc32 0))
        crc32 (bitcask-crc32 buf)]
    (.mark (first buf))
    (.putInt (first buf) crc32)
    (.reset (first buf))
    buf))

(defn decode-entry
  [buf]
  (let [entry (gio/decode bitcask-entry buf false)
        crc32 (bitcask-crc32 buf)]
    (assert (= crc32 (bit-and 0xffffffff (:crc32 entry))) "CRC32 didn't match!")
    entry))

(defn decode-all-keydir-entries
  "Turns bytes from the data-file into a sequence of KeyDirEntries."
  [data-file]
  (let [bytes (byte-streams/to-byte-buffers data-file) 
        decode-next (#'gio/decoder bitcask-entry)]
    (binding [gloss.core.protocols/complete? true]
      (loop [buf-seq (gio/to-buf-seq bytes)
             keydir-entries []
             curr-offset 0]
        (if-let [[entry remainder] (decode-next buf-seq)]
          (let [crc32 (bitcask-crc32 buf-seq)
                key (:key entry)
                tstamp (:tstamp entry)
                keysz (gloss.data.bytes.core/byte-count key)
                valsz (gloss.data.bytes.core/byte-count (:value entry))
                entry-len (+ 14 keysz valsz) 
                value-offset (+ curr-offset keysz)
                keydir-entry (core/->KeyDirEntry key
                                                 data-file
                                                 value-offset
                                                 valsz
                                                 tstamp)]
            (assert (= crc32 (bit-and 0xffffffff (:crc32 entry))))
            (recur remainder (conj keydir-entries keydir-entry) (+ curr-offset entry-len)))
          keydir-entries)))))

(defn decode-all-entries
  "Turns bytes into a sequence of Entries."
  [bytes]
  (let [decode-next (#'gio/decoder bitcask-entry)]
    (binding [gloss.core.protocols/complete? true]
      (loop [buf-seq (gloss.data.bytes/dup-bytes (gio/to-buf-seq bytes))
             vals    []]
        (let [crc32 (bitcask-crc32 buf-seq)]
          (if-let [[entry remainder] (decode-next buf-seq)]
            (do (assert (= crc32 (bit-and 0xffffffff (:crc32 entry))))
                (recur remainder (conj vals entry)))
            vals))))))

(gloss/defcodec bitcask-hint-header
  (gloss/ordered-map :tstamp :int32
                     :keysz :int16
                     :total-len :int32
                     :offset :int64))

(gloss/defcodec bitcask-hint
  (gloss/header
    bitcask-hint-header
    (fn [{:keys [tstamp keysz total-len offset]}]
      ;;TODO: compiling here might be very slow
      (gloss/ordered-map
        :tstamp tstamp
        :total-len total-len
        :offset offset
        :key (gloss/finite-block keysz)))
    (fn [{:keys [tstamp total-len offset key]}]
      {:tstamp tstamp
       :total-len total-len
       :offset offset
       :keysz (gloss.data.bytes.core/byte-count key)})))

(defn encode-hint
  [hint]
  (gio/encode bitcask-hint hint))

;;TODO: do the hint checksum
(defn decode-all-hints
  [buf]
  (gio/decode-all bitcask-hint buf))

(defrecord DataFile [data data-file hint active-size]
  core/IDataWriter
  (data-size [this]
    @active-size)
  (append-data [this bufs]
    (let [size (gloss.data.bytes.core/byte-count bufs)]
      (swap! active-size + size)
      (doseq [buf bufs]
        (.write data buf))))
  (append-hint [this bufs]
    (doseq [buf bufs]
      (.write hint buf)))
  (close [this]
    (.close data)
    (.close hint)))

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
