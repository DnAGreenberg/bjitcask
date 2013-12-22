(ns bjitcask.io
  (:require [bjitcask.core :as core]
            [gloss.core :as gloss]
            [gloss.io :as gio]
            [clojure.core.async :as async]
            byte-streams)
  (:import [java.io RandomAccessFile File FilenameFilter]
           [java.nio ByteBuffer]
           [java.util Arrays]))

(extend-protocol core/File
  RandomAccessFile
  (write-bufs [^RandomAccessFile file seq-of-buffers]
    (doseq [buf seq-of-buffers]
      (.write buf))))


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
                                (let [n (.getPath f)]
                                  (Long. (.substring n 0 (- (count n) suffix-len))))))
                         (reduce max)
                         atom)]
    (reify core/FileSystem
      (data-files [_] (data-files dir))
      (hint-file [_ data-file]
        (let [data-file ^File data-file
              path (.getPath data-file)]
          (assert (.endsWith path ".bitcask.data"))
          (let [hint (File. (str (.substring path 0 (- (count path) 5)) "hint"))]
            (when (.exists hint)
              hint))))
      (lock [_] "todo")
      (unlock [_ forec?] "todo")
      (scan [fs file]
        (core/scan fs file 0 (.length file)))
      (scan [fs file offset length]
        (-> file
            (RandomAccessFile. "r")
            (.getChannel)
            (.position offset)
            (byte-streams/convert (byte-streams/seq-of ByteBuffer))
            (gio/to-buf-seq)
            (gloss.data.bytes/take-bytes length)))
      (create [_]
        (let [id (swap! largest-int inc)
              data-file (File. dir (str id ".bitcask.data"))
              hint-file (File. dir (str id ".bitcask.hint"))
              data (.getChannel (RandomAccessFile. data-file "rw"))
              hint (.getChannel (RandomAccessFile. hint-file "rw"))]
          {:data data
           :data-file data-file
           :hint hint
           :hint-file hint-file})))))

(def ^:const page-size 4096)

(gloss/defcodec bitcask-entry-header
  (gloss/ordered-map :crc32 :int32
                     :tstamp :int32
                     :keysz :int16
                     :valsz :int32))

(gloss/defcodec bitcask-entry
  (gloss/header
    bitcask-entry-header
    (fn [{:keys [crc32 tstamp keysz valsz]}]
      (gloss/compile-frame (gloss/ordered-map
                             :crc32 crc32
                             :tstamp tstamp
                             :key (gloss/finite-block keysz)
                             :value (gloss/finite-block valsz))))
    (fn [{:keys [crc32 tstamp key value]}]
      {:crc32 crc32
       :tstamp tstamp
       :keysz (count key)
       :valsz (count value)})))

(defn bitcask-crc32
  "Takes the sequence of bitcask bytebuffers, and updates it with the proper crc32"
  [bufs]
  (let [{:keys [keysz valsz]} (gio/decode bitcask-entry-header bufs false)
        b (byte-array page-size)
        crc32 (java.util.zip.CRC32.)
        final-total (+ 14 keysz valsz)
        bufs (cons (doto (.asReadOnlyBuffer (first bufs))
                     (.getInt))
                   (next bufs))]
    (loop [total 4 ; skip the crc32
           [buf & bufs] bufs]
      (if (< total final-total)
        (let [buf (.asReadOnlyBuffer buf)]
          (recur (+ total
                    (loop [consumed 0]
                      (if (pos? (.remaining buf))
                        (let [n (min page-size
                                     (.remaining buf)
                                     (- final-total total))]
                          (.get buf b 0 n)
                          (.update crc32 b)
                          (recur (+ n consumed)))
                        consumed)))
                 bufs))))
    (.getValue crc32)))

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
    ;(assert (= crc32 (bit-and 0xffffffff (:crc32 entry))) "CRC32 didn't match!")
    entry))

(defn decode-all-entries
  "Turns bytes into a sequence of frame values.  If there are bytes left over at the end
   of the sequence, an exception is thrown."
  [bytes]
  (let [decode-next (#'gio/decoder bitcask-entry)]
    (binding [gloss.core.protocols/complete? true]
      (loop [buf-seq (gloss.data.bytes/dup-bytes (gio/to-buf-seq bytes))
             vals    []]
        (let [crc32 (when buf-seq
                      (bitcask-crc32 buf-seq))]
          (if-let [[entry remainder] (decode-next buf-seq)]
            (do ;(assert (= crc32 (bit-and 0xffffffff (:crc32 entry))))
                (recur remainder (conj vals entry)))
            vals))))))

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

  )

(comment (defn decode-entry
  [seq-of-buffers]
  ;; 4 byte CRC
  ;; 4 byte tstamp
  ;; 2 byte keysz
  ;; 4 byte valsz
  ;; key
  ;; val
  (let [b (first seq-of-buffers)
        crc32-value (.getInt b)
        crc32 (java.util.zip.CRC32.)
        _ (.update crc32 (.array b) 4 (- (.limit b) 4))
        tstamp (.getInt b)
        keysz (long (.getShort b))
        valsz (.getInt b)
        key (byte-array keysz)
        value (byte-array valsz)]
    (assert (= (.getValue crc32) crc32-value) "CRC32 validation failed")
    (.get b key)
    (.get b value)
    (core/->Entry key value tstamp))))

(comment (defn encode-entry
  [entry]
  (let [buf (ByteBuffer/wrap (byte-array page-size))
        ^bytes key (:key entry)
        ^bytes value (:value entry)]
    (.. buf
        (position 4)
        (putInt (:tstamp entry))
        (putShort (count key))
        (putInt (count value)))
    (let [total-pages (int (Math/ceil (/ (+ (count key) (count value) 14)
                                         page-size)))
          key-buf (ByteBuffer/wrap key)
          value-buf (ByteBuffer/wrap value)]
      ;;TODO support larger keys and vals
      (.. buf
          (put key-buf)
          (put value-buf)
          (rewind))
      (let [crc32 (java.util.zip.CRC32.)]
        (.update crc32 (.array buf) 4 (- (.limit buf) 4))
        (.putInt buf 0 (.getValue crc32)))
      [buf]))))

(comment
  (def e1 {:key (byte-array 10) :value (byte-array 22) :tstamp 7})

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

