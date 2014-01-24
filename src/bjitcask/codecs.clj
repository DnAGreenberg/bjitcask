(ns bjitcask.codecs
  (:require [bjitcask.core :as core]
            byte-streams
            potemkin
            [gloss.core :as gloss]
            [gloss.io :as gio]
            gloss.data.bytes.core))

(defn to-bytes
  "Converts arg to internal bytes representation."
  [x]
  (-> x
      (byte-streams/to-byte-buffers)
      (gloss.data.bytes.core/create-buf-seq)))

(potemkin/import-fn gloss.data.bytes.core/byte-count)

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
       :keysz (byte-count key)
       :valsz (byte-count value)})))

(defn bitcask-crc32
  "Takes the sequence of bitcask bytebuffers, and updates it with the proper crc32"
  [bufs]
  (when bufs
    (let [{:keys [keysz valsz]} (gio/decode bitcask-entry-header bufs false)
          b (byte-array core/page-size)
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
                keysz (byte-count key)
                valsz (byte-count (:value entry))
                entry-len (+ core/header-size keysz valsz) 
                value-offset (+ curr-offset keysz core/header-size)
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
       :keysz (byte-count key)})))

(defn encode-hint
  [hint]
  (gio/encode bitcask-hint hint))

;;TODO: do the hint checksum
(defn decode-all-hints
  [buf]
  (gio/decode-all bitcask-hint buf))
