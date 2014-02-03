(ns bjitcask.corrupt-test
  (:use clojure.test)
  (:require [bjitcask.registry]
            [bjitcask.core :as core]
            [bjitcask.codecs :as codecs]
            [bjitcask.keydir :as kd]
            [clj-logging-config.log4j :refer (set-logger!)])
  (:import [java.io RandomAccessFile File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]))

(set-logger! (org.apache.log4j.Logger/getRootLogger) :level :warn)

(def config
  {:merge-frequency 1000000000})

(defn rm-r
  "Recursively deletes the files."
  [f]
  (if (.isDirectory f)
    (do (doseq [f (.listFiles f)]
          (rm-r f))
        (.delete f))
    (.delete f)))

(deftest no-hint-file
  (let [my-bc  (bjitcask.registry/open "test-bc" config)
        _ (bjitcask.registry/close my-bc)
        _ (rm-r (java.io.File. "test-bc"))
        my-bc (bjitcask.registry/open "test-bc" config)
        max-byte-array-sz 200
        sample-set (map #(str "test" %)  (range 10))
        value-set (repeatedly 10 #(byte-array (inc (rand-int max-byte-array-sz))))
        puts (dotimes [i 10]
               (bjitcask.core/put (:keydir my-bc)
                                  (nth sample-set (mod i (count sample-set)))
                                  (nth value-set (mod i (count value-set)))))]
    (doseq [[k v] (map vector sample-set value-set)
            :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
      (is v' (str "Key " k  " is nil"))
      (is (byte-streams/bytes= v v')
          (format "(count v) = %d, (count v') = %d"
                  (count v)   
                  (bjitcask.codecs/byte-count v'))))

    (bjitcask.registry/close my-bc)

    ;; Delete all hint files.
    (->> (java.io.File. "test-bc")
         (.listFiles)
         (filter #(.endsWith (.getName %) "hint"))
         (map #(.delete %))
         (dorun))

    (let [my-bc (bjitcask.registry/open "test-bc" config)]
      (doseq [[k v] (map vector sample-set value-set)
              :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
        (is v' (str "Key " k  " is nil"))
        (is (byte-streams/bytes= v v'))))))

(deftest hint-file-corrupt
  (let [my-bc  (bjitcask.registry/open "test-bc" config)
        _ (bjitcask.registry/close my-bc)
        _ (rm-r (java.io.File. "test-bc"))
        my-bc (bjitcask.registry/open "test-bc" config)
        max-byte-array-sz 200
        sample-set (map #(str "test" %)  (range 10))
        value-set (repeatedly 10 #(byte-array (inc (rand-int max-byte-array-sz))))
        puts (dotimes [i 10]
               (bjitcask.core/put (:keydir my-bc)
                                  (nth sample-set (mod i (count sample-set)))
                                  (nth value-set (mod i (count value-set)))))]
    (doseq [[k v] (map vector sample-set value-set)
            :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
      (is v' (str "Key " k  " is nil"))
      (is (byte-streams/bytes= v v')
          (format "(count v) = %d, (count v') = %d"
                  (count v)   
                  (bjitcask.codecs/byte-count v'))))

    (bjitcask.registry/close my-bc)

    ;; Corrupt hint file.
    (-> (File. "test-bc/1.bitcask.hint")
        (RandomAccessFile. "rw")
        (.getChannel)
        (.write (ByteBuffer/allocate 4)))

    (let [my-bc (bjitcask.registry/open "test-bc" config)]
      (doseq [[k v] (map vector sample-set value-set)
              :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
        (is v' (str "Key " k  " is nil"))
        (is (byte-streams/bytes= v v'))))))

(deftest hint-files-valid-data-files-corrupt
  (let [my-bc (bjitcask.registry/open "test-bc" config)
        _ (bjitcask.registry/close my-bc)
        _ (rm-r (java.io.File. "test-bc"))
        my-bc (bjitcask.registry/open "test-bc" config)
        key "test0"
        value (byte-array 22)
        key-len (-> key
                    (codecs/to-bytes)
                    (codecs/byte-count))
        _ (bjitcask.core/put (:keydir my-bc)
                             key
                             value)
        v' (bjitcask.core/get (:keydir my-bc) key)]
    (is v' (str "Key " key  " is nil"))
    (is (byte-streams/bytes= value v')
        (format "(count v) = %d, (count v') = %d"
                (count value)   
                (bjitcask.codecs/byte-count v')))

    (bjitcask.registry/close my-bc)
    (Thread/sleep 100)

    ;; Corrupt the data file.
    (-> (File. "test-bc/1.bitcask.data")
        (RandomAccessFile. "rw")
        (.getChannel)
        ; corrupt the value
        (.write (ByteBuffer/allocate 22)))

    (let [my-bc (bjitcask.registry/open "test-bc" config)
          chm (core/keydir (:keydir my-bc))
          bkey (kd/make-binary-key key)]
      (is (.containsKey chm bkey)))))
