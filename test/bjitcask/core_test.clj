(ns bjitcask.core-test
  (:use clojure.test)
  (:require [bjitcask.registry]
            [byte-streams]))

(defmacro ms-time [& body]
  `(let [now# (System/currentTimeMillis)]
     ~@body
     (- (System/currentTimeMillis) now#)))

(defn rm-r
  "Recursively deletes the files."
  [f]
  (if (.isDirectory f)
    (do (doseq [f (.listFiles f)]
          (rm-r f))
        (.delete f))
    (.delete f)))

(deftest basic-functionality
  (let [my-bc  (bjitcask.registry/open "test-bc")
        _ (bjitcask.registry/close my-bc)
        _ (rm-r (java.io.File. "test-bc"))
        my-bc (bjitcask.registry/open "test-bc")
        max-byte-array-sz 200
        sample-set (map #(str "test" %)  (range 100))
        value-set (repeatedly 100 #(byte-array (inc (rand-int max-byte-array-sz))))
        put-time (ms-time
                   (dotimes [i 100]
                     (bjitcask.core/put (:keydir my-bc)
                                        (nth sample-set (mod i (count sample-set)))
                                        (nth value-set (mod i (count value-set))))))]
    (doseq [[k v] (map vector sample-set value-set)
            :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
      (is v' (str "Key " k  " is nil"))
      (is (byte-streams/bytes= v v')))

    (bjitcask.merge/process-bitcask my-bc)
    (doseq [[k v] (map vector sample-set value-set)
            :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
      (is v' (str "Key " k  " is nil"))
      (is (byte-streams/bytes= v v')))

    (bjitcask.registry/close my-bc)
    (let [my-bc (bjitcask.registry/open "test-bc")]
      (doseq [[k v] (map vector sample-set value-set)
              :let [v' (bjitcask.core/get (:keydir my-bc) k)]]
        (is v' (str "Key " k  " is nil"))
        (is (byte-streams/bytes= v v'))))))
