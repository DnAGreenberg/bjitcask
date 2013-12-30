(ns bjitcask.core-test
  (:use clojure.test)
  (:require [bjitcask.registry]))

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
        sample-set (map #(str "test" %)  (range 100))
        max-byte-array-sz 200
        put-time (ms-time
                   (dotimes [i 100]
                     (bjitcask.core/put (:keydir my-bc)
                                        (nth sample-set (mod i (count sample-set)))
                                        (byte-array  (inc (rand-int max-byte-array-sz))))))]
    (doseq [k sample-set
            :let [v (bjitcask.core/get (:keydir my-bc) k)]]
      (is v (str "Key " k  " is nil"))
      (is (<= (gloss.data.bytes.core/byte-count v) max-byte-array-sz)))
    (bjitcask.registry/close my-bc)
    (let [my-bc (bjitcask.registry/open "test-bc")]
      (doseq [k sample-set
              :let [v (bjitcask.core/get (:keydir my-bc) k)]]
        (is v (str "Key " k  " is nil"))
        (is (<= (gloss.data.bytes.core/byte-count v) max-byte-array-sz))))))
