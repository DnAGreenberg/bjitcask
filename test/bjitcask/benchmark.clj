(ns bjitcask.benchmark
  (:require [criterium.core :as crit]
            clj-leveldb
            [clojure.java.io :as io]
            bjitcask))

(defn rm-r
  "Recursively deletes the files."
  [f]
  (let [f (io/file f)]
    (if (.isDirectory f)
      (do (doseq [f (.listFiles f)]
            (rm-r f))
          (.delete f))
      (.delete f))))

(defn bench-db
  [db get put]
  (let [sample-set (map #(str "test" %)  (range 100))
        value-set (repeatedly 100
                              #(byte-array
                                 (inc
                                   (rand-int
                                     500000))))]
    (println)
    (println "Benchmarking puts")
  (crit/quick-bench (put db (rand-nth sample-set) (rand-nth value-set))) 
    (println)
    (println "Benchmarking gets")
  (crit/quick-bench (get db (rand-nth sample-set)))))

(defn -main
  "Runs a full benchmark suite"
  [& args]
  (println "\n\n#### leveldb ####\n\n")
  (let [db-path "ldb-tmp"
        db (clj-leveldb/create-db db-path nil)]
    (bench-db db clj-leveldb/get clj-leveldb/put)
    (.close db)
    (rm-r db-path)
    )

  (println "\n\n#### bjitcask ####\n\n")
  (let [db-path "bc-tmp"
        db (bjitcask/open db-path {})]
    (bench-db db bjitcask/get bjitcask/put)
    (bjitcask/close db)
    (rm-r db-path)
    )
  )

(comment
  (let [db-path "ldb-tmp"
        db (clj-leveldb/create-db db-path nil)]
    (bench-db db clj-leveldb/get clj-leveldb/put)
    (.close db)
    (rm-r db-path)
    )

  (let [db-path "bc-tmp"
        db (bjitcask/open db-path {})]
    (bench-db db bjitcask/get bjitcask/put)
    (bjitcask/close db)
    (rm-r db-path)
    )
  )
