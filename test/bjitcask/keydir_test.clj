(ns bjitcask.keydir-test
  (:use clojure.test
        bjitcask.keydir)
  (:require [bjitcask.core :as bc]))

(def file "file")
(def hfile "hint-file")
(def key "key")
(def value "22")
(def now 2222)
(def offset 1)
(def value-offset (+ offset 14 (.length key)))
(def total-len (+ (.length key) (.length value) 14))

(def data-bytes "byte buffer")

(def entry (bc/->Entry key value now))
(def hint (bc/->HintEntry key offset total-len now))
(def keydir-value (bc/->KeyDirValue file value-offset (.length value) now))

(def fs
  (reify
    bc/FileSystem
    (bc/data-files [fs] [file] [file])
    (bc/hint-file [fs data-file] hfile)
    (bc/lock [fs] nil)
    (bc/unlock [fs force?] nil)
    (bc/scan [fs file] (seq data-bytes))
    (bc/scan [fs file offset len] (seq data-bytes))
    (bc/create [fs] {:data file :hint hfile})))

(deftest put-test
  (testing "Testing put functionality."
    (let [keydir (KeyDir fs)]
      (bc/put keydir key value)
      (is (= (bc/get keydir key) value)))))
