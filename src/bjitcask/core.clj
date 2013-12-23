(ns bjitcask.core)

(defprotocol FileSystem
  (data-files [fs] "Returns collection of all data files in the bitcask")
  (hint-file [fs data-file] "Returns the hint file associated with the data-file if it exists; otherwise, return nil")
  (lock [fs] "Locks the bitcask on the file system")
  (unlock [fs force?] "Unlocks the bitcask on the filesystem.")
  (scan [fs file] [fs file offset len] "Returns a seq of byte buffers starting at offset of total length len. Defaults to scanning the whole file.")
  (create [fs] "Returns a map containing 2 keys: :data and :hint, which are each random access files."))

(defprotocol IDataWriter
  (data-size [file] "Returns the amount of space in bytes used by the data file")
  (append-data [file bufs] "Appends the given bufs to the associated data file. Returns false if the append failed and a new data file should be created.")
  (append-hint [file bufs] "Appends the given bufs to the associated hint file")
  (close [file] "Closes the file."))

(defrecord Entry [key value ^long tstamp])
(defrecord HintEntry [key ^long offset ^long total-len ^long tstamp])
(defrecord KeyDirValue [file ^long value-offset ^long value-len ^long tstamp])

(defprotocol SerDes
  (decode-entries [this seq-of-buffers] "Returns a seq of Entries.")
  (decode-hints [this seq-of-buffers] "Retunrs a seq of HintEntries.")
  (decode-entry [this seq-of-buffers] "Returns an Entry.")
  (decode-hint [this seq-of-buffers] "Returns a HintEntry.")
  (encode-entry [this entry] "Writes Entry to a seq of buffers.")
  (encode-hint [this hint] "Writes HintEntry to a seq of buffers."))

(defprotocol Bitcask
  (get [bitcask key] [bitcask key not-found] "Returns the value for the key in the bitcask.")
  (put [bitcask key value] "Stores the value for the given key.")
  (alter [bitcask fun] "fun must be a function that takes no arguments and returns a key-value pair to be `put`."))
