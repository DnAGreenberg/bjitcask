![bjitcask logo](https://raw.github.com/DnAGreenberg/bjitcask/master/doc/bjitcask_logo.png "bjitcask")
# bjitcask

[![Build Status](https://travis-ci.org/DnAGreenberg/bjitcask.png?branch=master)](https://travis-ci.org/DnAGreenberg/bjitcask)

A Clojure library that provides a high performance on-disk key-value store. Every get and put operation is guaranteed to trigger at most a single disk IO, so that the latency of operations is deterministic. This is a binary-compatibly implementation of Bitcask, which lives here: https://github.com/basho/bitcask and is described in the paper here: https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf?raw=true

Besides `get` and `put`, bjitcask supports `alter`, which offers a limited form of transactions: you can atomically read as many keys as you want, but you can only atomically write a single key in the transaction.

## Usage

bjitcask's public API can be found in the namespace `bjitcask`.

- `bjitcask/open` creates or opens a bjitcask at the given location
- `bjitcask/close` closes a bjitcask gracefully
- `bjitcask/get` reads a value from the bjitcask
- `bjitcask/put` writes a value to the bjitcask
- `bjitcask/alter` allows for multi-key read, single-key write transactions
- `bjitcask/delete` deletes the given key

## Caveats

bjitcask can open a lot of files. Maybe you need more file descriptors? Try:

```
ulimit -n 10000
```

## Gotchas

bjitcask currently doesn't release memory used to store keys when they're deleted.

## License

Copyright © 2013 Aysylu and David Greenberg

Distributed under the Eclipse Public License, the same as Clojure.
