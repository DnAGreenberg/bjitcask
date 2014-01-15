# bjitcask

A Clojure library that provides a high performance on-disk key-value store. Every get and put operation is guaranteed to trigger at most a single disk IO, so that the latency of operations is deterministic. This is a binary-compatibly implementation of Bitcask, which lives here: https://github.com/basho/bitcask and is described in the paper here: https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf?raw=true

Besides `get` and `put`, bjitcask supports `alter`, which offers a limited form of transactions: you can atomically read as many keys as you want, but you can only atomically write a single key in the transaction.

## Usage

More usage information to come...

Bjitcask can open a lot of files. Maybe you need more file descriptors? Try:

```
ulimit -n 10000
```

## License

Copyright © 2013 Aysylu and David Greenberg

Distributed under the Eclipse Public License, the same as Clojure.
