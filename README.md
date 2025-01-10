# Archive

This is my LSM-tree-on-blob-store. There are many like it, but this one is mine.
Don't expect much, I'm just having a bit of fun. Use something like [SlateDB][]
if you need this for real.

## Usage

Set connection vars:

```console
$ export MONGO_URL="mongodb+srv://user:pass@localhost:27017"
```

Initialize the datastore(s):

```console
$ ./archive init
OK
```

Write some stuff to the memtable:

```console
$ ./archive put << EOF
{"_id": 1, "name": "pikachu"}
{"_id": 2, "name": "bulbasaur"}
{"_id": 3, "name": "charmander"}
{"_id": 4, "name": "squirtle"}
EOF
Wrote 4 documents to mongodb://localhost:27017/db-whatever/blue
```

Flush the memtable to the blob store:

```console
$ ./archive flush
Flushed 3 documents to: s3://bucket-whatever/L1/1736476581.sstable
Active memtable is now: mongodb://localhost:27017/db-whatever/green
```

Read a document:

```console
$ ./archive get 2
Got 1 document from s3://bucket-whatever/L1/1736476581.sstable
{"_id": 2, "name": "bulbasaur"}
```

Write a new version of that document:

```console
$ ./archive put '{"_id": 2, "name": "bulbasaur", "trainer": "ash"}'
Wrote 1 document to mongodb://localhost:27017/db-whatever/green
```

Read it again:

```console
$ ./archive get 2
Got 1 document from mongodb://localhost:27017/db-whatever/green
{"_id": 2, "name": "bulbasaur", "trainer": "ash"}
```

## License

MIT.

[SlateDB]: https://github.com/slatedb/slatedb
