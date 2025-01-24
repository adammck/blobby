# Blobby

This is my key-val store on a blob store. There are many like it, but this one
is mine. Don't expect much, I'm just having a bit of fun. Use something like
[SlateDB][] if you need this for real.

## Usage

Set connection vars:

```console
$ export MONGO_URL="mongodb+srv://user:pass@localhost:27017"
$ export S3_BUCKET="bucket-whatever"
```

Initialize the datastore(s):

```console
$ ./blobby init
OK
```

Write some stuff to the memtable:

```console
$ ./blobby put << EOF
{"_id": 1, "name": "pikachu"}
{"_id": 2, "name": "bulbasaur"}
{"_id": 3, "name": "charmander"}
{"_id": 4, "name": "squirtle"}
EOF
Wrote 4 documents to mongodb://localhost:27017/db-whatever/blue
```

Flush the memtable to the blob store:

```console
$ ./blobby flush
Flushed 3 documents to: s3://bucket-whatever/L1/1736476581.sstable
Active memtable is now: mongodb://localhost:27017/db-whatever/green
```

Read a document:

```console
$ ./blobby get 2
Got 1 document from s3://bucket-whatever/L1/1736476581.sstable
{"_id": 2, "name": "bulbasaur"}
```

Write a new version of that document:

```console
$ echo '{"_id": 2, "name": "bulbasaur", "trainer": "ash"}' | ./blobby put
Wrote 1 document to mongodb://localhost:27017/db-whatever/green
```

Read it again:

```console
$ ./blobby get 2
Got 1 document from mongodb://localhost:27017/db-whatever/green
{"_id": 2, "name": "bulbasaur", "trainer": "ash"}
```

Compact all sstables into one:

```console
$ ./blobby compact
Compaction 1:
  Input files: 4
  Output files: 1
  Output 1: s3://bucket-whatever/1736478581.sstable (16 records, 2048 bytes)
```

Compact sstables together until they reach 1MB:

```console
$ ./blobby compact --order smallest-first --min-files 8 --max-size 1048576
Compaction 1:
  Input files: 8
  Output files: 1
  Output 1: s3://bucket-whatever/1736478582.sstable (128 records, 524288 bytes)
```

## License

MIT.

[SlateDB]: https://github.com/slatedb/slatedb
