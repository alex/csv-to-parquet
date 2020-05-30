# csv-to-parquet

Does what it says on the tin: takes a CSV (well, actually right now it takes a
TSV) and converts it to parquet.

Attempts to automatically detect which fields should be dictionary encoded, but
otherwise all fields are strings.

Usage:

```console
$ go run . /path/to/file.tsv
[logging]
$ # Now /path/to/file.parquet contains a parquet file
```
