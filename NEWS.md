# arkdb 0.0.12

- bugfix for arkdb

# arkdb 0.0.11

- make cached connection opt-out instead of applying only to read_only.  This
  allows cache to work on read-write connections by default.  This also avoids
  the condition of a connection being garbage-collected when functions call
  local_db internally.

# arkdb 0.0.10

- Better handling of read_only vs read_write connections.  Only caches
  read_only connections.  
- includes optional support for MonetDBLite

# arkdb 0.0.8

- bugfix for dplyr 2.0.0 release


# arkdb 0.0.7

- bugfix for upcoming dplyr 2.0.0 release

# arkdb 0.0.6

- support vroom as an opt-in streamable table
- export `process_chunks`
- Add mechanism to attempt a bulk importer, when available (#27)
- Bugfix for case when text contains `#` characters in base parser (#28)
- lighten core dependencies.  Fully recursive dependencies include only 4
  non-base packages now, as `progress` is now optional.
- Use "magic numbers" instead of extensions to guess compression type.
  (NOTE: requires that file is local and not a URL)
- Now that `duckdb` is on CRAN and `MonetDBLite` isn't, drop built-in
  support for `MonetDBLite` in favor of `duckdb` alone.

# arkdb 0.0.5 2018-10-31

- `ark()`'s default `keep-open` method would cut off header names for
   Postgres connections (due to variation in the behavior of SQL queries
   with `LIMIT 0`.)  The issue is now resolved by accessing the header in
   a more robust, general way.

# arkdb 0.0.4 2018-09-27

- `unark()` will strip out non-compliant characters in table names by default.
- `unark()` gains the optional argument `tablenames`, allowing the user to
   specify the corresponding table names manually, rather than enforcing
   they correspond with the incoming file names. 
   [#18](https://github.com/ropensci/arkdb/issues/18)
-  `unark()` gains the argument `encoding`, allowing users to directly set
   the encoding of incoming files.  Previously this could only be set by
   setting `options(encoding)`, which will still work as well. See
  `FAO.R` example in `examples` for an illustration.  
- `unark()` will now attempt to guess which streaming parser to use 
   (e.g `csv` or `tsv`) based on the file extension pattern, rather than
   defaulting to a `tsv` parser.  (`ark()` still defaults to exporting in
   the more portable `tsv` format).

# arkdb 0.0.3 2018-09-11

* Remove dependency on utils::askYesNo for backward compatibility, [#17](https://github.com/ropensci/arkdb/issues/17)

# arkdb 0.0.2 2018-08-20 (First release to CRAN)

* Ensure the suggested dependency MonetDBLite is available before running unit test using it.

# arkdb 0.0.1 2018-08-20

* overwrite existing tables of same name (with warning and
  interactive proceed) in both DB and text-files to avoid
  appending.

# arkdb 0.0.0.9000 2018-08-11

* Added a `NEWS.md` file to track changes to the package.
* Log messages improved as suggested by @richfitz
* Improved mechanism for windowing in most DBs, from @krlmlr [#8](https://github.com/ropensci/arkdb/pull/8)
* Support pluggable I/O, based on @richfitz suggestions [#3](https://github.com/ropensci/arkdb/issues/3), [#10](https://github.com/ropensci/arkdb/pull/10)

