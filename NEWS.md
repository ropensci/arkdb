
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

