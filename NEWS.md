
# arkdb 0.0.4

- `unark()` will strip out non-compliant characters by default.
- `unark()` is also be more flexible, allowing the user to specify
   the corresponding table names manually, rather than enforcing
   they correspond with the incoming csv names. [#18](https://github.com/ropensci/arkdb/issues/18)
- Technical tweak: readLines call inside `unark()` method will use encoding
  directly from `getOption("encoding")`, e.g. allowing encoding to be set to UTF-8. 
  This can resolve parsing errors when using the readr parser on certain files.  See
  `FAO.R` example in `examples` for an illustration.  

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

