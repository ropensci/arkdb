# arkdb 0.0.2

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

