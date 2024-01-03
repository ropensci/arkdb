
# arkdb <img src="man/figures/logo.svg" align="right" alt="" width="120" />

[![R build
status](https://github.com/ropensci/arkdb/workflows/R-CMD-check/badge.svg)](https://github.com/ropensci/arkdb/actions)
[![Travis build
status](https://app.travis-ci.com/ropensci/arkdb.svg?branch=master)](https://app.travis-ci.com/ropensci/arkdb)
[![Coverage
status](https://codecov.io/gh/ropensci/arkdb/branch/master/graph/badge.svg)](https://app.codecov.io/github/ropensci/arkdb?branch=master)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/arkdb)](https://cran.r-project.org/package=arkdb)
[![](https://badges.ropensci.org/224_status.svg)](https://github.com/ropensci/software-review/issues/224)
[![lifecycle](https://img.shields.io/badge/lifecycle-stable-brightgreen.svg)](https://lifecycle.r-lib.org/articles/stages.html)
[![CRAN RStudio mirror
downloads](http://cranlogs.r-pkg.org/badges/grand-total/arkdb)](https://CRAN.R-project.org/package=arkdb)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1343943.svg)](https://doi.org/10.5281/zenodo.1343943)
<!-- badges: end -->

<!-- README.md is generated from README.Rmd. Please edit that file -->

The goal of `arkdb` is to provide a convenient way to move data from
large compressed text files (tsv, csv, etc) into any DBI-compliant
database connection (e.g. MYSQL, Postgres, SQLite; see
[DBI](https://solutions.rstudio.com/db/r-packages/DBI/)), and move
tables out of such databases into text files. The key feature of `arkdb`
is that files are moved between databases and text files in chunks of a
fixed size, allowing the package functions to work with tables that
would be much too large to read into memory all at once. There is also
functionality for filtering and applying transformation to data as it is
extracted from the database.

The `arkdb` package is easily extended to use custom read and write
methods allowing you to dictate your own output formats. See
`R/streamable_table.R` for examples that include using:

-   Base c/tsv
-   Apache arrow’s parquet
-   The `readr` package for c/tsv

## Links

-   A more detailed introduction to package design and use can be found
    in the package
    [Vignette](https://docs.ropensci.org/arkdb/articles/arkdb.html)
-   [Online versions of package
    documentation](https://docs.ropensci.org/arkdb/)

## Installation

You can install arkdb from GitHub with:

``` r
# install.packages("devtools")
devtools::install_github("cboettig/arkdb")
```

# Basic use

``` r
library(arkdb)

# additional libraries just for this demo
library(dbplyr)
library(dplyr)
library(fs)
```

## Creating an archive of a database

Consider the `nycflights` database in SQLite:

``` r
tmp <- tempdir() # Or can be your working directory, "."
db <- dbplyr::nycflights13_sqlite(tmp)
#> Caching nycflights db at /tmp/Rtmpm6YZ0e/nycflights13.sqlite
#> Creating table: airlines
#> Creating table: airports
#> Creating table: flights
#> Creating table: planes
#> Creating table: weather
```

Create an archive of the database:

``` r
dir <- fs::dir_create(fs::path(tmp, "nycflights"))
ark(db, dir, lines = 50000)
#> Exporting airlines in 50000 line chunks:
#>  ...Done! (in 0.005531788 secs)
#> Exporting airports in 50000 line chunks:
#>  ...Done! (in 0.02239442 secs)
#> Exporting flights in 50000 line chunks:
#>  ...Done! (in 11.78997 secs)
#> Exporting planes in 50000 line chunks:
#>  ...Done! (in 0.03349638 secs)
#> Exporting weather in 50000 line chunks:
#>  ...Done! (in 0.8155148 secs)
```

## Unarchive

Import a list of compressed tabular files (i.e. `*.csv.bz2`) into a
local SQLite database:

``` r
files <- fs::dir_ls(dir)
new_db <- DBI::dbConnect(RSQLite::SQLite(), fs::path(tmp, "local.sqlite"))

unark(files, new_db, lines = 50000)
#> Importing /tmp/Rtmpm6YZ0e/nycflights/airlines.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.0131464 secs)
#> Importing /tmp/Rtmpm6YZ0e/nycflights/airports.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.02401853 secs)
#> Importing /tmp/Rtmpm6YZ0e/nycflights/flights.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 7.150884 secs)
#> Importing /tmp/Rtmpm6YZ0e/nycflights/planes.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.0348866 secs)
#> Importing /tmp/Rtmpm6YZ0e/nycflights/weather.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.2378168 secs)
```

## Using filters

This package can also be used to generate slices of data that are
required for analytical or operational purposes. In the example below we
archive to disk only the flight data that occurred in the month of
December. It is recommended to use filters on a single table at a time.

``` r
ark(db, dir, lines = 50000, tables = "flights", filter_statement = "WHERE month = 12")
```

## Using callbacks

It is possible to use a callback to perform just-in-time data
transformations before ark writes your data object to disk in your
preferred format. In the example below, we write a simple transformation
to convert the flights data `arr_delay` field, from minutes, to hours.
It is recommended to use callbacks on a single table at a time. A
callback function can be anything you can imagine so long as it returns
a data.frame that can be written to disk.

``` r
mins_to_hours <- function(data) {
  data$arr_delay <- data$arr_delay/60
  data
}

ark(db, dir, lines = 50000, tables = "flights", callback = mins_to_hours)
```

## ark() in parallel

There are two strategies for using `ark` in parallel. One is to loop
over the tables, re-using the ark function per table in parallel. The
other, introduced in 0.0.15, is to use the “window-parallel” method
which loops over chunks of your table. This is particularly useful if
your tables are very large and can speed up the process significantly.

Note: `window-parallel` currently only works in conjunction with
`streamable_parquet`

``` r
# Strategy 1: Parallel over tables
library(arkdb)
library(future.apply)

plan(multisession)

# Any streamable_table method is acceptable
future_lapply(vector_of_tables, function(x) ark(db, dir, lines, tables = x))

# Strategy 2: Parallel over chunks of a table
library(arkdb)
library(future.apply)

plan(multisession)

ark(
  db, 
  dir, 
  streamable_table = streamable_parquet(), # required for window-parallel
  lines = 50000, 
  tables = "flights", 
  method = "window-parallel"
)

# Strategy 3: Parallel over tables and chunks of tables
library(arkdb)
library(future.apply)
# 16 core machine for example
plan(list(tweak(multisession, n = 4), tweak(multisession, n = 4)))

# 4 tables at a time, 4 threads per table
future_lapply(vector_of_tables, function(x) { 
  ark(
    db, 
    dir, 
    streamable_table = streamable_parquet(), # required for window-parallel
    lines = 50000, 
    tables = x, 
    method = "window-parallel")
  }
)
```

## ETLs with arkdb

The `arkdb` package can also be used to create a number of ETL pipelines
involving text archives or databases given its ability to filter, and
use callbacks. In the example below, we leverage `duckdb` to read a
fictional folder of files by US state, filter by `var_filtered`, apply a
callback transformation `transform_fun` to `var_transformed` save as
parquet, and then load a folder of parquet files for analysis with
Apache Arrow.

``` r
library(arrow)
library(duckdb)

db <- dbConnect(duckdb::duckdb())

transform_fun <- function(data) {
  data$var_transformed <- sqrt(data$var_transformed)
  data
}

for(state in c("DC", state.abb)) {
  path <- paste0("path/to/archives/", state, ".gz")
  
  ark(
    db,
    dir = paste0("output/", state),
    streamable_table = streamable_parquet(), # parquet files of nline rows
    lines = 100000,
    # See: https://duckdb.org/docs/data/csv
    tables = sprintf("read_csv_auto('%s')", path), 
    compress = "none", # Compression meaningless for parquet as it's already compressed
    overwrite = T, 
    filenames = state, # Overload tablename
    filter_statement = "WHERE var_filtered = 1",
    callback = transform_fun
  )
}

# The result is trivial to read in with arrow 
ds <- open_dataset("output", partitioning = "state")
```

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of
Conduct](https://ropensci.org/code-of-conduct/). By participating in
this project you agree to abide by its terms.

[![ropensci_footer](https://ropensci.org/public_images/ropensci_footer.png)](https://ropensci.org)
