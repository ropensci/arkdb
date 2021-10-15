
# arkdb <img src="man/figures/logo.svg" align="right" alt="" width="120" />

[![R build
status](https://github.com/ropensci/arkdb/workflows/R-CMD-check/badge.svg)](https://github.com/ropensci/arkdb/actions)
[![Travis build
status](https://travis-ci.org/ropensci/arkdb.svg?branch=master)](https://travis-ci.org/ropensci/arkdb)
[![Coverage
status](https://codecov.io/gh/ropensci/arkdb/branch/master/graph/badge.svg)](https://codecov.io/github/ropensci/arkdb?branch=master)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/arkdb)](https://cran.r-project.org/package=arkdb)
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
[DBI](https://db.rstudio.com/dbi/)), and move tables out of such
databases into text files. The key feature of `arkdb` is that files are
moved between databases and text files in chunks of a fixed size,
allowing the package functions to work with tables that would be much
too large to read into memory all at once. There is also functionality
for filtering and applying transformation to data as it is extracted
from the database.

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
#> Caching nycflights db at /tmp/RtmpoqsIiq/nycflights13.sqlite
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
#>  ...Done! (in 0.002958298 secs)
#> Exporting airports in 50000 line chunks:
#>  ...Done! (in 0.01185942 secs)
#> Exporting flights in 50000 line chunks:
#>  ...Done! (in 6.424041 secs)
#> Exporting planes in 50000 line chunks:
#>  ...Done! (in 0.01805758 secs)
#> Exporting weather in 50000 line chunks:
#>  ...Done! (in 0.4436579 secs)
```

## Unarchive

Import a list of compressed tabular files (i.e. `*.csv.bz2`) into a
local SQLite database:

``` r
files <- fs::dir_ls(dir)
new_db <- DBI::dbConnect(RSQLite::SQLite(), fs::path(tmp, "local.sqlite"))

unark(files, new_db, lines = 50000)
#> Importing /tmp/RtmpoqsIiq/nycflights/airlines.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.006127834 secs)
#> Importing /tmp/RtmpoqsIiq/nycflights/airports.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.01151252 secs)
#> Importing /tmp/RtmpoqsIiq/nycflights/flights.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 3.744042 secs)
#> Importing /tmp/RtmpoqsIiq/nycflights/planes.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.01825166 secs)
#> Importing /tmp/RtmpoqsIiq/nycflights/weather.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.1292217 secs)
```

## Using filters

This package can also be used to generate slices of data that are
required for analytical or operational purposes. In the example below we
archive to disk only the flight data that occured in the month of
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

[![ropensci\_footer](https://ropensci.org/public_images/ropensci_footer.png)](https://ropensci.org)
