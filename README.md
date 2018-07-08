
[![Travis build
status](https://travis-ci.org/cboettig/arkdb.svg?branch=master)](https://travis-ci.org/cboettig/arkdb)
[![Coverage
status](https://codecov.io/gh/cboettig/arkdb/branch/master/graph/badge.svg)](https://codecov.io/github/cboettig/arkdb?branch=master)
[![AppVeyor Build
Status](https://ci.appveyor.com/api/projects/status/github/cboettig/arkdb?branch=master&svg=true)](https://ci.appveyor.com/project/cboettig/arkdb)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/arkdb)](https://cran.r-project.org/package=arkdb)
[![](https://badges.ropensci.org/224_status.svg)](https://github.com/ropensci/onboarding/issues/224)

<!-- README.md is generated from README.Rmd. Please edit that file -->

# arkdb

The goal of arkdb is to provide a convienent way to move data from large
compressed text files (tsv, csv, etc) into any DBI-compliant database
connection (e.g. MYSQL, Postgres, SQLite; see
[DBI](https://db.rstudio.com/dbi/)), and move tables out of such
databases into text files. The key feature of arkdb is that files are
moved between databases and text files in chunks of a fixed size,
allowing the package functions to work with tables that would be much to
large to read into memory all at once.

## Links

  - A more detailed introduction to package design and use can be found
    in the package
    [Vignette](https://cboettig.github.io/arkdb/articles/arkdb_intro.html)
  - [Online versions of package
    documentation](https://cboettig.github.io/arkdb)

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
db <- dbplyr::nycflights13_sqlite(".")
#> Caching nycflights db at ./nycflights13.sqlite
#> Creating table: airlines
#> Creating table: airports
#> Creating table: flights
#> Creating table: planes
#> Creating table: weather
```

Create an archive of the database:

``` r
ark(db, ".", lines = 50000)
#> Exporting in 50000 line chunks:
#> airlines
#> Warning: Closing open result set, pending rows
#> ...Done! (in 0.007887125 secs)
#> Warning: Closing open result set, pending rows
#> Exporting in 50000 line chunks:
#> airports
#> Warning: Closing open result set, pending rows
#> ...Done! (in 0.03519011 secs)
#> Warning: Closing open result set, pending rows
#> Exporting in 50000 line chunks:
#> flights
#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows

#> Warning: Closing open result set, pending rows
#> ...Done! (in 8.650942 secs)
#> Warning: Closing open result set, pending rows
#> Exporting in 50000 line chunks:
#> planes
#> Warning: Closing open result set, pending rows
#> ...Done! (in 0.06782579 secs)
#> Warning: Closing open result set, pending rows
#> Exporting in 50000 line chunks:
#> weather
#> Warning: Closing open result set, pending rows
#> ...Done! (in 0.5270221 secs)
```

## Unarchive

Import a list of compressed tabular files (i.e. `*.tsv.bz2`) into a
local SQLite database:

``` r
files <- fs::dir_ls(glob = "*.tsv.bz2")
new_db <- src_sqlite("local.sqlite", create=TRUE)

unark(files, new_db, lines = 50000)
#> Importing in 50000 line chunks:
#> airlines.tsv.bz2
#> ...Done! (in 0.06034207 secs)
#> Importing in 50000 line chunks:
#> airports.tsv.bz2
#> ...Done! (in 0.02559996 secs)
#> Importing in 50000 line chunks:
#> flights.tsv.bz2
#> ...Done! (in 6.464484 secs)
#> Importing in 50000 line chunks:
#> planes.tsv.bz2
#> ...Done! (in 0.04460192 secs)
#> Importing in 50000 line chunks:
#> weather.tsv.bz2
#> ...Done! (in 0.5441582 secs)

new_db
#> src:  sqlite 3.22.0 [local.sqlite]
#> tbls: airlines, airports, flights, planes, weather
```

-----

Please note that this project is released with a [Contributor Code of
Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree
to abide by its terms.
