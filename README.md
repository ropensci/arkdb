
[![Travis build
status](https://travis-ci.org/cboettig/arkdb.svg?branch=master)](https://travis-ci.org/cboettig/arkdb)  
[![Coverage
status](https://codecov.io/gh/cboettig/arkdb/branch/master/graph/badge.svg)](https://codecov.io/github/cboettig/arkdb?branch=master)
[![AppVeyor Build
Status](https://ci.appveyor.com/api/projects/status/github/cboettig/arkdb?branch=master&svg=true)](https://ci.appveyor.com/project/cboettig/arkdb)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/arkdb)](https://cran.r-project.org/package=arkdb)

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
#> Importing in 50000 line chunks:
#> airlines
#> ...Done! (in 0.01396799 secs)
#> Importing in 50000 line chunks:
#> airports
#> ...Done! (in 0.05178308 secs)
#> Importing in 50000 line chunks:
#> flights
#> ...Done! (in 0.5828159 secs)
#> Importing in 50000 line chunks:
#> planes
#> ...Done! (in 0.09557915 secs)
#> Importing in 50000 line chunks:
#> weather
#> ...Done! (in 0.3005581 secs)
```

## Unarchive

Import a list of compressed tabular files (`*.tsv.bz2`) into a local
SQLite database:

``` r
files <- fs::dir_ls(glob = "*.tsv.bz2")
new_db <-  unark(files, dbname = "local.sqlite", lines = 50000)
#> Importing in 50000 line chunks:
#> airlines.tsv.bz2
#> ...Done! (in 0.06316018 secs)
#> Importing in 50000 line chunks:
#> airports.tsv.bz2
#> ...Done! (in 0.04496694 secs)
#> Importing in 50000 line chunks:
#> flights.tsv.bz2
#> ...Done! (in 0.1594269 secs)
#> Importing in 50000 line chunks:
#> planes.tsv.bz2
#> ...Done! (in 0.1210289 secs)
#> Importing in 50000 line chunks:
#> weather.tsv.bz2
#> ...Done! (in 0.2026539 secs)

new_db
#> src:  sqlite 3.22.0 [local.sqlite]
#> tbls: airlines, airports, flights, planes, weather
```

-----

Please note that this project is released with a [Contributor Code of
Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree
to abide by its terms.
