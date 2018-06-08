
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

## Installation

You can install arkdb from github with:

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
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:dbplyr':
#> 
#>     ident, sql
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
library(nycflights13)
library(fs)
```

## Creating an archive of an existing database

First, we’ll need an example database to work with. Conveniently, there
is a nice example using the NYC flights data built into the dbplyr
package.

``` r
db <- dbplyr::nycflights13_sqlite(".")
#> Caching nycflights db at ./nycflights13.sqlite
#> Creating table: airlines
#> Creating table: airports
#> Creating table: flights
#> Creating table: planes
#> Creating table: weather
```

To create an archive, we just give `ark` the connection to the database
and tell it where we want the `*.tsv.bz2` files to be archived:

``` r
dir <- fs::dir_create("nycflights")
ark(db, dir)
#> Importing in 10000 line chunks:
#> airlines
#> ...Done! (in 0.03933191 secs)
#> Importing in 10000 line chunks:
#> airports
#> ...Done! (in 0.07561302 secs)
#> Importing in 10000 line chunks:
#> flights
#> ...Done! (in 3.38947 secs)
#> Importing in 10000 line chunks:
#> planes
#> ...Done! (in 0.1876791 secs)
#> Importing in 10000 line chunks:
#> weather
#> ...Done! (in 0.544976 secs)
```

We can take a look and confirm the files have been written. Note that we
can use `fs::dir_info` to get a nice snapshot of the file sizes. Compare
the compressed sizes to the original database:

``` r
fs::dir_info(dir) %>% select(path, size)
#> # A tibble: 5 x 2
#>   path                               size
#>   <fs::path>                  <fs::bytes>
#> 1 nycflights/airlines.tsv.bz2         260
#> 2 nycflights/airports.tsv.bz2       28.2K
#> 3 nycflights/flights.tsv.bz2       148.5K
#> 4 nycflights/planes.tsv.bz2           12K
#> 5 nycflights/weather.tsv.bz2         109K
fs::file_info("nycflights13.sqlite") %>% select(path, size)
#> # A tibble: 1 x 2
#>   path                       size
#>   <fs::path>          <fs::bytes>
#> 1 nycflights13.sqlite       45.1M
```

## Unarchive

Now that we’ve gotten all the database into (compressed) plain text
files, let’s get them back out. We simply need to pass `unark` a list of
these compressed files:

``` r
files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
```

`unark` defaults to an SQLite database. Use the `dbname` argument to
specify an persistant on-disk location to store the data:

``` r
new_db <-  unark(files, dbname = "local.sqlite")
#> Importing in 10000 line chunks:
#> nycflights/airlines.tsv.bz2
#> ...Done! (in 0.140043 secs)
#> Importing in 10000 line chunks:
#> nycflights/airports.tsv.bz2
#> ...Done! (in 0.09119105 secs)
#> Importing in 10000 line chunks:
#> nycflights/flights.tsv.bz2
#> ...Done! (in 0.208385 secs)
#> Importing in 10000 line chunks:
#> nycflights/planes.tsv.bz2
#> ...Done! (in 0.2518289 secs)
#> Importing in 10000 line chunks:
#> nycflights/weather.tsv.bz2
#> ...Done! (in 0.238585 secs)
```

`unark` returns a `dplyr` databse connection that we can use in the
usual way:

``` r
tbl(new_db, "flights")
#> # Source:   table<flights> [?? x 19]
#> # Database: sqlite 3.22.0 [local.sqlite]
#>     year month   day dep_time sched_dep_time dep_delay arr_time
#>    <int> <int> <int>    <int>          <int>     <int>    <int>
#>  1  2013     1     1      517            515         2      830
#>  2  2013     1     1      533            529         4      850
#>  3  2013     1     1      542            540         2      923
#>  4  2013     1     1      544            545        -1     1004
#>  5  2013     1     1      554            600        -6      812
#>  6  2013     1     1      554            558        -4      740
#>  7  2013     1     1      555            600        -5      913
#>  8  2013     1     1      557            600        -3      709
#>  9  2013     1     1      557            600        -3      838
#> 10  2013     1     1      558            600        -2      753
#> # ... with more rows, and 12 more variables: sched_arr_time <int>,
#> #   arr_delay <int>, carrier <chr>, flight <int>, tailnum <chr>,
#> #   origin <chr>, dest <chr>, air_time <int>, distance <int>, hour <int>,
#> #   minute <int>, time_hour <dbl>
```

## Footnote on package rationale

Note that while most relational database backends implement some form of
`COPY` or `IMPORT` that allows them to read in and export out plain text
files directly, these methods are not consistent across database types
and not part of the standard SQL interface. Most importantly for our
case, they also cannot be called directly from R, but require a separate
stand-alone installation of the database client.
