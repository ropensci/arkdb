
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
too large to read into memory all at once.

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
#> Caching nycflights db at /tmp/RtmpTor2hC/nycflights13.sqlite
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
#>  ...Done! (in 0.008597136 secs)
#> Exporting airports in 50000 line chunks:
#>  ...Done! (in 0.02015805 secs)
#> Exporting flights in 50000 line chunks:
#>  ...Done! (in 10.31823 secs)
#> Exporting planes in 50000 line chunks:
#>  ...Done! (in 0.0298245 secs)
#> Exporting weather in 50000 line chunks:
#>  ...Done! (in 0.6843452 secs)
```

## Unarchive

Import a list of compressed tabular files (i.e. `*.csv.bz2`) into a
local SQLite database:

``` r
files <- fs::dir_ls(dir)
new_db <- DBI::dbConnect(RSQLite::SQLite(), fs::path(tmp, "local.sqlite"))

unark(files, new_db, lines = 50000)
#> Importing /tmp/RtmpTor2hC/nycflights/airlines.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.01362443 secs)
#> Importing /tmp/RtmpTor2hC/nycflights/airports.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.02313852 secs)
#> Importing /tmp/RtmpTor2hC/nycflights/flights.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 7.650134 secs)
#> Importing /tmp/RtmpTor2hC/nycflights/planes.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.03760815 secs)
#> Importing /tmp/RtmpTor2hC/nycflights/weather.tsv.bz2 in 50000 line chunks:
#>  ...Done! (in 0.2434587 secs)
```

# Output Formats and Custom Read and Write

The `arkdb` package is easily extended to use custom read and write
methods allowing you to dictate your own output formats. See
`R/streamable_table.R` for examples that include using:

-   Base c/tsv
-   Apache arrow’s parquet
-   The `readr` package for c/ts

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of
Conduct](https://ropensci.org/code-of-conduct/). By participating in
this project you agree to abide by its terms.

[![ropensci\_footer](https://ropensci.org/public_images/ropensci_footer.png)](https://ropensci.org)
