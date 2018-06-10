## ----setup, include = FALSE----------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----gh-installation, eval = FALSE---------------------------------------
## # install.packages("devtools")
## devtools::install_github("cboettig/arkdb")

## ----message = FALSE-----------------------------------------------------
library(arkdb)

# additional libraries just for this demo
library(dbplyr)
library(dplyr)
library(nycflights13)
library(fs)

## ----example-------------------------------------------------------------
db <- dbplyr::nycflights13_sqlite(".")

## ------------------------------------------------------------------------
dir <- fs::dir_create("nycflights")
ark(db, dir, lines = 50000)


## ------------------------------------------------------------------------
fs::dir_info(dir) %>% select(path, size)
fs::file_info("nycflights13.sqlite") %>% select(path, size)


## ------------------------------------------------------------------------
files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
new_db <- src_sqlite("local.sqlite", create = TRUE)


## ------------------------------------------------------------------------
unark(files, new_db, lines = 50000)  

## ------------------------------------------------------------------------
tbl(new_db, "flights")

## ----include=FALSE-------------------------------------------------------
unlink("nycflights", TRUE)
unlink("local.sqlite")
unlink("nycflights13.sqlite")

## dbplyr handles this automatically
#DBI::dbDisconnect(db[[1]])
#DBI::dbDisconnect(new_db[[1]])

