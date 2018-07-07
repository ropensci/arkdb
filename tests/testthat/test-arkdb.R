library(arkdb)
library(dbplyr)
library(dplyr)
library(nycflights13)
library(fs)

testthat::test_that("we can ark and unark a db", {
  
  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  ark(db, dir, lines = 50000)
  
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 5)

  ## unark
  new_db <- src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  myflights <- tbl(new_db, "flights")
  
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
 
  testthat::expect_is(myflights, "tbl_dbi")

  myflights <- collect(myflights)
  dim(myflights[[1]])
  ## Classes not preserved, we get read_tsv guesses on class
  
  unlink("nycflights", TRUE)
  unlink("local.sqlite")
  unlink("nycflights13.sqlite")
  
})



testthat::test_that("we can ark and unark a db in plain text", {
  
  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  ark(db, dir, lines = 50000, compress = "none")
  
  files <- fs::dir_ls(dir, glob = "*.tsv")
  testthat::expect_length(files, 5)
  
  ## unark
  new_db <- src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  flights <- tbl(new_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  unlink("nycflights", TRUE)
  unlink("local.sqlite")
  unlink("nycflights13.sqlite")
  
})



## dbplyr handles this automatically
#DBI::dbDisconnect(db[[1]])
#DBI::dbDisconnect(new_db[[1]])

