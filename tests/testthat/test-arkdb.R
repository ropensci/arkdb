library(arkdb)
#library(dbplyr)
#library(dplyr)
#library(nycflights13)
#library(fs)
#library(RSQLite)
#library(MonetDBLite)

# Setup
tmp <- tempdir()
db <- dbplyr::nycflights13_sqlite(tmp)
dir <- file.path(tmp, "nycflights")
dir.create(dir, showWarnings = FALSE)
new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)

## Note: later tests will overwrite existing tables and files, throwing warnings

testthat::context("basic")
testthat::test_that("we can ark and unark a db", {

  ark(db, dir, lines = 50000, overwrite = TRUE)
  
  #files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  testthat::expect_length(files, 5)

  myflights <- suppressMessages(
    readr::read_tsv(file.path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

})


testthat::context("plain-txt")

testthat::test_that("we can ark and unark a db in plain text", {
  #db <- dbplyr::nycflights13_sqlite(tmp)
  #dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ark(db, dir, lines = 50000, compress = "none", overwrite = TRUE)
  
  #files <- fs::dir_ls(dir, glob = "*.tsv")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  testthat::expect_length(files, 5)
 
   myflights <- suppressMessages(
    readr::read_tsv(file.path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  #new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

})


testthat::context("alternate method")

testthat::test_that("alternate method for ark", {
  
  ark(db, dir, lines = 50000, method = "window", overwrite = TRUE)
  
  myflights <- suppressMessages(
    readr::read_tsv(file.path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  ## unark
  testthat::expect_warning(
    unark(files, new_db, lines = 50000, overwrite = TRUE), 
  "overwriting")
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

  
})

testthat::context("MonetDB")
testthat::test_that("try with MonetDB & alternate method", {
  
  ## SETUP, with text files:
  data <-  list(airlines = nycflights13::airlines, 
                airports = nycflights13::airports, 
                flights = nycflights13::flights)
  sink <- lapply(names(data), function(x) 
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv"))))
  files <- fs::dir_ls(dir, glob = "*.tsv")
  testthat::expect_length(files, 3)

  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  monet_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  flights <- dplyr::tbl(monet_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  ## clean out the text files
  unlink(dir, TRUE) # ark'd text files
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ## Test has_between
  testthat::expect_false( has_between(monet_db, "airlines") )
  
  #### Test ark ######
  ark(monet_db, dir, lines = 50000L, method = "window", overwrite = TRUE)
  
  ## test ark results
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 3)
  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  
  
})

## Cleanup 
DBI::dbDisconnect(db)
DBI::dbDisconnect(new_db)
unlink(monet_dir, TRUE)
unlink(dir, TRUE) # ark'd text files
