library(arkdb)
#library(dbplyr)
#library(dplyr)
#library(nycflights13)
#library(fs)
#library(RSQLite)
#library(MonetDBLite)

tmp <- tempdir()

testthat::context("basic")
testthat::test_that("we can ark and unark a db", {

  db <- dbplyr::nycflights13_sqlite(tmp)
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  ark(db, dir, lines = 50000, overwrite = TRUE)
  
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 5)

  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
 
  dim(myflights[[1]])
  ## Classes not preserved, we get read_tsv guesses on class
  
  DBI::dbDisconnect(new_db$con)
  unlink(dir, TRUE)
  unlink(fs::path(tmp, "local.sqlite"))

})


testthat::context("plain-txt")

testthat::test_that("we can ark and unark a db in plain text", {
  db <- dbplyr::nycflights13_sqlite(tmp)
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ark(db, dir, lines = 50000, compress = "none", overwrite = TRUE)
  
  files <- fs::dir_ls(dir, glob = "*.tsv")
  testthat::expect_length(files, 5)
  
  ## unark
  new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  flights <- dplyr::tbl(new_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  DBI::dbDisconnect(new_db$con)
  unlink(dir, TRUE)
  unlink(fs::path(tmp, "local.sqlite"))
})


testthat::context("alternate method")

testthat::test_that("alternate method for ark", {
  
  db <- dbplyr::nycflights13_sqlite(tmp)
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ark(db, dir, lines = 50000, method = "window", overwrite = TRUE)
  
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 5)
  
  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  ## unark
  new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  DBI::dbDisconnect(new_db$con)
  unlink(dir, TRUE)
  unlink(fs::path(tmp, "local.sqlite"))
})

testthat::context("MonetDB")
testthat::test_that("try with MonetDB & alternate method", {
  
  ## SETUP, with text files:
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  data <-  list(airlines = nycflights13::airlines, 
                airports = nycflights13::airports, 
                flights = nycflights13::flights)
  tmp <- lapply(names(data), function(x) 
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv"))))
  files <- fs::dir_ls(dir, glob = "*.tsv")
  testthat::expect_length(files, 3)

  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  new_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  flights <- dplyr::tbl(new_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  ## clean out the text files
  unlink(dir, TRUE) # ark'd text files
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ## Test has_between
  testthat::expect_false( has_between(new_db, "airlines") )
  
  
  
  
  #### Test ark ######
  ark(new_db, dir, lines = 50000L, method = "window", overwrite = TRUE)
  
  ## test ark results
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 3)
  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  ## Cleanup 
  DBI::dbDisconnect(new_db)
  unlink(monet_dir, TRUE)
  unlink(dir, TRUE) # ark'd text files
  
})


