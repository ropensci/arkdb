library(arkdb)
#library(dbplyr)
#library(dplyr)
#library(nycflights13)
#library(fs)
#library(RSQLite)
#library(MonetDBLite)


testthat::context("basic")
testthat::test_that("we can ark and unark a db", {

  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  ark(db, dir, lines = 50000)
  
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 5)

  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  new_db <- dplyr::src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
 
  dim(myflights[[1]])
  ## Classes not preserved, we get read_tsv guesses on class
  
  unlink(dir, TRUE)
  unlink("local.sqlite")
  
})


testthat::context("plain-txt")

testthat::test_that("we can ark and unark a db in plain text", {
  
  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  
  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  ark(db, dir, lines = 50000, compress = "none")
  
  files <- fs::dir_ls(dir, glob = "*.tsv")
  testthat::expect_length(files, 5)
  
  ## unark
  new_db <- dplyr::src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  flights <- dplyr::tbl(new_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  unlink(dir, TRUE)
  unlink("local.sqlite")

})


testthat::context("alternate method")

testthat::test_that("alternate method for ark", {
  
  db <- dbplyr::nycflights13_sqlite(".")
  dir <- fs::dir_create("nycflights")
  ark(db, dir, lines = 50000, method = "window")
  
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 5)
  
  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  ## unark
  new_db <- dplyr::src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  unlink(dir, TRUE)
  unlink("local.sqlite")
  
})

testthat::context("MonetDB")
testthat::test_that("try with MonetDB & alternate method", {
  
  ## SETUP, with text files:
  dir <- fs::dir_create("nycflights")
  data <-  list(airlines = nycflights13::airlines, 
                airports = nycflights13::airports, 
                flights = nycflights13::flights)
  tmp <- lapply(names(data), function(x) 
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv.gz"))))
  files <- fs::dir_ls(dir, glob = "*.tsv.gz")
  testthat::expect_length(files, 3)

  # test alternate DB
  monet_dir <- fs::dir_create("monet")
  new_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  unark(files, new_db, lines = 50000)
  
  flights <- dplyr::tbl(new_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  ## clean out the text files
  unlink(dir, TRUE) # ark'd text files
  dir <- fs::dir_create("nycflights")
  
  ark(new_db, dir, lines = 50000L, method = "window")
  
  ## test ark results
  files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  testthat::expect_length(files, 3)
  
  myflights <- suppressMessages(
    readr::read_tsv(fs::path(dir, "flights.tsv.bz2")))
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  DBI::dbDisconnect(new_db)
  unlink(monet_dir, TRUE)
  unlink("local.sqlite")     # unarked db
  unlink(dir, TRUE) # ark'd text files
  
})



testthat::context("has_windowing")
testthat::test_that("we can test for windowing", {
  
  ## SETUP, with text files:
  dir <- fs::dir_create("nycflights")
  data <-  list(airlines = nycflights13::airlines, 
                airports = nycflights13::airports)
  tmp <- lapply(names(data), function(x) 
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv.gz"))))
  files <- fs::dir_ls(dir, glob = "*.tsv.gz")
  testthat::expect_length(files, 2)
  
  
  # set up db
  ## MonetDB LITE has BETWEEN but not ROWNUM
  
  # db_dir <- fs::dir_create("monet")
  # new_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), db_dir)
  # unark(files, new_db, lines = 50000)
  # unlink(db_dir, TRUE)
  
  
  new_db <- dplyr::src_sqlite("local.sqlite", create = TRUE)
  unark(files, new_db, lines = 50000)
  
  testthat::expect_false( has_between(new_db, "airlines") )
  
  
  unlink(dir, TRUE) # ark'd text files
  unlink("local.sqlite")     # unarked db
  
  
})




## dbplyr handles this automatically
#DBI::dbDisconnect(db[[1]])
#DBI::dbDisconnect(new_db[[1]])

