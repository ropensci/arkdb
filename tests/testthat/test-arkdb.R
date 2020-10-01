context("basic")

# Setup
  tmp <- tempdir()
  db <- dbplyr::nycflights13_sqlite(tmp)
  dir <- file.path(tmp, "nycflights")
  dir.create(dir, showWarnings = FALSE)
  dbdir <- fs::path(tmp, "local.sqlite")
  new_db <- DBI::dbConnect(RSQLite::SQLite(), dbdir)

## Note: later tests will overwrite existing tables and files, throwing warnings

testthat::test_that("we can ark and unark a db", {

  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  ark(db, dir, lines = 50000, overwrite = TRUE)
  

  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  #files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  testthat::expect_length(files, 5)
  unark(files, new_db, lines = 50000, overwrite = TRUE)
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

})


testthat::context("plain-txt")

testthat::test_that("we can ark and unark a db in plain text", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  ark(db, dir, lines = 50000, compress = "none", overwrite = TRUE)
  
  #files <- fs::dir_ls(dir, glob = "*.tsv")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  testthat::expect_length(files, 5)
 
  suppressWarnings( # ignore overwrite warning
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  ## unark
  #new_db <- dplyr::src_sqlite(fs::path(tmp, "local.sqlite"), create = TRUE)
  suppressWarnings( # ignore overwrite warning
    unark(files, new_db, lines = 50000, overwrite = TRUE)
  )
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

})


testthat::context("alternate method")

testthat::test_that("alternate method for ark", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  testthat::expect_warning( 
    ark(db, dir, lines = 50000, method = "window", overwrite = TRUE),
  "overwriting")
  
  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  
  ## unark
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  testthat::expect_warning(
    unark(files, new_db, lines = 50000, overwrite = TRUE), 
  "overwriting")
  
  myflights <- dplyr::tbl(new_db, "flights")
  testthat::expect_is(myflights, "tbl_dbi")
  myflights <- dplyr::collect(myflights)
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  

  
})

testthat::context("MonetDB")
testthat::test_that("try with MonetDB & alternate method", {
  
  skip_if_not_installed("MonetDBLite")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("fs")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  ## SETUP, with text files. (Could skip as these now exist from above tests)
  data <-  list(airlines = nycflights13::airlines, 
                airports = nycflights13::airports, 
                flights = nycflights13::flights)
  sink <- lapply(names(data), function(x) 
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv"))))

  files <- fs::dir_ls(dir, glob = "*.tsv")
  
  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  monet_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  unark(files, monet_db, lines = 50000, overwrite = TRUE)
  
  flights <- dplyr::tbl(monet_db, "flights")
  testthat::expect_equal(dim(flights)[[2]], 19)
  testthat::expect_is(flights, "tbl_dbi")
  
  ## clean out the text files
  unlink(dir, TRUE) # ark'd text files
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))
  
  ## Test has_between
  testthat::expect_false( arkdb:::has_between(monet_db, "airlines") )
  
  #### Test ark ######
  ark(monet_db, dir, lines = 50000L, method = "window", overwrite = TRUE)
  
  ## test ark results
  suppressWarnings(
    myflights <- readr::read_tsv(fs::path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  DBI::dbDisconnect(monet_db)
  unlink(monet_dir, TRUE)
  
  
})

  ## Cleanup 
  if (packageVersion("dbplyr") > "1.99") {
    DBI::dbDisconnect(db)
  } else {
    DBI::dbDisconnect(db$con)
  }
  DBI::dbDisconnect(new_db)
  unlink(dir, TRUE) # ark'd text files


