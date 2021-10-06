context("callbacks")

# Setup
tmp <- tempdir()
db <- dbplyr::nycflights13_sqlite(tmp)
dir <- file.path(tmp, "nycflights")
dir.create(dir, showWarnings = FALSE)
dbdir <- fs::path(tmp, "local.sqlite")
new_db <- DBI::dbConnect(RSQLite::SQLite(), dbdir)

## Note: later tests will overwrite existing tables and files, throwing warnings

testthat::test_that("ark with keep-open and callback", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  # Callback to convert arr_delay in the flights
  # data from minutes to hours
  callback <- function(data) {
    data$arr_delay <- as.numeric(data$arr_delay/60)
    data
  }
  
  ark(db, dir, lines = 50000, tables = "flights", method = "keep-open", overwrite = TRUE, callback = callback)  
  
  
  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  testthat::expect_equal(myflights$arr_delay, nycflights13::flights$arr_delay/60)
  
})


testthat::test_that("ark with window and callback", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  # Callback to convert arr_delay in the flights
  # data from minutes to hours
  callback <- function(data) {
    data$arr_delay <- as.numeric(data$arr_delay/60)
    data
  }
  
  ark(db, dir, lines = 50000, tables = "flights", method = "window", overwrite = TRUE, callback = callback)  
  
  
  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  testthat::expect_equal(myflights$arr_delay, nycflights13::flights$arr_delay/60)
  
})

testthat::test_that("ark with sql-window and callback", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  
  # Callback to convert arr_delay in the flights
  # data from minutes to hours
  callback <- function(data) {
    data$arr_delay <- as.numeric(data$arr_delay/60)
    data
  }
  
  ark(db, dir, lines = 50000, tables = "flights", method = "sql-window", overwrite = TRUE, callback = callback)  
  
  
  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  testthat::expect_equal(dim(myflights), 
                         dim(nycflights13::flights))
  
  testthat::expect_equal(myflights$arr_delay, nycflights13::flights$arr_delay/60)
  
})


disconnect(db)
disconnect(new_db)
unlink(dir, TRUE) # ark'd text/parquet files