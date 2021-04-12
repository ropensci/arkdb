library(arkdb)
library(testthat)

testthat::context("Bulk importers")
test_that("We can do fast bulk import with duckdb in most cases", {
  skip_on_cran()
  skip_if_offline()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  library(nycflights13)
  
  
  tmp <- tempdir()
  duck_dir <- fs::dir_create(fs::path(tmp))
  duck_file <- fs::path(duck_dir, "flights", ext = "db")
  db_con <- DBI::dbConnect(duckdb::duckdb(), duck_file)
  
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
  files <- paste(tmp, c("airlines.tsv.xz", 
                        "planes.tsv.xz", 
                        "flights.tsv.xz"), 
                 sep="/")
  
  expect_message(unark(files, db_con), "Native bulk importer found")
  expect_failure(expect_message(suppressWarnings(unark(files, db_con, overwrite = TRUE)),
                                "Native import failed, falling back on R-based parser"))
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  expect_is(remote_flights, "tbl_duckdb_connection")
  lapply(who, function(tbl) DBI::dbRemoveTable(db_con, tbl))
  
  expect_message(
    suppressWarnings(unark(files, db_con, try_native = FALSE)),
    "line chunks"
  )
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  expect_is(remote_flights, "tbl_duckdb_connection")
  
  ## import from URL
  url <- paste0("https://github.com/ropensci/piggyback",
                "/releases/download/v0.0.3/mtcars.tsv.xz")
  expect_message(unark(url, db_con), "Native bulk importer found")
  
  remote_cars <- dplyr::collect(dplyr::tbl(db_con, "mtcars"))
  expect_is(remote_cars, "data.frame")
  expect_equivalent(mtcars, remote_cars)
  
  DBI::dbDisconnect(db_con, shutdown = TRUE)
  unlink(duck_dir, TRUE)
  unlink(tmp)
})


test_that("We can do fast bulk import with MonetDBLite in most cases", {
  skip_on_cran()
  skip_if_offline()
  skip_if_not_installed("MonetDBLite")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  library(nycflights13)
  
  MonetDBLite::monetdblite_shutdown()
  
  tmp <- tempdir()
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  db_con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
  files <- paste(tmp, c("airlines.tsv.xz", 
                        "planes.tsv.xz", 
                        "flights.tsv.xz"), 
                 sep="/")
  
  expect_message(unark(files, db_con), "Native bulk importer found")
  expect_failure(
    expect_message(
      suppressWarnings(unark(files, db_con, overwrite = TRUE)),
      "Native import failed, falling back on R-based parser")
  )
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  expect_is(remote_flights, "tbl_MonetDBEmbeddedConnection")
  lapply(who, function(tbl) DBI::dbRemoveTable(db_con, tbl))
  
  expect_message(suppressWarnings(
    unark(files, db_con, try_native = FALSE, overwrite = TRUE)),
    "line chunks")
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  lapply(who, function(tbl) DBI::dbRemoveTable(db_con, tbl))
  
  ## import from URL
  url <- paste0("https://github.com/ropensci/piggyback",
                "/releases/download/v0.0.3/mtcars.tsv.xz")
  unark(url, db_con)
  
  remote_cars <- dplyr::collect(dplyr::tbl(db_con, "mtcars"))
  expect_is(remote_cars, "data.frame")
  expect_equivalent(mtcars, remote_cars)
  
  DBI::dbDisconnect(db_con)
  unlink(monet_dir, TRUE)
  unlink(tmp)
})


