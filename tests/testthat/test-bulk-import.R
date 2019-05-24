library(DBI)
library(MonetDBLite)
library(readr)
library(nycflights13)
library(arkdb)
library(testthat)

test_that("We can do fast bulk import with MonetDBLite in most cases", {
  tmp <- tempdir()
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
  db_con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), tempdir())
  #f <- arkdb:::bulk_importer(db_con, arkdb::streamable_readr_tsv())
  #f(db_con,  file.path(tmp, "flights.tsv.xz"), "flights")
  
  files <- paste(tmp, c("airlines.tsv.xz", 
                        "planes.tsv.xz", 
                        "flights.tsv.xz"), 
                 sep="/")
  # 3.003 seconds
  system.time({
    unark(files, db_con)
  })
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  expect_is(remote_flights, "tbl_MonetDBEmbeddedConnection")
})




test_that("We can do fast bulk import with MonetDBLite in most cases", {
  tmp <- tempdir()
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
  db_con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), tempdir())

  files <- paste(tmp, c("airlines.tsv.xz", 
                        "planes.tsv.xz", 
                        "flights.tsv.xz"), 
                 sep="/")
  # 8.256 seconds
  system.time({
    unark(files, db_con, try_native = FALSE)
  })
  
  who <- DBI::dbListTables(db_con)
  expect_identical(sort(who), sort(c("airlines", "planes", "flights")))
  remote_flights <- dplyr::tbl(db_con, "flights")
  expect_is(remote_flights, "tbl_MonetDBEmbeddedConnection")
})