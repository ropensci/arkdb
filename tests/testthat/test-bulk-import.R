library(arkdb)
library(testthat)

testthat::context("MonetDBLite bulk importer")
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
  
  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  db_con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
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
  
  ## import from URL
  url <- paste0("https://github.com/ropensci/piggyback",
                "/releases/download/v0.0.3/mtcars.tsv.xz")
  unark(url, db_con)
  
  remote_cars <- dplyr::collect(dplyr::tbl(db_con, "mtcars"))
  expect_identical(mtcars, remote_cars)
  
  DBI::dbDisconnect(db_con)
  unlink(monet_dir, TRUE)
  unlink(tmp)
})




test_that("We can do fast bulk import with MonetDBLite in most cases", {
  
  skip_on_cran()
  skip_if_not_installed("MonetDBLite")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  library(nycflights13)
  
  MonetDBLite::monetdblite_shutdown()
  tmp <- tempdir()
  readr::write_tsv(airlines, file.path(tmp, "airlines.tsv.xz"))
  readr::write_tsv(planes, file.path(tmp, "planes.tsv.xz"))
  readr::write_tsv(flights, file.path(tmp, "flights.tsv.xz"))
  
  
  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  db_con <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  
  
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
  

  
  DBI::dbDisconnect(db_con)
  unlink(monet_dir, TRUE)
  unlink(tmp)
})