context("basic")

skip_if_not_installed("dbplyr")
skip_if_not_installed("nycflights13")
skip_if_not_installed("RSQLite")

# Setup
tmp <- tempdir()
db <- dbplyr::nycflights13_sqlite(tmp)
dir <- file.path(tmp, "nycflights")
dir.create(dir, showWarnings = FALSE)
dbdir <- fs::path(tmp, "local.sqlite")
#new_db <- DBI::dbConnect(RSQLite::SQLite(), dbdir)
new_db <- local_db()
## Note: later tests will overwrite existing tables and files, throwing warnings

test_that("we can ark and unark a db", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_on_os("solaris")

  ark(db, dir, lines = 50000, overwrite = TRUE)


  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )

  ## unark
  # files <- fs::dir_ls(dir, glob = "*.tsv.bz2")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  expect_length(files, 5)
  unark(files, new_db, lines = 50000, overwrite = TRUE)

  myflights <- dplyr::tbl(new_db, "flights")
  expect_is(myflights, "tbl_dbi")

  myflights <- dplyr::collect(myflights)
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )
})


context("plain-txt")

test_that("we can ark and unark a db in plain text", {

  skip_on_os("solaris")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")

  ark(db, dir, lines = 50000, compress = "none", overwrite = TRUE)

  # files <- fs::dir_ls(dir, glob = "*.tsv")
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  expect_length(files, 5)

  suppressWarnings( # ignore overwrite warning
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )

  ## unark
  suppressWarnings( # ignore overwrite warning
    unark(files, new_db, lines = 50000, overwrite = TRUE)
  )
  myflights <- dplyr::tbl(new_db, "flights")
  expect_is(myflights, "tbl_dbi")

  myflights <- dplyr::collect(myflights)
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )
})


context("alternate method")

test_that("alternate method for ark", {
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_on_os("solaris")
  
  expect_warning( 
    ark(db, dir, lines = 50000, method = "window", overwrite = TRUE),
    "overwriting"
  )

  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )


  ## unark
  files <- list.files(dir, pattern = "[.]tsv.bz2$", full.names = TRUE)
  expect_warning(
    unark(files, new_db, lines = 50000, overwrite = TRUE),
    "overwriting"
  )

  myflights <- dplyr::tbl(new_db, "flights")
  expect_is(myflights, "tbl_dbi")
  myflights <- dplyr::collect(myflights)
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )
})

context("MonetDB")
test_that("try with MonetDB & alternate method", {
  
  skip_if_not_installed("MonetDBLite")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("fs")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_on_os("solaris")
  skip_on_cran()
  
  ## SETUP, with text files. (Could skip as these now exist from above tests)
  data <- list(
    airlines = nycflights13::airlines,
    airports = nycflights13::airports,
    flights = nycflights13::flights
  )
  sink <- lapply(names(data), function(x) {
    readr::write_tsv(data[[x]], fs::path(dir, paste0(x, ".tsv")))
  })

  files <- fs::dir_ls(dir, glob = "*.tsv")

  # test unark on alternate DB
  monet_dir <- fs::dir_create(fs::path(tmp, "monet"))
  monet_db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), monet_dir)
  unark(files, monet_db, lines = 50000, overwrite = TRUE)

  flights <- dplyr::tbl(monet_db, "flights")
  expect_equal(dim(flights)[[2]], 19)
  expect_is(flights, "tbl_dbi")

  ## clean out the text files
  unlink(dir, TRUE) # ark'd text files
  dir <- fs::dir_create(fs::path(tmp, "nycflights"))

  ## Test has_between
  expect_false(arkdb:::has_between(monet_db, "airlines"))

  #### Test ark ######
  ark(monet_db, dir, lines = 50000L, method = "window", overwrite = TRUE)

  ## test ark results
  suppressWarnings(
    myflights <- readr::read_tsv(fs::path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )

  disconnect(monet_db)
  unlink(monet_dir, TRUE)
})







context("parquet")
test_that("try with parquet & alternate method", {
  
  
  skip_if_not_installed("arrow") # Arrow installs on solaris, it just doesn't work
  skip_if_not_installed("dplyr")
  skip_if_not_installed("fs")
  skip_if_not_installed("nycflights13")
  skip_on_os("solaris")

  
  # test ark to parquet from sqlite
  ark(
    db,
    dir,
    lines = 25000,
    compress = "none",
    streamable_table = streamable_parquet(),
    method = "keep-open",
    overwrite = TRUE
  )

  # Test that parts are written out appropriately

  # airlines
  expect_equal(
    "part-00001.parquet",
    list.files(paste0(dir, "/", "airlines"))
  )

  expect_equal(
    nrow(arrow::open_dataset(paste0(dir, "/", "airlines"))),
    dbGetQuery(db, "SELECT COUNT(*) FROM airlines")[[1]]
  )

  # airports
  expect_equal(
    "part-00001.parquet",
    list.files(paste0(dir, "/", "airports"))
  )

  expect_equal(
    nrow(arrow::open_dataset(paste0(dir, "/", "airports"))),
    dbGetQuery(db, "SELECT COUNT(*) FROM airports")[[1]]
  )

  # flights
  expect_true("part-00001.parquet" %in% list.files(paste0(dir, "/", "flights")))
  expect_true("part-00014.parquet" %in% list.files(paste0(dir, "/", "flights")))

  expect_length(list.files(paste0(dir, "/", "flights")), 14)

  expect_equal(
    nrow(arrow::open_dataset(paste0(dir, "/", "flights"))),
    dbGetQuery(db, "SELECT COUNT(*) FROM flights")[[1]]
  )

  expect_equal(
    dim(arrow::open_dataset(paste0(dir, "/", "flights"))),
    dim(nycflights13::flights)
  )
})




context("inject filters")

test_that("e2e with filter for flights month = 2: readr tsv", {
  # summary(factor(nycflights13::flights$month)) filter month == 2, 24951 records
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_on_os("solaris")
  
  file.remove(paste0(dir, "/flights.tsv.bz2"))

  ark(db, dir,
    streamable_table = streamable_readr_tsv(),
    lines = 50000, tables = "flights", overwrite = TRUE,
    filter_statement = "WHERE month = 2"
  )

  r <- read.csv(paste0(dir, "/flights.tsv.bz2"), sep = "\t")
  expect_true(all(r$month == 2))
  expect_true(nrow(r) == nrow(nycflights13::flights[nycflights13::flights$month == 2, ]))
})




test_that("e2e with filter for flights month = 12: parquet", {
  # summary(factor(nycflights13::flights$month)) filter month = 12, 28132 records

  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("arrow")
  skip_on_os("solaris")
  
  # clean up previous test
  # unlink(paste0(dir, "/flights"), TRUE)


  expect_warning(ark(db, dir,
    streamable_table = streamable_parquet(),
    lines = 50000, tables = "flights", overwrite = TRUE,
    filter_statement = "WHERE month = 12"
  ), "Parquet is already compressed")

  r <- arrow::read_parquet(paste0(dir, "/flights/part-00001.parquet"))
  expect_true(all(r$month == 12))
  expect_true(nrow(r) == nrow(nycflights13::flights[nycflights13::flights$month == 12, ]))

  # clean up previous test
  # unlink(paste0(dir, "/flights"), TRUE)

  ark(db, dir,
    streamable_table = streamable_parquet(),
    lines = 50000, tables = "flights", overwrite = TRUE,
    filter_statement = "WHERE month = 12", method = "window", compress = "none"
  )

  r <- arrow::read_parquet(paste0(dir, "/flights/part-00001.parquet"))
  expect_true(all(r$month == 12))
  expect_true(nrow(r) == nrow(nycflights13::flights[nycflights13::flights$month == 12, ]))
})

test_that("Errors on window-parallel and not streamable parquet", {
  expect_error(
    ark(db = function(x) return(new_db), dir, tables = "fake_table", method = "window-parallel"), "only compatible with parquet"
  )
})


test_that("Warns when applying filter to multiple tables", {
  
  skip_on_os("solaris")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_if_not_installed("arrow")
  
  
  # clean up previous test
  #unlink(paste0(dir, "/flights"), TRUE)
  #unlink(paste0(dir, "/weather"), TRUE)
  expect_warning(
    ark(db, dir,
      streamable_table = streamable_parquet(),
      lines = 50000, tables = c("flights", "weather"), overwrite = TRUE,
      filter_statement = "WHERE month = 12", compress = "none"
    ),
    "Your filter statement will be applied to all tables"
  )

  r <- arrow::read_parquet(paste0(dir, "/weather/part-00001.parquet"))
  expect_true(all(r$month == 12))
  expect_true(nrow(r) == nrow(nycflights13::weather[
    nycflights13::weather$month == 12,
  ]))
})


context("callbacks")

test_that("ark with keep-open and callback", {
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_if_not_installed("arrow")
  skip_on_os("solaris")
  
  # Callback to convert arr_delay in the flights
  # data from minutes to hours
  callback <- function(data) {
    data$arr_delay <- as.numeric(data$arr_delay / 60)
    data
  }

  suppressWarnings(
    ark(db, dir, lines = 50000, tables = "flights", method = "keep-open", overwrite = TRUE, callback = callback)
  )


  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )

  expect_equal(myflights$arr_delay, nycflights13::flights$arr_delay / 60)
})




test_that("ark with window and callback", {
  
  
  skip_if_not_installed("dplyr")
  skip_if_not_installed("nycflights13")
  skip_if_not_installed("readr")
  skip_on_os("solaris")
  
  # Callback to convert arr_delay in the flights
  # data from minutes to hours
  callback <- function(data) {
    data$arr_delay <- as.numeric(data$arr_delay / 60)
    data
  }

  suppressWarnings(
    ark(db, dir, lines = 50000, tables = "flights", method = "window", overwrite = TRUE, callback = callback)
  )


  suppressWarnings(
    myflights <- readr::read_tsv(file.path(dir, "flights.tsv.bz2"))
  )
  expect_equal(
    dim(myflights),
    dim(nycflights13::flights)
  )

  expect_equal(myflights$arr_delay, nycflights13::flights$arr_delay / 60)
})


disconnect(db)
disconnect(new_db)
unlink(dir, recursive = TRUE) # ark'd text/parquet files
