context("test-errors")


skip_if_not_installed("dbplyr")
skip_if_not_installed("nycflights13")
skip_if_not_installed("RSQLite")
skip_if_not_installed("duckdb")

test_that("we can detect errors on types", {
  x <- NULL
  #  testthat::expect_warning(assert_files_exist("not-a-file"), "not found")
  #  testthat::expect_warning(assert_files_exist(
  #    c("not-a-file", "not")), "not found")
  #  testthat::expect_warning(assert_files_exist(x), "no file specified")

  testthat::expect_error(assert_dir_exists("not-a-dir"), "not found")
  testthat::expect_error(assert_dbi(x), "DBI")
  testthat::expect_error(assert_connection(x), "connection")
  testthat::expect_error(assert_streamable(x), "streamable")
})

testthat::test_that("we can handle cases overwriting a file, with a warning", {
  
  filename <- tempfile(fileext = ".txt")
  skip_if_not(dir.exists(dirname(filename)))
  data <- datasets::iris

  testthat::expect_silent(assert_overwrite(filename, TRUE))
  write.table(data, filename)
  testthat::expect_warning(assert_overwrite(filename, TRUE), basename(filename))

  unlink(filename)
})




testthat::test_that("we can handle cases overwriting a table, with a warning", {

  skip_if_not_installed("datasets")
  skip_if_not_installed("fs")
  skip_if_not_installed("RSQLite")
  
  data <- datasets::iris
  dir <- tempdir()

  dbdir <- fs::path(dir, "local.sqlite")
  db <- arkdb::local_db(dbdir)
  tbl <- "iris"

  testthat::expect_silent(assert_overwrite_db(db, tbl, FALSE))

  DBI::dbWriteTable(db, tbl, data)
  testthat::expect_warning(assert_overwrite_db(db, tbl, TRUE), tbl)
  # testthat::expect_message(assert_overwrite_db(db, tbl, FALSE), tbl)


  DBI::dbDisconnect(db)
})
