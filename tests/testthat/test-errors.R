testthat::context("test-errors")

testthat::test_that("we can detect errors on types", {
  
  x <- NULL
  testthat::expect_warning(assert_files_exist("not-a-file"), "not found")
  testthat::expect_warning(assert_files_exist(c("not-a-file", "not")), "not found")
  testthat::expect_warning(assert_files_exist(x), "no file specified")
  
  testthat::expect_error(assert_dir_exists("not-a-dir"), "not found")
  testthat::expect_error(assert_dbi(x), "DBI")
  testthat::expect_error(assert_connection(x), "connection")
  testthat::expect_error(assert_streamable(x), "streamable")
  
})

testthat::test_that("we can handle cases overwriting a file, with a warning",{
  
  
  filename <- tempfile(fileext = ".txt")
  data <- datasets::iris
  
  testthat::expect_silent(assert_overwrite(filename))
  write.table(data, filename)
  if(!interactive()){
    testthat::expect_warning(assert_overwrite(filename), filename)
  }
  
  unlink(filename)
  
})




testthat::test_that("we can handle cases overwriting a table, with a warning",{
  dir <- tempdir()
  data <- datasets::iris
  db <- dplyr::src_sqlite(file.path(dir, "local.sqlite"), create=TRUE)
  tbl <- "iris"
  
  testthat::expect_silent(assert_overwrite_db(db, tbl))
  
  DBI::dbWriteTable(db$con, tbl, data)
  
  if(!interactive()){
    testthat::expect_warning(assert_overwrite_db(db, tbl), tbl)
  }
  
  DBI::dbDisconnect(db)
  
})