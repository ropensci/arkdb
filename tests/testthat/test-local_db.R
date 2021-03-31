context("local_db")


test_that("local_db works with default provider",{
  db <- local_db()
  expect_is(db, "DBIConnection")
  local_db_disconnect(db)
})

test_that("local_db works readonly",{
  db <- local_db(readonly = TRUE)
  expect_is(db, "DBIConnection")
  local_db_disconnect(db)
  
  ## and in a non-existant location
  expect_message({
  db <- local_db(tempfile(), readonly = TRUE)
  })
  expect_is(db, "DBIConnection")
  local_db_disconnect(db)
  
})

test_that("local_db works with MonetDBLite", {
  
  skip_if_not_installed("MonetDBLite")
  skip_on_cran() # some CRAN engines appear not to obey skip_if_not_installed

  ## Make sure any previous monetDB use is also shut down
  MonetDBLite::monetdblite_shutdown()
  
  db <- local_db(driver = "MonetDBLite")
  expect_is(db, "DBIConnection")
  expect_is(db, "MonetDBEmbeddedConnection")
  
  local_db_disconnect(db)
  
  })


test_that("local_db works with RSQLite", {
  
  db <- local_db(driver = "RSQLite")
  expect_is(db, "DBIConnection")
  expect_is(db, "SQLiteConnection")
  local_db_disconnect(db)
  
  })

test_that("local_db works with duckdb", {
  
  skip_if_not_installed("duckdb")
  
  
  db <- local_db(tempfile(), driver = "duckdb")
  expect_is(db, "DBIConnection")
  expect_is(db, "duckdb_connection")
  local_db_disconnect(db)
  
})

test_that("Caching local db works", {
  
  # default
  db1 <- local_db(cache_connection = TRUE)
  db2 <- local_db(cache_connection = TRUE)
  expect_identical(db1, db2)
  
  db3 <- local_db(cache_connection = FALSE)
  expect_false(identical(db1, db3))
})

