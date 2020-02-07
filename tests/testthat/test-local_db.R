context("local_db")


test_that("local_db works with default provider",{
  
  db <- local_db()
  expect_is(db, "DBIConnection")
  
  local_db_disconnect()
})

test_that("local_db works with MonetDBLite", {

  db <- local_db(driver = "MonetDBLite")
  expect_is(db, "DBIConnection")
  expect_is(db, "MonetDBEmbeddedConnection")
  
  local_db_disconnect()
  
  })


test_that("local_db works with RSQLite", {
  
  db <- local_db(driver = "RSQLite")
  expect_is(db, "DBIConnection")
  expect_is(db, "SQLiteConnection")
  local_db_disconnect()
  
  })

test_that("local_db works with duckdb", {
  
  db <- local_db(driver = "duckdb")
  expect_is(db, "DBIConnection")
  expect_is(db, "duckdb_connection")
  local_db_disconnect()
  
})

