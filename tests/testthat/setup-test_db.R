
## Optionally: Force a backend type, otherwise will use the best available
# Sys.setenv(ARKDB_DRIVER="MonetDBLite")
# Sys.setenv(ARKDB_DRIVER="RSQLite")
# Sys.setenv(ARKDB_DRIVER="duckdb")

library(arkdb)

## All tests only write to tempdir
test_db <- file.path(tempdir(), "arkdb")
Sys.setenv(ARKDB_HOME = test_db)

print(paste("Testing using backend", class(local_db())))

disconnect <- function(db) {
  ## Cleanup
  if (inherits(db, "DBIConnection")) {
    DBI::dbDisconnect(db, shutdown = TRUE)
  } else {
    DBI::dbDisconnect(db$con, shutdown = TRUE)
  }
}
