
## Optionally: Force a backend type, otherwise will use the best available
## (which is currently MonetDBLite, since it is suggested)
#Sys.setenv(ARKDB_DRIVER="MonetDBLite")
#Sys.setenv(ARKDB_DRIVER="RSQLite")
#Sys.setenv(ARKDB_DRIVER="duckdb")

library(arkdb)

## All tests only write to tempdir
test_db <- file.path(tempdir(), "arkdb")
Sys.setenv(ARKDB_HOME=test_db)

print(paste("Testing using backend", class(local_db())))

