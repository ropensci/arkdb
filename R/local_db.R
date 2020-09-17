#' Connect to a local stand-alone database
#'
#' This function will provide a connection to the best available database.
#' This function is a drop-in replacement for `[DBI::dbConnect]` with behaviour
#' that makes it more subtle for R packages that need a database backend with
#' minimal complexity, as described in details. 
#' 
#' @details This function provides several abstractions to `[DBI::dbConnect]` to 
#' provide a seamless backend for use inside other R packages.  
#' 
#' First, this  provides a generic method that allows the use of a `[RSQLite::SQLite]``
#' connection if nothing else is available, while being able to automatically
#' select a much faster, more powerful backend from `[duckdb::duckdb]` 
#' if available.  An argument or environmental variable can be used to override this
#' to manually set a database endpoint for testing purposes. 
#' 
#' Second, this function will cache the database connection in an R environment and 
#' load that cache.  That means you can call `local_db()` as fast/frequently as you 
#' like without causing errors that would occur by rapid calls to `[DBI::dbConnect]`
#' 
#' Third, this function defaults to persistent storage location set by `[rappdirs::user_data_dir]`
#' and configurable by setting the environmental variable `ARKDB_HOME`.  This allows 
#' a package to provide persistent storage out-of-the-box, and easily switch that storage
#' to a temporary directory (e.g. for testing purposes, or custom user configuration) without
#' having to edit database calls directly.  
#'  
#' @param dbdir Path to the database.
#' @param driver Default driver, one of "duckdb", "MonetDBLite", "RSQLite".
#'   It will select the first one of those it finds available if a
#'   driver is not set. This fallback can be overwritten either by explicit
#'   argument or by setting the environmental variable `ARKDB_DRIVER`.
#' @param readonly Should the database be opened read-only? (duckdb only).
#'  This allows multiple concurrent connections (e.g. from different R sessions)
#' @return Returns a `[DBIcoonection]` connection to the default duckdb database
#'
#' @importFrom DBI dbConnect dbIsValid
# @importFrom duckdb duckdb
#' @export
#' @examples \donttest{
#' ## OPTIONAL: you can first set an alternative home location,
#' ## such as a temporary directory:
#' Sys.setenv(ARKDB_HOME=tempdir())
#'
#' ## Connect to the database:
#' db <- local_db()
#'
#' }
local_db <- function(dbdir = arkdb_dir(),
                     driver = Sys.getenv("ARKDB_DRIVER"),
                     readonly = FALSE){

  dbname <- file.path(dbdir, "database")
  db <- mget("ark_db", envir = arkdb_cache, ifnotfound = NA)[[1]]
  if (inherits(db, "DBIConnection")) {
    if (DBI::dbIsValid(db)) {
      return(db)
    }
  }

  dir.create(dbname, showWarnings = FALSE, recursive = TRUE)

  db <- db_driver(dbname, driver)
  assign("ark_db", db, envir = arkdb_cache)
  db
}



db_driver <- function(dbname, 
                      driver = Sys.getenv("ARKDB_DRIVER"), 
                      readonly = FALSE){

  ## Evaluate capabilities in reverse-priority order
  drivers <- "dplyr"

  if (requireNamespace("RSQLite", quietly = TRUE)){
    SQLite <- getExportedValue("RSQLite", "SQLite")
    drivers <- c("RSQLite", drivers)
  }
  
#  if (requireNamespace("MonetDBLite", quietly = TRUE)){
#    MonetDBLite <- getExportedValue("MonetDBLite", "MonetDBLite")
#    drivers <- c("MonetDBLite", drivers)
#  }

  if (requireNamespace("duckdb", quietly = TRUE)){
    duckdb <- getExportedValue("duckdb", "duckdb")
    drivers <- c("duckdb", drivers)
  }

  ## If driver is undefined or not in available list, use first from the list
  if (  !(driver %in% drivers) ) driver <- drivers[[1]]

  
  dir.create(dbname, showWarnings = FALSE, recursive = TRUE)
  
  
  
  db <- switch(driver,
         duckdb = DBI::dbConnect(duckdb(),
                                 dbdir = file.path(dbname,"duckdb"),
                                 read_only = readonly),
#         MonetDBLite = monetdblite_connect(file.path(dbname,"MonetDBLite")),
         RSQLite = DBI::dbConnect(SQLite(),
                                  file.path(dbname, "sqlite.sqlite")),
         dplyr = NULL,
         NULL)
}




#' Disconnect from the arkdb database.
#'
#' @param db a DBI connection. By default, will call [local_db] for the default connection.
#' @param env The environment where the function looks for a connection. 
#' @details This function manually closes a connection to the `arkdb` database.
#'
#' @importFrom DBI dbConnect dbIsValid
# @importFrom duckdb duckdb
#' @export
#' @examples \donttest{
#'
#' ## Disconnect from the database:
#' local_db_disconnect()
#'
#' }

local_db_disconnect <- function(db = local_db(), env = arkdb_cache){
  if (inherits(db, "DBIConnection")) {
    suppressWarnings({
      if(inherits(db, "MonetDBEmbeddedConnection")) 
        DBI::dbDisconnect(db, shutdown=TRUE)
      else
        DBI::dbDisconnect(db)

    })
  }
  if(exists("ark_db", envir =env)){
    rm("ark_db", envir = env)
  }
}

## Enironment to store the cached copy of the connection
## and a finalizer to close the connection on exit.
arkdb_cache <- new.env()
reg.finalizer(arkdb_cache, local_db_disconnect, onexit = TRUE)


arkdb_dir <- function(){
  Sys.getenv("ARKDB_HOME",  rappdirs::user_data_dir("arkdb"))
}
