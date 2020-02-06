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
#' select a much faster, more powerful engine like `[duckdb::duckdb]` or `[MonetDBLite::MonetDBLite]`
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
#' @return Returns a `src_dbi` connection to the default duckdb database
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
                       driver = Sys.getenv("ARKDB_DRIVER")){

  dbname <- file.path(dbdir, "database")
  db <- mget("td_db", envir = arkdb_cache, ifnotfound = NA)[[1]]
  if (inherits(db, "DBIConnection")) {
    if (DBI::dbIsValid(db)) {
      return(db)
    }
  }

  dir.create(dbname, showWarnings = FALSE, recursive = TRUE)

  db <- db_driver(dbname, driver)
  #db <- monetdblite_connect(dbname)
  assign("td_db", db, envir = arkdb_cache)
  db
}

db_driver <- function(dbname, driver = Sys.getenv("ARKDB_DRIVER")){

  ## Evaluate capabilities in reverse-priorty order
  drivers <- "dplyr"

  if (requireNamespace("RSQLite", quietly = TRUE)){
    SQLite <- getExportedValue("RSQLite", "SQLite")
    drivers <- c("RSQLite", drivers)
  }
  if (requireNamespace("MonetDBLite", quietly = TRUE)){
    MonetDBLite <- getExportedValue("MonetDBLite", "MonetDBLite")
    drivers <- c("MonetDBLite", drivers)
  }
  ## duckdb lacks necessary stability
  ## https://github.com/cwida/duckdb/issues/58
  if (requireNamespace("duckdb", quietly = TRUE)){
    duckdb <- getExportedValue("duckdb", "duckdb")
    drivers <- c("duckdb", drivers)
  }

  ## If driver is undefined or not in available list, use first from the list
  if (  !(driver %in% drivers) ) driver <- drivers[[1]]


  db <- switch(driver,
         duckdb = DBI::dbConnect(duckdb( dbdir = file.path(dbname,"duckdb")),
                                 dbname = file.path(dbname,"duckdb")),
         MonetDBLite = monetdblite_connect(file.path(dbname,"MonetDBLite")),
         RSQLite = DBI::dbConnect(SQLite(),
                                  file.path(dbname, "sqlite.sqlite")),
         dplyr = NULL,
         NULL)
}




# Provide an error handler for connecting to monetdblite if locked
# by another session
# @importFrom MonetDBLite MonetDBLite
monetdblite_connect <- function(dbname, ignore_lock = TRUE){


  if (requireNamespace("MonetDBLite", quietly = TRUE))
    MonetDBLite <- getExportedValue("MonetDBLite", "MonetDBLite")

  db <- tryCatch({
    if (ignore_lock) unlink(file.path(dbname, ".gdk_lock"))
    DBI::dbConnect(MonetDBLite(), dbname = dbname)
    },
    error = function(e){
      if(grepl("Database lock", e))
        stop(paste("Local arkdb database is locked by another R session.\n",
                   "Try closing that session first or set the arkdb_HOME\n",
                   "environmental variable to a new location.\n"),
             call. = FALSE)
      else stop(e)
    },
    finally = NULL
  )
  db
}

#' Disconnect from the arkdb database.
#'
#' @param env The environment where the function looks for a connection.
#' @details This function manually closes a connection to the `arkdb` database.
#'
#' @importFrom DBI dbConnect dbIsValid
# @importFrom duckdb duckdb
#' @export
#' @examples \donttest{
#'
#' ## Disconnect from the database:
#' td_disconnect()
#'
#' }

td_disconnect <- function(env = arkdb_cache){
  db <- mget("td_db", envir = env, ifnotfound = NA)[[1]]
  if (inherits(db, "DBIConnection")) {
    suppressWarnings(
    DBI::dbDisconnect(db)
    )
  }
}

## Enironment to store the cached copy of the connection
## and a finalizer to close the connection on exit.
arkdb_cache <- new.env()
reg.finalizer(arkdb_cache, td_disconnect, onexit = TRUE)


arkdb_dir <- function(){
  Sys.getenv("ARKDB_HOME",  rappdirs::user_data_dir("arkdb"))
}
