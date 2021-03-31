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
#' Third, this function defaults to persistent storage location set by `[tools::R_user_dir]`
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
#' @param memory_limit Set a memory limit for duckdb, in GB.  This can
#' also be set for the session by using options, e.g. 
#' `options(duckdb_memory_limit=10)` for a limit of 10GB.  On most systems 
#' duckdb will automatically set a limit to 80% of machine capacity if not 
#' set explicitly.  
#' @param cache_connection should we preserve a cache of the connection? allows
#' faster load times and prevents connection from being garbage-collected.  However,
#' keeping open a read-write connection to duckdb or MonetDBLite will block access of
#' other R sessions to the database.  
#' @param ... additional arguments (not used at this time)
#' @return Returns a `[DBIconnection]` connection to the default database
#'
#' @importFrom DBI dbConnect dbIsValid
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
local_db <- function (dbdir = arkdb_dir(), 
                      driver = Sys.getenv("ARKDB_DRIVER", "duckdb"),
                      readonly = FALSE, 
                      cache_connection = TRUE,
                      memory_limit = getOption("duckdb_memory_limit", NA), 
                      ...) {
  
  if(driver == ""){
    message("driver not specified, trying duckdb")
    driver = "duckdb"
  }
  
  dbname <- dbdir
  read_only <- readonly
  
  if (!file.exists(dbname)){
    dir.create(dbname, FALSE, TRUE)
  }

  ## Cannot open read-only on a database that does not exist
  ## So we open it read-write, and close it.
  if (!file.exists(file.path(dbname, driver)) && read_only) {
    message(paste("initializing", driver, "at", dbname))
    db <- db_driver(dbname, driver = driver, read_only = FALSE)
    DBI::dbDisconnect(db, shutdown=TRUE)
  }
  
  db <- mget("arkdb_db", envir = arkdb_cache, ifnotfound = NA)[[1]]
  cached_driver <- mget("arkdb_driver", envir = arkdb_cache, ifnotfound = NA)[[1]]
  cached_dbname <- mget("arkdb_dbname", envir = arkdb_cache, ifnotfound = NA)[[1]]
  
  ## ONLY return cached connection if requested to, & driver and dir match!!
  if (inherits(db, "DBIConnection")) {
    if (DBI::dbIsValid(db)) {
      if (cache_connection & cached_dbname == dbname & cached_driver == driver) {
        return(db)
      } else {
        ## shut down the cached (read_only) connection first 
        ## so we can make a new connection with write privileges
        DBI::dbDisconnect(db, shutdown = TRUE)
      }
    }
  }
  db <- db_driver(dbname, driver = driver, read_only = read_only)
  
  if(!is.na(memory_limit) && driver =="duckdb"){
    pragma <- paste0("PRAGMA memory_limit='", memory_limit, "GB'")
    DBI::dbExecute(db, pragma)
  }
  
  ## Only cache read-only connections
  if (cache_connection) {
    assign("arkdb_db", db, envir = arkdb_cache)
    assign("arkdb_driver", driver, envir = arkdb_cache)
    assign("arkdb_dbname", dbname, envir = arkdb_cache)
    
  }
  
  db
}


#' delete the local arkdb database
#' 
#' @param db_dir neon database location 
#' @param ask Ask for confirmation first?
#' @details Just a helper function that deletes the database
#' files.  Usually unnecessary but can be
#' helpful in resetting a corrupt database.  
#' 
#' @importFrom utils askYesNo
#' @export
#' @examples 
#' 
#' # Create a db
#' dir <- tempfile()
#' db <- local_db(dir)
#' 
#' # Delete it
#' arkdb_delete_db(dir, ask = FALSE)
#' 
#' 
arkdb_delete_db <- function(db_dir = arkdb_dir(), ask = interactive()){
  continue <- TRUE
  if(ask){
    continue <- utils::askYesNo(paste("Delete local DB in", db_dir, "?"))
  }
  if(continue){
    db_files <- list.files(db_dir, "^database.*", full.names = TRUE)
    lapply(db_files, unlink, TRUE)
  }
  purge_cache()
  
  return(invisible(continue))
}


purge_cache <- function(){
  if (exists("arkdb_db", envir = arkdb_cache)) {
    suppressWarnings({
      rm("arkdb_db", envir = arkdb_cache)
      rm("arkdb_driver", envir = arkdb_cache)
      rm("arkdb_dbname", envir = arkdb_cache)
    })
  }
}


db_driver <- function(dbname, 
                      driver = Sys.getenv("ARKDB_DRIVER"), 
                      read_only = FALSE){
  
  ## Evaluate capabilities in reverse-priority order
  
  drivers <- NULL
  if (requireNamespace("dplyr", quietly = TRUE)){
    drivers <- "dplyr"
  }
  
  if (requireNamespace("RSQLite", quietly = TRUE)){
    SQLite <- getExportedValue("RSQLite", "SQLite")
    drivers <- c("RSQLite", drivers)
  }
  
  if (requireNamespace("MonetDBLite", quietly = TRUE)){
    MonetDBLite <- getExportedValue("MonetDBLite", "MonetDBLite")
    drivers <- c("MonetDBLite", drivers)
  }
  
  if (requireNamespace("duckdb", quietly = TRUE)){
    duckdb <- getExportedValue("duckdb", "duckdb")
    drivers <- c("duckdb", drivers)
  }
  
  if(is.null(drivers)) stop("No drivers found. see ?arkdb::local_db")
  
  ## If driver is undefined or not in available list, use first from the list
  if (  !(driver %in% drivers) ) driver <- drivers[[1]]
  
  db <- switch(driver,
               duckdb = DBI::dbConnect(duckdb(),
                                       file.path(dbname, driver),
                                       read_only = read_only),
               MonetDBLite = monetdblite_connect(file.path(dbname, driver)),
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
      if(inherits(db, "MonetDBEmbeddedConnection") || 
         inherits(db, "duckdb_connection")) 
        DBI::dbDisconnect(db, shutdown=TRUE)
      else
        DBI::dbDisconnect(db)
      
    })
  }
  purge_cache()
}

## Environment to store the cached copy of the connection
## and a finalizer to close the connection on exit.
arkdb_cache <- new.env()
reg.finalizer(arkdb_cache, local_db_disconnect, onexit = TRUE)


arkdb_dir <- function(){
  Sys.getenv("ARKDB_HOME",  tools::R_user_dir("arkdb"))
}




