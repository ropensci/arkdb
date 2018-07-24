
#' Archive tables from a database as flat files
#' 
#' @param db_con a database connection
#' @param dir a directory where we will write the compressed text files output
#' @param lines the number of lines to use in each single chunk
#' @param compress file compression algorithm. Should be one of "bzip2" (default),
#' "gzip" (faster write times, a bit less compression), "xz", or "none", for
#' no compression.
#' @param tables a list of tables from the database that should be 
#' archived.  By default, will archive all tables. 
#' @details `ark` will archive tables from a database as (compressed) tsv files.
#' `ark` does this by reading only chunks at a time into memory, allowing it to
#' process tables that would be too large to read into memory all at once (which
#' is probably why you are using a database in the first place!)  Compressed
#' text files will likely take up much less space, making them easier to store and
#' transfer over networks.  Compressed plain-text files are also more archival
#' friendly, as they rely on widely available and long-established open source compression
#' algorithms and plain text, making them less vulnerable to loss by changes in 
#' database technology and formats. 
#' 
#' @importFrom DBI dbListTables
#' @export
#' @return the path to `dir` where output files are created (invisibly), for piping.
#' @examples 
#' \donttest{
#' # setup
#' library(dplyr)
#' dir <- tempdir() 
#' db <- dbplyr::nycflights13_sqlite(tempdir())
#' 
#' ## And here we go:
#' ark(db, dir)
#' 
#' 
#' }
ark <- function(db_con, dir, lines = 10000L, 
                compress = c("bzip2", "gzip", "xz", "none"),
                tables = list_tables(db_con)){
  
  compress <- match.arg(compress)
  
  tables <- tables[!grepl("sqlite_", tables)]
  
  lapply(tables, 
         ark_file, 
         db_con = normalize_con(db_con), 
         lines = lines, 
         dir = dir, 
         compress = compress)
  
  invisible(dir)
}

list_tables <- function(db){
  db <- normalize_con(db)
  DBI::dbListTables(db)
}

#' @importFrom DBI dbSendQuery dbFetch dbClearResult dbGetQuery
ark_file <- function(tablename, 
                     db_con, 
                     lines = 10000L, 
                     dir = ".", 
                     compress = c("bzip2", "gzip", "xz", "none")){
  
  ## Set up compressed connection
  compress <- match.arg(compress)
  ext <- switch(compress,
                "bzip2" = ".bz2",
                "gzip" = ".gz",
                "xz" = ".xz",
                "none" = "",
                ".bz2")
  filename <- file.path(dir, paste0(tablename, ".tsv", ext))
  con <- compressed_file(filename, "w")
  on.exit(close(con))
  
  ## Progress reporting
  message(sprintf("Exporting %s in %d line chunks:", tablename, lines))
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  t0 <- Sys.time()
 
  if(FALSE){
    alternate_method(db_con, lines, dir, compress)
    return(invisible(TRUE))
  }
  
  
  ## Create header to avoid duplicate column names
  query <- paste("SELECT * FROM", tablename, "LIMIT 0")
  header <- DBI::dbGetQuery(db_con, query)
  readr::write_tsv(header, con, append = FALSE)

  ## 
  res <- DBI::dbSendQuery(db_con, paste("SELECT * FROM", tablename))
  while (TRUE) {
    p$tick()
    data <- dbFetch(res, n = lines)
    if (nrow(data) == 0) break
    readr::write_tsv(data, con, append = TRUE)
  }
  DBI::dbClearResult(res)
  
  message(sprintf("\t...Done! (in %s)", format(Sys.time() - t0)))
  
}


## Fallback method, If a dbSendQuery() immediately transfers every thing to the
## client, the below solution works better. But we're not aware of DBI backends
## that do that.  This may later be deprecated.

alternate_method <- function(db_con, lines, dir, compress){
  size <- DBI::dbGetQuery(db_con, paste("SELECT COUNT(*) FROM", tablename))
  end <- size[[1]][[1]]
  start <- 1
  repeat {
    p$tick()
    ## Do stuff
    ark_chunk(db_con, tablename, start = start, 
              lines = lines, dir = dir, compress = compress)
    start <- start + 1  
    if ( (start - 1)*lines > end) {
      break
    }
  }
  message(sprintf("\t...Done! (in %s)", format(Sys.time() - t0)))
  
}
  
#' @importFrom readr write_tsv  
ark_chunk <- function(db_con, tablename, start = 1, 
                      lines = 10000L, dir = ".", 
                      compress  = c("bzip2", "gzip", "xz", "none")){
  
  compress <- match.arg(compress)
  
  
  if (has_between()) {
    ## Windowed queries are faster but not universally supported
    query <- paste("SELECT * FROM", 
                   tablename, 
                   "WHERE rownum BETWEEN",
                   (start - 1) * lines, 
                   "AND", 
                   start * lines)    
  } else { 
    ## Any SQL DB can do offset
    query <- paste("SELECT * FROM", tablename, "LIMIT", 
                   lines, "OFFSET", (start-1)*lines)

  }
  chunk <- DBI::dbGetQuery(db_con, query)
  
  append <- start != 1
  
  ext <- switch(compress,
                "bzip2" = ".bz2",
                "gzip" = ".gz",
                "xz" = ".xz",
                "none" = "",
                ".bz2")
  
  readr::write_tsv(chunk, 
                   file.path(dir, paste0(tablename, ".tsv", ext)), 
                   append = append)

}

arkdb_cache <- new.env()
has_between <- function(db_con){
  cache <- mget("db_supports_between", 
                ifnotfound = list(db_supports_between = NA), 
                envir = arkdb_cache)
  if(is.na(cache[[1]])){
    db_supports_between <- 
      tryCatch(DBI::dbGetQuery(db_con, 
               "SELECT * FROM table WHERE rownum BETWEEN 1 and 2"), 
               error = function(e) FALSE, 
               finally = TRUE)
    
    assign("db_supports_between", db_supports_between, envir = arkdb_cache)
    db_supports_between
  } else {
    cache[["db_supports_between"]]
  }
}

