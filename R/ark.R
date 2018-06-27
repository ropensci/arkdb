
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
#' @importFrom methods is
#' @export
#' 
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
                tables = DBI::dbListTables(db_con$con)){
  compress <- match.arg(compress)
  
  #tables <- DBI::dbListTables(db_con$con)
  tables <- tables[!grepl("sqlite_", tables)]
  
  lapply(tables, ark_file, 
         db_con = db_con, lines = lines, 
         dir = dir, compress = compress)
  
  invisible(dir)
}


#' @importFrom dplyr collect summarise tbl n
ark_file <- function(tablename, 
                     db_con, 
                     lines = 10000L, 
                     dir = ".", 
                     compress = c("bzip2", "gzip", "xz", "none")){
  
  compress <- match.arg(compress)
  d <- dplyr::tbl(db_con, tablename)
  n <- dplyr::summarise(d, n())
  end <- dplyr::collect(n)[[1]] 
  
  start <- 1
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  message(sprintf("Importing in %d line chunks:\n%s",
                  lines, tablename))
  t0 <- Sys.time()
  repeat {
    p$tick()
    ## Do stuff
    ark_chunk(db_con, tablename, start = start, 
              lines = lines, dir = dir, compress = compress)
    start <- start+lines  
    if (start > end) {
      break
    }
  }
  message(sprintf("...Done! (in %s)", format(Sys.time() - t0)))
}
  
#' @importFrom readr write_tsv  
#' @importFrom dplyr filter between row_number sql collect tbl  
ark_chunk <- function(db_con, tablename, start = 1, 
                      lines = 10000L, dir = ".", 
                      compress  = c("bzip2", "gzip", "xz", "none")){
  
  compress <- match.arg(compress)
  
  
  if (is(db_con$con, "SQLiteConnection") |
      is(db_con$con, "MySQLConnection") |
      Sys.getenv("arkdb_windowing") == "FALSE") {
    query <- paste("SELECT * FROM", tablename, "LIMIT", 
                   lines, "OFFSET", (start-1)*lines)
    chunk <- dplyr::collect( dplyr::tbl(db_con, dplyr::sql(query)) )
  } else {
  ## Postgres can do windowing
    chunk <- collect(filter(tbl(db_con,tablename), 
              between(row_number(), start, start+lines)))
  }
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
