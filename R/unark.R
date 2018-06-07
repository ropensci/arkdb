
# Adapted from @richfitz, MIT licensed
# https://github.com/vimc/montagu-r/blob/4fe82fd29992635b30e637d5412312b0c5e3e38f/R/util.R#L48-L60




## FIXMEs: 
## 1. consider pluggable system for flat files, and 
## 2. compression format. (currently assumes readr_tsv w/ bz2 compression)
## 3. consider taking a directory instead of a filename and looping over all


#' Unarchive a list of `*.tsv.bz2` files into a databaase
#' @param files vector of filenames to be read in. Must be `*.tsv.bz2` format at present.
#' @param lines number of lines to read in a chunk.
#' @inheritParams DBI::dbConnect
#' @importFrom dbplyr src_dbi
#' @importFrom DBI dbConnect
#' @importFrom RSQLite SQLite
#' 
#' @return a database connection (invisibly)
unark <- function(files, lines = 10000L, drv = RSQLite::SQLite(), ...){
  db_con <- dbplyr::src_dbi(DBI::dbConnect(drv, ...))
  lapply(files, function(f) unark_file(f, db_con, lines = lines))
  invisible(db_con)  
}


#' Unarchive a single tsv file into an existing database
#' 
#' @param filename a *.tsv.bz2 file to uncompress
#' @param db a database src (`src_dbi` object from `dplyr`)
#' @param lines number of lines to read in a chunk. 
#' 
#' @return the database connection (`src_dbi`, invisibly)
#' 
#' @details `unark_file` will read in a file in chunks and 
#' write them into a database.  This is essential for processing
#' large compressed tables which may be too large to read into
#' memory before writing into a database.  In general, increasing
#' the `lines` parameter will result in a faster total transfer
#' but require more free memory for working with these larger chunks.
#' 
#' @importFrom readr read_tsv
#' @importFrom DBI dbWriteTable
#' @importFrom progress progress_bar
#' @export
#' @examples \donttest{
#' 
#' ## set up example files and database
#' tsv <- tempfile("flights", fileext=".tsv.bz2")
#' sqlite <- tempfile("nycflights", fileext=".sql")
#' readr::write_tsv(nycflights13::flights, tsv)
#' db <- src_sqlite(sqlite, create = TRUE)
#' 
#' ## and here we go:
#' db_con <- unark_file(tsv, db)
#' db_con
#' 
#' unlink(tsv)
#' unlink(sql)
#' }
#' 
unark_file <- function(filename, db_con, lines = 10000L){
    
  tbl_name <- base_name(filename)
    
  ## guess connection, don't assume bz2!
  con <- bzfile(filename, "r")
  #con <- file(filename, "r")
  on.exit(close(con))
  
  header <- readLines(con, n = 1L)
  reader <- read_chunked(con, lines)
  
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  message(sprintf("Importing in %d line chunks:\n%s",
                  lines, filename))
  t0 <- Sys.time()
  repeat {
    d <- reader()
    body <- paste0(c(header, d$data), "\n", collapse = "")
    p$tick()
    chunk <- readr::read_tsv(body)
    DBI::dbWriteTable(db_con[[1]], tbl_name, chunk, append=TRUE)
    
    if (d$complete) {
      break
    }
  }
  message(sprintf("...Done! (in %s)", format(Sys.time() - t0)))
  
  #DBI::dbDisconnect(db_con)
  
  invisible(db_con)
}


assert_connection <- function(x, name = deparse(substitute(x))) {
  if (!inherits(x, "connection")) {
    stop(sprintf("'%s' must be a connection object", name), call. = FALSE)
  }
}


read_chunked <- function(con, n) {
  assert_connection(con)
  next_chunk <- readLines(con, n)
  if (length(next_chunk) == 0L) {
    stop("connection has already been completely read")
  }
  function() {
    data <- next_chunk
    next_chunk <<- readLines(con, n)
    complete <- length(next_chunk) == 0L
    list(data = data, complete = complete)
  }
}


## Do repeatedly to remove compression extension and file extension
base_name <- function(filename){
  path <- basename(filename)
  ext_regex <- "(?<!^|[.])[.][^.]+$"
  path <- sub(ext_regex, "", path, perl = TRUE)
  path <- sub(ext_regex, "", path, perl = TRUE)
  sub(ext_regex, "", path, perl = TRUE)
}