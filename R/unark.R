

#' Unarchive a list of compressed tsv files into a database
#' @param files vector of filenames to be read in. Must be `tsv`
#' format, optionally compressed using `bzip2`, `gzip`, `zip`,
#' or `xz` format at present.
#' @param db_con a database src (`src_dbi` object from `dplyr`)
#' @param lines number of lines to read in a chunk.
#' @inheritDotParams readr::read_tsv
#' @details `unark` will read in a files in chunks and 
#' write them into a database.  This is essential for processing
#' large compressed tables which may be too large to read into
#' memory before writing into a database.  In general, increasing
#' the `lines` parameter will result in a faster total transfer
#' but require more free memory for working with these larger chunks.
#' #' 
#' @return the database connection (invisibly)
#' 
#' @examples \donttest{
#' ## Setup: create an archive.
#' library(dplyr)
#' dir <- tempdir() 
#' db <- dbplyr::nycflights13_sqlite(tempdir())
#' 
#' ## database -> .tsv.bz2 
#' ark(db, dir)
#' 
#' ## list all files in archive (full paths)
#' files <- list.files(dir, "[.]tsv\\.bz2$", full.names = TRUE)
#' 
#' ## Read archived files into a new database (another sqlite in this case)
#' new_db <- src_sqlite(file.path(dir, "local.sqlite"), create=TRUE)
#' unark(files, new_db)
#' 
#' ## Prove table is returned successfully.
#' tbl(new_db, "flights")
#' 
#' }
#' @export
unark <- function(files, db_con, streamable_table = streamable_readr_tsv(),
                  lines = 10000L, ...){
  db <- normalize_con(db_con)
  lapply(files, unark_file, db, streamable_table, lines = lines, ...)
  invisible(db_con)  
}

normalize_con <- function(db_con){
  ## Handle both dplyr and DBI style connections
  ## Return whichever one we are given.
  if(is(db_con, "src_dbi")){
    db_con$con
  } else {
    db_con
  }
}


#' @importFrom DBI dbWriteTable
#' @importFrom progress progress_bar
unark_file <- function(filename, db_con, streamable_table, lines = 10000L, ...){
    
  tbl_name <- base_name(filename)
  con <- compressed_file(filename, "r")
  on.exit(close(con))

  ## Handle case of col_names != TRUE ?
  ## What about skips and comments?
  header <- readLines(con, n = 1L)
  if(length(header) == 0){ # empty file, would throw error
    return(invisible(db_con))
  }
  reader <- read_chunked(con, lines)

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  
  # May throw an error if we need to read more than 'total' chunks?
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  message(sprintf("Importing in %d line chunks:\n%s",
                  lines, filename))
  t0 <- Sys.time()
  repeat {
    d <- reader()
    body <- paste0(c(header, d$data), "\n", collapse = "")
    p$tick()

    writeLines(body, tmp)
    chunk <- streamable_table$read(tmp, ...)
    DBI::dbWriteTable(db_con, tbl_name, chunk, append=TRUE)
    
    if (d$complete) {
      break
    }
  }
  message(sprintf("...Done! (in %s)", format(Sys.time() - t0)))
  
  invisible(db_con)
}


# Adapted from @richfitz, MIT licensed
# https://github.com/vimc/montagu-r/blob/4fe82fd29992635b30e637d5412312b0c5e3e38f/R/util.R#L48-L60



assert_connection <- function(x, name = deparse(substitute(x))) {
  if (!inherits(x, "connection")) {
    stop(sprintf("'%s' must be a connection object", name), call. = FALSE)
  }
}


read_chunked <- function(con, n) {
  assert_connection(con)
  next_chunk <- readLines(con, n)
  if (length(next_chunk) == 0L) {
    # if we don't stop, we will hit an error!
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

#' @importFrom tools file_ext
compressed_file <- function(path, ...){
  con <- switch(tools::file_ext(path),
         gz = gzfile(path, ...),
         bz2 = bzfile(path, ...),
         xz = xzfile(path, ...),
         zip = unz(path, ...),
         file(path, ...))
}
