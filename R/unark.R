#' Unarchive a list of compressed tsv files into a database
#' @param files vector of filenames to be read in. Must be `tsv`
#' format, optionally compressed using `bzip2`, `gzip`, `zip`,
#' or `xz` format at present.
#' @param db_con a database src (`src_dbi` object from `dplyr`)
#' @param streamable_table interface for serializing/deserializing in chunks
#' @param lines number of lines to read in a chunk.
#' @param overwrite should any existing text files of the same name be overwritten?
#' default is "ask", which will ask for confirmation in an interactive session, and
#' overwrite in a non-interactive script.  TRUE will always overwrite, FALSE will
#' always skip such tables.
#' @param encoding encoding to be assumed for input files.
#' @param tablenames vector of tablenames to be used for corresponding files.
#' By default, tables will be named using lowercase names from file basename with
#' special characters replaced with underscores (for SQL compatibility).
#' @param try_native logical, default TRUE. Should we try to use a native bulk
#' import method for the database connection?  This can substantially speed up
#' read times and will fall back on the DBI method for any table that fails
#' to import.  Currently only MonetDBLite connections support this.
#' @param ... additional arguments to `streamable_table$read` method.
#' @details `unark` will read in a files in chunks and 
#' write them into a database.  This is essential for processing
#' large compressed tables which may be too large to read into
#' memory before writing into a database.  In general, increasing
#' the `lines` parameter will result in a faster total transfer
#' but require more free memory for working with these larger chunks.
#' 
#' If using `readr`-based streamable-table, you can suppress the progress bar
#' by using `options(readr.show_progress = FALSE)` when reading in large 
#' files.
#'
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
unark <- function(files, 
                  db_con,
                  streamable_table = NULL, 
                  lines = 50000L, 
                  overwrite = "ask",
                  encoding = Sys.getenv("encoding", "UTF-8"),
                  tablenames = NULL,
                  try_native = TRUE,
                  ...){
  
  assert_files_exist(files)
  assert_dbi(db_con)
  
  ## Guess streamable table
  if(is.null(streamable_table)){
    streamable_table <- guess_stream(files[[1]])
  }

  assert_streamable(streamable_table)
  
  
  if(is.null(tablenames)){
    tablenames <- vapply(files, base_name, character(1))
  }
  
  db <- normalize_con(db_con)
  
  lapply(seq_along(files), 
         function(i){
           unark_file(files[[i]],
                      db_con = db, 
                      streamable_table = streamable_table, 
                      lines = lines, 
                      overwrite = overwrite,
                      encoding = encoding,
                      tablename = tablenames[[i]],
                      try_native = try_native,
                      ...)
           })
  
  invisible(db_con)  
}

normalize_con <- function(db_con){
  ## Handle both dplyr and DBI style connections
  ## Return whichever one we are given.
  if(inherits(db_con, "src_dbi")){
    db_con$con
  } else {
    db_con
  }
}

#' @importFrom DBI dbWriteTable
#' @importFrom progress progress_bar
unark_file <- function(filename,
                       db_con,
                       streamable_table,
                       lines,
                       overwrite,
                       encoding,
                       tablename = base_name(filename),
                       try_native = try_native,
                       ...){
    

  if(!assert_overwrite_db(db_con, tablename, overwrite)){
    return(NULL)
  }
    
  ## Check for a bulk importer first
  bulk <- bulk_importer(db_con, streamable_table)
  if(!is.null(bulk) && try_native){
    status <- 
      tryCatch(bulk(db_con, filename, tablename),
               error = function(e) 1)
    if(status == 0) return(invisible(db_con))
  }
  
  con <- compressed_file(filename, "r", encoding = encoding)
  on.exit(close(con))
  
  ## Handle case of `col_names != TRUE`?
  ## readr method needs UTF-8 encoding for these newlines to be newlines
  header <- read_lines(con, n = 1L, encoding = encoding)
  if(length(header) == 0){ # empty file, would throw error
    return(invisible(db_con))
  }
  reader <- read_chunked(con, lines, encoding)
  
  # May throw an error if we need to read more than 'total' chunks?
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  message(sprintf("Importing %s in %d line chunks:",
                  basename(filename), lines))
  t0 <- Sys.time()
  repeat {
    d <- reader()
    body <- paste0(c(header, d$data), "\n", collapse = "")
    p$tick()
    chunk <- streamable_table$read(body, ...)
    DBI::dbWriteTable(db_con, tablename, chunk, append=TRUE)
    
    if (d$complete) {
      break
    }
  }
  message(sprintf("\t...Done! (in %s)", format(Sys.time() - t0)))
  
  invisible(db_con)
}


# Adapted from @richfitz, MIT licensed
# https://github.com/vimc/montagu-r
# /blob/4fe82fd29992635b30e637d5412312b0c5e3e38f/R/util.R#L48-L60

read_chunked <- function(con, n, encoding) {
  assert_connection(con)
  next_chunk <- read_lines(con, n, encoding = encoding)
  if (length(next_chunk) == 0L) {
    warning("connection has already been completely read")
    return(function() list(data = character(0), complete = TRUE))
  }
  function() {
    data <- next_chunk
    next_chunk <<- read_lines(con, n, encoding = encoding)
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
  path <- sub(ext_regex, "", path, perl = TRUE)
  ## Remove characters not permitted in table names
  path <- gsub("[^a-zA-Z0-9_]", "_", path, perl = TRUE)
  tolower(path)
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


read_lines <- function(con,
                       n,
                       encoding = "unknown",
                       warn = FALSE){
  out <- readLines(con,
                   n = n,
                   encoding = encoding,
                   warn = FALSE)

}

guess_stream <- function(x){  
  ext <- tools::file_ext(x)
  ## if compressed, chop off that and try again
  if(ext %in% c("gz", "bz2", "xz", "zip")){
    ext <- tools::file_ext(gsub("\\.([[:alnum:]]+)$", "", x))
  }
  streamable_table <- 
    switch(ext,
           "csv" = streamable_base_csv(),
           "tsv" = streamable_base_tsv(),
           stop(paste("Streaming file parser could not be", 
                      "guessed from file extension.",
                      "Please specify a streamable_table option"))
    )
  streamable_table
}
