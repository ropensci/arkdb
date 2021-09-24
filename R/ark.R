
#' Archive tables from a database as flat files
#' 
#' @param db_con a database connection
#' @param dir a directory where we will write the compressed text files output
#' @param streamable_table interface for serializing/deserializing in chunks
#' @param lines the number of lines to use in each single chunk
#' @param compress file compression algorithm. Should be one of "bzip2" (default),
#' "gzip" (faster write times, a bit less compression), "xz", or "none", for
#' no compression.
#' @param tables a list of tables from the database that should be 
#' archived.  By default, will archive all tables. Table list should specify
#' schema if appropriate, see examples. 
#' @param method method to use to query the database, see details. 
#' @param overwrite should any existing text files of the same name be overwritten?
#' default is "ask", which will ask for confirmation in an interactive session, and
#' overwrite in a non-interactive script.  TRUE will always overwrite, FALSE will
#' always skip such tables.
#' @param filter_statement Typically an SQL "WHERE" clause, specific to your
#' dataset. (e.g., `WHERE year = 2013`) 
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
#' In almost all cases, the default method should be the best choice.
#' If the [DBI::dbSendQuery()] implementation for your database platform returns the
#' full results to the client immediately rather than supporting chunking with `n`
#' parameter, you may want to use "window" method, which is the most generic.  The
#' "sql-window" method provides a faster alternative for databases like PostgreSQL that
#' support windowing natively (i.e. `BETWEEN` queries).  
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
#' } 
#' \dontrun{
#' 
#' ## For a Postgres DB with schema, we can append schema names first
#' ## to each of the table names, like so: 
#' schema_tables <- dbGetQuery(db, sqlInterpolate(db,
#' "SELECT table_name FROM information_schema.tables 
#' WHERE table_schema = ?schema", schema = "schema_name"))
#' 
#' ark(db, dir, tables = paste0("schema_name",".", schema_tables$table_name))
#' }
#' 
ark <- function(db_con, 
                dir, 
                streamable_table = streamable_base_tsv(),
                lines = 50000L, 
                compress = c("bzip2", "gzip", "xz", "none"),
                tables = list_tables(db_con),
                method = c("keep-open", "window", "sql-window"),
                overwrite = "ask",
                filter_statement = NULL){
  
  assert_dbi(db_con)
  assert_dir_exists(dir)
  assert_streamable(streamable_table)
  
  if(!is.null(filter) & length(tables) > 1) {
    warning("Your filter statement will be applied to all tables.")
  }

  
  method <- match.arg(method)
  compress <- match.arg(compress)
  lines <- as.integer(lines)
  
  stopifnot(inherits(streamable_table, "streamable_table"))
  
  ## exclude sqlite's internal tables
  tables <- tables[!grepl("sqlite_", tables)]
  
  lapply(tables, 
         ark_file, 
         db_con = normalize_con(db_con),
         streamable_table = streamable_table,
         lines = lines, 
         dir = dir, 
         compress = compress,
         method = method,
         overwrite = overwrite, 
         filter_statement = filter_statement)
  
  invisible(dir)
}

list_tables <- function(db){
  db <- normalize_con(db)
  DBI::dbListTables(db)
}

#' @importFrom DBI dbSendQuery dbFetch dbClearResult dbGetQuery
ark_file <- function(tablename, 
                     db_con, 
                     streamable_table,
                     lines, 
                     dir, 
                     compress,
                     method,
                     overwrite, 
                     filter_statement){
  
  ## Set up compressed connection
  ext <- switch(compress,
                "bzip2" = ".bz2",
                "gzip" = ".gz",
                "xz" = ".xz",
                "none" = "",
                ".bz2")
  
  dest <- sprintf("%s.%s%s", tablename, streamable_table$extension, ext)
  filename <- file.path(dir, dest)
  
  ## Handle case in which file already exists. Otherwise, we'll append to it
  if(!assert_overwrite(filename, overwrite)){
    return(NULL)
  }
  
  con <- generic_connection(filename, "wb")
  on.exit(close(con))
  
  ## Progress reporting
  message(sprintf("Exporting %s in %d line chunks:", tablename, lines))
  p <- progress("[:spin] chunk :current", total = 100000)
  t0 <- Sys.time()
 
  switch(method,
          "keep-open" = keep_open(db_con, streamable_table, lines, 
                                  p, tablename, con, filter_statement),
             "window" = window(db_con, streamable_table, lines, 
                               compress, p, tablename, con, filter_statement),
         "sql-window" = sql_window(db_con, streamable_table, lines, 
                                   compress, p, tablename, con, filter_statement),
         keep_open(db_con, streamable_table, lines, p, tablename, con, 
                   filter_statement)
  )
  
  message(sprintf("\t...Done! (in %s)", format(Sys.time() - t0)))
  
}


## Generic way to get header
get_header <- function(db, tablename){
  fields <- DBI::dbListFields(db, tablename)
  names(fields) <- fields
  as.data.frame(lapply(fields, function(x) character(0)))
}

keep_open <- function(db_con, streamable_table, lines, p, tablename, con, 
                      filter_statement) {
  ## Create header to avoid duplicate column names
  header <- get_header(db_con, tablename)
  streamable_table$write(header, con, omit_header = FALSE)
  
  ## 
  if(is.null(filter_statement)) {
    res <- DBI::dbSendQuery(db_con, paste("SELECT * FROM", tablename))
  } else {
    res <- DBI::dbSendQuery(
      db_con, 
      paste("SELECT * FROM", filter_statement, tablename)
    )
  }

  while (TRUE) {
    p$tick()
    data <- DBI::dbFetch(res, n = lines)
    if (nrow(data) == 0) break
    streamable_table$write(data, con, omit_header = TRUE)
  }
  
  DBI::dbClearResult(res)
}


windowing <- function(sql_supports_windows)
  function(db_con, streamable_table, lines, compress, p, tablename, con, 
           filter_statement) {
  
  if(is.null(filter_statement)) {
    size <- DBI::dbGetQuery(db_con, paste("SELECT COUNT(*) FROM", tablename))
  } else {
    size <- DBI::dbGetQuery(
      db_con, 
      paste("SELECT COUNT(*) FROM", filter_statement, tablename))
  }

  end <- size[[1]][[1]]
  start <- 1
  repeat {
    p$tick()
    ## Do stuff
    ark_chunk(db_con, 
              streamable_table = streamable_table,
              tablename = tablename, 
              start = start, 
              lines = lines,
              compress = compress, 
              con = con, 
              sql_supports_windows = sql_supports_windows,
              filter_statement = filter_statement)
    start <- start + 1  
    if ( (start - 1)*lines > end) {
      break
    }
  }
}
  
ark_chunk <- function(db_con,
                      streamable_table,
                      tablename, 
                      start, 
                      lines, 
                      compress,
                      con,
                      sql_supports_windows, 
                      filter_statement = NULL){
  
  if (sql_supports_windows) {
    ## Windowed queries are faster but not universally supported
    if(is.null(filter_statement)) {
      query <- paste("SELECT * FROM", tablename, filter_statement, 
                     "AND rownum BETWEEN", sql_int((start - 1) * lines), 
                     "AND", sql_int(start * lines))
    } else {
      query <- paste("SELECT * FROM", tablename, "WHERE rownum BETWEEN",
                     sql_int((start - 1) * lines), "AND", sql_int(start * lines))      
    }

  } else { 
    ## Any SQL DB can do offset
    if(is.null(filter_statement)) {
      query <- paste("SELECT * FROM", tablename, "LIMIT", 
                     sql_int(lines), 
                     "OFFSET", 
                     sql_int((start-1)*lines))
    } else {
      query <- paste("SELECT * FROM", tablename, filter_statement, "LIMIT", 
                     sql_int(lines), 
                     "OFFSET", 
                     sql_int((start-1)*lines))      

    }


  }
  data <- DBI::dbGetQuery(db_con, query)
  
  omit_header <- start != 1
  streamable_table$write(data, con, omit_header = omit_header)
  
}

## need to convert large integers to characters
sql_int <- function(x){
  sprintf("%.0f", x)
}

window <- windowing(FALSE)
sql_window <- windowing(TRUE)


