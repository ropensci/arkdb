## FIXME rewrite bulk_monetdb to use S3 method
## to use more robust SQL generalization of monet.read.csv that does not involve 
## collisions in arguments between it and passing to read.table (quotes, delim/sep, nrows)
## Should have an S3 generic: bulk(con, file, tablename, ...) that passes all ...
## to read.table / read_tsv and understands column headers etc

#' @importFrom tools file_path_sans_ext
#' @importFrom utils download.file
bulk_importer <- function(db_con, streamable_table){
  
  delim <- switch(streamable_table$extension,
         "tsv" = "\t",
         "csv" = ",",
         NULL)

  if(is.null(delim)) return(NULL)
  
  quote <- switch(streamable_table$extension,
                  "tsv" = "", # hmmm...
                  "csv" = "\"",
                  NULL)
  
  ## Method returns this function
  bulk_fn <- get_bulk_fn(db_con, delim, quote)
  
  return(bulk_fn)
}

get_bulk_fn <- function(db_con, delim, quote, ...) {
  UseMethod("get_bulk_fn", db_con)
}

get_bulk_fn.default <- function(db_con, delim, quote, ...) {
  NULL
}

get_bulk_fn.MonetDBEmbeddedConnection <- function(db_con, delim, quote, ...) {
  function(conn, filename, tablename, ...){
    filename <- expand_if_compressed(filename)
    suppress_msg({
      
      #  if (requireNamespace("MonetDBLite", quietly = TRUE)){
      monet.read.csv <- getExportedValue("MonetDBLite", "monet.read.csv")
      #  }  
      
      monet.read.csv(
        conn, 
        files = filename, 
        tablename = tablename,
        delim = delim, 
        quote = quote,
        header = TRUE,
        nrow.check = 1e4,
        best.effort = FALSE) 
      ## For now, omit ... from monet.read.csv call.  `...` may
      ## contain arguments to readr::read_tsv that will not be recognized by `read.table`
      ## and thus may cause errors
    })
    return(0)
  }
}

get_bulk_fn.duckdb_connection <- function(db_con, delim, quote, ...) {
  function(conn, filename, tablename, ...){
    filename <- expand_if_compressed(filename)
    suppress_msg({
      
      duckdb_read_csv <- getExportedValue("duckdb", "duckdb_read_csv")

      duckdb_read_csv(
        conn = conn, 
        name = tablename,
        files = filename, 
        delim = delim, 
        quote = quote,
        header = TRUE,
        nrow.check = 1e4) 
    })
    return(0)
  }
}


## FIXME Consider migrating these up into arkdb::unark behavior, so that standard R-DBI method
## can also work with URLs of .bz2 and .xz files (currently bzfile & xzfile don't work over URLs)
download_if_remote <- function(file){
  if(grepl("://", file)){ # replace URL paths with local path
    tmp <- file.path(tempdir(), basename(file))
    utils::download.file(file, tmp)
    tmp
  } else if (file.exists(file)) {
    file
  } else {
    warning(paste("file", file, "not found."), call.=FALSE)
  }
}
expand_if_compressed <- function(file){
  if(tools::file_ext(file) %in% c("gz", "bz2", "xz")){
    file <- download_if_remote(file)
    dest <- tools::file_path_sans_ext(file)
    if(!file.exists(dest)){ # assumes if it exists that it is the same thing :/
      if(requireNamespace("R.utils", quietly = TRUE)){
        R.utils::gunzip(file, dest, remove = FALSE)
      } else {
        warning("R.utils is required for automatic expansion of compressed files")
      }
    }
  } else {
    dest <- file
  }
  dest
}








suppress_msg <- function(expr, pattern = "reserved SQL"){
  withCallingHandlers(expr,
                      message = function(e){
                        if(grepl(pattern, e$message))
                          invokeRestart("muffleMessage")
                      })
}
