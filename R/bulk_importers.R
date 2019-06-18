## FIXME rewrite bulk_monetdb to use S3 method
## to use more robust SQL generalization of monet.read.csv that does not involve 
## collisions in arguments between it and passing to read.table (quotes, delim/sep, nrows)
## Should have an S3 generic: bulk(con, file, tablename, ...) that passes all ...
## to read.table / read_tsv and understands column headers etc


#' @importFrom R.utils gunzip
#' @importFrom tools file_path_sans_ext
#' @importFrom utils download.file
bulk_importer <- function(db_con, streamable_table){
  
  delim <- switch(streamable_table$extension,
         "tsv" = "\t",
         "csv" = ",",
         NULL)

  if(is.null(delim)) return(NULL)
  
  quote <- switch(streamable_table$extension,
                  "tsv" = "\"", # hmmm...
                  "csv" = "\"",
                  NULL)
  
  ## Method returns this function
  bulk_monetdb <- function(conn, file, tablename, ...){
    file <- expand_if_compressed(file)
    suppress_msg({
      MonetDBLite::monet.read.csv(
        conn, 
        file, 
        tablename,
        delim = delim, 
        quote = quote,
        nrow.check = 1e4,
        best.effort = FALSE) 
      ## For now, omit ... from monet.read.csv call.  `...` may
      ## contain arguments to readr::read_tsv that will not be recognized by `read.table`
      ## and thus may cause errors
    })
    return(0)
  }
  
  ## Should do this as an S3 method for MonetDBEmbeddedConnection
  if (inherits(db_con, "MonetDBEmbeddedConnection")){
    return(bulk_monetdb)
  }

  ## Make this the return if no method can be found?
  NULL
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
    if(!file.exists(dest)) # assumes if it exists that it is the same thing :/
      R.utils::gunzip(file, dest, remove = FALSE)
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
