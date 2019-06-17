


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
                  "tsv" = "",
                  "csv" = "\"",
                  NULL)
  
  ## Method returns this function
  bulk_monetdb <- function(conn, file, tablename){
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

download_if_remote <- function(file){
  if(grepl("://", file)){ # replace URL paths with local path
    tmp <- file.path(tempdir(), basename(file))
    utils::download.file(file, tmp)
    tmp
  }
}
expand_if_compressed <- function(file){
  if(tools::file_ext(file) %in% c("gz", "bz2", "xz")){
    file <- download_if_remote(file)
    dest <- tools::file_path_sans_ext(file)
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
