
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
  
  bulk_monetdb <- function(conn, file, tablename){
    if(tools::file_ext(file) %in% c("gz", "bz2", "xz")){
      if(grepl("://", file)){ # replace URL paths with local path
        tmp <- file.path(tempdir(), basename(file))
        utils::download.file(file, tmp)
        file <- tmp
      }
      dest <- tools::file_path_sans_ext(file)
      R.utils::gunzip(file, dest)
    } else {
      dest <- file
    }
    suppress_msg({
    MonetDBLite::monet.read.csv(
      conn, dest, tablename,
      delim = delim, 
      quote = quote,
      nrow.check = 1e4,
      best.effort = FALSE) 
    })
    
    return(0)
  }
  
  if (inherits(db_con, "MonetDBEmbeddedConnection")){
    return(bulk_monetdb)
  }

  NULL
}


suppress_msg <- function(expr, pattern = "reserved SQL"){
  withCallingHandlers(expr,
                      message = function(e){
                        if(grepl(pattern, e$message))
                          invokeRestart("muffleMessage")
                      })
}
