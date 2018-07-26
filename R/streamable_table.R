
#' streamable table
#'
#' @param read read function. Arguments should be "`file`" 
#' (must be able to take a [connection()] object) and "`...`" (for)
#' additional arguments.
#' @param write write function. Arguments should be "`data`" (a data.frame),
#' `file` (must be able to take a [connection()] object), and "`append`" logical,
#' append to existing file or create new?
#' @param extension file extension to use (e.g. "tsv", "csv")
#'
#' @return a `streamable_table` object (S3)
#' @export
#'
#' @examples
#' 
#' streamable_readr_tsv <- function() {
#'   streamable_table(
#'     function(file, ...) readr::read_tsv(file, ...),
#'     function(x, path, append)
#'       readr::write_tsv(x = x, path = path, append = append),
#'     "tsv")
#' }
#' 
streamable_table <- function(read, write, extension) {
  stopifnot(is.function(read),
            is.function(write),
            is.character(extension), length(extension) == 1L, !is.na(extension))
  ret <- list(read = read,
              write = write,
              extension = extension)
  class(ret) <- "streamable_table"
  ret
}


#' @importFrom readr write_tsv read_tsv
streamable_readr_tsv <- function() {
  
    read_tsv <- function(file, ...) {
      readr::read_tsv(file, ...)
    }
    write_tsv <- function(x, path, append = FALSE) {
      readr::write_tsv(x = x, path = path, append = append)
    }
    
  streamable_table(read_tsv, write_tsv, "tsv")
}

#' @importFrom readr write_csv read_csv
streamable_readr_csv <- function() {
  
  read_csv <- function(file, ...) {
    readr::read_csv(file, ...)
  }
  write_csv <- function(x, path, append = FALSE) {
    readr::write_csv(x = x, path = path, append = append)
  }
  
  streamable_table(read_csv, write_csv, "csv")
}


# https://www.iana.org/assignments/media-types/text/tab-separated-values
streamable_base_tsv <- function() {
  read_tsv <- function(file, ...) {
    utils::read.table(textConnection(file), 
                      header = TRUE, 
                      sep = "\t", 
                      quote = "",
                      stringsAsFactors = FALSE,
                      ...)
  }
  write_tsv <- function(x, path, append) {
    utils::write.table(x,
                       file = path, 
                       append = append, 
                       sep = "\t",
                       quote = FALSE,
                       row.names = FALSE,
                       col.names = !append)
  }
  streamable_table(read_tsv, write_tsv, "tsv")
}

## FIXME, user might want to set more of these, e.g. header = FALSE...
streamable_base_csv <- function() {
  read_csv <- function(file, ...) {
    utils::read.table(textConnection(file), 
                      header = TRUE, 
                      sep = ",", 
                      quote = "\"",
                      stringsAsFactors = FALSE,
                      ...)
  }
  ## write.csv does not permit setting col.names = FALSE, so cannot append
  write_csv <- function(x, path, append) {
      utils::write.table(x,
                       file = path, 
                       sep = ",", 
                       quote = TRUE,
                       qmethod = "double",
                       row.names = FALSE,
                       col.names = !append,
                       append = append
      )
  }
  streamable_table(read_csv, write_csv, "csv")
}