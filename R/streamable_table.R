
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

#' streamable tsv using `readr`
#' 
#' @return a `streamable_table` object (S3)
#' @export
#' @seealso [readr::read_tsv()], [readr::write_tsv()]
#' 
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

#' streamable csv using `readr`
#' 
#' @return a `streamable_table` object (S3)
#' @export
#' @seealso [readr::read_csv()], [readr::write_csv()]
#' 
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

#' streamable tsv using base R functions
#' 
#' @return a `streamable_table` object (S3)
#' @export
#' 
#' @details
#' Follows the tab-separate-values standard using [utils::read.table()],
#' see IANA specification at:
#' <https://www.iana.org/assignments/media-types/text/tab-separated-values>
#' 
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



#' streamable csv using base R functions
#' 
#' @return a `streamable_table` object (S3)
#' @export
#' 
#' @details
#' Follows the comma-separate-values standard using [utils::read.table()]
#' 
streamable_base_csv <- function() {
  read_csv <- function(file, ...) {
    ## Consider case of header = FALSE...
    utils::read.table(textConnection(file), 
                      header = TRUE, 
                      sep = ",", 
                      quote = "\"",
                      stringsAsFactors = FALSE,
                      ...)
  }
  ## NOTE: write.csv does not permit setting 
  ## `col.names = FALSE``, so cannot append
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