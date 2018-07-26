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

# https://www.iana.org/assignments/media-types/text/tab-separated-values
streamable_base_tsv <- function() {
  read_tsv <- function(file, ...) {
    utils::read.table(file, 
                      header = TRUE, 
                      sep = "\t", 
                      quote = "",
                      stringsAsFactors = FALSE,
                      ...)
  }
  write_tsv <- function(x, path, append) {
    utils::write.table(file, 
                       append = append, 
                       sep = "\t",
                       quote = FALSE,
                       row.names = FALSE,
                       col.names = !append,
                       stringsAsFactors = FALSE)
  }
  streamable_table(read_tsv, write_tsv, "tsv")
}

## FIXME, user might want to set more of these, e.g. header = FALSE...
streamable_base_csv <- function() {
  read_csv <- function(file, ...) {
    utils::read.csv(file, 
                      header = TRUE, 
                      sep = ",", 
                      quote = "\"",
                      stringsAsFactors = FALSE,
                      ...)
  }
  write_csv <- function(x, path, append) {
    utils::write.csv(file, 
                       append = append, 
                       sep = ",",
                       quote = "\"",
                       row.names = FALSE,
                       col.names = !append,
                       stringsAsFactors = FALSE)
  }
  streamable_table(read_csv, write_csv, "csv")
}