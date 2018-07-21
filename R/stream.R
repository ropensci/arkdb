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


streamable_readr_tsv <- function() {
  streamable_table(readr::read_tsv, readr::write_tsv, "tsv")
}


streamable_base_tsv <- function() {
  read_tsv <- function(file, header) {
    utils::read.table(file, header, sep = "\t", header = header)
  }
  write_tsv <- function(x, path, append) {
    utils::write.table(file, header, append = append, sep = "\t",
                       col.names = FALSE)
  }
  streamable_table(read_tsv, write_tsv, "tsv")
}
