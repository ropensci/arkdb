
#' process a table in chunks
#'
#' @inheritParams unark
#' @param file path to a file
#' @param process_fn a function of a `chunk`
#' @export
#'
#' @examples
#' con <- system.file("extdata/mtcars.tsv.gz", package = "arkdb")
#' dummy <- function(x) message(paste(dim(x), collapse = " x "))
#' process_chunks(con, dummy, lines = 8)
process_chunks <- function(file,
                           process_fn,
                           streamable_table = NULL,
                           lines = 5e4L,
                           encoding = Sys.getenv("encoding", "UTF-8"),
                           ...) {

  ## Guess streamable table
  if (is.null(streamable_table)) {
    streamable_table <- guess_stream(file)
  }
  
  if(streamable_table$extension == "parquet") { 
    chunk <- streamable_table$read(file)
    process_fn(chunk)
  } else {
    con <- generic_connection(file, "rb", encoding = encoding)
    on.exit(close(con))
    
    
    header <- readLines(con, n = 1L, encoding = encoding, warn = FALSE)
    if (length(header) == 0) { # empty file, would throw error
      return(NULL)
    }
    reader <- read_chunked(con, lines, encoding)
    
    # May throw an error if we need to read more than 'total' chunks?
    p <- progress("[:spin] chunk :current", total = 100000)
    message(sprintf(
      "Importing %s in %d line chunks:",
      summary(con)$description, lines
    ))
    t0 <- Sys.time()
    repeat {
      d <- reader()
      body <- paste0(c(header, d$data), "\n", collapse = "")
      p$tick()
      chunk <- streamable_table$read(body, ...)
      process_fn(chunk)
      
      if (d$complete) {
        break
      }
    }
    message(sprintf("\t...Done! (in %s)", format(Sys.time() - t0)))
  }
}



# Adapted from @richfitz, MIT licensed
# https://github.com/vimc/montagu-r
# /blob/4fe82fd29992635b30e637d5412312b0c5e3e38f/R/util.R#L48-L60

read_chunked <- function(con, n, encoding) {
  assert_connection(con)
  next_chunk <- readLines(con, n, encoding = encoding, warn = FALSE)
  if (length(next_chunk) == 0L) {
    warning("connection has already been completely read")
    return(function() list(data = character(0), complete = TRUE))
  }
  function() {
    data <- next_chunk
    next_chunk <<- readLines(con, n, encoding = encoding, warn = FALSE)
    complete <- length(next_chunk) == 0L
    list(data = data, complete = complete)
  }
}
