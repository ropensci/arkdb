
# Adapted from @richfitz, MIT licensed
# https://github.com/vimc/montagu-r/blob/4fe82fd29992635b30e637d5412312b0c5e3e38f/R/util.R#L48-L60
assert_connection <- function(x, name = deparse(substitute(x))) {
  if (!inherits(x, "connection")) {
    stop(sprintf("'%s' must be a connection object", name), call. = FALSE)
  }
}


read_chunked <- function(con, n) {
  assert_connection(con)
  next_chunk <- readLines(con, n)
  if (length(next_chunk) == 0L) {
    stop("connection has already been completely read")
  }
  function() {
    data <- next_chunk
    next_chunk <<- readLines(con, n)
    complete <- length(next_chunk) == 0L
    list(data = data, complete = complete)
  }
}


## Do twice to remove compression extension and file extension
base_name <- function(filename){
  path <- basename(filename)
  ext_regex <- "(?<!^|[.])[.][^.]+$"
  path <- sub(ext_regex, "", path, perl = TRUE)
  path <- sub(ext_regex, "", path, perl = TRUE)
  sub(ext_regex, "", path, perl = TRUE)
}


# filename <- unark("data/flights.tsv.bz2", db_path = "data/flights.sql")
unark <- function(filename, lines = 10000L, drv = RSQLite::SQLite(), ...){
  
  db_con <- dbConnect(drv, ...)
  tbl_name <- base_name(filename)
    
  ## guess connection, don't assume bz2!
  con <- bzfile(filename, "r")
  #con <- file(filename, "r")
  on.exit(close(con))
  
  header <- readLines(con, n = 1L)
  reader <- read_chunked(con, lines)
  
  p <- progress::progress_bar$new("[:spin] chunk :current", total = 100000)
  message(sprintf("Importing in %d line chunks:\n%s",
                  lines, filename))
  t0 <- Sys.time()
  repeat {
    d <- reader()
    body <- paste0(c(header, d$data), "\n", collapse = "")
    p$tick()
    chunk <- readr::read_tsv(body)
    DBI::dbWriteTable(db_con, tbl_name, chunk, append=TRUE)
    
    if (d$complete) {
      break
    }
  }
  message(sprintf("...Done! (in %s)", format(Sys.time() - t0)))
  
  #DBI::dbDisconnect(db_con)
  
  db_con
}