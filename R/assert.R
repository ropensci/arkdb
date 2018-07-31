
assert_files_exist <- function(files){
  if(length(files) == 0){
    warning("no file specified")
  }
  if(length(files) == 1){
    if(!file.exists(files))
      warning(sprintf("file '%s' not found", files), call. = FALSE)
  }
  lapply(files, function(f) 
    if(!file.exists(f)) 
      warning(sprintf("'%s' not found", f), call. = FALSE))
}

assert_dir_exists <- function(dir){
  if(!dir.exists(dir)) 
    stop(sprintf("'%s' not found", dir), call. = FALSE)
}

assert_dbi <- function(x, name = deparse(substitute(x))) {
  if (! (inherits(x, "DBIConnection") || inherits(x, "src_dbi"))) {
    stop(sprintf("'%s' must be a DBIConnection or src_dbi object", name),
         call. = FALSE)
  }
}

assert_connection <- function(x, name = deparse(substitute(x))) {
  if (!inherits(x, "connection")) {
    stop(sprintf("'%s' must be a connection object", name), call. = FALSE)
  }
}


has_between <- function(db_con, tablename){
  tryCatch(DBI::dbGetQuery(normalize_con(db_con), 
           paste("SELECT * FROM", tablename, "WHERE ROWNUM BETWEEN 1 and 2")),
           error = function(e) FALSE, 
           finally = TRUE)
  
}

