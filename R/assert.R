
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

assert_overwrite <- function(filename){
  if(file.exists(filename)){
    if(interactive()){
      continue <- readline(prompt = paste("a file named", basename(filename), 
                 "\nalready exists. Overwrite?\n\t 1 = yes\n\t 2 = no\n"))
      if(as.numeric(continue) != 1){
        return(NULL)
      }
    } else {
      warning(paste("overwriting", basename(filename)))
    }
    file.remove(filename)
  }
}



assert_overwrite_db <- function(db_con, tbl_name){
  con <- normalize_con(db_con)
  if(DBI::dbExistsTable(con, tbl_name)){
    if(interactive()){
      continue <- readline(prompt = paste("a table named", tbl_name, 
                 "\nalready exists. Overwrite?\n\t 1 = yes\n\t 2 = no\n"))
      if(as.numeric(continue) != 1){
        return(NULL)
      }
    } else {
      warning(paste("overwriting", tbl_name))
    }
    DBI::dbRemoveTable(con, tbl_name)
  }
}



has_between <- function(db_con, tablename){
  tryCatch(DBI::dbGetQuery(normalize_con(db_con), 
           paste("SELECT * FROM", tablename, "WHERE ROWNUM BETWEEN 1 and 2")),
           error = function(e) FALSE, 
           finally = TRUE)
  
}

