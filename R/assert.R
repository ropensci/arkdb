
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
  if (!DBI::dbIsValid(x)) {
    stop(sprintf("'%s' must be a DBIConnection or src_dbi object", name),
         call. = FALSE)
  }
}

assert_connection <- function(x, name = deparse(substitute(x))) {
  if (!inherits(x, "connection")) {
    stop(sprintf("'%s' must be a connection object", name), call. = FALSE)
  }
}


assert_overwrite <- function(filename, overwrite){
  
  if(!file.exists(filename)){
    return(TRUE)  
  }
  
  switch(as.character(overwrite),
         "TRUE"  = overwrite(filename),
         "FALSE" = dont_overwrite(filename),
         "ask"   = ask_overwrite(filename))
}

overwrite <- function(filename){
  warning(paste("overwriting", basename(filename)))
  file.remove(filename)
  TRUE
}

dont_overwrite <- function(filename){
  message(paste("not overwriting", basename(filename)))
  FALSE
}

ask_overwrite <- function(filename){
  if(!interactive()){
    return(overwrite(filename))
  }
    
  replace <- askyesno(paste0("Overwrite ", basename(filename), "?"))
  if(is.na(replace)){
    stop(paste("operation cancelled."), call. = FALSE)
  } 
  if(!replace){
    return(dont_overwrite(filename))
  }
  
  overwrite(filename)
}

# utils::askYesKnow is new to R 3.5.0; avoid for backwards compatibility
askyesno <- function(msg){
    
    prompts <- c("Yes", "No", "Cancel")
    choices <- tolower(prompts)
    msg1 <- paste0("(", paste(choices, collapse = "/"), ") ")
    
    if (nchar(paste0(msg, msg1)) > 250) {
      cat(msg, "\n")
      msg <- msg1
    }
    else msg <- paste0(msg, " ", msg1)
    
    ans <- readline(msg)
    match <- pmatch(tolower(ans), tolower(choices))
    
    if (!nchar(ans)) 
      TRUE
    else if (is.na(match)) 
      stop("Unrecognized response ", dQuote(ans))
    else c(TRUE, FALSE, NA)[match]
}


assert_overwrite_db <- function(db_con, tablename, overwrite){
  
  con <- normalize_con(db_con)
  if(!DBI::dbExistsTable(con, tablename)){
    return(TRUE)
  }
  
  switch(as.character(overwrite),
         "TRUE"  = overwrite_db(con, tablename),
         "FALSE" = dont_overwrite(tablename),
         "ask"   = ask_overwrite_db(con, tablename))
}

overwrite_db <- function(con, tablename){
  warning(paste("overwriting", tablename))
  DBI::dbRemoveTable(con, tablename)
  TRUE
}

ask_overwrite_db <- function(con, tablename){
  if(!interactive()){
    return(overwrite_db(con, tablename))
  }
  
  if(!askyesno(paste0("Overwrite ", tablename, "?"))){
    return(dont_overwrite(tablename))
  }
  
  overwrite_db(con, tablename)
}





has_between <- function(db_con, tablename){
  tryCatch(DBI::dbGetQuery(normalize_con(db_con), 
           paste("SELECT * FROM", tablename, "WHERE ROWNUM BETWEEN 1 and 2")),
           error = function(e) FALSE, 
           finally = TRUE)
  
}

