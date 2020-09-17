

# Provide an error handler for connecting to monetdblite if locked
# by another session
# @importFrom MonetDBLite MonetDBLite
monetdblite_connect <- function(dbname, ignore_lock = TRUE){
  
  
  if (requireNamespace("MonetDBLite", quietly = TRUE))
    MonetDBLite <- getExportedValue("MonetDBLite", "MonetDBLite")
  
  db <- tryCatch({
    if (ignore_lock) unlink(file.path(dbname, ".gdk_lock"))
    DBI::dbConnect(MonetDBLite(), dbname = dbname)
  },
  error = function(e){
    if(grepl("Database lock", e))
      stop(paste("Local arkdb database is locked by another R session.\n",
                 "Try closing that session first or set the arkdb_HOME\n",
                 "environmental variable to a new location.\n"),
           call. = FALSE)
    else stop(e)
  },
  finally = NULL
  )
  db
}