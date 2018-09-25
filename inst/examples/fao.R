library(arkdb)
#unzip("~/Desktop/FAOSTAT.zip")
#lapply(x, unzip)

x <- list.files("~/FAOSTAT/", pattern="[.]csv",full.names = TRUE)
dbdir <- rappdirs::user_data_dir("faostat")
db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), dbdir)


### using the readr parser ###
options(encoding = "UTF-8") # Must enforce UTF-8 for readr parsing
unark(x, db, streamable_table = streamable_readr_csv(), lines = 5e5, overwrite = TRUE)

## Inspect
tbls <- DBI::dbListTables(db)
DBI::dbListFields(db, tbls[[42]])
dplyr::tbl(db, tbls[[42]])

#############################################################
### Alternative Approach: custom streamable_table method ####
#############################################################

## A slightly modified base read.csv function is used here to standardize column names
  read <- function(file, ...) {
    tbl <- utils::read.table(textConnection(file), header = TRUE, 
                             sep = ",", quote = "\"", stringsAsFactors = FALSE, 
                             ...)
    ## ADDING THESE LINES to the default method.  use lowercase column names
    names(tbl) <- tolower(names(tbl))
    names(tbl) <- gsub("\\.", "_", names(tbl))
    tbl
  }
  
  
  read <- function(file, ...) {
    readr::read_csv(file = file, ...)
  }
  
  write <- function(x, path, omit_header) {
    utils::write.table(x, file = path, sep = ",", quote = TRUE, 
                       qmethod = "double", row.names = FALSE, col.names = !omit_header, 
                       append = omit_header)
  }
  stream <- arkdb::streamable_table(read, write, "csv")



unark(x, db, streamable_table = stream, lines = 5e5, overwrite = TRUE)
