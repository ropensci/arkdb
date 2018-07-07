#' arkdb: Archive and Unarchive Databases Using Flat Files
#'
#' Flat text files provide a more robust, compressible,
#' and portable way to store tables.  This package provides convenient
#' functions for exporting tables from relational database connections
#' into compressed text files and streaming those text files back into
#' a database without requiring the whole table to fit in working memory.
#'
#' It has two functions:
#'
#' - [ark()]: archive a database into flat files, chunk by chunk.
#' - [unark()]: Unarchive flat files back int a database connection.
#' 
#' arkdb will work with any `DBI` supported connection.  This makes it
#' a convenient and robust way to migrate between different databases
#' as well.
#
#'
"_PACKAGE"
