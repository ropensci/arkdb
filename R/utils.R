
progress <- function(txt, total = 100000){
  
  if (requireNamespace("progress", quietly = TRUE)){
    progress_bar <- getExportedValue("progress", "progress_bar")
    p <- progress_bar$new("[:spin] chunk :current", total = 100000)
  } else {
    ## dummy progress bar if we don't have progress installed
    p <- function(){ list(tick = function()  invisible(NULL)) }
  }
  
  p
}