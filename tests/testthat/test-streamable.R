context("streamable_table")


test_stream <- function(stream) {
  filename <- tempfile(fileext = ".txt")

  con <- generic_connection(filename, "wb")
  on.exit(close(con))

  data <- datasets::iris
  data$Species <- as.character(data$Species)
  stream$write(data, con, omit_header = FALSE)
  stream$write(data, con, omit_header = TRUE)

  close(con)

  con <- generic_connection(filename, "rb")
  on.exit(close(con))

  ## unark works via readLines.  This is necessary for
  ## readr methods which cannot read non-binary connection
  ## but not necessary for base methods.
  chunk <- paste0(readLines(con), "\n", collapse = "")
  df <- stream$read(chunk)
  testthat::expect_equivalent(df, rbind(data, data))
}

testthat::test_that("streamable_base_csv", {
  test_stream(streamable_base_csv())
})

testthat::test_that("streamable_base_tsv", {
  test_stream(streamable_base_tsv())
})

testthat::test_that("streamable_readr_csv", {
  test_stream(streamable_readr_csv())
})
testthat::test_that("streamable_readr_tsv", {
  test_stream(streamable_readr_tsv())
})

testthat::test_that("streamable_vroom", {
  test_stream(streamable_vroom())
})


testthat::test_that("internal closure constructor", {
  window <- windowing(FALSE)
  testthat::expect_is(window, "function")
})
