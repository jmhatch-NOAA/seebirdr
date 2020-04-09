#' Read in csv data files and concatenate.
#'
#' @param directory_path A directory path string.
#' @param ... Arguments to be passed to readr::read_csv.
#' @return The concatenated tibble with column names in snake case.
upload_data_csv <- function(.directory_path, ...) {

  # list of file names for upload
  .file_list <- list.files(.directory_path, pattern = '*.csv$', full.names = TRUE)

  # read in data files and concatenate
  .data <- purrr::map_dfr(.x = .file_list, .f = function(.x) { readr::read_csv(file = .x, ...) })

  # clean column names
  .data %<>% janitor::clean_names(case = 'snake')

  # output concatenated tibble
  return(.data)

}

#' Read in excel data files and concatenate.
#'
#' @inheritParams upload_data_csv
#' @param ... Arguments to be passed to readxl::read_excel.
#' @inheritReturns upload_data_csv
upload_data_excel <- function(.directory_path, ...) {

  # list of file names for upload
  .file_list <- list.files(.directory_path, pattern = '*.xlsx$', full.names = TRUE)

  # read in data files and concatenate
  .data <- purrr::map_dfr(.x = .file_list, .f = function(.x) { readxl::read_excel(path = .x, ...) })

  # clean column names
  .data %<>% janitor::clean_names(case = 'snake')

  # output concatenated tibble
  return(.data)

}
