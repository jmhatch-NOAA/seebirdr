#' Remove apostrophes from column field.
#'
#' @param .data A data frame or data frame extension (e.g., tibble).
#' @param ... Name of the column(s) to remove apostrophes from.
#' @return The mutated tibble.
remove_apostrophe <- function(.data, ...) {

  # quote the expression
  vars <- dplyr::enquos(..., .named = TRUE)

  # modify quoted expression
  mutate_vars <- purrr::map(vars, function(var) { dplyr::expr(gsub("'", "", !!var)) })

  # replace apostrophes with empty spaces
  dplyr::mutate(.data, !!!mutate_vars)

}

#' Create a datetime column from character strings.
#' Assumes datetime has a timezone of UTC, but this can be modified.
#'
#' @inheritParams remove_apostrophe
#' @param .datetime Name of the column field that identifies the datetime.
#' @param .tz Time zone of the datetime.
#' @return The mutated tibble with a new column labeled datetime_[.tz].
datetime_stampify <- function(.data, .datetime, .tz = "UTC") {

  # quote the expression
  datetime_name_var <- dplyr::enquo(.datetime)

  # new column name as string
  datetime_name_str <- paste0('datetime_', .tz %>% tolower())

  # split datetime string into date and time components and assign to new column name
  .data %<>% dplyr::mutate(!!datetime_name_str := paste(stringr::str_sub(!!datetime_name_var, start = 1, end = 10), stringr::str_sub(!!datetime_name_var, start = 12, end = 19), sep = " "))

  # convert datetime column to POSIXct class
  .data %>% dplyr::mutate(!!datetime_name_str := as.POSIXct(.data[[!!datetime_name_str]], format = "%Y-%m-%d %H:%M:%S", tz = .tz))

}

#' Replace observer name with code from database.
#'
#' @inheritParams remove_apostrophe
#' @param .channel A DBI object that connects to the internal NEFSC Oracle database.
#' @param .observer_name Name of the column field that identifies the observer name.
#' @inheritReturns remove_apostrophe
observer_codify <- function(.data, .channel, .observer_name) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of observer names and clean column names so they're snake case
  dbs_obs_name <- DBI::dbGetQuery(conn = .channel, statement = "SELECT LASTNAME, FIRSTNAME, OBSCODE FROM MAMMAL_SOUND.MAMMAL_OBSERVER") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_obs_name %<>% dplyr::mutate(link = paste0(lastname, ', ', firstname))

  # quote the expression
  obs_name_var <- dplyr::enquo(.observer_name)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!obs_name_var := !!obs_name_var %>% toupper) %>% dplyr::rename(link = !!obs_name_var)

  # left join in obsever codes
  .data %<>% dplyr::left_join(dbs_obs_name %>% dplyr::select(link, obscode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # reaname obscode to .oserver_name
  .data %>% dplyr::rename(!!obs_name_var := obscode)

}

#' Replace observer position with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .observer_position Name of the column field that identifies the observer position.
#' @inheritReturns remove_apostrophe
position_codify <- function(.data, .channel, .observer_position) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of observer names and clean column names so they're snake case
  dbs_obs_pos <- DBI::dbGetQuery(conn = .channel, statement = "SELECT SIDEOFSHIP, SIDE FROM MAMMAL_SOUND.BIRDSIDEOFSHIP") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_obs_pos %<>% dplyr::rename(link = side)

  # quote the expression
  obs_pos_var <- dplyr::enquo(.observer_position)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!obs_pos_var := !!obs_pos_var %>% tolower) %>% dplyr::rename(link = !!obs_pos_var)

  # left join in obsever codes
  .data %<>% dplyr::left_join(dbs_obs_pos %>% dplyr::select(link, sideofship), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # reaname obscode to .oserver_name
  .data %>% dplyr::rename(!!obs_pos_var := sideofship)

}

#' Replace seabird behavior with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .seabird_behavior Name of the column field that identifies the seabird behavior.
#' @inheritReturns remove_apostrophe
behavior_codify <- function(.data, .channel, .seabird_behavior) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of observer names and clean column names so they're snake case
  dbs_bird_behave <- DBI::dbGetQuery(conn = .channel, statement = "SELECT BEHAVIORDESC, BEHAVIORCODE FROM MAMMAL_SOUND.BIRDBEHAVIOR") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_bird_behave %<>% dplyr::rename(link = behaviordesc)

  # quote the expression
  bird_behave_var <- dplyr::enquo(.seabird_behavior)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!bird_behave_var := !!bird_behave_var %>% tolower) %>% dplyr::rename(link = !!bird_behave_var)

  # left join in obsever codes
  .data %<>% dplyr::left_join(dbs_bird_behave %>% dplyr::select(link, behaviorcode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # replace NA with ""
  .data %<>% dplyr::mutate(behaviorcode = replace_na(behaviorcode, ""))

  # reaname obscode to .oserver_name
  .data %>% dplyr::rename(!!bird_behave_var := behaviorcode)

}
