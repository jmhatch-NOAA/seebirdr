#' remove_apostrophe
#'
#' Remove apostrophes from column field.
#'
#' @param .data A data frame or data frame extension (e.g., tibble).
#' @param ... Name of the column(s) to remove apostrophes from.
#'
#' @return The mutated tibble.
#'
#' @export
remove_apostrophe <- function(.data, ...) {

  # quote the expression
  vars <- dplyr::enquos(..., .named = TRUE)

  # modify quoted expression
  mutate_vars <- purrr::map(vars, function(var) { dplyr::expr(gsub("'", "", !!var)) })

  # replace apostrophes with empty spaces
  dplyr::mutate(.data, !!!mutate_vars)

}

#' datetime_stampify
#'
#' Create a datetime column from character strings or POSIXct class.
#' Assumes datetime has a timezone of UTC, but this can be modified with the .tz argument.
#'
#' @inheritParams remove_apostrophe
#' @param .datetime Name of the column field that identifies the datetime.
#' @param .tz Time zone of the datetime.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble with a new column labeled datetime_[.tz].
#'
#' @export
datetime_stampify <- function(.data, .datetime, .tz = "UTC") {

  # quote the expression
  datetime_name_var <- dplyr::enquo(.datetime)

  # new column name as string
  datetime_name_str <- paste0('datetime_', .tz %>% tolower())

  # make sure .datetime field is type character, otherwise assume POSIXct class
  if(!is.character(.data %>% dplyr::pull(!!datetime_name_var))) {

    # call warning
    warning("Warning: Datetime field is not a character type.")

    # rename datetime column
    .data %<>% dplyr::rename(!!datetime_name_str := !!datetime_name_var)

  } else {

    # split datetime string into date and time components and assign to new column name
    .data %<>% dplyr::mutate(!!datetime_name_str := paste(stringr::str_sub(!!datetime_name_var, start = 1, end = 10), stringr::str_sub(!!datetime_name_var, start = 12, end = 19), sep = " "))

    # convert datetime column to POSIXct class
    .data %<>% dplyr::mutate(!!datetime_name_str := as.POSIXct(.data[[!!datetime_name_str]], format = "%Y-%m-%d %H:%M:%S", tz = .tz))

  }

}

#' observer_codify
#'
#' Replace observer name with code from database.
#'
#' @inheritParams remove_apostrophe
#' @param .channel A DBI object that connects to the internal NEFSC Oracle database.
#' @param .observer_name Name of the column field that identifies the observer name.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
observer_codify <- function(.data, .channel, .observer_name) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of observer name codes and clean column names so they're snake case
  dbs_obs_name <- DBI::dbGetQuery(conn = .channel, statement = "SELECT LASTNAME, FIRSTNAME, OBSCODE FROM MAMMAL_SOUND.MAMMAL_OBSERVER") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_obs_name %<>% dplyr::mutate(link = paste0(lastname, ', ', firstname))

  # quote the expression
  obs_name_var <- dplyr::enquo(.observer_name)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!obs_name_var := !!obs_name_var %>% toupper) %>% dplyr::rename(link = !!obs_name_var)

  # left join in observer name codes
  .data %<>% dplyr::left_join(dbs_obs_name %>% dplyr::select(link, obscode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # rename obscode to .oserver_name
  .data %>% dplyr::rename(!!obs_name_var := obscode)

}

#' position_codify
#'
#' Replace observer position with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .observer_position Name of the column field that identifies the observer position.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
position_codify <- function(.data, .channel, .observer_position) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of observer position codes and clean column names so they're snake case
  dbs_obs_pos <- DBI::dbGetQuery(conn = .channel, statement = "SELECT SIDEOFSHIP, SIDE FROM MAMMAL_SOUND.BIRDSIDEOFSHIP") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_obs_pos %<>% dplyr::rename(link = side)

  # quote the expression
  obs_pos_var <- dplyr::enquo(.observer_position)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!obs_pos_var := !!obs_pos_var %>% tolower) %>% dplyr::rename(link = !!obs_pos_var)

  # left join in observer position codes
  .data %<>% dplyr::left_join(dbs_obs_pos %>% dplyr::select(link, sideofship), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # rename sideofship code to .oserver_position
  .data %>% dplyr::rename(!!obs_pos_var := sideofship)

}

#' behavior_codify
#'
#' Replace seabird behavior with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .seabird_behavior Name of the column field that identifies the seabird behavior.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
behavior_codify <- function(.data, .channel, .seabird_behavior) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # quote the expression
  bird_behave_var <- dplyr::enquo(.seabird_behavior)

  # modify seabird behavior variable to match database descriptions
  .data %<>% mutate(!!bird_behave_var := case_when(
    !!bird_behave_var == 'Plunge Diving' ~ 'diving',
    !!bird_behave_var == 'Blowing' ~ 'swimming',
    !!bird_behave_var == 'Kleptoparasitizing' ~ 'piracy',
    !!bird_behave_var == 'Stationary' ~ 'milling',
    !!bird_behave_var == 'Resting' ~ 'sitting',
    !!bird_behave_var == 'Under attack' ~ NA_character_,
    !!bird_behave_var == 'Taking Off' ~ 'unknown flight',
    !!bird_behave_var == 'Flying' ~ 'unknown flight',
    !!bird_behave_var == 'Following' ~ 'following ship',
    TRUE ~ as.character(!!bird_behave_var)
  ))

  # get database table of seabird behavior codes and clean column names so they're snake case
  dbs_bird_behave <- DBI::dbGetQuery(conn = .channel, statement = "SELECT BEHAVIORDESC, BEHAVIORCODE FROM MAMMAL_SOUND.BIRDBEHAVIOR") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_bird_behave %<>% dplyr::rename(link = behaviordesc)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!bird_behave_var := !!bird_behave_var %>% tolower) %>% dplyr::rename(link = !!bird_behave_var)

  # left join in seabird behavior codes
  .data %<>% dplyr::left_join(dbs_bird_behave %>% dplyr::select(link, behaviorcode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # reaname behaviorcode to .seabird_behavior
  .data %>% dplyr::rename(!!bird_behave_var := behaviorcode)

}

#' direction_codify
#'
#' Replace flight direction with code from database.
#' Flight direction should be in degrees and is to be entered relative to the ship.
#'
#' @inheritParams remove_apostrophe
#' @param .flight_direction Name of the column field that identifies the flight direction.
#'
#' @importFrom dplyr "%>%"
#'
#' @return The mutated tibble.
#'
#' @export
direction_codify <- function(.data, .flight_direction) {

  # quote the expression
  flight_dir_var <- dplyr::enquo(.flight_direction)

  # modify flight direction variable to match database descriptions
  .data %>% mutate(!!flight_dir_var := case_when(
    !!flight_dir_var == 'N' ~ '0',
    !!flight_dir_var == 'NE' ~ '45',
    !!flight_dir_var == 'E' ~ '90',
    !!flight_dir_var == 'SE' ~ '135',
    !!flight_dir_var == 'S' ~ '180',
    !!flight_dir_var == 'SW' ~ '225',
    !!flight_dir_var == 'W' ~ '270',
    !!flight_dir_var == 'NW' ~ '315',
    !!flight_dir_var == 'No Direction' ~ NA_character_,
    !!flight_dir_var == '' ~ NA_character_,
    TRUE ~ as.character(!!flight_dir_var)
  ))

}

#' age_codify
#'
#' Replace bird age with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .bird_age Name of the column field that identifies the bird age.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
age_codify <- function(.data, .channel, .bird_age) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # quote the expression
  bird_age_var <- dplyr::enquo(.bird_age)

  # modify bird age variable to match database descriptions
  .data %<>% mutate(!!bird_age_var := case_when(
    !!bird_age_var == 'Immature' ~ 'subadult',
    !!bird_age_var == 'Juvenile' ~ 'subadult',
    !!bird_age_var == 'First cycle' ~ 'subadult',
    !!bird_age_var == 'Second cycle' ~ 'subadult',
    !!bird_age_var == 'Third cycle' ~ 'subadult',
    !!bird_age_var == 'Fourth cycle' ~ 'subadult',
    !!bird_age_var == 'First year' ~ 'subadult',
    !!bird_age_var == 'Second year' ~ 'subadult',
    !!bird_age_var == 'Third year' ~ 'subadult',
    !!bird_age_var == 'Fourth year' ~ 'subadult',
    TRUE ~ as.character(!!bird_age_var)
  ))

  # get database table of bird age codes and clean column names so they're snake case
  dbs_bird_age <- DBI::dbGetQuery(conn = .channel, statement = "SELECT AGECODE, AGE FROM MAMMAL_SOUND.BIRDAGE") %>% janitor::clean_names(case = 'snake')

  # create field to link tables (ensure l ower case)
  dbs_bird_age %<>% dplyr::rename(link = age) %>% mutate(link = link %>% tolower)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!bird_age_var := !!bird_age_var %>% tolower) %>% dplyr::rename(link = !!bird_age_var)

  # left join in bird age codes
  .data %<>% dplyr::left_join(dbs_bird_age %>% dplyr::select(link, agecode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # rename agecode to .bird_age
  .data %>% dplyr::rename(!!bird_age_var := agecode)

}

#' association_codify
#'
#' Replace seabird association with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .seabird_association Name of the column field that identifies the seabird association.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
association_codify <- function(.data, .channel, .seabird_association) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # quote the expression
  bird_assoc_var <- dplyr::enquo(.seabird_association)

  # modify seabird association variable to match database descriptions
  .data %<>% mutate(!!bird_assoc_var := case_when(
    !!bird_assoc_var == 'Associated with or on a buoy' ~ 'buoy',
    !!bird_assoc_var == 'Associated with observation platform' ~ 'observation platform',
    !!bird_assoc_var == 'Sitting on observation platform' ~ 'observation platform',
    !!bird_assoc_var == 'Associated with other species feeding in same location' ~ 'associated with other individuals',
    !!bird_assoc_var == 'Approaching observation platform' ~ 'observation platform',
    !!bird_assoc_var == 'Associated with sea weed' ~ 'floating weed',
    !!bird_assoc_var == 'Associated with fishing vessel' ~ 'fishing vessel',
    !!bird_assoc_var == 'Associated with cetaceans' ~ 'cetaceans',
    !!bird_assoc_var == 'Associated with fish shoal' ~ 'fish shoal',
    !!bird_assoc_var == 'Associated with other vessel (excluding fishing vessel; see code 26)' ~ 'non-fishing vessel',
    TRUE ~ as.character(!!bird_assoc_var)
  ))

  # get database table of seabird association codes and clean column names so they're snake case
  dbs_bird_assoc <- DBI::dbGetQuery(conn = .channel, statement = "SELECT ASSOCDESC, ASSOCCODE FROM MAMMAL_SOUND.BIRDASSOCIATION") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_bird_assoc %<>% dplyr::rename(link = assocdesc)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!bird_assoc_var := !!bird_assoc_var %>% tolower) %>% dplyr::rename(link = !!bird_assoc_var)

  # left join in seabird association codes
  .data %<>% dplyr::left_join(dbs_bird_assoc %>% dplyr::select(link, assoccode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # reaname assoccode to .seabird_association
  .data %>% dplyr::rename(!!bird_assoc_var := assoccode)

}

#' height_codify
#'
#' Replace flight height with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .flight_height Name of the column field that identifies the flight height.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
height_codify <- function(.data, .channel, .flight_height) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # get database table of flight height codes and clean column names so they're snake case
  dbs_fly_high <- DBI::dbGetQuery(conn = .channel, statement = "SELECT HEIGHTRANGE, HEIGHTCODE FROM MAMMAL_SOUND.BIRDFLIGHTHEIGHT") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_fly_high %<>% dplyr::rename(link = heightrange)

  # quote the expression
  fly_high_var <- dplyr::enquo(.flight_height)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!fly_high_var := !!fly_high_var %>% tolower) %>% dplyr::rename(link = !!fly_high_var)

  # left join in flight height codes
  .data %<>% dplyr::left_join(dbs_fly_high %>% dplyr::select(link, heightcode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # rename heightcode to .flight_height
  .data %>% dplyr::rename(!!fly_high_var := heightcode)

}

#' plumage_codify
#'
#' Replace seabird plumage with code from database.
#'
#' @inheritParams remove_apostrophe
#' @inheritParams observer_codify
#' @param .seabird_plumage Name of the column field that identifies the seabird plumage.
#'
#' @importFrom dplyr "%>%"
#' @importFrom magrittr "%<>%"
#'
#' @return The mutated tibble.
#'
#' @export
plumage_codify <- function(.data, .channel, .seabird_plumage) {

  # check database connection is from DBI
  channel_valid <- inherits(.channel, "DBIConnection")

  # issue error if invalid DBI connection
  stopifnot(channel_valid)

  # make sure there is no column named 'link', otherwise exit
  if('link' %in% colnames(.data)) stop("Error: Cannot have a column named 'link'.")

  # quote the expression
  bird_plum_var <- dplyr::enquo(.seabird_plumage)

  # modify seabird plumage variable to match database descriptions
  .data %<>% mutate(!!bird_plum_var := case_when(
    !!bird_plum_var == 'Non-breeding/Basic fall and winter plumage' ~ 'non-breeding',
    !!bird_plum_var == 'Breeding/Alternate spring and summer plumage' ~ 'breeding',
    !!bird_plum_var == 'Gannet plumage 1' ~ NA_character_,
    !!bird_plum_var == 'Gannet plumage 2' ~ NA_character_,
    TRUE ~ as.character(!!bird_plum_var)
  ))

  # get database table of seabird plumage codes and clean column names so they're snake case
  dbs_bird_plum <- DBI::dbGetQuery(conn = .channel, statement = "SELECT PLUMAGEDESC, PLUMAGECODE FROM MAMMAL_SOUND.BIRDPLUMAGE") %>% janitor::clean_names(case = 'snake')

  # create field to link tables
  dbs_bird_plum %<>% dplyr::rename(link = plumagedesc)

  # set the linking 'by' variable in .data to 'link'
  .data %<>% dplyr::mutate(!!bird_plum_var := !!bird_plum_var %>% tolower) %>% dplyr::rename(link = !!bird_plum_var)

  # left join in seabird plumage codes
  .data %<>% dplyr::left_join(dbs_bird_plum %>% dplyr::select(link, plumagecode), by = "link")

  # drop 'by' var in .data
  .data %<>% dplyr::select(-link)

  # rename plumagecode to .seabird_plumage
  .data %>% dplyr::rename(!!bird_plum_var := plumagecode)

}

#' sex_codify
#'
#' Replace seabird sex with code from database.
#'
#' @inheritParams remove_apostrophe
#' @param .sex Name of the column field that identifies the seabird sex.
#'
#' @importFrom dplyr "%>%"
#'
#' @return The mutated tibble.
#'
#' @export
sex_codify <- function(.data, .sex) {

  # quote the expression
  bird_sex_var <- dplyr::enquo(.sex)

  # modify flight direction variable to match database descriptions
  .data %>% mutate(!!bird_sex_var := case_when(
    !!bird_sex_var == 'Unknown' ~ '1',
    !!bird_sex_var == 'Female' ~ '2',
    !!bird_sex_var == 'Male' ~ '3',
    TRUE ~ as.character(!!bird_sex_var)
  ))

}
