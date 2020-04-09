# seebirdr
IN DEVELOPMENT PHASE

## To do:
- [ ] add license to DESCRIPTION
- [ ] improve documentation (e.g. examples)
- [ ] create hex sticker

## Usage

### Installation

```R
remotes::install_github("jmhatch/seebirdr")
```

### Database connectivity

Several of the functions require a connection to the internal NEFSC database following:

```R
.channel <- DBI::dbConnect(odbc::odbc(), driver, DBQ, UID, PWD)
```

### Example

An example workflow of how to use the functionality of seebirdr is provided below:

```R
# load libraries
library(seebirdr)
library(dplyr)
library(magrittr)

# database connection
dbs_con <- .channel

# directory path to data
dir_path <- system.file("extdata", "GU1905_SeaScribe", package = "seebirdr")

# load data
gu1905 <- dir_path %>% upload_data_excel()

# format full dataset
gu1905 %<>% remove_apostrophe(observation_comment, species_common_name) %>%
  datetime_stampify(observation_timestamp) %>%
  observer_codify(dbs_con, observer)

# subset full dataset to get only sightings
sightings <- gu1905 %>% filter(!is.na(species_code) & species_code != "")

# format sightings data
sightings %<>% position_codify(dbs_con, position) %>%
  behavior_codify(dbs_con, behavior) %>%
  direction_codify(direction) %>%
  age_codify(dbs_con, age) %>%
  association_codify(dbs_con, association) %>%
  height_codify(dbs_con, flight_height) %>%
  plumage_codify(dbs_con, plumage) %>%
  sex_codify(sex)
```

***
_This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government._
