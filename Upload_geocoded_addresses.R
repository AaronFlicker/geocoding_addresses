# Move newly geocoded addresses to TempTable.
options(tigris_use_cache = TRUE)
library(tidyr)
library(dplyr)
library(odbc)
library(DBI)
library(stringr)
library(purrr)
library(readr)
library(glue)
library(tigris)

con <- dbConnect(odbc::odbc(), "ClarityProd")
date_for_file <- Sys.Date()-1

cols(
  address = col_character(),
  address_for_geocoding = col_character(),
  city_hx_for_geocoding = col_character(),
  state_for_geocoding = col_character(),
  zip5_for_geocoding = col_character(),
  street = col_character(),
  zip = col_character(),
  city = col_character(),
  state = col_character(),
  lat = col_character(),
  lon = col_character(),
  score = col_double(),
  prenum = col_character(),
  number = col_double(),
  precision = col_character()
)

dedauss_columns <- cols(
  address_for_geocoding = col_character(),
  city_hx_for_geocoding = col_character(),
  state_for_geocoding = col_character(),
  zip5_for_geocoding = col_character(),
  address = col_character(),
  matched_street = col_character(),
  matched_zip = col_character(),
  matched_city = col_character(),
  matched_state = col_character(),
  lat = col_character(),
  lon = col_character(),
  score = col_double(),
  precision = col_character(),
  geocode_result = col_character()
)

DeGauss_import<-read_csv(
  paste0(
    "C:/DeGauss/ADDRESSES_TO_GEOCODE_BMI_",
    str_replace_all(as.character(date_for_file),'-','_'),
    "_geocoded_v3.0.2.csv"),
  col_types = degauss_columns) |>
    rename(
      street = matched_street,
      zip = matched_zip,
      city = matched_city,
      state = matched_state
    )  |>
    filter(precision == 'range', geocode_result != 'imprecise_geocode') |>
  sf::st_as_sf(coords = c("lon","lat"), crs = 'NAD83', remove=FALSE)

all_states <- states(year = 2019)
sts <- all_states$statefp
combined <- rbind_tigris(
  lapply(sts, function(x) {
    tracts(x, cb = TRUE, year=2019)
  })
)

add_tracts_to_degauss <- sf::st_join(degauss_import, combined) %>%
  select(address_for_geocoding,
         city_hx_for_geocoding,
         state_for_geocoding,
         zip5_for_geocoding,
         GEOID,
         X = lon,
         Y = lat,
         statefp,
         countyfp,
         tractce,
         GEOID) |>
  mutate(geocode_attempted = TRUE) |>
  group_by(address_for_geocoding, city_hx_for_geocoding, zip5_for_geocoding) |>
  arrange(tractce) |>
  mutate(row_num = row_number()) |>
  filter(row_num == 1) |>
  ungroup()

update_list <- addr_unique_update |>
  inner_join(
    add_tracts_to_degauss, 
    by = c(
      "address_for_geocoding",
      "city_hx_for_geocoding",
      "state_for_geocoding",
      "zip5_for_geocoding"
      )
    ) |>
  mutate(geocode_attempted = replace_na(geocode_attempted, FALSE)) |>
  filter(
    as.numeric(X) != 0,
    as.numeric(Y) != 0,
    address_for_geocoding != "565 CLL ABOLICIN 2ND FLOOR"
    ) |>
  unique()

# Some characters don't read in well
update_list$addr_hx_line1 <- 
  iconv(update_list$addr_hx_line1, "WINDOWS-1252", "UTF-8")
update_list$addr_hx_line2 <- 
  iconv(update_list$addr_hx_line2, "WINDOWS-1252", "UTF-8")
update_list$city_hx <-
  iconv(update_list$city_hx, "WINDOWS-1252", "UTF-8")
update_list$city_hx_for_geocoding <-
  iconv(update_list$city_hx_for_geocoding, "WINDOWS-1252", "UTF-8")
update_list$address_for_geocoding <- 
  iconv(update_list$address_for_geocoding, "WINDOWS-1252", "UTF-8")
# UPDATE_LIST$ADD_LINE_2<-iconv(ADDRESSES_ALREADY_DONE$ADD_LINE_2,"WINDOWS-1252","UTF-8")
# UPDATE_LIST$CITY<-iconv(ADDRESSES_ALREADY_DONE$CITY,"WINDOWS-1252","UTF-8")

export_eolumns <-
  c("addr_hx_line1",
    "addr_hx_line2",
    "city_hx",
    "state",
    "zip_hx",
    "address_for_geocoding",
    "city_hx_for_geocoding",
    "state_for_geocoding",
    "zip5_for_geocoding",
    "foster",
    "RMH",
    "POBox",
    "CCHMC",
    "StJoe",
    "unknown_address",
    "foreign_address",
    "num_street_for",
    "filter_for_geocoding",
    "X",
    "Y",
    "statefp",
    "countyfp",
    "tractce",
    "GEOID",
    "geocode_attempted")

tester <- update_list[,export_columns]

#Export_Date<-paste("UPDATE_LIST_GEOCODE_",str_replace_all(as.character(Sys.Date()),"-","_"),".csv",sep="")
#dbExecute(con,"drop table ##UPLOAD_ADDRESSES")
dbWriteTable(
  con,
  "##UPLOAD_ADDRESSES",
  update_list[,export_columns],
  temporary = FALSE
  )

dbExecute(con, "
  IF object_d('temptable.dbo.update_geocode') IS NOT NULL
  DROP TABLE temptable.dbo.update_geocode

  --USE IMPORTED TABLE
  SELECT * 
    INTO temptable.dbo.update_geocode from ##upload_addresses
          ")

dbExecute(con, "
  UPDATE temptable.dbo.update_geocode
  SET addr_hx_line1 = cASE WHEN addr_hx_line1 = '' 
    THEN NULL ELSE addr_hx_line1 END,
  addr_hx_line2 = CASE WHEN addr_hx_line2 = '' THEN NULL ELSE addr_hx_line2 end,
  city_hx = CASE WHEN city_hx = '' THEN NULL ELSE city_hx END,
  state = CASE WHEN state = '' THEN NULL ELSE state END,
  zip_hx = CASE WHEN zip_hx = '' THEN NULL ELSE zip_hx END,
  X = CASE WHEN X= '' THEN NULL ELSE X END,
  Y = CASE WHEN Y= '' THEN NULL ELSE Y END,
  statefp = CASE WHEN statefp = '' THEN NULL ELSE statefp END,
  countyfp = CASE WHEN countyfp = '' THEN NULL ELSE countyfp END,
  tractce = CASE WHEN tractce = '' THEN NULL ELSE tractce END,
  GEOID = CASE WHEN GEOID = '' THEN NULL ELSE GEOID END,
  geocode_attempted = CASE WHEN geocode_attempted = '' THEN NULL 
    ELSE geocode_attempted END
          ;")

#This throws an error but rename seems to work...
dbExecute(con, "
  USE temptable;
  EXEC SP_RENAME 'temptable.dbo.update_geocode.addr_hx_line1', add_line_1;
  EXEC SP_RENAME 'temptable.dbo.update_geocode.addr_hx_line2', add_line_2;
  EXEC SP_RENAME 'temptable.dbo.update_geocode.city_hx', city;
  EXEC SP_RENAME 'temptable.dbo.update_geocode.zip_hx', zip;
          ")

dbExecute(con, "
  DELETE FROM temptable.dbo.update_geocode 
  WHERE ISNULL(DATALENGTH(add_line_1), 0)
    + ISNULL(DATALENGGTH(add_line_2), 0)
    + ISNULL(DATALENGTH(city) ,0)
    + ISNULL(DATALENGTH(state),0)
    + ISNULL(DATALENGTH(zip), 0) > 900
          ")

dbExecute(con, "
  DELETE p 
  FROM temptable.dbo.update_geocode p
    INNER JOIN temptable.dbo.full_list_geocode g
      ON ((p.add_line_1 = g.add_line_1) 
        OR (p.add_line_1 IS NULL AND g.add_line_1 IS NULL))
    AND ((p.add_line_2 = g.add_line_2 
      OR (p.add_line_2 IS NULL AND g.add_line_2 IS NULL))
    AND ((p.city = g.city) OR (p.city IS NULL AND g.city IS NULL))
    AND ((p.zip = g.zip) OR (p.zip IS NULL AND g.zip IS NULL))
    AND ((p.state = g.state) OR (p.state IS NULL AND g.state IS NULL))
    WHERE (g.zip5_for_geocoding IS NOT NULL)
          ")

dbExecute(con, "
  ALTER TABLE temptable.dbo.update_geocode
  ADD [GeoLocation] geography;
          ")

dbExecute(con, "
  UPDATE temptable.dbo.update_geocode
  SET [GeoLocation]=geography::STPointFromText('Point('+left(X,12)+' '+left(Y,12)+')', 4326)
  WHERE [GeoLocation] IS NULL AND X IS NOT NULL AND Y IS NOT NULL
          ")

dbExecute(con, "
  INSERT INTO temptable.dbo.full_list_geocode
  SELECT g.* FROM temptable.dbo.update_geocode g
")

dbExecute(con, "
  DROP TABLE temptable.dbo.update_geocode;
          ")

#write_csv(UPDATE_LIST[,Export_Columns],Export_Date,na="")
