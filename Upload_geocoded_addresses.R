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
date_for_file <- Sys.Date()

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

degauss_columns <- cols(
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

degauss_import<-read_csv(
  paste0(
    "addresses_to_geocode_bmi_",
    str_replace_all(as.character(date_for_file),'-','_'),
    "_geocoded_v3.0.2.csv"),
  col_types = degauss_columns) |>
    rename(
      street = matched_street,
      zip = matched_zip,
      city = matched_city,
      state = matched_state
    )  |>
    filter(precision == 'range', 
           geocode_result != 'imprecise_geocode') |>
  sf::st_as_sf(coords = c("lon","lat"), crs = 'NAD83', remove=FALSE)

all_states <- states(year = 2019)
sts <- all_states$STATEFP
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
         STATEFP,
         COUNTYFP,
         TRACTCE,
         GEOID) |>
  mutate(geocode_attempted = TRUE) |>
  group_by(address_for_geocoding, city_hx_for_geocoding, zip5_for_geocoding) |>
  arrange(TRACTCE) |>
  mutate(row_num = row_number()) |>
  filter(row_num == 1) |>
  ungroup()

additional_address <- dbGetQuery(con,"
  SELECT DISTINCT
      UPPER(p.addr_hx_line1) addr_hx_line1
      ,UPPER(p.addr_hx_line2) addr_hx_line2
      ,UPPER(p.city_hx) city_hx
      ,UPPER(p.state) state
      ,UPPER(p.zip_hx) zip_hx
    FROM hpceclarity.dbo.chmc_adt_addr_hx p 
    LEFT JOIN temptable.dbo.full_list_geocode g
      ON ((p.addr_hx_line1 = g.add_line_1) 
        OR (p.addr_hx_line1 IS NULL AND g.add_line_1 IS NULL))
      AND ((p.addr_hx_line2 = g.add_line_2) 
        OR (p.addr_hx_line2 IS NULL AND g.add_line_2 IS NULL))
      AND ((p.city_hx = g.city) 
        OR ((p.city_hx IS NULL OR p.city_hx = '') AND g.city IS NULL))
      AND ((p.zip_hx = g.zip) 
        OR (p.zip_hx IS NULL AND g.zip IS NULL))
      AND ((p.state = g.state) 
        OR (p.state IS NULL AND g.state IS NULL))
    WHERE ((p.addr_hx_line1 IS NOT NULL OR p.addr_hx_line2 IS NOT NULL) 
        AND p.zip_hx IS NOT NULL)
      AND g.geocode_attempted IS NULL
      AND ISNULL(DATALENGTH(p.addr_hx_line1), 0)
        + ISNULL(DATALENGTH(p.addr_hx_line2), 0)
        + ISNULL(DATALENGTH(p.city_hx), 0) 
        + ISNULL(DATALENGTH(p.state), 0) 
        + ISNULL(DATALENGTH(p.zip_hx), 0) <= 900
  UNION
  SELECT DISTINCT 
      UPPER(p.add_line_1) addr_hx_line1
      ,UPPER(p.add_line_2) addr_hx_line2
      ,UPPER(p.city) city_hx
      ,UPPER(p.state) state
      ,UPPER(p.zip) zip_hx
    FROM hpceclarity.bmi.patient p
      LEFT JOIN temptable.dbo.full_list_geocode g
        ON ((p.add_line_1 = g.add_line_1) 
          OR (p.add_line_1 IS NULL AND g.add_line_1 IS NULL))
        AND ((p.add_line_2 = g.add_line_2)
          OR (p.add_line_2 IS NULL AND g.add_line_2 IS NULL))
        AND ((p.city = g.city) 
          OR ((p.city IS NULL OR p.[CITY]='') AND g.city IS NULL))
        AND ((p.zip = g.zip) 
          OR (p.zip IS NULL AND g.zip IS NULL))
        AND ((p.state = g.state) 
          OR (p.state IS NULL AND g.state IS NULL))
    WHERE ((p.add_line_1 IS NOT NULL OR p.add_line_2 IS NOT NULL) 
        AND p.zip is not null)
      AND g.geocode_attempted IS NULL
      AND ISNULL(DATALENGTH(p.add_line_1), 0)
        + ISNULL(DATALENGTH(p.add_line_2), 0)
        + ISNULL(DATALENGTH(p.city), 0)
        + ISNULL(DATALENGTH(p.state), 0)
        + ISNULL(DATALENGTH(p.zip), 0) <=900
      AND is_valid_pat_yn = 'Y'
  UNION
  SELECT DISTINCT
      UPPER(ha.pat_addr_1) addr_hx_line1
      ,UPPER(ha.pat_addr_2) addr_hx_line2
      ,UPPER(ha.pat_city) city_hx
      ,UPPER(zc_s.name) state
      ,UPPER(ha.pat_zip) zip_hx
    FROM hpceclarity.bmi.hsp_account ha 
      LEFT JOIN temptable.dbo.zc_state zc_s 
        ON zc_s.state_c = ha.pat_state_c
      LEFT JOIN temptable.dbo.full_list_geocode g
        ON ((ha.pat_addr_1 = g.add_line_1) 
          OR (ha.pat_addr_1 IS NULL AND g.add_line_1 IS NULL))
        AND ((ha.pat_addr_2 = g.add_line_2)  
          OR (ha.pat_addr_2 IS NULL AND g.add_line_2 IS NULL))
        AND ((ha.pat_city = g.city) 
          OR (ha.PAT_CITY IS NULL AND g.city IS NULL))
        AND ((ha.pat_zip = g.zip) 
          OR (ha.pat_zip IS NULL AND g.zip IS NULL))
        AND ((zc_s.name = g.state) 
          OR (zc_s.name IS NULL AND g.state IS NULL))
    WHERE ((ha.pat_addr_1 IS NOT NULL OR ha.pat_addr_2 IS NOT NULL) 
        AND ha.pat_zip IS NOT NULL)
      AND g.geocode_attempted IS NULL
      AND ISNULL(DATALENGTH(ha.pat_addr_1), 0)
        + ISNULL(DATALENGTH(ha.pat_addr_2), 0)
        + ISNULL(DATALENGTH(ha.pat_city), 0)
        + ISNULL(DATALENGTH(zc_s.name), 0)
        + ISNULL(DATALENGTH(ha.pat_zip), 0) <= 900
  ") 

current_geocoded <- dbGetQuery(con,"
  SELECT DISTINCT
      g.address_for_geocoding,
      g.city_hx_for_geocoding,
      g.state_for_geocoding,
      g.zip5_for_geocoding,
      g.x,
      g.y,
      g.statefp,
      g.countyfp,
      g.tractce,
      g.GEOID,
      g.geocode_attempted
  FROM temptable.dbo.full_list_geocode g
                               ")

addr_unique_update <- additional_address |>
  #Some non-compliant characters stored on server, need to remove
  mutate(
    addr_hx_line1_clean = str_replace_all(addr_hx_line1,"[^[:graph:]]", " "),
    addr_hx_line2_clean=str_replace_all(addr_hx_line2,"[^[:graph:]]", " ")
    ) |>
  mutate(
    addr_hx_line1_clean = str_replace_all(addr_hx_line1_clean, fixed('\\'), ''),
    # commonly mistyped address
    addr_hx_line1_clean = gsub(
      ' WN Bend',
      ' W North Bend', 
      addr_hx_line1_clean,
      ignore.case = TRUE
      ), 
    # commonly mistyped address
    addr_hx_line1_clean = gsub(
      ' Northbend',
      'North Bend',
      addr_hx_line1_clean,
      ignore.case = TRUE
      ), 
    # removes 1/2 from addresses
    addr_hx_line1_clean = gsub('1/2', '', addr_hx_line1_clean),
    #removes street_num-apt_num contructs
    addr_hx_line1_clean=gsub('(\\d+?)(?:\\-\\d+)', '\\1', addr_hx_line1_clean),
    addr_hx_line1_clean = str_replace_all(addr_hx_line1_clean, fixed('"'), ''),
    addr_hx_line1_clean = str_replace_all(
      addr_hx_line1_clean, 
      '[^[:alnum:] ]', 
      ''
      ),
    addr_hx_line1_clean = str_replace_all(
      addr_hx_line1_clean,
      regex('((APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s+#*\\s*\\S+\\b|\\s(APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s*#*\\s*\\S+\\b|\\sS*LOT\\.*#*(\\s*\\d+|\\s+\\S)\\w*\\b)',
            ignore.case=TRUE),
      ''
      ),
    addr_hx_line1_clean = str_replace_all(addr_hx_line1_clean, '\\s[\\s]+',' '),
    addr_hx_line1_clean = str_squish(addr_hx_line1_clean),
    addr_hx_line1_clean = replace(
      addr_hx_line1_clean, 
      which(addr_hx_line1_clean == ""),
      NA
      )
    ) |>
  mutate(
    addr_hx_line2_clean = str_replace_all(addr_hx_line2_clean, fixed('\\'), ''),
    # commonly mistyped address
    addr_hx_line2_clean = gsub(
      ' WN Bend',
      ' W North Bend',
      addr_hx_line2_clean,
      ignore.case = TRUE
      ),
    # commonly mistyped address
    addr_hx_line2_clean = gsub(
      ' Northbend',
      'North Bend',
      addr_hx_line2_clean,
      ignore.case = TRUE
      ),
    # removes 1/2 from addresses
    addr_hx_line2_clean = gsub('1/2', '', addr_hx_line2_clean),
    #removes street_num-apt_num constructs
    addr_hx_line2_clean = gsub('(\\d+?)(?:\\-\\d+)','\\1', addr_hx_line2_clean), 
    addr_hx_line2_clean = str_replace_all(addr_hx_line2_clean, fixed('"'), ''),
    addr_hx_line2_clean = str_replace_all(
      addr_hx_line2_clean, 
      '[^[:alnum:] ]', 
      ''
      ),
    addr_hx_line2_clean = str_replace_all(
      addr_hx_line2_clean,
      regex(
        '((APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s+#*\\s*\\S+\\b|\\s(APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s*#*\\s*\\S+\\b|\\sS*LOT\\.*#*(\\s*\\d+|\\s+\\S)\\w*\\b)',
        ignore.case=TRUE
        ),
      ''),
    addr_hx_line2_clean = str_replace_all(addr_hx_line2_clean, '\\s[\\s]+',' '),
    addr_hx_line2_clean = str_squish(addr_hx_line2_clean),
    addr_hx_line2_clean = replace(
      addr_hx_line2_clean,
      which(addr_hx_line2_clean == ""),
      NA)
    ) |>
  mutate(
    address_for_geocoding = coalesce(addr_hx_line1_clean, addr_hx_line2_clean),
    address_for_geocoding = str_squish(address_for_geocoding),
    address_for_geocoding = replace(
      address_for_geocoding,
      which(address_for_geocoding == ""),
      NA
      ),
    zip5 = str_sub(zip_hx, 1, 5),
    address_for_geocoding_zip = str_squish(
      paste(address_for_geocoding, zip5, sep=' ')
      )
    ) |>
  # Set up dummy variables for various concerning addresses
  mutate(foster = str_detect(
    address_for_geocoding_zip, 
    regex(
    "(222\\s*(E[\\.ASTY\\s]*)*\\s*C[ENTRALU]+\\s*P[ARKWYU\\s]+.*452.*)|(2400\\s+CL[AIERMOUNT]+\\s.*(D[RIVE])+.*\\s+OH(IO)*\\s*451.*)|(601\\sW[ASHINGTO]*N\\sA.*NEW.*410.*)|(130\\sW.*43.*ST.*COVING.*410.*)|(8311\\sU\\s*S.*42.*KENTUCKY\\s410.*)|(300\\sN[ORTH\\.]*.*\\sFAIR\\sA.*450.*)|(416\\sS[\\.OUTH]*\\s*E[\\.AST]*\\s.*450.*)|(775\\sM[\\.OUNT]*\\s*O[RABN]*\\s.*451.*)|(1025\\sS[OUTH\\.]*\\s*S[\\.OUTH]*\\sST.*451.*)|(1500\\sPARK\\sAV.*453.*)|(3304\\sN[ORTHJ\\.]*\\s*M[AIN]*\\s.*454.*)|(601\\sLE[DBETR]*\\s.*453.*)|(230\\sMARY+\\sAV.*470.*)(230\\s((MARY)|(MERRY))\\s((AV)|(ST)).*470.*)|(12048\\sS[AINT]+\\sMARYS*\\s.*47.*)|(125\\sN[ORTH\\.]*\\sWALNUT.*470.*)|(506\\sFERRY\\sST[RETS56\\s]*\\s.*470.*)|(.*J[OBS]*\\s*(AND)*\\s*F[AMILYES]*\\s*S[ERVICS]*.*)|(.*PROTECTIVE.*)|(.*\\sDCHS.*)|(.*\\sMCCS.*)|(.*\\sHUMAN\\sSER.*)|(.*SERVICES.*)")
    )
    ) |>
  mutate(RMH = str_detect(
    address_for_geocoding_zip, 
    regex(
    "(350\\s*E[RKIU]+[RKEINBCGH]+.*45[0-9]+9)|.*(RONALD.*HOUSE).*"
    )
    )
    ) |>
  mutate(
    POBox = str_detect(address_for_geocoding, regex("^P\\s*O\\s*(B[ox])*"))
    ) |>
  mutate(
    CCHMC = str_detect(
      address_for_geocoding_zip, 
      regex("(3333\\s*BURNETT*\\s*A.*45229)")
      )
    ) |>
  mutate(StJoe = str_detect(
    address_for_geocoding_zip, 
    regex("(10722\\sWYS.*OH.*)|(.*S[AIN]*T\\sJO[SEPH]*\\s(OR(PHANGE)*|HOME).*)")
    )
    ) |>
  mutate(
    unknown_address = str_detect(
      address_for_geocoding_zip, 
      regex(
        ".*(UNKNOWN).*|.*(VERIFY).*")
      )
    | (zip5 == '00000' | zip5 == '99999' | is.na(zip5))
    ) |>
  mutate(
    foreign_address = state == 'FOREIGN COUNTRY' | 
      str_detect(address_for_geocoding, 'EMBASSY') | 
      str_detect(address_for_geocoding, 'CONSULATE')
    ) |>
  mutate(
    num_street_for = str_detect(
      address_for_geocoding,
      regex("[0-9]+\\s[A-Z]+")
      )
    ) |>
  mutate(
    filter_for_geocoding = !is.na(address_for_geocoding) &
      POBox == FALSE & 
      foreign_address == FALSE & 
      unknown_address == FALSE & 
      num_street_for == TRUE
    ) |>
  dplyr::select(
    -c(address_for_geocoding_zip, addr_hx_line1_clean, addr_hx_line2_clean)
    )

addr_unique_update <- addr_unique_update |>
  mutate(
    address_for_geocoding = str_squish(address_for_geocoding),
    city_hx_for_geocoding = str_squish(city_hx),
    city_hx_for_geocoding = case_when(
      str_detect(city_hx,"^\\s*$") ~ NA_character_, 
      TRUE ~ city_hx_for_geocoding
      ),
    state_for_geocoding = str_squish(state),
    zip5_for_geocoding = str_squish(zip5)
  ) |>
  unique()

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
  unique() |>
  group_by(address_for_geocoding, city_hx_for_geocoding, state_for_geocoding, zip5_for_geocoding) |>
  mutate(count = n()) |>
  arrange(-count, address_for_geocoding)

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

export_columns <-
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
    "STATEFP",
    "COUNTYFP",
    "TRACTCE",
    "GEOID",
    "geocode_attempted")

tester <- update_list[,export_columns]

export_date<-paste("UPDATE_LIST_GEOCODE_",str_replace_all(as.character(Sys.Date()),"-","_"),".csv",sep="")
#dbExecute(con,"drop table ##UPLOAD_ADDRESSES")
dbWriteTable(
  con,
  "##UPLOAD_ADDRESSES",
  update_list[,export_columns],
  temporary = FALSE
  )

dbExecute(con, "
  IF object_id('temptable.dbo.update_geocode') IS NOT NULL
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
    + ISNULL(DATALENGTH(add_line_2), 0)
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
    AND ((p.add_line_2 = g.add_line_2) 
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

write_csv(UPDATE_LIST[,Export_Columns],Export_Date,na="")
