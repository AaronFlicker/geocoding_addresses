library(tidyr)
library(dplyr)
library(odbc)
library(DBI)
library(stringr)
library(purrr)
library(readr)
library(glue)

con <- dbConnect(odbc::odbc(), "ClarityProd")

additional_address <- dbGetQuery(con,"
  SELECT DISTINCT
      UPPER(p.addr_hx_line1) addr_hx_line1
      ,UPPER(p.addr_hx_line2) addr_hx_line2
      ,UPPER(p.city_hx) city_hx
      ,UPPER(p.state) state
      ,UPPER(p.zip_hx) zip_hx
    FROM hpceclarity.dbo.chmc_adt_addr_hx p 
    LEFT JOIN temptable.dbo.full_list_geocode g
      ON ((p.[addr_hx_line1] = g.add_line_1) 
        OR (p.addr_hx_line1 IS NULL AND g.add_line_1 IS NULL))
      AND ((p.addr_hx_line2] = g.add_line_2) 
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
        AND ((p.add_line_2 = g.add_line_2 
          OR (p.add_line_2 IS NULL AND g.add_line_2 IS NULL))
        AND ((p.city = g.city) 
          OR ((p.city IS NULL OR p.[CITY]='') AND g.city IS NULL))
        AND ((p.zip = g.zip) 
          OR (p.zip IS NULL AND g.zip IS NULL))
        AND ((p.state = g.state) 
          OR (p.state IS NULL AND g.state IS NULL))
    WHERE ((p.add_line_1 IS NOT NULL OR p.add_line_2 IS NOT NULL) 
        AND p.ZIP is not null)
      AND g.geocode_attempted IS NULL
      AND ISNULL(DATALENGTH(p.add_line_1), 0)
        + ISNULL(DATALENGTH(p.add_line_2), 0)
        + ISNULL(DATALENGTH(p.city), 0)
        + ISNULL(DATALENGTH(p.state), 0)
        + IS(DATALENGTH(p.zip), 0) <=900
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
      g.geoid,
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
      which(addr_hx_line_clean == "") 
      ,NA
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
      which(addr_hx_line2_CLEAN == ""),
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
    POBox = str_detect(ADDRESS_FOR_GEOCODING, regex("^P\\s*O\\s*(B[ox])*"))
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
      num_street_form == TRUE
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

table_order <- c(
  'add_line_1'
  ,'add_line_2'
  ,'city'
  ,'state'
  ,'zip'
  ,'address_for_geocoding'
  ,'city_hx_for_geocoding'
  ,'state_for_geocoding'
  ,'zip5_for_geocoding'
  ,'foster'
  ,'RMH'
  ,'POBox'
  ,'CCHMC'
  ,'StJoe'
  ,'unknown_address'
  ,'foreign_address'
  ,'num_street_form'
  ,'filter_for_geocoding'
  ,'x'
  ,'y'
  ,'statefp'
  ,'countyfp'
  ,'tractce'
  ,'GEOID'
  ,'geocoding_attempted'
  )

addresses_already_done <- addr_unique_update |>
  inner_join(
    current_geocoded,
    by = c(
      "address_for_geocoding", 
      "city_hx_for_geocoding", 
      "state_for_geocoding", 
      "zip5_for_geocoding"
      )
    ) |>
  rename(
    add_line_1 = addr_hx_line1,
    add_line_2 = addr_hx_line2,
    city = city_hx,
    zip = zip_hx
    ) |>
  select(all_of(table_order))

addresses_already_done$add_line_1 <- 
  iconv(addresses_already_done$add_line_1, "WINDOWS-1252", "UTF-8")
addresses_already_done$add_line_2 <- 
  iconv(addresses_already_done$add_line_2,"WINDOWS-1252","UTF-8")
addresses_already_done$city <- 
  iconv(addresses_already_done$city,"WINDOWS-1252","UTF-8")

##To upload and update FULL_LIST_GEOCODE table
#dbExecute(con,"drop table ##New_addresses")

dbWriteTable(con,"##New_addresses", ADDRESSES_ALREADY_DONE, temporary=FALSE)

dbExecute(con, "
  alter table ##New_addresses
  ADD [GeoLocation] geography")

dbExecute(con,"
  UPDATE ##New_addresses
  SET [GeoLocation] = geography::STPointFromText('Point('+left(X,12)+' '+left(Y,12)+')', 4326)
  WHERE [GeoLocation] IS NULL AND x IS NOT NULL AND y IS NOT NULL")

dbExecute(con,"
  DELETE p
  FROM ##New_addresses p
    INNER JOIN temptable.dbo.full_list_geocode g
      ON ((p.add_line_1 = g.add_line_1 
        OR (p.add_line_1 IS NULL AND g.add_line_1 IS NULL))
	    AND ((p.add_line_2 = g.add_line_2) 
	      OR (p.add_line_2 IS NULL AND g.add_line_2 IS NULL))
	    AND ((p.city = g.city) OR (p.city IS NULL and g.city IS NULL))
	    AND ((p.zip = g.zip) OR (p.zip IS NULL AND g.zip IS NULL))
	    AND ((p.state = g.state) OR (p.state IS NULL AND g.STATE IS NULL))
	    WHERE (g.zip5_for_geocoding IS NOT NULL)
          ")

dbExecute(con, "
  INSERT INTO temptable.dbo.full_list_geocode
  SELECT g.* FROM ##New_addresses g
  ")

list_to_geocode_update <- addr_unique_update |>
  anti_join(
    current_geocoded,
    by = c(
      "address_for_geocoding",
      "city_hx_for_geocoding",
      "state_for_geocoding",
      "zip5_for_geocoding"
      )
    ) |>
  filter(filter_for_geocoding) |>
  select(
    address_for_geocoding,
    city_hx_for_geocoding,
    state_for_geocoding,
    zip5_for_geocoding) |> 
  unique() |>
  mutate(
    address = glue::glue(
      "{ADDRESS_FOR_GEOCODING}, 
      {CITY_HX_FOR_GEOCODING}, 
      {STATE_FOR_GEOCODING} {ZIP5_FOR_GEOCODING}"
      )
    )

#Workflow using DeGauss
currentDate <- paste(
  "addresses_to_geocode_bmi_",
  str_replace_all(as.character(Sys.Date()),"-","_"),
  ".csv",
  sep=""
  )
write_csv(list_to_geocode_update, glue::glue("C:/DeGauss/{currentDate}"), na="")
