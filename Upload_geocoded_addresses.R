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
date_for_file<-Sys.Date()-1

cols(
  address = col_character(),
  ADDRESS_FOR_GEOCODING = col_character(),
  CITY_HX_FOR_GEOCODING = col_character(),
  STATE_FOR_GEOCODING = col_character(),
  ZIP5_FOR_GEOCODING = col_character(),
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

DeGauss_columns<-cols(
  ADDRESS_FOR_GEOCODING = col_character(),
  CITY_HX_FOR_GEOCODING = col_character(),
  STATE_FOR_GEOCODING = col_character(),
  ZIP5_FOR_GEOCODING = col_character(),
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

DeGauss_import<-read_csv(paste0("C:/DeGauss/ADDRESSES_TO_GEOCODE_BMI_",str_replace_all(as.character(date_for_file),'-','_'),"_geocoded_v3.0.2.csv"),col_types=DeGauss_columns) %>%
    rename(
      street = matched_street,
      zip = matched_zip,
      city = matched_city,
      state = matched_state
    )  %>%
    filter(precision=='range',geocode_result!='imprecise_geocode') %>%
  sf::st_as_sf(coords = c( "lon","lat"), crs = 'NAD83',remove=FALSE)

all_states<-states(year=2019)
sts <- all_states$STATEFP
combined <- rbind_tigris(
  lapply(sts, function(x) {
    tracts(x, cb = TRUE,year=2019)
  })
)

Add_tracts_to_degauss<-sf::st_join(DeGauss_import,combined) %>%
  select(ADDRESS_FOR_GEOCODING,
         CITY_HX_FOR_GEOCODING,
         STATE_FOR_GEOCODING,
         ZIP5_FOR_GEOCODING,
         GEOID,
         X=lon,
         Y=lat,
         STATEFP,
         COUNTYFP,
         TRACTCE,
         GEOID) %>%
  mutate(GEOCODE_ATTEMPTED=TRUE) %>%
  group_by(ADDRESS_FOR_GEOCODING,CITY_HX_FOR_GEOCODING,ZIP5_FOR_GEOCODING) %>%
  arrange(TRACTCE) %>%
  mutate(row_num=row_number()) %>%
  filter(row_num==1) %>%
  ungroup()

# Old ARC GIS way
# GEO_UPDATE <- read_delim("C:/GIS Local/to_be_geocoded/Results_of_geocoding/Addresses_2020_06_12_with_tracts.txt",delim=",",col_types=cols_only("ADDRESS_FO"='c',"CITY_HX_FO"='c',"STATE_FOR_"='c',"ZIP5_FOR_G"='c',"Loc_name"='c',"X"='c',"Y"='c',"STATEFP"='c',"COUNTYFP"='c',"TRACTCE"='c',"GEOID"='c',"NAME"='c')) %>%
#   rename(ADDRESS_FOR_GEOCODING=ADDRESS_FO,
#          STATE=STATE_FOR_,
#          CITY_HX=CITY_HX_FO,
#          ZIP5=ZIP5_FOR_G) %>%
#   mutate(
#     ZIP5=str_pad(ZIP5,5,"left","0") 
#     ,GEOCODE_ATTEMPTED=TRUE) %>%
#   unique()

# UPDATE_LIST <- ADDR_UNIQUE_UPDATE %>%
#   inner_join(GEO_UPDATE,by=c("ADDRESS_FOR_GEOCODING","CITY_HX_FOR_GEOCODING"="CITY_HX","STATE_FOR_GEOCODING"="STATE","ZIP5_FOR_GEOCODING"="ZIP5")) %>%
#   mutate(GEOCODE_ATTEMPTED=replace_na(GEOCODE_ATTEMPTED,FALSE)) %>%
#   filter(as.numeric(X)!=0,as.numeric(Y)!=0,ADDRESS_FOR_GEOCODING!='565 CLL ABOLICIN 2ND FLOOR') %>%
#   unique()

UPDATE_LIST <- ADDR_UNIQUE_UPDATE %>%
  inner_join(Add_tracts_to_degauss,by=c("ADDRESS_FOR_GEOCODING","CITY_HX_FOR_GEOCODING","STATE_FOR_GEOCODING","ZIP5_FOR_GEOCODING")) %>%
  mutate(GEOCODE_ATTEMPTED=replace_na(GEOCODE_ATTEMPTED,FALSE)) %>%
  filter(as.numeric(X)!=0,as.numeric(Y)!=0,!ADDRESS_FOR_GEOCODING %in% c('565 CLL ABOLICIN 2ND FLOOR')) %>%
  unique()

# Some characters don't read in well
UPDATE_LIST$ADDR_HX_LINE1<-iconv(UPDATE_LIST$ADDR_HX_LINE1,"WINDOWS-1252","UTF-8")
UPDATE_LIST$ADDR_HX_LINE2<-iconv(UPDATE_LIST$ADDR_HX_LINE2,"WINDOWS-1252","UTF-8")
UPDATE_LIST$CITY_HX<-iconv(UPDATE_LIST$CITY_HX,"WINDOWS-1252","UTF-8")
UPDATE_LIST$CITY_HX_FOR_GEOCODING<-iconv(UPDATE_LIST$CITY_HX_FOR_GEOCODING,"WINDOWS-1252","UTF-8")
UPDATE_LIST$ADDRESS_FOR_GEOCODING<-iconv(UPDATE_LIST$ADDRESS_FOR_GEOCODING,"WINDOWS-1252","UTF-8")
# UPDATE_LIST$ADD_LINE_2<-iconv(ADDRESSES_ALREADY_DONE$ADD_LINE_2,"WINDOWS-1252","UTF-8")
# UPDATE_LIST$CITY<-iconv(ADDRESSES_ALREADY_DONE$CITY,"WINDOWS-1252","UTF-8")
tester<-UPDATE_LIST[,Export_Columns]


Export_Columns<-c("ADDR_HX_LINE1","ADDR_HX_LINE2","CITY_HX","STATE","ZIP_HX","ADDRESS_FOR_GEOCODING","CITY_HX_FOR_GEOCODING","STATE_FOR_GEOCODING","ZIP5_FOR_GEOCODING","foster","RMH","POBox","CCHMC","STJOE","UNKNOWN_ADDRESS","FOREIGN_ADDRESS","NUM_STREET_FORM","FILTER_FOR_GEOCODING","X","Y","STATEFP","COUNTYFP","TRACTCE","GEOID","GEOCODE_ATTEMPTED")

#Export_Date<-paste("UPDATE_LIST_GEOCODE_",str_replace_all(as.character(Sys.Date()),"-","_"),".csv",sep="")
#dbExecute(con,"drop table ##UPLOAD_ADDRESSES")
dbWriteTable(con,"##UPLOAD_ADDRESSES",UPDATE_LIST[,Export_Columns],temporary=FALSE)
dbExecute(con,"
if OBJECT_ID('TempTable.dbo.UPDATE_GEOCODE') is not null
drop table TempTable.dbo.UPDATE_GEOCODE

--USE IMPORTED TABLE
SELECT * INTO TempTable.dbo.UPDATE_GEOCODE from ##UPLOAD_ADDRESSES
")
dbExecute(con,"
UPDATE TempTable.[dbo].UPDATE_GEOCODE
SET ADDR_HX_LINE1=case when ADDR_HX_LINE1='' then NULL else ADDR_HX_LINE1 end,
ADDR_HX_LINE2=case when ADDR_HX_LINE2='' then NULL else ADDR_HX_LINE2 end,
CITY_HX=case when CITY_HX='' then NULL else CITY_HX end,
[STATE]=case when [STATE]='' then NULL else [STATE] end,
ZIP_HX=case when ZIP_HX='' then NULL else ZIP_HX end,
X=case when X='' then NULL else X end,
Y=case when Y='' then NULL else Y end,
STATEFP=case when STATEFP='' then NULL else STATEFP end,
COUNTYFP=case when COUNTYFP='' then NULL else COUNTYFP end,
TRACTCE=case when TRACTCE='' then NULL else TRACTCE end,
GEOID=case when GEOID='' then NULL else GEOID end,
GEOCODE_ATTEMPTED=case when GEOCODE_ATTEMPTED='' then NULL else GEOCODE_ATTEMPTED end
;")

#This throws an error but rename seems to work...
dbExecute(con,"
use TempTable;
EXEC sp_rename 'TempTable.[dbo].UPDATE_GEOCODE.ADDR_HX_LINE1', ADD_LINE_1;
EXEC sp_rename 'TempTable.[dbo].UPDATE_GEOCODE.ADDR_HX_LINE2', ADD_LINE_2;
EXEC sp_rename 'TempTable.[dbo].UPDATE_GEOCODE.CITY_HX', CITY;
EXEC sp_rename 'TempTable.[dbo].UPDATE_GEOCODE.ZIP_HX', ZIP;
")

dbExecute(con,"
DELETE FROM TempTable.[dbo].[UPDATE_GEOCODE] WHERE  isnull(datalength(ADD_LINE_1),0)+isnull(datalength(ADD_LINE_2),0)+isnull(datalength(CITY),0)+isnull(datalength(STATE),0)+isnull(datalength(ZIP),0)>900
")
dbExecute(con,"
DELETE p 
FROM TempTable.[dbo].[UPDATE_GEOCODE] p
inner join TempTable.dbo.FULL_LIST_GEOCODE g
on ((p.[ADD_LINE_1]=g.ADD_LINE_1) or (p.[ADD_LINE_1] is null and g.ADD_LINE_1 is null))
and ((p.[ADD_LINE_2]=g.ADD_LINE_2)  or (p.[ADD_LINE_2] is null and g.ADD_LINE_2 is null))
and ((p.[CITY]=g.CITY) or (p.[CITY] is null and g.CITY is null))
and ((p.[ZIP]=g.[ZIP]) or (p.ZIP is null and g.ZIP is null))
and ((p.[STATE]=g.[STATE]) or (p.[STATE] is null and g.[STATE] is null))
where (g.ZIP5_FOR_GEOCODING is not null)
")

dbExecute(con,"
alter table temptable.[dbo].[UPDATE_GEOCODE]
ADD [GeoLocation] geography;
")

dbExecute(con,"
UPDATE temptable.[dbo].[UPDATE_GEOCODE]
SET [GeoLocation]=geography::STPointFromText('Point('+left(X,12)+' '+left(Y,12)+')', 4326)
where [GeoLocation] is null and X is not null and Y is not null
")

dbExecute(con,"
INSERT INTO TempTable.[dbo].[FULL_LIST_GEOCODE]
SELECT g.* FROM TempTable.[dbo].[UPDATE_GEOCODE] g
")

dbExecute(con,"
DROP TABLE TempTable.[dbo].[UPDATE_GEOCODE];
")

#write_csv(UPDATE_LIST[,Export_Columns],Export_Date,na="")
