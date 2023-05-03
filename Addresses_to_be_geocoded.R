library(tidyr)
library(dplyr)
library(odbc)
library(DBI)
library(stringr)
library(purrr)
library(readr)
library(glue)

con <- dbConnect(odbc::odbc(), "ClarityProd")

ADDITIONAL_ADDRESS<-dbGetQuery(con,"
select distinct
   UPPER(p.ADDR_HX_LINE1) ADDR_HX_LINE1
   ,UPPER(p.ADDR_HX_LINE2) ADDR_HX_LINE2
   ,UPPER(p.CITY_HX) CITY_HX
   ,UPPER(p.STATE) STATE
   ,UPPER(p.ZIP_HX) ZIP_HX
 from HPCEClarity.dbo.CHMC_ADT_ADDR_HX p 
 left join TempTable.dbo.FULL_LIST_GEOCODE g
   on ((p.[ADDR_HX_LINE1]=g.ADD_LINE_1) or (p.[ADDR_HX_LINE1] is null and g.ADD_LINE_1 is null))
   and ((p.[ADDR_HX_LINE2]=g.ADD_LINE_2)  or (p.[ADDR_HX_LINE2] is null and g.ADD_LINE_2 is null))
   and ((p.[CITY_HX]=g.CITY) or ((p.[CITY_HX] is null  or p.[CITY_HX]='') and g.CITY is null))
   and ((p.[ZIP_HX]=g.[ZIP]) or (p.ZIP_HX is null and g.ZIP is null))
   and ((p.[STATE]=g.[STATE]) or (p.[STATE] is null and g.[STATE] is null))
   where ((p.ADDR_HX_LINE1 is not null or p.ADDR_HX_LINE2 is not null) AND p.ZIP_HX is not null)
   and g.GEOCODE_ATTEMPTED is null
   and isnull(datalength(p.ADDR_HX_LINE1),0)+isnull(datalength(p.ADDR_HX_LINE2),0)+isnull(datalength(p.CITY_HX),0)+isnull(datalength(p.STATE),0)+isnull(datalength(p.ZIP_HX),0)<=900
union
 select distinct 
   UPPER(p.ADD_LINE_1) ADDR_HX_LINE1
   , UPPER(p.ADD_LINE_2) ADDR_HX_LINE2
   , UPPER(p.CITY) CITY_HX
   , UPPER(p.STATE) STATE
   , UPPER(p.ZIP) ZIP_HX
 from HPCECLARITY.[BMI].[PATIENT] p
 left join TempTable.dbo.FULL_LIST_GEOCODE g
   on  ((p.[ADD_LINE_1]=g.ADD_LINE_1) or (p.[ADD_LINE_1] is null and g.ADD_LINE_1 is null))
   and ((p.[ADD_LINE_2]=g.ADD_LINE_2)  or (p.[ADD_LINE_2] is null and g.ADD_LINE_2 is null))
   and ((p.[CITY]=g.CITY) or ((p.[CITY] is null  or p.[CITY]='') and g.CITY is null))
   and ((p.[ZIP]=g.[ZIP]) or (p.ZIP is null and g.ZIP is null))
   and ((p.[STATE]=g.[STATE]) or (p.[STATE] is null and g.[STATE] is null))
   where ((p.ADD_LINE_1 is not null or p.ADD_LINE_2 is not null) AND p.ZIP is not null)
   and g.GEOCODE_ATTEMPTED is null
   and isnull(datalength(p.ADD_LINE_1),0)+isnull(datalength(p.ADD_LINE_2),0)+isnull(datalength(p.CITY),0)+isnull(datalength(p.STATE),0)+isnull(datalength(p.ZIP),0)<=900
   and IS_VALID_PAT_YN='Y'
union
  select distinct
  UPPER(ha.PAT_ADDR_1) ADDR_HX_LINE1
   , UPPER(ha.PAT_ADDR_2) ADDR_HX_LINE2
   , UPPER(ha.PAT_CITY) CITY_HX
   , UPPER(zc_s.NAME) STATE
   , UPPER(ha.PAT_ZIP) ZIP_HX
  from HPCEClarity.bmi.HSP_ACCOUNT ha 
  left join TempTable.dbo.ZC_STATE zc_s on zc_s.STATE_C=ha.PAT_STATE_C
  left join TempTable.dbo.FULL_LIST_GEOCODE g
    on ((ha.PAT_ADDR_1=g.ADD_LINE_1) or (ha.PAT_ADDR_1 is null and g.ADD_LINE_1 is null))
    and ((ha.PAT_ADDR_2=g.ADD_LINE_2)  or (ha.PAT_ADDR_2 is null and g.ADD_LINE_2 is null))
    and ((ha.PAT_CITY=g.CITY) or (ha.PAT_CITY is null and g.CITY is null))
    and ((ha.PAT_ZIP=g.[ZIP]) or (ha.PAT_ZIP is null and g.ZIP is null))
    and ((zc_s.NAME=g.[STATE]) or (zc_s.NAME is null and g.[STATE] is null))
  where ((ha.PAT_ADDR_1 is not null or ha.PAT_ADDR_2 is not null) AND ha.PAT_ZIP is not null)
   and g.GEOCODE_ATTEMPTED is null
   and isnull(datalength(ha.PAT_ADDR_1),0)+isnull(datalength(ha.PAT_ADDR_2),0)+isnull(datalength(ha.PAT_CITY),0)+isnull(datalength(zc_s.NAME),0)+isnull(datalength(ha.PAT_ZIP),0)<=900
  ") 

CURRENT_GEOCODED<-dbGetQuery(con,"
select distinct
  g.ADDRESS_FOR_GEOCODING,
  g.CITY_HX_FOR_GEOCODING,
  g.STATE_FOR_GEOCODING,
  g.ZIP5_FOR_GEOCODING,
  g.X,
  g.Y,
  g.STATEFP,
  g.COUNTYFP,
  g.TRACTCE,
  g.GEOID,
  g.GEOCODE_ATTEMPTED
from TempTable.dbo.FULL_LIST_GEOCODE g
")

ADDR_UNIQUE_UPDATE<-ADDITIONAL_ADDRESS %>%
  #Some non-compliant characters stored on server, need to remove
  mutate(ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1,"[^[:graph:]]", " ")
         ,ADDR_HX_LINE2_CLEAN=str_replace_all(ADDR_HX_LINE2,"[^[:graph:]]", " ")  ) %>%
  mutate(ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1_CLEAN, fixed('\\'), ''),
         ADDR_HX_LINE1_CLEAN=gsub(' WN Bend',' W North Bend',ADDR_HX_LINE1_CLEAN,ignore.case = TRUE), # commonly mistyped address
         ADDR_HX_LINE1_CLEAN=gsub(' Northbend','North Bend',ADDR_HX_LINE1_CLEAN,ignore.case = TRUE), # commonly mistyped address
         ADDR_HX_LINE1_CLEAN=gsub('1/2','',ADDR_HX_LINE1_CLEAN), # removes 1/2 from addresses
         ADDR_HX_LINE1_CLEAN=gsub('(\\d+?)(?:\\-\\d+)','\\1',ADDR_HX_LINE1_CLEAN), #removes street_num-apt_num contructs
         ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1_CLEAN, fixed('"'), ''),
         ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1_CLEAN, '[^[:alnum:] ]', ''),
         ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1_CLEAN, 
                                               regex('((APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s+#*\\s*\\S+\\b|\\s(APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s*#*\\s*\\S+\\b|\\sS*LOT\\.*#*(\\s*\\d+|\\s+\\S)\\w*\\b)',ignore.case=TRUE),''),
         ADDR_HX_LINE1_CLEAN = str_replace_all(ADDR_HX_LINE1_CLEAN, '\\s[\\s]+',' '),
         ADDR_HX_LINE1_CLEAN = str_squish(ADDR_HX_LINE1_CLEAN),
         ADDR_HX_LINE1_CLEAN = replace(ADDR_HX_LINE1_CLEAN,which(ADDR_HX_LINE1_CLEAN==""),NA)) %>%
  mutate(ADDR_HX_LINE2_CLEAN = str_replace_all(ADDR_HX_LINE2_CLEAN, fixed('\\'), ''),
         ADDR_HX_LINE2_CLEAN=gsub(' WN Bend',' W North Bend',ADDR_HX_LINE2_CLEAN,ignore.case = TRUE), # commonly mistyped address
         ADDR_HX_LINE2_CLEAN=gsub(' Northbend','North Bend',ADDR_HX_LINE2_CLEAN,ignore.case = TRUE), # commonly mistyped address
         ADDR_HX_LINE2_CLEAN=gsub('1/2','',ADDR_HX_LINE2_CLEAN), # removes 1/2 from addresses
         ADDR_HX_LINE2_CLEAN=gsub('(\\d+?)(?:\\-\\d+)','\\1',ADDR_HX_LINE2_CLEAN), #removes street_num-apt_num contructs
         ADDR_HX_LINE2_CLEAN = str_replace_all(ADDR_HX_LINE2_CLEAN, fixed('"'), ''),
         ADDR_HX_LINE2_CLEAN = str_replace_all(ADDR_HX_LINE2_CLEAN, '[^[:alnum:] ]', ''),
         ADDR_HX_LINE2_CLEAN = str_replace_all(ADDR_HX_LINE2_CLEAN, 
                                               regex('((APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s+#*\\s*\\S+\\b|\\s(APT|APARTMENT|UNIT|B(UI)*LD(IN)*G|#|SUITE)\\.*\\s*#*\\s*\\S+\\b|\\sS*LOT\\.*#*(\\s*\\d+|\\s+\\S)\\w*\\b)',ignore.case=TRUE),''),
         ADDR_HX_LINE2_CLEAN = str_replace_all(ADDR_HX_LINE2_CLEAN, '\\s[\\s]+',' '),
         ADDR_HX_LINE2_CLEAN = str_squish(ADDR_HX_LINE2_CLEAN),
         ADDR_HX_LINE2_CLEAN = replace(ADDR_HX_LINE2_CLEAN,which(ADDR_HX_LINE2_CLEAN==""),NA)) %>%
  mutate(ADDRESS_FOR_GEOCODING = coalesce(ADDR_HX_LINE1_CLEAN,ADDR_HX_LINE2_CLEAN),
         ADDRESS_FOR_GEOCODING = str_squish(ADDRESS_FOR_GEOCODING),
         ADDRESS_FOR_GEOCODING = replace(ADDRESS_FOR_GEOCODING,which(ADDRESS_FOR_GEOCODING==""),NA),
         ZIP5=str_sub(ZIP_HX,1,5),
         ADDRESS_FOR_GEOCODING_zip=str_squish(paste(ADDRESS_FOR_GEOCODING,ZIP5,sep=' '))) %>%
  # Set up dummy variables for various concerining addresses
  mutate(foster = str_detect(ADDRESS_FOR_GEOCODING_zip, regex(
    "(222\\s*(E[\\.ASTY\\s]*)*\\s*C[ENTRALU]+\\s*P[ARKWYU\\s]+.*452.*)|(2400\\s+CL[AIERMOUNT]+\\s.*(D[RIVE])+.*\\s+OH(IO)*\\s*451.*)|(601\\sW[ASHINGTO]*N\\sA.*NEW.*410.*)|(130\\sW.*43.*ST.*COVING.*410.*)|(8311\\sU\\s*S.*42.*KENTUCKY\\s410.*)|(300\\sN[ORTH\\.]*.*\\sFAIR\\sA.*450.*)|(416\\sS[\\.OUTH]*\\s*E[\\.AST]*\\s.*450.*)|(775\\sM[\\.OUNT]*\\s*O[RABN]*\\s.*451.*)|(1025\\sS[OUTH\\.]*\\s*S[\\.OUTH]*\\sST.*451.*)|(1500\\sPARK\\sAV.*453.*)|(3304\\sN[ORTHJ\\.]*\\s*M[AIN]*\\s.*454.*)|(601\\sLE[DBETR]*\\s.*453.*)|(230\\sMARY+\\sAV.*470.*)(230\\s((MARY)|(MERRY))\\s((AV)|(ST)).*470.*)|(12048\\sS[AINT]+\\sMARYS*\\s.*47.*)|(125\\sN[ORTH\\.]*\\sWALNUT.*470.*)|(506\\sFERRY\\sST[RETS56\\s]*\\s.*470.*)|(.*J[OBS]*\\s*(AND)*\\s*F[AMILYES]*\\s*S[ERVICS]*.*)|(.*PROTECTIVE.*)|(.*\\sDCHS.*)|(.*\\sMCCS.*)|(.*\\sHUMAN\\sSER.*)|(.*SERVICES.*)"))) %>%
  mutate(RMH = str_detect(ADDRESS_FOR_GEOCODING_zip, regex(
    "(350\\s*E[RKIU]+[RKEINBCGH]+.*45[0-9]+9)|.*(RONALD.*HOUSE).*"))) %>%
  mutate(POBox = str_detect(ADDRESS_FOR_GEOCODING, regex(
    "^P\\s*O\\s*(B[ox])*"))) %>%
  mutate(CCHMC = str_detect(ADDRESS_FOR_GEOCODING_zip, regex(
    "(3333\\s*BURNETT*\\s*A.*45229)"))) %>%
  mutate(STJOE = str_detect(ADDRESS_FOR_GEOCODING_zip, regex(
    "(10722\\sWYS.*OH.*)|(.*S[AIN]*T\\sJO[SEPH]*\\s(OR(PHANGE)*|HOME).*)"))) %>%
  mutate(UNKNOWN_ADDRESS = str_detect(ADDRESS_FOR_GEOCODING_zip, regex(
    ".*(UNKNOWN).*|.*(VERIFY).*"))|(ZIP5=='00000'|ZIP5=='99999'|is.na(ZIP5))) %>%
  mutate(FOREIGN_ADDRESS = STATE=='FOREIGN COUNTRY' | str_detect(ADDRESS_FOR_GEOCODING,'EMBASSY') | str_detect(ADDRESS_FOR_GEOCODING,'CONSULATE')) %>%
  mutate(NUM_STREET_FORM = str_detect(ADDRESS_FOR_GEOCODING,regex("[0-9]+\\s[A-Z]+"))) %>%
  mutate(FILTER_FOR_GEOCODING=!is.na(ADDRESS_FOR_GEOCODING) & POBox==FALSE & FOREIGN_ADDRESS==FALSE & UNKNOWN_ADDRESS==FALSE & NUM_STREET_FORM==TRUE) %>%
  dplyr::select(-ADDRESS_FOR_GEOCODING_zip,-ADDR_HX_LINE1_CLEAN,-ADDR_HX_LINE2_CLEAN) 

ADDR_UNIQUE_UPDATE <- ADDR_UNIQUE_UPDATE %>%
  mutate(
    ADDRESS_FOR_GEOCODING=str_squish(ADDRESS_FOR_GEOCODING),
    CITY_HX_FOR_GEOCODING=str_squish(CITY_HX),
    CITY_HX_FOR_GEOCODING = case_when(str_detect(CITY_HX,"^\\s*$") ~ NA_character_, TRUE ~ CITY_HX_FOR_GEOCODING),
    STATE_FOR_GEOCODING=str_squish(STATE),
    ZIP5_FOR_GEOCODING=str_squish(ZIP5)
  ) %>%
  unique()

table_order<-c(
                'ADD_LINE_1'
               ,'ADD_LINE_2'
               ,'CITY'
               ,'STATE'
               ,'ZIP'
               ,'ADDRESS_FOR_GEOCODING'
               ,'CITY_HX_FOR_GEOCODING'
               ,'STATE_FOR_GEOCODING'
               ,'ZIP5_FOR_GEOCODING'
               ,'foster'
               ,'RMH'
               ,'POBox'
               ,'CCHMC'
               ,'STJOE'
               ,'UNKNOWN_ADDRESS'
               ,'FOREIGN_ADDRESS'
               ,'NUM_STREET_FORM'
               ,'FILTER_FOR_GEOCODING'
               ,'X'
               ,'Y'
               ,'STATEFP'
               ,'COUNTYFP'
               ,'TRACTCE'
               ,'GEOID'
               ,'GEOCODE_ATTEMPTED')

ADDRESSES_ALREADY_DONE<-ADDR_UNIQUE_UPDATE %>%
  inner_join(CURRENT_GEOCODED,by=c("ADDRESS_FOR_GEOCODING","CITY_HX_FOR_GEOCODING","STATE_FOR_GEOCODING","ZIP5_FOR_GEOCODING")) %>%
  rename(ADD_LINE_1=ADDR_HX_LINE1,
         ADD_LINE_2=ADDR_HX_LINE2,
         CITY=CITY_HX,
         ZIP=ZIP_HX
         ) %>%
  select(all_of(table_order))

ADDRESSES_ALREADY_DONE$ADD_LINE_1<-iconv(ADDRESSES_ALREADY_DONE$ADD_LINE_1,"WINDOWS-1252","UTF-8")
ADDRESSES_ALREADY_DONE$ADD_LINE_2<-iconv(ADDRESSES_ALREADY_DONE$ADD_LINE_2,"WINDOWS-1252","UTF-8")
ADDRESSES_ALREADY_DONE$CITY<-iconv(ADDRESSES_ALREADY_DONE$CITY,"WINDOWS-1252","UTF-8")

##To upload and update FULL_LIST_GEOCODE table
#dbExecute(con,"drop table ##New_addresses")

dbWriteTable(con,"##New_addresses",ADDRESSES_ALREADY_DONE,temporary=FALSE)

dbExecute(con,"
alter table ##New_addresses
ADD [GeoLocation] geography")

dbExecute(con,"
UPDATE ##New_addresses
SET [GeoLocation]=geography::STPointFromText('Point('+left(X,12)+' '+left(Y,12)+')', 4326)
where [GeoLocation] is null and X is not null and Y is not null")

dbExecute(con,"
DELETE p
FROM ##New_addresses p
inner join TempTable.dbo.FULL_LIST_GEOCODE g
	on ((p.[ADD_LINE_1]=g.ADD_LINE_1) or (p.[ADD_LINE_1] is null and g.ADD_LINE_1 is null))
	and ((p.[ADD_LINE_2]=g.ADD_LINE_2)  or (p.[ADD_LINE_2] is null and g.ADD_LINE_2 is null))
	and ((p.[CITY]=g.CITY) or (p.[CITY] is null and g.CITY is null))
	and ((p.[ZIP]=g.[ZIP]) or (p.ZIP is null and g.ZIP is null))
	and ((p.[STATE]=g.[STATE]) or (p.[STATE] is null and g.[STATE] is null))
where (g.ZIP5_FOR_GEOCODING is not null)
")

dbExecute(con,"
INSERT INTO TempTable.[dbo].[FULL_LIST_GEOCODE]
SELECT g.* FROM ##New_addresses g
")

LIST_TO_GEOCODE_UPDATE <- ADDR_UNIQUE_UPDATE %>%
  anti_join(CURRENT_GEOCODED,by=c("ADDRESS_FOR_GEOCODING","CITY_HX_FOR_GEOCODING","STATE_FOR_GEOCODING","ZIP5_FOR_GEOCODING")) %>%
  filter(FILTER_FOR_GEOCODING) %>%
  select(ADDRESS_FOR_GEOCODING,CITY_HX_FOR_GEOCODING,STATE_FOR_GEOCODING,ZIP5_FOR_GEOCODING) %>% 
  unique() %>%
  mutate(address=glue::glue("{ADDRESS_FOR_GEOCODING}, {CITY_HX_FOR_GEOCODING}, {STATE_FOR_GEOCODING} {ZIP5_FOR_GEOCODING}"))

#Workflow using DeGauss
currentDate<-paste("ADDRESSES_TO_GEOCODE_BMI_",str_replace_all(as.character(Sys.Date()),"-","_"),".csv",sep="")
write_csv(LIST_TO_GEOCODE_UPDATE,file = glue::glue("C:/DeGauss/{currentDate}"),na="")
