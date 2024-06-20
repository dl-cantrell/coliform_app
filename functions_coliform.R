library(janitor)
library(lubridate)
library(dplyr)
library(sqldf)

# Suppress grouping info from summarise.
options(dplyr.summarise.inform = FALSE)

# get previous month's date range -----------------------------------------
# Find previous month's date range and use for date input
# make this a function called from ui()
if (month(today()) == 1) {
  previous_mo <- 12
} else {
  previous_mo <- month(today()) - 1
}
def_start <- date(sprintf("%d-%02d-%02d", year(today()), previous_mo, 1))
def_end <- date(sprintf("%d-%02d-%02d", year(today()), previous_mo, days_in_month(previous_mo)))


water_systems <- function() {
  # sdwis base query --------------------------------------------------------
  sdwis_base <- dbGetQuery(
    sdwis,
    "SELECT DISTINCT TINWSYS.NAME as 'water_system_name', TRIM(TINWSYS.NUMBER0) as 'water_system_no', TINWSYS.ACTIVITY_STATUS_CD, srvCon.SumofSVC_CONNECT_CNT, TINWSYS.D_PWS_FED_TYPE_CD,
                               TINLGENT.NAME AS Regulating_Agency                                          
                            FROM (SELECT TINSCC.TINWSYS_IS_NUMBER, Sum(TINSCC.SVC_CONNECT_CNT) AS SumOfSVC_CONNECT_CNT 
                            		  FROM TINSCC GROUP BY TINSCC.TINWSYS_IS_NUMBER) AS srvCon

                            INNER JOIN TINWSYS ON srvCon.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER
                            INNER JOIN TINRAA ON TINWSYS.TINWSYS_IS_NUMBER = TINRAA.TINWSYS_IS_NUMBER AND TINRAA.ACTIVE_IND_CD = 'A'
                            INNER JOIN TINLGENT ON TINRAA.TINLGENT_IS_NUMBER = TINLGENT.TINLGENT_IS_NUMBER
                                   and (tinlgent.name like 'District%' or tinlgent.name like 'LPA%')
                             
                            WHERE TINWSYS.ACTIVITY_STATUS_CD = 'A'
							 and TINWSYS.D_PWS_FED_TYPE_CD <> 'NP'
						ORDER BY TINWSYS.NAME"
  )
}


# sdwis dbp query ------------------------------------------------------------------------------
get_dbp_mcls <- function(system, start_year, end_year) {
  dbpQuery <- "SELECT 
RTRIM(TINWSYS.NUMBER0) AS water_system_no,
TINWSYS.NAME AS water_system_name,
RTRIM(TSASMPPT.IDENTIFICATION_CD) AS sample_site,
RTRIM(TINWSYS.NUMBER0) + '_' + RTRIM(TINWSF.ST_ASGN_IDENT_CD) + '_' + RTRIM(TSASMPPT.IDENTIFICATION_CD) AS pscode,
TSASMPPT.DESCRIPTION_TEXT as samp_site_description,
TINLGENT.NAME as reg_agency_name,
TSASAMPL.COLLLECTION_END_DT as collection_date,
TSASAR.CONCENTRATION_MSR as result,
TSASAR.UOM_CODE as units,
TSAANLYT.CODE as analyte_code,
RTRIM(TSAANLYT.NAME) AS analyte,
TSAANLYT.STATE_CLASS_CODE as state_class_code,
TINWSF.ST_ASGN_IDENT_CD as facility_type

FROM TINWSYS

INNER JOIN TINWSF
ON TINWSF.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER

INNER JOIN TSASMPPT
ON TSASMPPT.TINWSF0IS_NUMBER = TINWSF.TINWSF_IS_NUMBER

INNER JOIN TINRAA
ON TINRAA.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER

INNER JOIN TINLGENT
ON TINLGENT.TINLGENT_IS_NUMBER = TINRAA.TINLGENT_IS_NUMBER
 and (tinlgent.name like 'District%' or tinlgent.name like 'LPA%')

INNER JOIN TSASAMPL
ON TSASAMPL.TSASMPPT_IS_NUMBER = TSASMPPT.TSASMPPT_IS_NUMBER

INNER JOIN TSASAR
ON TSASAR.TSASAMPL_IS_NUMBER = TSASAMPL.TSASAMPL_IS_NUMBER

INNER JOIN TSAANLYT
ON TSAANLYT.TSAANLYT_IS_NUMBER = TSASAR.TSAANLYT_IS_NUMBER

WHERE
TSAANLYT.STATE_CLASS_CODE = 'DBP'
	AND (TSAANLYT.NAME = 'TTHM' OR TSAANLYT.NAME = 'TOTAL HALOACETIC ACIDS (HAA5)' OR TSAANLYT.NAME = 'BROMATE')
	AND (TINWSF.ST_ASGN_IDENT_CD = 'DST')
--	AND TSASAMPL.COLLLECTION_END_DT >= DATEADD(year, -5, GETDATE())
  AND TINWSYS.NUMBER0 = ?sys
 ORDER BY TSASAMPL.COLLLECTION_END_DT"
  
  #the ?system bit in the above query is flagging that spot as a placeholder, and we tell the query which 
  #pws system we want to use for that placeholder in the sqlInterpolate command
  #which will return a query with the ?system filled in
  #the value for "system" comes from what pws_no the user chooses in the UI

  # Interpolate the SQL query with parameters
  interpolated_query <- sqlInterpolate(sdwis, dbpQuery, sys = system)

      # Execute the query, with the filled in ?system from the step above
  dbp_df <- as_tibble(dbGetQuery(sdwis, interpolated_query)) %>%
       # Add a column for year
    mutate(year = ymd(substr(collection_date, 1, 4), truncated = 2L)) %>%
    filter(year >= start_year & year <= end_year )
  # Select only rows within the year of of interest
}



# sdwis violations query ------------------------------------------------------------------------------
get_vio <- function(sys_vio) {
  vio_query <- "select distinct 
					TINWSYS.NAME as 'water_system_name',
				TRIM(TINWSYS.NUMBER0) as 'water_system_no',
					TMNVIOL.EXTERNAL_SYS_NUM as 'violation_id',
					TMNVTYPE.TYPE_CODE as 'violation_type',
					TMNVTYPE.name as 'violation_name',
				    TMNVTYPE.CATEGORY_CODE as 'violation_category',
					--TMNVIOL.STATUS_TYPE_CODE as 'violation_status',
	
				tmnviol.DETERMINATION_DATE as 'determination_date',
					--tmnviol.STATE_PRD_BEGIN_DT as 'state_begin_date',
					--tmnviol.STATE_PRD_END_DT as 'state_end_date',
					--TMNVIOL.TINWSYS_IS_NUMBER as 'viol_connector',
					--tmnviol.TMNVIOL_IS_NUMBER as 'ea_connector',
					tmnviol.EXCEEDENCES_CNT as 'number_of_exceedances',
					tmnviol.SAMPLES_MISSNG_CNT as 'samples_missing_count',
					tmnviol.SAMPLES_RQD_CNT as 'samples_required',
					tmnviol.ANALYSIS_RESULT_TE as 'analysis_result',
					TMNVIOL.ANALYSIS_RESULT_UO as 'analysis_unit_of_measure',
					TSAANLYT.NAME as 'analyte_name',
					TSAANLYT.CODE as 'analyte_code',
					TMNVIOL.D_INITIAL_TS as 'viol_creation_date'
  
  
					from TMNVIOL
					inner join TINWSYS on TINWSYS.TINWSYS_IS_NUMBER = TMNVIOL.TINWSYS_IS_NUMBER
					inner join TMNVTYPE on TMNVIOL.TMNVTYPE_IS_NUMBER = TMNVTYPE.TMNVTYPE_IS_NUMBER
						and TMNVIOL.STATUS_TYPE_CODE = 'V'
					    AND TMNVIOL.TMNVTYPE_ST_CODE = TMNVTYPE.TMNVTYPE_ST_CODE
					inner join TSAANLYT on TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							and TSAANLYT.CODE in ('2456', '2956', '1011')  
							AND TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
					left join TMNVGRP on TMNVGRP.TMNVGRP_IS_NUMBER = TMNVIOL.TMNVGRP_IS_NUMBER
							and TMNVGRP.TMNVGRP_ST_CODE = TMNVIOL.TMNVGRP_ST_CODE
					left join TSAANGRP on TSAANGRP.TSAANGRP_IS_NUMBER = TMNVGRP.TSAANGRP_IS_NUMBER
							and TSAANGRP.TSAANGRP_ST_CODE = TMNVGRP.TSAANGRP_ST_CODE  
					
					order by TINWSYS.NAME"

# Interpolate the SQL query with parameters
#int_vio_query <- sqlInterpolate(sdwis, vio_query, sys = sys_vio)
  
# Execute the query, with the filled in ?sys from the step above
vio_df <- dbGetQuery(sdwis, vio_query) %>%
# Drop unnecessary columns
#select(!c(TSASAR_IS_NUMBER, ST_ASGN_IDENT_CD, CODE, STATE_CLASS_CODE)) %>%
# Add a column for year
#mutate(YEAR = ymd(substr(viol_create_date, 1, 4), truncated = 2L)) %>%
filter(water_system_no == sys_vio) %>%
  mutate(year = as.integer(substr(determination_date, 1, 4) ) )
  # Select only rows within the year of of interest
}





####################################################################################
#extra code
#try <- dbGetQuery(sdwis, dbpQuery)
#try2 <- try %>%
 # mutate(YEAR = ymd(substr(COLLLECTION_END_DT, 1, 4), truncated = 2L)) %>%
  #filter(YEAR == try_year  )

#try_year <- ymd(2023, truncated = 2L) 
  