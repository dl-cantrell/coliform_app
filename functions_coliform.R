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
    sdwis_tdt,
    "
     
    SELECT DISTINCT TINWSYS.NAME as 'water_system_name', TRIM(TINWSYS.NUMBER0) as 'water_system_no', TINWSYS.ACTIVITY_STATUS_CD, srvCon.SumofSVC_CONNECT_CNT, TINWSYS.D_PWS_FED_TYPE_CD,
                               TINLGENT.NAME AS Regulating_Agency                                          
                            FROM (SELECT TINSCC.TINWSYS_IS_NUMBER, Sum(TINSCC.SVC_CONNECT_CNT) AS SumOfSVC_CONNECT_CNT 
                            		  FROM TINSCC GROUP BY TINSCC.TINWSYS_IS_NUMBER) AS srvCon

                            INNER JOIN TINWSYS ON srvCon.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER
                            INNER JOIN TINRAA ON TINWSYS.TINWSYS_IS_NUMBER = TINRAA.TINWSYS_IS_NUMBER AND TINRAA.ACTIVE_IND_CD = 'A'
                            INNER JOIN TINLGENT ON TINRAA.TINLGENT_IS_NUMBER = TINLGENT.TINLGENT_IS_NUMBER
                                   and (tinlgent.name like 'District%' or tinlgent.name like 'LPA%')
                             
                            WHERE TINWSYS.ACTIVITY_STATUS_CD = 'A'
							 --and TINWSYS.D_PWS_FED_TYPE_CD <> 'NP'
						ORDER BY TINWSYS.NAME"
  )
}


# sdwis coli query ------------------------------------------------------------------------------
get_coli <- function(system, start_year, end_year) {
  coli_query <- "with coliform_data as
	(
	select distinct
	tinlgent.NAME										as 'Regulating Agency',
	trim(tinwsys.NUMBER0)								as 'Water System Number',
	tinwsys.NAME										as 'Water System Name',
	tinwsys.ACTIVITY_STATUS_CD							as 'System Status',
	trim(tinwsys.D_PWS_FED_TYPE_CD)						as 'Water System Classification',
	tinwsys.D_PRIN_CNTY_SVD_NM							as 'Principal County Served',
	tinwsys.D_POPULATION_COUNT							as 'Population Served',
	scc.SVC_CONNECT_ALL									as 'Service Connections',
	concat(trim(tinwsys.NUMBER0),'_',
		(tinwsf.ST_ASGN_IDENT_CD),'_',
		trim(tsasmppt.IDENTIFICATION_CD))				as 'PS Code',
	tsasmppt.DESCRIPTION_TEXT							as 'Sampling Point Name',
	tinwsf.TYPE_CODE									as 'Facility Type',
	tinwsf.ACTIVITY_STATUS_CD							as 'Facility Status',
	tsasampl.TYPE_CODE									as 'Sample Type',
	format(tsasampl.COLLLECTION_END_DT, 'yyyy-MM-dd')	as 'sample_date', 
	case
		when tsasampl.COLLCTN_END_TIME is null
		then 'NULL'
		else convert(varchar,tsasampl.COLLCTN_END_TIME, 108)
		end												as 'Sample Time',
	tsasampl.COLLECTION_ADDRESS 'Sample Address',
	tsamcsmp.FF_CHLOR_RES_MSR 'Free Cl',
	tsamcsmp.FLDTOT_CHL_RES_MSR 'Tot Cl',
	tsamcsmp.FIELD_PH_MEASURE 'pH',
	tsamcsmp.FIELD_TEMP_MSR 'Temp',
	tsamcsmp.TEMP_MEAS_TYPE_CD 'Temp Units',
	tsamcsmp.FIELD_TURBID_MSR 'Turbidity',
	case
		when tsasar.ANALYSIS_COMPL_DT is null
		then format(tsasar.ANALYSIS_START_DT, 'yyyy-MM-dd')
		else format(tsasar.ANALYSIS_COMPL_DT, 'yyyy-MM-dd')
		end												as 'Analysis Date',
	tsaanlyt.CODE										as 'Analyte Code',
	trim(tsaanlyt.NAME)									as 'analyte_name',
	--tsasar.MICRO_RSLT_IND								as 'Microbial Result?', --Not sure how helpful this field is
	tsamar.PRESENCE_IND_CODE							as 'presence',
	tsamar.COUNT_TYPE									as 'Count Type',
	tsamar.COUNT_QTY									as 'Count',
	tsamar.COUNT_UOM_CODE								as 'Count UOM',
	tsamar.TEST_TYPE									as 'Test Type', --Need to explore this field, do we need?
	tsasampl.LAB_ASGND_ID_NUM							as 'Lab Sample ID',
	trim(tsalab.LAB_ID_NUMBER)							as 'ELAP Cert#',
	labnm.NAME											as 'Lab Name',
	tsasmn.CODE											as 'Method',
	tsasar.D_INITIAL_USERID

	from tinwsys 
	inner join tinwsf on tinwsf.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	inner join tsasmppt on tsasmppt.TINWSF0IS_NUMBER = tinwsf.TINWSF_IS_NUMBER
		and tsasmppt.IDENTIFICATION_CD = 'RTCR' --select PS Codes for RTCR data
	inner join tsasampl on tsasampl.TSASMPPT_IS_NUMBER = tsasmppt.TSASMPPT_IS_NUMBER
		and tsasampl.TYPE_CODE <> 'FB' --removes PFAS Field Reagent Blanks
	inner join tsamcsmp on tsamcsmp.TSASAMPL_IS_NUMBER = tsasampl.TSASAMPL_IS_NUMBER
	inner join tsasar on tsasar.TSASAMPL_IS_NUMBER = tsasampl.TSASAMPL_IS_NUMBER
	--	and tsasar.D_INITIAL_USERID in ('CLIP1.0') -- 'CLIP1.0' selects only data submitted via CLIP (excludes staff user IDs for coliform data entries and excludes user ID of 'CDS%' for the SDWIS generated data)
		and tsasar.DATA_QTY_RSN_CD = '  ' -- removes data with a DQ flag
	inner join tsamar on tsamar.TSASAR_IS_NUMBER = tsasar.TSASAR_IS_NUMBER
	left join tsasmn on tsasmn.TSASMN_IS_NUMBER = tsasar.TSASMN_IS_NUMBER
	inner join tsaanlyt on tsaanlyt.TSAANLYT_IS_NUMBER = tsasar.TSAANLYT_IS_NUMBER 
		and tsaanlyt.TSAANLYT_ST_CODE = tsasar.TSAANLYT_ST_CODE
	inner join tsalab on tsalab.TSALAB_IS_NUMBER = tsasampl.TSALAB_IS_NUMBER
	inner join tsallea on tsallea.TSALAB0IS_NUMBER = tsalab.TSALAB_IS_NUMBER 
		and tsallea.ACTIVE_IND_CODE = 'A'
	inner join tinlgent labnm on labnm.TINLGENT_IS_NUMBER = tsallea.TINLGENT0IS_NUMBER 
		and labnm.TYPE_CODE = 'CM'
	left join
		(select 
			TINWSYS_IS_NUMBER, 
			sum(SVC_CONNECT_CNT) 'SVC_CONNECT_ALL'
		from tinscc 
		group by TINWSYS_IS_NUMBER
		) scc on scc.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	inner join tinraa on tinraa.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER 
		and tinraa.ACTIVE_IND_CD = 'A'
	inner join tinlgent on tinlgent.TINLGENT_IS_NUMBER = tinraa.TINLGENT_IS_NUMBER 
		and (tinlgent.name like 'lpa%' or tinlgent.name like 'district%')
	)

select *
from coliform_data"
  
  #the ?system bit in the above query is flagging that spot as a placeholder, and we tell the query which 
  #pws system we want to use for that placeholder in the sqlInterpolate command
  #which will return a query with the ?system filled in
  #the value for "system" comes from what pws_no the user chooses in the UI

#system = "CA1910002"
#start_year = 2020
#end_year = 2024
    # Interpolate the SQL query with parameters
  interpolated_query <- sqlInterpolate(sdwis_tdt, coli_query)

      # Execute the query, with the filled in ?system from the step above
  coli_df <- as_tibble(dbGetQuery(sdwis_tdt, interpolated_query)) %>%
      # Add a column for year
    mutate(year = ymd(substr(sample_date, 1, 4), truncated = 2L)) %>%
    filter(year >= start_year & year <= end_year )
  # Select only rows within the year of of interest
}

#############################################################################################################################################
###end of code
#############################################################################################################################################