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
    "SELECT DISTINCT TINWSYS.NAME as 'water_system_name', TRIM(TINWSYS.NUMBER0) as 'water_system_no', TINWSYS.ACTIVITY_STATUS_CD, srvCon.SumofSVC_CONNECT_CNT, TINWSYS.D_PWS_FED_TYPE_CD,
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
get_coli <- function(system, start_date, end_date) {
  coli_query <- "
with coliform_data as
	(
	select distinct

trim(tinwsys.NUMBER0)								as 'water_system_no',
tinwsys.NAME									    	as 'water_system_name',
tinlgent.NAME										    as 'regulating_agency',
tinwsys.ACTIVITY_STATUS_CD				  as 'system_status',
trim(tinwsys.D_PWS_FED_TYPE_CD)			as 'water_system_classification',
tinwsys.D_PRIN_CNTY_SVD_NM					as 'principal_county_served',
tinwsys.D_POPULATION_COUNT					as 'population_served',
scc.SVC_CONNECT_ALL									as 'service_connections',
concat(trim(tinwsys.NUMBER0),'_',
		(tinwsf.ST_ASGN_IDENT_CD),'_',
		trim(tsasmppt.IDENTIFICATION_CD))				as 'ps_code',
tsasmppt.DESCRIPTION_TEXT						as 'sampling_point_name',
tinwsf.TYPE_CODE									  as 'facility_type',
tinwsf.ACTIVITY_STATUS_CD						as 'facility_status',
tsasampl.TYPE_CODE									as 'sample_type',
format(tsasampl.COLLLECTION_END_DT, 'yyyy-MM-dd')	as 'sample_date', 
case when tsasampl.COLLCTN_END_TIME is null
		then 'NULL'
		else convert(varchar,tsasampl.COLLCTN_END_TIME, 108)
		end												    as 'sample_time',
tsasampl.COLLECTION_ADDRESS 'sample_address',
tsamcsmp.FF_CHLOR_RES_MSR 'free_cl',
tsamcsmp.FLDTOT_CHL_RES_MSR 'tot_cl',
tsamcsmp.FIELD_PH_MEASURE 'ph',
tsamcsmp.FIELD_TEMP_MSR 'temp',
tsamcsmp.TEMP_MEAS_TYPE_CD 'temp_units',
tsamcsmp.FIELD_TURBID_MSR 'turbidity',
case when tsasar.ANALYSIS_COMPL_DT is null
		then format(tsasar.ANALYSIS_START_DT, 'yyyy-MM-dd')
		else format(tsasar.ANALYSIS_COMPL_DT, 'yyyy-MM-dd')
		end												      as 'analysis_date',
tsaanlyt.CODE										    as 'analyte_code',
trim(tsaanlyt.NAME)									as 'analyte_name',
	--tsasar.MICRO_RSLT_IND						as 'Microbial Result?', --Not sure how helpful this field is
tsamar.PRESENCE_IND_CODE						as 'presence',
--tsamar.COUNT_TYPE									as 'count_type',
tsamar.COUNT_QTY									  as 'count',
--	tsamar.COUNT_UOM_CODE						as 'count_uom',
--  tsamar.TEST_TYPE								as 'test_type', --Need to explore this field, do we need?
tsasampl.LAB_ASGND_ID_NUM						as 'lab_sample_id',
trim(tsalab.LAB_ID_NUMBER)					as 'elap_cert_num',
labnm.NAME											    as 'lab_name',
tsasmn.CODE											    as 'method',
tsasar.D_INITIAL_USERID

from tinwsys 
	inner join tinwsf on tinwsf.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	 AND TINWSYS.NUMBER0 = ?sys
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
			sum(SVC_CONNECT_CNT) 'svc_connect_all'
		from tinscc 
		group by TINWSYS_IS_NUMBER
		) scc on scc.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	inner join tinraa on tinraa.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER 
		and tinraa.ACTIVE_IND_CD = 'A'
	inner join tinlgent on tinlgent.TINLGENT_IS_NUMBER = tinraa.TINLGENT_IS_NUMBER 
		and (tinlgent.name like 'lpa%' or tinlgent.name like 'district%')
	)

select *
from coliform_data
order by sample_date"
  
  #the ?system bit in the above query is flagging that spot as a placeholder, and we tell the query which 
  #pws system we want to use for that placeholder in the sqlInterpolate command
  #which will return a query with the ?system filled in
  #the value for "system" comes from what pws_no the user chooses in the UI

    # Interpolate the SQL query with parameters
  interpolated_query <- sqlInterpolate(sdwis_tdt, coli_query, sys = system)
    # Execute the query, with the filled in ?system from the step above
  coli_df <- as_tibble(dbGetQuery(sdwis_tdt, interpolated_query)) %>%
    filter(sample_date >= start_date & sample_date <= end_date )

}

# sdwis violations query ------------------------------------------------------------------------------
get_vio <- function(sys_vio) {
  vio_query <- "select distinct 
					TINWSYS.NAME as PWS_NAME,
					TINWSYS.NUMBER0 as water_system_no,
					TMNVIOL.EXTERNAL_SYS_NUM as 'Violation ID',
					--TMNVTYPE.TYPE_CODE as 'Violation Type',
					TMNVTYPE.name as 'Violation Name',
				    TMNVTYPE.CATEGORY_CODE as 'Violation Category',
					--TMNVIOL.STATUS_TYPE_CODE as 'Violation Status',
					tmnviol.DETERMINATION_DATE as 'determination_date',
					--tmnviol.STATE_PRD_BEGIN_DT as 'State Begin Date',
					--tmnviol.STATE_PRD_END_DT as 'State End Date',
					TMNVIOL.TINWSYS_IS_NUMBER as 'viol_connector',
					tmnviol.TMNVIOL_IS_NUMBER as 'ea_connector',
					tmnviol.EXCEEDENCES_CNT as 'number of exceedances',
					tmnviol.SAMPLES_MISSNG_CNT as 'samples missing count',
					tmnviol.SAMPLES_RQD_CNT as 'samples required',
					tmnviol.ANALYSIS_RESULT_TE as 'Analysis Result',
					TMNVIOL.ANALYSIS_RESULT_UO as 'Analysis Unit of Measure',
					TSAANLYT.NAME as 'Analyte Name',
					TSAANLYT.CODE as 'Analyte Code',
					TMNVIOL.D_INITIAL_TS as 'Viol create date'
					
				--	TMNVIOL.D_INITIAL_USERID as 'User Name',
					--TMNVIOL.TMNVTYPE_ST_CODE


					from TMNVIOL
					inner join TINWSYS on TINWSYS.TINWSYS_IS_NUMBER = TMNVIOL.TINWSYS_IS_NUMBER
					AND TINWSYS.NUMBER0 = ?sys
					inner join TMNVTYPE on TMNVIOL.TMNVTYPE_IS_NUMBER = TMNVTYPE.TMNVTYPE_IS_NUMBER
						and TMNVIOL.STATUS_TYPE_CODE = 'V'
					    AND TMNVIOL.TMNVTYPE_ST_CODE = TMNVTYPE.TMNVTYPE_ST_CODE
					inner join TSAANLYT on TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							  and TSAANLYT.CODE = '8000' -- this is for coliform (revised total coliform rule)
							--and TSAANLYT.CODE in ('2456', '2956', '1011')   -- this is for dbp's
							AND TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							--and (TMNVTYPE.TYPE_CODE = '01' or  TMNVTYPE.TYPE_CODE = '02') 
					left join TMNVGRP on TMNVGRP.TMNVGRP_IS_NUMBER = TMNVIOL.TMNVGRP_IS_NUMBER
							and TMNVGRP.TMNVGRP_ST_CODE = TMNVIOL.TMNVGRP_ST_CODE
					left join TSAANGRP on TSAANGRP.TSAANGRP_IS_NUMBER = TMNVGRP.TSAANGRP_IS_NUMBER
							and TSAANGRP.TSAANGRP_ST_CODE = TMNVGRP.TSAANGRP_ST_CODE  
				
					Where TINWSYS.NUMBER0 = ?sys
					order by TINWSYS.NUMBER0"

  
  # Interpolate the SQL query with parameters
  int_vio_query <- sqlInterpolate(sdwis_tdt, vio_query, sys = sys_vio)
  
  # Execute the query, with the filled in ?sys from the step above
  vio_df <- dbGetQuery(sdwis_tdt, int_vio_query)  %>%
      mutate(year = substr(determination_date, 1, 4))
  
  
}



#############################################################################################################################################
###end of code
#############################################################################################################################################