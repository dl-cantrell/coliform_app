with coliform_data as
	(
	select distinct

trim(tinwsys.NUMBER0)					as 'water_system_no',
tinwsys.NAME				    		as 'water_system_name',
tinlgent.NAME							as 'regulating_agency',
tinwsys.ACTIVITY_STATUS_CD				as 'system_status',
trim(tinwsys.D_PWS_FED_TYPE_CD)			as 'water_system_classification',
tinwsys.D_PRIN_CNTY_SVD_NM				as 'principal_county_served',
tinwsys.D_POPULATION_COUNT				as 'population_served',
scc.SVC_CONNECT_ALL						as 'service_connections',
concat(trim(tinwsys.NUMBER0),'_',
		(tinwsf.ST_ASGN_IDENT_CD),'_',	
		trim(tsasmppt.IDENTIFICATION_CD))	as 'ps_code',
tsasmppt.DESCRIPTION_TEXT					as 'sampling_point_name',
tinwsf.TYPE_CODE							as 'facility_type',
tinwsf.ACTIVITY_STATUS_CD					as 'facility_status',
tsasampl.TYPE_CODE							as 'sample_type',
format(tsasampl.COLLLECTION_END_DT, 'yyyy-MM-dd')	as 'sample_date', 
case when tsasampl.COLLCTN_END_TIME is null
		then 'NULL'
		else convert(varchar,tsasampl.COLLCTN_END_TIME, 108)
		end									as 'sample_time',
tsasampl.COLLECTION_ADDRESS					as 'sample_address',
tsamcsmp.FF_CHLOR_RES_MSR					as 'free_cl',
tsamcsmp.FLDTOT_CHL_RES_MSR					as 'tot_cl',
tsamcsmp.FIELD_PH_MEASURE					as 'ph',
tsamcsmp.FIELD_TEMP_MSR						as 'temp',
tsamcsmp.TEMP_MEAS_TYPE_CD					as 'temp_units',
tsamcsmp.FIELD_TURBID_MSR					as 'turbidity',
case when tsasar.ANALYSIS_COMPL_DT is null
		then format(tsasar.ANALYSIS_START_DT, 'yyyy-MM-dd')
		else format(tsasar.ANALYSIS_COMPL_DT, 'yyyy-MM-dd')
		end									 as 'analysis_date',
tsaanlyt.CODE								 as 'analyte_code',
trim(tsaanlyt.NAME)							 as 'analyte_name',
	--tsasar.MICRO_RSLT_IND					 as 'Microbial Result?', --Not sure how helpful this field is
tsamar.PRESENCE_IND_CODE					 as 'presence',
--tsamar.COUNT_TYPE					    	 as 'count_type',
tsamar.COUNT_QTY							 as 'count',
--	tsamar.COUNT_UOM_CODE					 as 'count_uom',
--  tsamar.TEST_TYPE						 as 'test_type', --Need to explore this field, do we need?
tsasampl.LAB_ASGND_ID_NUM					 as 'lab_sample_id',
trim(tsalab.LAB_ID_NUMBER)					 as 'elap_cert_num',
labnm.NAME									 as 'lab_name',
tsasmn.CODE									 as 'method',
tsasar.D_INITIAL_USERID

from tinwsys 
	inner join tinwsf on tinwsf.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	-- AND TINWSYS.NUMBER0 = ?sys
	inner join tsasmppt on tsasmppt.TINWSF0IS_NUMBER = tinwsf.TINWSF_IS_NUMBER
	--	and tsasmppt.IDENTIFICATION_CD = 'RTCR' --select PS Codes for RTCR data
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
order by sample_date