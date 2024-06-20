use SDWIS32
go

with coliform_data as
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
	format(tsasampl.COLLLECTION_END_DT, 'yyyy-MM-dd')	as 'Sample Date', 
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
	trim(tsaanlyt.NAME)									as 'Analyte Name',
	--tsasar.MICRO_RSLT_IND								as 'Microbial Result?', --Not sure how helpful this field is
	tsamar.PRESENCE_IND_CODE							as 'Present?',
	tsamar.COUNT_TYPE									as 'Count Type',
	tsamar.COUNT_QTY									as 'Count',
	tsamar.COUNT_UOM_CODE								as 'Count UOM',
	tsamar.TEST_TYPE									as 'Test Type', --Need to explore this field, do we need?
	tsasampl.LAB_ASGND_ID_NUM							as 'Lab Sample ID',
	trim(tsalab.LAB_ID_NUMBER)							as 'ELAP Cert#',
	labnm.NAME											as 'Lab Name',
	tsasmn.CODE											as 'Method'

	from tinwsys 
	inner join tinwsf on tinwsf.TINWSYS_IS_NUMBER = tinwsys.TINWSYS_IS_NUMBER
	inner join tsasmppt on tsasmppt.TINWSF0IS_NUMBER = tinwsf.TINWSF_IS_NUMBER
		and tsasmppt.IDENTIFICATION_CD = 'RTCR' --select PS Codes for RTCR data
	inner join tsasampl on tsasampl.TSASMPPT_IS_NUMBER = tsasmppt.TSASMPPT_IS_NUMBER
		and tsasampl.TYPE_CODE <> 'FB' --removes PFAS Field Reagent Blanks
	inner join tsamcsmp on tsamcsmp.TSASAMPL_IS_NUMBER = tsasampl.TSASAMPL_IS_NUMBER
	inner join tsasar on tsasar.TSASAMPL_IS_NUMBER = tsasampl.TSASAMPL_IS_NUMBER
		and tsasar.D_INITIAL_USERID in ('CLIP1.0') -- 'CLIP1.0' selects only data submitted via CLIP (excludes staff user IDs for coliform data entries and excludes user ID of 'CDS%' for the SDWIS generated data)
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
from coliform_data
