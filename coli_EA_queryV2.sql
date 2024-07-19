Use SDWIS32
go

with pws as (		--public water system info
					select Distinct
					TINWSYS.TINWSYS_IS_NUMBER as 'connector',
					TINLGENT.name as 'regulating_agency',
					TINWSYS.NUMBER0 as 'water_system_no',
					TINWSYS.NAME as 'water_system_name',
					TINWSYS.ACTIVITY_STATUS_CD as 'activity_status',
					tinwsys.D_PRIN_CNTY_SVD_NM as 'Principal County Served',
					TINWSYS.D_PWS_FED_TYPE_CD as 'water_system_classification',
					tinwsys.D_POPULATION_COUNT as 'Population Count',
					COUNT(TINWSYS.TINWSYS_IS_NUMBER) as 'total PWS'
					
					

					From
					TINWSYS
					inner join TINRAA on TINRAA.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER
						and TINRAA.ACTIVE_IND_CD ='A' 
						and TINWSYS.ACTIVITY_STATUS_CD = 'A' and tinwsys.D_PWS_FED_TYPE_CD <>'NP'
					inner join TINLGENT on TINRAA.TINLGENT_IS_NUMBER=TINLGENT.TINLGENT_IS_NUMBER
						and (TINLGENT.name like 'District%' or TINLGENT.name like 'Lpa%')
					group by TINWSYS.TINWSYS_IS_NUMBER,
					TINLGENT.name, TINWSYS.NUMBER0, TINWSYS.NAME, TINWSYS.ACTIVITY_STATUS_CD, tinwsys.D_PRIN_CNTY_SVD_NM, TINWSYS.D_PWS_FED_TYPE_CD, tinwsys.D_POPULATION_COUNT
			),



viol as (			-- violation information
					select distinct 
					TMNVIOL.EXTERNAL_SYS_NUM as 'violation_id',
					TMNVTYPE.TYPE_CODE as 'violation_type',
					TMNVTYPE.name as 'violation_name',
					TMNVTYPE.CATEGORY_CODE as 'Violation Category',
					TMNVIOL.STATUS_TYPE_CODE as 'Violation Status',
					tmnviol.DETERMINATION_DATE as 'determination_date',
					--tmnviol.STATE_PRD_BEGIN_DT as 'State Begin Date',
					--tmnviol.STATE_PRD_END_DT as 'State End Date',
					TMNVIOL.TINWSYS_IS_NUMBER as 'viol_connector',
					tmnviol.TMNVIOL_IS_NUMBER as 'ea_connector',
					tmnviol.EXCEEDENCES_CNT as 'number of exceedances',
					tmnviol.SAMPLES_MISSNG_CNT as 'samples missing count',
					tmnviol.SAMPLES_RQD_CNT as 'samples requried',
					tmnviol.ANALYSIS_RESULT_TE as 'Analysis Result',
					TMNVIOL.ANALYSIS_RESULT_UO as 'Analysis Unit of Measure',
					TSAANLYT.NAME as 'analyte_name',
					TSAANLYT.CODE as 'analyte_code',
					--TMNVIOL.D_INITIAL_TS as 'Viol create date',
					TMNVIOL.D_INITIAL_USERID as 'User Name',
					TMNVIOL.TMNVTYPE_ST_CODE

					from TMNVIOL
					inner join TMNVTYPE on TMNVIOL.TMNVTYPE_IS_NUMBER = TMNVTYPE.TMNVTYPE_IS_NUMBER
						and TMNVIOL.STATUS_TYPE_CODE = 'V'
						AND TMNVIOL.TMNVTYPE_ST_CODE = TMNVTYPE.TMNVTYPE_ST_CODE
					left join TSAANLYT on TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							AND TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
					left join TMNVGRP on TMNVGRP.TMNVGRP_IS_NUMBER = TMNVIOL.TMNVGRP_IS_NUMBER
							and TMNVGRP.TMNVGRP_ST_CODE = TMNVIOL.TMNVGRP_ST_CODE
					left join TSAANGRP on TSAANGRP.TSAANGRP_IS_NUMBER = TMNVGRP.TSAANGRP_IS_NUMBER
							and TSAANGRP.TSAANGRP_ST_CODE = TMNVGRP.TSAANGRP_ST_CODE
				--where
					--TMNVTYPE.TYPE_CODE = 'OP'
						--tmnviol.STATE_PRD_BEGIN_DT >='2012-09-25'
				
)
,

	ea as (		--enforcement action information
				select distinct
				TENENACT.EXTERNAL_SYS_NUM AS 'Enforcement Action ID',
				TENENACT.TENENACT_ST_CODE, 
				TENACTYP.NAME as 'Enforcement Action Name',
				TENENACT.TENENACT_IS_NUMBER, 
				TENACTYP.LOCATION_TYPE_CODE+TENACTYP.FORMAL_TYPE_CODE+TENACTYP.SUB_CATEGORY_CODE AS 'enforcement_action_code', 
				TENENACT.STATUS_DATE AS 'enforcement_action_date', 
				TENENACT.D_STATUS_CODE,
				TENENACT.D_INITIAL_TS AS ENF_TS,
				TMNVIEAA.TMNVIOL_IS_NUMBER as 'ea_viol_connector',
				TENENACT.D_INITIAL_TS as 'EA Created Date',
				TENENACT.D_INITIAL_USERID as 'User ID',
				TENENACT.TENENACT_IS_NUMBER as 'Tenenact IS Num',
				tmnvieaa.D_LAST_UPDT_TS as 'time stamp VIEAA'


				from
				TENENACT
				left join TMNVIEAA on TENENACT.TENENACT_IS_NUMBER = TMNVIEAA.TENENACT_IS_NUMBER
				left join TENACTYP on TENENACT.TENACTYP_IS_NUMBER = TENACTYP.TENACTYP_IS_NUMBER
									and TENENACT.TENACTYP_ST_CODE = TENACTYP.TENACTYP_ST_CODE
			
),



final as (select
	pws.[water_system_name],
	pws.[water_system_no],
	pws.[regulating_agency],
	pws.[activity_status],
	pws.[water_system_classification],
	viol.[violation_name],
	viol.[violation_type],
	viol.[violation_id],
	viol.[analyte_name],
	viol.[analyte_code],
	viol.[determination_date],
	--format(viol.[State Begin Date], 'yyyy-MM-dd') as ' violation_begin_date',
	--format(viol.[State End Date], 'yyyy-MM-dd') as 'violation_end_date',
	--viol.[violation_create_date],
	ea.[enforcement_action_code],
	format(ea.[EA Created Date], 'yyyy-MM-dd') as 'enforcement_action_date'--,


	from
	pws
	inner join viol on pws.connector = viol.viol_connector
	inner join ea on viol.ea_connector = ea.ea_viol_connector

	
	union

	select	
	pws.[water_system_name],
	pws.[water_system_no],
	pws.[regulating_agency],
	pws.[activity_status],
	pws.[water_system_classification],
	viol.[violation_name],
	viol.[violation_type],
	viol.[violation_id],
	viol.[analyte_name],
	viol.[analyte_code],
	viol.[determination_date],
	--format(viol.[State Begin Date], 'yyyy-MM-dd') as ' violation_begin_date',
	--format(viol.[State End Date], 'yyyy-MM-dd') as 'violation_end_date',
	--format(viol.[Viol create date],'yyyy-MM-dd') as '
	ea.[enforcement_action_code],
	format(ea.[EA Created Date], 'yyyy-MM-dd') as 'enforcement_action_date'--,


	from
	pws
	inner join viol on pws.connector = viol.viol_connector
	inner join ea on viol.ea_connector = ea.ea_viol_connector

	)

	select 
	final.*
	from
	final

	
	where final.[analyte_code] in ('8000', '3100', '3029','3013', '3000')
	--and final.[water_system_no] = ?sys
	order by final.[determination_date]