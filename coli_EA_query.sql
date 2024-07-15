with pws as (		--public water system info
					select Distinct
					TINWSYS.TINWSYS_IS_NUMBER as 'connector',
					TINLGENT.name as 'Regulating Agency',
					TINWSYS.NUMBER0 as 'Public Water System ID',
					TINWSYS.NAME as 'Public Water System',
					TINWSYS.ACTIVITY_STATUS_CD as 'Activity Status',
					tinwsys.D_PRIN_CNTY_SVD_NM as 'Principal County Served',
					TINWSYS.D_PWS_FED_TYPE_CD as 'Federal Type',
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
					TMNVIOL.EXTERNAL_SYS_NUM as 'Violation ID',
					TMNVTYPE.TYPE_CODE as 'Violation Type',
					TMNVTYPE.name as 'Violation Name',
					TMNVTYPE.CATEGORY_CODE as 'Violation Category',
					TMNVIOL.STATUS_TYPE_CODE as 'Violation Status',
					tmnviol.DETERMINATION_DATE as 'Determination Date',
					tmnviol.STATE_PRD_BEGIN_DT as 'State Begin Date',
					tmnviol.STATE_PRD_END_DT as 'State End Date',
					TMNVIOL.TINWSYS_IS_NUMBER as 'viol_connector',
					tmnviol.TMNVIOL_IS_NUMBER as 'ea_connector',
					tmnviol.EXCEEDENCES_CNT as 'number of exceedances',
					tmnviol.SAMPLES_MISSNG_CNT as 'samples missing count',
					tmnviol.SAMPLES_RQD_CNT as 'samples requried',
					tmnviol.ANALYSIS_RESULT_TE as 'Analysis Result',
					TMNVIOL.ANALYSIS_RESULT_UO as 'Analysis Unit of Measure',
					TSAANLYT.NAME as 'Analyte Name',
					TSAANLYT.CODE as 'Analyte Code',
					TMNVIOL.D_INITIAL_TS as 'Viol create date',
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
				TENACTYP.LOCATION_TYPE_CODE+TENACTYP.FORMAL_TYPE_CODE+TENACTYP.SUB_CATEGORY_CODE AS 'Enforcement Action Code', 
				TENENACT.STATUS_DATE AS 'Enforcement Action Date', 
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
			
)

sdwisuser as (

			select distinct
			tinuser.USERID as 'connector',

			case
				when tinuser.USERID = 'MWIEDEM'									then 'DISTRICT 21 - VALLEY'
				when tinuser.USERID = 'SMARZOO'									then 'DISTRICT 03 - MENDOCINO'
				when tinuser.USERID = 'LRAMIRE1'								then 'DISTRICT 12 - VISALIA'
				when tinuser.USERID = 'EBREWER'									then 'DISTRICT 13 - SAN BERNARDINO'
				when tinuser.USERID = 'FSTIERIN'								then 'DISTRICT 15 - METROPOLITAN'
				when tinuser.USERID = 'HBUI'									then 'DISTRICT 08 - SANTA ANA'
				when tinuser.USERID in ('SRAFIQUE', 'CJUAREZ','BPOTTER')		then 'HQ - SOUTHERN ENGAGEMENT UNIT'
				when tinuser.USERID in ('BKIDWELL','SKLINE')					then 'HQ - NORTHERN ENGAGEMENT UNIT'
				when tinuser.USERID in ('DLESLIE')								then 'HQ - NEEDS ANALYSIS UNIT'
				when tinuser.USERID in ('JCHAO','SROSILEL')						then 'HQ - RECYCLED WATER UNIT'
				when tinuser.USERID in ('GGUTIERR','SBURKE','DWANG','WKILLOU')	then 'HQ - DATA SUPPORT UNIT'
				when tinuser.USERID in ('SWONG')								then 'HQ - REGULATORY DEVELOPMENT UNIT'
				when tinuser.USERID in ('ELEUNG')								then 'HQ - TREATMENT TECHNOLOGY UNIT'
				when tinuser.USERID in ('LJENSEN','JGUZMAN','ALOPES','BOSULLI') then 'HQ - COUNTY ENGAGEMENT UNIT'
				when tinuser.USERID in ('SHAFEZN')								then 'HQ - SOUTHERN CALIFORNIA FOB'
				when tinuser.USERID in ('JALBRECH','ALITTLE')					then 'HQ - RURAL SOLUTIONS UNIT'
				when tinuser.USERID in ('EZUNIGA')								then 'HQ - DROUGHT UNIT'
				when tinuser.USERID in ('MEASTER','JEMOND','RZARGHAM','GPENALES','RSPRINGB','PWILLIAM')									
				then 'HQ - DATA MANAGEMENT UNIT'
				when tinuser.USERID in ('SDWISADM','APPTEST','MMACINTO','LSCHIEDE','MGERMINO','ELEVY','ATALPASA','JTESTER','KVAITHI')	then 'DIT'
				else tinlgent.name end as 'SDWIS User Regulating Agency',
			tinuser.USERID 'SDWIS UserID',
			a.NAME 'SDWIS User',
			tinlgcom.ELECTRONIC_ADDRESS 'SDWIS User Email'

			from tinuser
			inner join tinlgent a on a.TINLGENT_IS_NUMBER = tinuser.TINLGENT_IS_NUMBER
			inner join tinuserl on tinuserl.TINLGENT_IS_NUMBER = a.TINLGENT_IS_NUMBER and tinuserl.EFF_END_DATE is null
			left join tinlgcom on tinlgcom.TINLGENT_IS_NUMBER = a.TINLGENT_IS_NUMBER and tinlgcom.PURPOSE_CODE = 'email'
			left join tinlgent on tinlgent.TINLGENT_IS_NUMBER = tinuser.CURR_TINGOVAG_ISN
			
),

final as (select
	pws.[Regulating Agency],
	pws.[Public Water System ID],
	pws.[Public Water System],
	pws.[Activity Status],
	pws.[Federal Type],
	viol.[Violation Name],
	viol.[Violation Type],
	viol.[Violation ID],
	viol.[Analyte Name],
	viol.[Analyte Code],
	format(viol.[State Begin Date], 'yyyy-MM-dd') as ' Violation Begin Date',
	format(viol.[State End Date], 'yyyy-MM-dd') as 'Violation End Date',
	--viol.[Viol create date],
	ea.[Enforcement Action Code],
	format(ea.[EA Created Date], 'yyyy-MM-dd') as 'Enforcement Action Date'--,
	--sdwisuser.[SDWIS User],
	--sdwisuser.[SDWIS User Email]

	from
	pws
	inner join viol on pws.connector = viol.viol_connector
	inner join ea on viol.ea_connector = ea.ea_viol_connector
--	inner join sdwisuser on sdwisuser.connector = viol.[User Name]
	
	union

	select
	pws.[Regulating Agency],
	pws.[Public Water System ID],
	pws.[Public Water System],
	pws.[Activity Status],
	pws.[Federal Type],
	viol.[Violation Name],
	viol.[Violation Type],
	viol.[Violation ID],
	viol.[Analyte Name],
	viol.[Analyte Code],
	format(viol.[State Begin Date], 'yyyy-MM-dd') as ' Violation Begin Date',
	format(viol.[State End Date], 'yyyy-MM-dd') as 'Violation End Date',
	--format(viol.[Viol create date],'yyyy-MM-dd') as '
	ea.[Enforcement Action Code],
	format(ea.[EA Created Date], 'yyyy-MM-dd') as 'Enforcement Action Date'--,
	--sdwisuser.[SDWIS User],
	--sdwisuser.[SDWIS User Email]

	from
	pws
	inner join viol on pws.connector = viol.viol_connector
	inner join ea on viol.ea_connector = ea.ea_viol_connector
	inner join sdwisuser on sdwisuser.connector = viol.[User Name]
	
	),

final_rtc as (
				select distinct
				final.*
				from
				final
				where
				final.[Enforcement Action Code] in ('SOX','SO0','SO6'))

		
	select 
	final.*
	from
	final
	where
	final.[Enforcement Action Code] not in ('SOX','SO0','SO6')