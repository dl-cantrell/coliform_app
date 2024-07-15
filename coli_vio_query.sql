					use SDWIS32
go
					select distinct 
					TINWSYS.NAME as PWS_NAME,
					TINWSYS.NUMBER0 as PWS_NO,
					TMNVIOL.EXTERNAL_SYS_NUM as 'Violation ID',
					--TMNVTYPE.TYPE_CODE as 'Violation Type',
					TMNVTYPE.name as 'Violation Name',
				    TMNVTYPE.CATEGORY_CODE as 'Violation Category',
					--TMNVIOL.STATUS_TYPE_CODE as 'Violation Status',
					tmnviol.DETERMINATION_DATE as 'Determination Date',
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
					
					order by TINWSYS.NUMBER0
	