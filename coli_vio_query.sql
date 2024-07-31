Use SDWIS
go


select distinct 
					TINLGENT.NAME as 'regulatory_agency',
					TINWSYS.NAME as water_system_name,
					TINWSYS.NUMBER0 as water_system_no,
					TMNVIOL.EXTERNAL_SYS_NUM as 'violation_id',
					TMNVTYPE.TYPE_CODE as 'violation_type',
					TMNVTYPE.name as 'violation_name',
				    TMNVTYPE.CATEGORY_CODE as 'violation_category',
					--TMNVIOL.STATUS_TYPE_CODE as 'Violation Status',
					tmnviol.FED_PRD_BEGIN_DT as 'federal_begin_date',
					tmnviol.FED_PRD_END_DT as 'federal_end_date',
					tmnviol.EXCEEDENCES_CNT as 'number_of_exceedances',
					tmnviol.SAMPLES_MISSNG_CNT as 'samples_missing_count',
					tmnviol.SAMPLES_RQD_CNT as 'samples_required',
					tmnviol.ANALYSIS_RESULT_TE as 'analysis_result',
					TMNVIOL.ANALYSIS_RESULT_UO as 'analysis_units_of_measure',
					TSAANLYT.NAME as 'analyte_name',
					TSAANLYT.CODE as 'analyte_code'
					
						
					from TMNVIOL
					inner join TINWSYS on TINWSYS.TINWSYS_IS_NUMBER = TMNVIOL.TINWSYS_IS_NUMBER
				--	AND TINWSYS.NUMBER0 = ?sys
					inner join TMNVTYPE on TMNVIOL.TMNVTYPE_IS_NUMBER = TMNVTYPE.TMNVTYPE_IS_NUMBER
						and TMNVIOL.STATUS_TYPE_CODE = 'V'
					    AND TMNVIOL.TMNVTYPE_ST_CODE = TMNVTYPE.TMNVTYPE_ST_CODE
					inner join TSAANLYT on TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							  and TSAANLYT.CODE in ('8000', '3100', '3029','3013', '3000') -- this is for coliform (revised total coliform rule) and three other older coliform rules
							--and TSAANLYT.CODE in ('2456', '2956', '1011')   -- this is for dbp's
							AND TMNVIOL.TSAANLYT_IS_NUMBER = TSAANLYT.TSAANLYT_IS_NUMBER
							--and (TMNVTYPE.TYPE_CODE = '01' or  TMNVTYPE.TYPE_CODE = '02') 
					left join TMNVGRP on TMNVGRP.TMNVGRP_IS_NUMBER = TMNVIOL.TMNVGRP_IS_NUMBER
							and TMNVGRP.TMNVGRP_ST_CODE = TMNVIOL.TMNVGRP_ST_CODE
					left join TSAANGRP on TSAANGRP.TSAANGRP_IS_NUMBER = TMNVGRP.TSAANGRP_IS_NUMBER
							and TSAANGRP.TSAANGRP_ST_CODE = TMNVGRP.TSAANGRP_ST_CODE  
					inner join TINRAA on TINRAA.TINWSYS_IS_NUMBER = TINWSYS.TINWSYS_IS_NUMBER
					inner join TINLGENT on TINRAA.TINLGENT_IS_NUMBER=TINLGENT.TINLGENT_IS_NUMBER
						and (TINLGENT.name like 'District%' or TINLGENT.name like 'Lpa%')
				
					Where TINWSYS.NUMBER0 = ?sys
					order by tmnviol.FED_PRD_BEGIN_DT