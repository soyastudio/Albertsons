--liquibase formatted sql
--changeset SYSTEM:SP_DIMENSION_LOAD_D1_Clip runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_DIMENSION_LOAD_D1_CLIP
("SRC_WRK_TBL" VARCHAR(16777216), "ANL_DB" VARCHAR(16777216), "ANL_SCHEMA" VARCHAR(16777216), "WRK_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$ 
        // **************        Load for D1_Clip table BEGIN *****************
        var src_wrk_tbl = SRC_WRK_TBL;
        var anl_db = ANL_DB;
        var anl_schema = ANL_SCHEMA;
        var wrk_schema = WRK_SCHEMA; 
    	
    	// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
        
        var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".D1_Clip_WRK";
        var tgt_tbl = anl_db + "." + anl_schema + ".D1_Clip";   
		
		var cnf_db = "EDM_CONFIRMED_PRD";
		var loyalty_schema = "DW_C_LOYALTY";
		var CLIP_DETAILS_tbl = cnf_db + "." + loyalty_schema + ".CLIP_DETAILS";
        

        var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
        						SELECT   src.Clip_Platform_Cd
												,src.Clip_Source_Nm											
												,src.Clip_Type_Cd
    											,src.DW_LOGICAL_DELETE_IND
    											,CASE WHEN tgt.Clip_Platform_Cd is NULL and tgt.Clip_Source_Nm is null 
												and tgt.Clip_Type_Cd is null then 'I' ELSE 'U' END as DML_Type
    									FROM (
    										SELECT 	distinct Clip_Platform_Cd
    												,Clip_Source_Nm
													,Clip_Type_Cd
    												,FALSE AS DW_Logical_delete_ind
    										FROM (
    												SELECT case when Clip_Source_Cd in ('chkios','chkand','appan_','appios','emmd','11223_','mobile','app','appandroid','112233445566778000',
												'mobil_','mobile-android-shop','mobile-ios-shop') then 'UMA'
                                             when Clip_Source_Cd in ('emjou','chkweb','dlink','flipp','emju','web-p_','web-portal') then 'WEB' 
                                             when Clip_Source_Cd in ('OCGP-_','RXWA','United','q-app','239eb_','???') then 'Other'
                                             when Clip_Source_Cd in ('SVSC','SVCT','ccarw_','svct') then 'CCA' end as Clip_Platform_Cd												
    														,case when Clip_Source_Cd in ('chkweb','chkios','chkan','web-p_','mobil_','web-portal','mobile-android-shop','mobile-ios-shop') then 'Cart' 
																		else 'Gallery' end as Clip_Source_Nm
															,CLIP_TYPE_CD as Clip_Type_Cd    														
    												FROM   ` + CLIP_DETAILS_tbl +` 
													where offer_id in (select distinct offer_id from `+ src_wrk_tbl +`)
													and dw_current_version_ind = TRUE
													and dw_logical_delete_ind = FALSE
    												
    												
    											)    										
    									) src
    								LEFT JOIN
    										(
    										 SELECT 
    											    Clip_Platform_Cd
    												,Clip_Source_Nm
													,Clip_Type_Cd
													,DW_LOGICAL_DELETE_IND
    										 FROM   ` + tgt_tbl + `
    										 ) tgt 	on 	src.Clip_Type_Cd = tgt.Clip_Type_Cd
														and nvl(src.Clip_Source_Nm, '-1') = nvl(tgt.Clip_Source_Nm, '-1')
														and nvl(src.Clip_Platform_Cd, '-1') = nvl(tgt.Clip_Platform_Cd, '-1') 
    								 where 
    								   (
    								  nvl(tgt.Clip_Platform_Cd, '-1') <> nvl(src.Clip_Platform_Cd, '-1') OR
									  nvl(tgt.Clip_Source_Nm, '-1') <> nvl(src.Clip_Source_Nm, '-1') OR
									  nvl(tgt.Clip_Type_Cd, '-1') <> nvl(src.Clip_Type_Cd, '-1') OR
    								  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                         )
    								  `;
        
        try {
            snowflake.execute (
                {sqlText: cr_src_wrk_tbl  }
            )
        }
        catch (err)  {
            return "Creation of D1_Clip tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
            }

		 
    var sql_begin = "BEGIN"

        // Processing Inserts
        var sql_inserts = `INSERT INTO ` + tgt_tbl + `
            ( 
             Clip_Platform_Cd
    		,Clip_Source_Nm
			,Clip_Type_Cd
    		,DW_CREATE_TS 
			,Dw_Last_Update_Ts
    		,DW_LOGICAL_DELETE_IND
        )
        SELECT distinct
             Clip_Platform_Cd
    		,Clip_Source_Nm
			,Clip_Type_Cd
    		,current_timestamp() AS DW_CREATE_TS 
			,'9999-12-31 00:00:00.000 -0600' AS Dw_Last_Update_Ts 
    		,DW_LOGICAL_DELETE_IND
        FROM ` + tgt_wrk_tbl + `
        WHERE DML_Type = 'I'
       `; 

        var sql_commit = "COMMIT"
        var sql_rollback = "ROLLBACK"
        try {
            snowflake.execute (
                {sqlText: sql_begin}
            );
            snowflake.execute (
                {sqlText: sql_inserts}
            );
            snowflake.execute (
                {sqlText: sql_commit}
            );    
        }
        catch (err) {
            snowflake.execute (
                {sqlText: sql_rollback}
            );
            return "Loading of D1_Clip " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
                // **************        Load for D1_Clip ENDs *****************
                
        return "Done"

$$;
