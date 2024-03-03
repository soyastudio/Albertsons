--liquibase formatted sql
--changeset SYSTEM:SP_OMSSTOREGROUP_TO_BIM_LOAD_OMSOFFER_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_OMSSTOREGROUP_TO_BIM_LOAD_OMSOFFER_FLAT
(SRC_WRK_TBL VARCHAR(16777216), CNF_DB VARCHAR(16777216), C_PROD VARCHAR(16777216), C_STAGE VARCHAR(16777216), C_LOC VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$
    var src_wrk_tbl = SRC_WRK_TBL;
    var cnf_db = CNF_DB;
    var cnf_schema = C_PROD ;
    var wrk_schema = C_STAGE ;
    var loc_schema = C_LOC ;
    var tgt_tbl =  "EDM_REFINED_PRD.dw_r_product.offeroms_flat";
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".StoreGroup_OMSOffer_Flat_WRK";
    

	    var cr_src_wrk_tbl = `	CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` 
								AS
								SELECT DISTINCT A.PAYLOAD_ID, A.Store_Group_Id, A.PAYLOAD_EFFECTIVEENDDATE
								FROM 
								(								
								Select PAYLOAD_ID, QSG.VALUE:id::STRING AS Store_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => payload_qualificationStoreGroups_redemptionStoreGroups, outer => TRUE)as QSG
								
								union ALL
								
								Select PAYLOAD_ID,TSG.VALUE::STRING AS Store_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => payload_testStoreGroups, outer => TRUE)  as TSG								
								
								)A
								JOIN ` + src_wrk_tbl +` B ON A.Store_Group_Id = B.PAYLOAD_ID
                            `;
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of OMSOffer_Flat "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

	var sql_updates = `UPDATE ${tgt_tbl} as tgt
					   SET DW_CREATE_TS = DATEADD(minute, 1, DW_CREATE_TS)
					   WHERE PAYLOAD_ID in (SELECT DISTINCT PAYLOAD_ID			
							FROM `+ tgt_wrk_tbl +` WHERE PAYLOAD_EFFECTIVEENDDATE>=CURRENT_DATE)`;
    
	var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    
	try {
        
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}



                // **************        Load for OMSOffer_Flat table ENDs *****************
$$;
