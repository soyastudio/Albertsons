--liquibase formatted sql
--changeset SYSTEM:SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMSOFFER_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;


CREATE OR REPLACE PROCEDURE SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMSOFFER_FLAT
(SRC_WRK_TBL VARCHAR(16777216), CNF_DB VARCHAR(16777216), C_PROD VARCHAR(16777216), C_STAGE VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS
$$ 
   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_db = CNF_DB;
   var cnf_schema = C_PROD;
   var wrk_schema = C_STAGE;
   var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.ProductGroup_OMSOffer_Flat_WRK`;
   var tgt_tbl = "EDM_REFINED_PRD.dw_r_product.offeroms_flat";
   

	    var cr_src_wrk_tbl = `	CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` 
								AS
								SELECT DISTINCT A.PAYLOAD_ID, A.product_Group_Id, A.PAYLOAD_EFFECTIVEENDDATE
								FROM 
								(
								Select PAYLOAD_ID, QPG.VALUE:id::STRING AS product_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE)as QPG
								union 
								Select PAYLOAD_ID, QPG.VALUE:excludedProductGroupId::STRING AS product_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE)as QPG
								union 
								Select PAYLOAD_ID, PAYLOAD_QUALIFICATIONPRODUCTDISQUALIFIER AS product_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								union 
								Select PAYLOAD_ID, pbd.VALUE:includeProductGroupId::STRING AS product_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => payload_benefit_discount, outer => TRUE)as pbd
								union 
								Select PAYLOAD_ID, pbd.VALUE:excludeProductGroupId::STRING AS product_Group_Id, date(PAYLOAD_EFFECTIVEENDDATE) as PAYLOAD_EFFECTIVEENDDATE
								FROM `+ tgt_tbl +`
								,lateral flatten(input => payload_benefit_discount, outer => TRUE)as pbd
								)A
								JOIN ` + src_wrk_tbl +` B ON A.product_Group_Id = B.PAYLOAD_ID
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
