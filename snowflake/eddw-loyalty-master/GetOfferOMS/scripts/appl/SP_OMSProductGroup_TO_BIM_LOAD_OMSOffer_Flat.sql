--liquibase formatted sql
--changeset SYSTEM:SP_OMSProductGroup_TO_BIM_LOAD_OMSOffer_Flat runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMSOFFER_FLAT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_db = CNF_DB;
   var cnf_schema = C_PROD;
   var wrk_schema = C_STAGE;
   var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.ProductGroup_OMSOffer_Flat_WRK`;
   var tgt_tbl = `${cnf_db}.${cnf_schema}.OfferOMS_Flat`;
   

	    var cr_src_wrk_tbl = `	CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` 
								AS
								SELECT DISTINCT A.PAYLOAD_ID, A.product_Group_Id
								FROM 
								(
								Select PAYLOAD_ID, QPG.VALUE:id::STRING AS product_Group_Id
								FROM `+ tgt_tbl +`
								,lateral flatten(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE)as QPG
								union 
								Select PAYLOAD_ID, QPG.VALUE:excludedProductGroupId::STRING AS product_Group_Id
								FROM `+ tgt_tbl +`
								,lateral flatten(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE)as QPG
								union 
								Select PAYLOAD_ID, PAYLOAD_QUALIFICATIONPRODUCTDISQUALIFIER AS product_Group_Id
								FROM `+ tgt_tbl +`
								union 
								Select PAYLOAD_ID, pbd.VALUE:includeProductGroupId::STRING AS product_Group_Id
								FROM `+ tgt_tbl +`
								,lateral flatten(input => payload_benefit_discount, outer => TRUE)as pbd
								union 
								Select PAYLOAD_ID, pbd.VALUE:excludeProductGroupId::STRING AS product_Group_Id
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
					   FROM ( 
							SELECT DISTINCT PAYLOAD_ID			
							FROM `+ tgt_wrk_tbl +`
					   	) src
					   	WHERE SRC.PAYLOAD_ID = TGT.PAYLOAD_ID AND DATE(TGT.PAYLOAD_EFFECTIVEENDDATE) >= CURRENT_DATE
					   	`;
    
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
