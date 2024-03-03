--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Qualification_Product_Group_Tier runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_QUALIFICATION_PRODUCT_GROUP_TIER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PRODUCT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Qualification_Product_Group_Tier_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer_Qualification_Product_Group_Tier`;

                       
    // **************        Load for OMS_Offer_Qualification_Product_Group_Tier table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
		var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;


	    var cr_src_wrk_tbl = `INSERT INTO `+ tgt_wrk_tbl +` 
		
							with flat_tmp as
                            ( select 
                             payload_id as  OMS_Offer_Id
							 ,PAYLOAD_QUALIFICATIONPRODUCTGROUPS
                            ,filename
                            ,row_number() over ( PARTITION BY OMS_Offer_Id
                                                  ORDER BY to_timestamp_ltz(LastUpdateTs) desc) as rn
                             from ` + src_wrk_tbl +` 
                             WHERE OMS_Offer_Id IS NOT NULL AND PAYLOAD_QUALIFICATIONPRODUCTGROUPS  IS NOT NULL	
                             )
							 	
								SELECT DISTINCT
								OMS_Offer_Id,
								Product_Group_Id,
								Tier_Level_Nbr,
								Tier_Amt,
								filename
								FROM (
										SELECT  
										OMS_Offer_Id,
										PAYLOAD_PRODUCTGROUPS.value:id::string AS Product_Group_Id,
										PAYLOAD_PRODUCTGROUPSTIERS.value:level::string AS Tier_Level_Nbr,
										PAYLOAD_PRODUCTGROUPSTIERS.value:amount::string AS Tier_Amt,
										filename,
										rn
										FROM flat_tmp
										,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE ) as PAYLOAD_PRODUCTGROUPS
										,LATERAL FLATTEN(input => PAYLOAD_PRODUCTGROUPS.value:tiers, outer => TRUE ) as PAYLOAD_PRODUCTGROUPSTIERS
									)
    
                                WHERE 
							    OMS_Offer_Id is not NULL AND
							    Product_Group_Id is not NULL AND
								Tier_Level_Nbr is not NULL
                                and rn = 1 
   `;
    try {
    snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of OMS_Offer_Qualification_Product_Group_Tier tgt_wrk_tbl table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

	sql_deletes = `DELETE FROM ` + tgt_tbl + `
                   WHERE (OMS_Offer_Id) in
                   (SELECT OMS_Offer_Id 
                   FROM `+ tgt_wrk_tbl +`) 
                   `;

 try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }   catch (err)  {
        return "Delete records for OMS_Offer_Qualification_Product_Group_Tier  table " + tgt_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
	// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
	OMS_Offer_Id,
	Product_Group_Id,
	Tier_Level_Nbr,
	DW_First_Effective_dt, 
	DW_Last_Effective_dt, 
	Tier_Amt,
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND  
	)
	SELECT
		OMS_Offer_Id,
		Product_Group_Id,
		Tier_Level_Nbr,
		CURRENT_DATE as DW_First_Effective_dt,
		'31-DEC-9999',
		Tier_Amt,
		CURRENT_TIMESTAMP,
		FALSE AS DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
		OMS_Offer_Id is not NULL AND
		Product_Group_Id is not NULL AND
		Tier_Level_Nbr is not NULL
	`;
    
	var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    
	try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
                // **************        Load for OMS_Offer_Qualification_Product_Group_Tier table ENDs *****************
                $$;
