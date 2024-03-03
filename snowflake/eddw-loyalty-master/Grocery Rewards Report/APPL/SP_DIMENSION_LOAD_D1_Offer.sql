--liquibase formatted sql
--changeset SYSTEM:SP_DIMENSION_LOAD_D1_Offer runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_DIMENSION_LOAD_D1_Offer
(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$ 
        // **************        Load for D1_Offer table BEGIN *****************
        var src_wrk_tbl = SRC_WRK_TBL;
        var anl_db = ANL_DB;
        var anl_schema = ANL_SCHEMA;
        var wrk_schema = WRK_SCHEMA;
		
		var cnf_db = "EDM_CONFIRMED_PRD";
		var cnf_schema = "DW_C_PRODUCT";
		var purchase_schema = "DW_C_PURCHASING";
    	
    	// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
        
        var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".D1_Offer_WRK";
        var tgt_tbl = anl_db + "." + anl_schema + ".D1_Offer";    

		var OMS_OFFER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER";	
		var Offer_Request_Requirement_Type_tbl = cnf_db + "." + purchase_schema + ".Offer_Request_Requirement_Type";
		var OMS_OFFER_BENEFIT_DISCOUNT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT";
		var OMS_OFFER_BENEFIT_DISCOUNT_tier_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT_tier";
		
        
        var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
        						SELECT   src.Offer_Id
												,src.Offer_Nm
												,src.Offer_Program_Cd
												,src.Offer_Type_Cd
												,src.Offer_Status_Cd
												,src.Offer_Benefit_Value_Type_Dsc
												,src.Offer_Dollar_Value_Amt
												,src.Offer_Reward_Value_Qty
    											,src.DW_LOGICAL_DELETE_IND
    											,CASE WHEN tgt.Offer_Id is null then 'I' ELSE 'U' END as DML_Type
    									FROM (
    										SELECT 	 
													Offer_Id
													,Offer_Nm
													,Offer_Program_Cd
													,Offer_Type_Cd
													,Offer_Status_Cd
													,Offer_Benefit_Value_Type_Dsc
													,Offer_Dollar_Value_Amt
													,Offer_Reward_Value_Qty
    												,FALSE AS DW_Logical_delete_ind
    										FROM 
    												
											(	
												SELECT DISTINCT oms.OMS_Offer_Id as Offer_Id,
													   Offer_Nm as Offer_Nm,
													   Program_cd as Offer_Program_Cd,
													   offer_prototype_cd as Offer_Type_Cd,
													   Offer_Status_Cd as Offer_Status_Cd,
													   benefit_value_type_dsc as Offer_Benefit_Value_Type_Dsc,
													   discount_tier_amt as Offer_Dollar_Value_Amt,
													   required_qty as Offer_Reward_Value_Qty
											from `+OMS_OFFER_tbl+` oms
left join `+OMS_OFFER_BENEFIT_DISCOUNT_tbl+` bd on bd.oms_offer_id = oms.oms_offer_id and 
						bd.dw_current_version_ind = true and bd.dw_logical_delete_ind = false and bd.DISCOUNT_ID = 1
left join `+OMS_OFFER_BENEFIT_DISCOUNT_tier_tbl+` tier on tier.oms_offer_id = oms.oms_offer_id and 
                        tier.dw_current_version_ind = true and tier.dw_logical_delete_ind = false and tier.DISCOUNT_TIER_ID = 1
left join `+Offer_Request_Requirement_Type_tbl+` req on oms.offer_request_id = req.offer_request_id
                        and req.dw_current_version_ind = true and req.dw_logical_delete_ind = false and req.REQUIREMENT_TYPE_CD = 'Rewards'
WHERE OMS.OMS_OFFER_ID IN (SELECT DISTINCT OFFER_ID FROM  `+ src_wrk_tbl +`)
and oms.PROGRAM_CD = 'GR' and OMS.DW_CURRENT_VERSION_IND = TRUE

   											
    											)    										
    									) src
										
    								LEFT JOIN
    										(
    										 SELECT 
    											    Offer_Id
													,Offer_Nm
													,Offer_Program_Cd
													,Offer_Type_Cd
													,Offer_Status_Cd
													,Offer_Benefit_Value_Type_Dsc
													,Offer_Dollar_Value_Amt
													,Offer_Reward_Value_Qty
													,DW_LOGICAL_DELETE_IND
    										 FROM   ` + tgt_tbl + `
    										 ) tgt 	on 	src.Offer_Id = tgt.Offer_Id
    								 where tgt.Offer_Id is null OR
    								   (
    								  nvl(tgt.Offer_Nm, '-1') <> nvl(src.Offer_Nm, '-1') OR
									  nvl(tgt.Offer_Program_Cd, '-1') <> nvl(src.Offer_Program_Cd, '-1') OR
									  nvl(tgt.Offer_Type_Cd, '-1') <> nvl(src.Offer_Type_Cd, '-1') OR
									  nvl(tgt.Offer_Status_Cd, '-1') <> nvl(src.Offer_Status_Cd, '-1') OR
									  nvl(tgt.Offer_Benefit_Value_Type_Dsc, '-1') <> nvl(src.Offer_Benefit_Value_Type_Dsc, '-1') OR
									  nvl(tgt.Offer_Dollar_Value_Amt, '-1') <> nvl(src.Offer_Dollar_Value_Amt, '-1') OR
									  nvl(tgt.Offer_Reward_Value_Qty, '-1') <> nvl(src.Offer_Reward_Value_Qty, '-1') OR
    								  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                         )
    								  `;
        
        try {
            snowflake.execute (
                {sqlText: cr_src_wrk_tbl  }
            )
        }
        catch (err)  {
            return "Creation of D1_Offer tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
            }

var sql_updates = // Processing Updates of Type 1 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  Offer_Nm                      = src.Offer_Nm
						,Offer_Program_Cd              = src.Offer_Program_Cd
						,Offer_Type_Cd                 = src.Offer_Type_Cd
						,Offer_Status_Cd               = src.Offer_Status_Cd
						,Offer_Benefit_Value_Type_Dsc  = src.Offer_Benefit_Value_Type_Dsc
						,Offer_Dollar_Value_Amt        = src.Offer_Dollar_Value_Amt
						,Offer_Reward_Value_Qty        = src.Offer_Reward_Value_Qty                      
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   
									 Offer_Id
									,Offer_Nm
									,Offer_Program_Cd
									,Offer_Type_Cd
									,Offer_Status_Cd
									,Offer_Benefit_Value_Type_Dsc
									,Offer_Dollar_Value_Amt
									,Offer_Reward_Value_Qty                            
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Offer_Id = tgt.Offer_Id`;  
					
		 
    var sql_begin = "BEGIN"

        // Processing Inserts
        var sql_inserts = `INSERT INTO ` + tgt_tbl + `
            ( 
             Offer_Id
			,Offer_Nm
			,Offer_Program_Cd
			,Offer_Type_Cd
			,Offer_Status_Cd
			,Offer_Benefit_Value_Type_Dsc
			,Offer_Dollar_Value_Amt
			,Offer_Reward_Value_Qty  
    		,DW_CREATE_TS 
			,Dw_Last_Update_Ts
    		,DW_LOGICAL_DELETE_IND
        )
        SELECT
             Offer_Id
			,Offer_Nm
			,Offer_Program_Cd
			,Offer_Type_Cd
			,Offer_Status_Cd
			,Offer_Benefit_Value_Type_Dsc
			,Offer_Dollar_Value_Amt
			,Offer_Reward_Value_Qty  
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
				{sqlText: sql_updates}
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
            return "Loading of D1_Offer " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
                // **************        Load for D1_Offer ENDs *****************
                
        return "Done"

$$;
