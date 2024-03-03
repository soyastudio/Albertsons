--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Benefit_Discount_Tier runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_BENEFIT_DISCOUNT_TIER("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_PROD" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS    
$$
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_PROD;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Offer_Benefit_Discount_Tier_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Offer_Benefit_Discount_Tier`;

// ************** Load for OMS_Offer_Benefit_Discount_Tier table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
     
// Empty the target work table
		var sql_empty_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl +` `;
		try {
			snowflake.execute ({sqlText: sql_empty_tgt_wrk_tbl });
			}
		catch (err) { 
			throw "Truncation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
		}
		
var sql_command = `INSERT INTO ${tgt_wrk_tbl}
                            WITH src_wrk_tbl_recs as
                            (
							SELECT DISTINCT 
							payload_id as OMS_Offer_Id
						   ,PAYLOAD_BENEFIT_DISCOUNT
						   ,lastUpdateTS
						   ,Filename
						   ,Row_number() OVER ( partition BY OMS_Offer_Id ORDER BY To_timestamp_ntz(lastUpdateTS) DESC) AS rn                            
						   FROM ${src_wrk_tbl}
						    WHERE 
							OMS_Offer_Id is not NULL and 
							PAYLOAD_BENEFIT_DISCOUNT is not null						  
                          )
						  
						  SELECT DISTINCT 
                             OMS_Offer_Id
							,Discount_Id							 
							,Discount_Tier_Id
							,Discount_Tier_Level_Nbr
							,Discount_Tier_Amt
							,Discount_Tier_Up_To_Nbr
							,Item_Limit_Qty
							,Weight_Limit_Qty
							,Dollar_Limit_Amt
							,Receipt_Txt
							,lastUpdateTS
							,FileName
							from
                            (
							SELECT  
                            OMS_Offer_Id
						   ,(PAYLOAD_BENEFIT_DISCOUNT1.index + 1 ) as Discount_Id
						   ,(PAYLOAD_BENEFIT_DISCOUNT2.index + 1 ) as Discount_Tier_Id
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:level::string  as Discount_Tier_Level_Nbr
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:amount::string  as Discount_Tier_Amt
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:upTo::string  as Discount_Tier_Up_To_Nbr
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:itemLimit::string  as Item_Limit_Qty
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:weightLimit::string as Weight_Limit_Qty
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:dollarLimit::string as Dollar_Limit_Amt
						   ,PAYLOAD_BENEFIT_DISCOUNT2.value:receiptText::string as Receipt_Txt
						   ,lastUpdateTS
						   ,Filename
						   ,rn
						   FROM src_wrk_tbl_recs
						  ,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_DISCOUNT, outer => TRUE ) as PAYLOAD_BENEFIT_DISCOUNT1
                          ,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_DISCOUNT1.value:discountTier, outer => TRUE ) as PAYLOAD_BENEFIT_DISCOUNT2
                          )
			          WHERE 
							            OMS_Offer_Id is not NULL AND
							            Discount_Id is not NULL AND
								    Discount_Tier_Id is not NULL AND
										rn = 1 
                          `;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of OMS_Offer_Benefit_Discount_Tier work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
        return "Delete records for OMS_Offer_Benefit_Discount_Tier  table " + tgt_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
var sql_begin = "BEGIN"
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
					 OMS_Offer_Id
					,Discount_Id
					,Discount_Tier_Id
					,DW_First_Effective_Dt      
					,DW_Last_Effective_Dt
					,Discount_Tier_Level_Nbr
					,Discount_Tier_Amt
					,Discount_Tier_Up_To_Nbr
					,Item_Limit_Qty
					,Weight_Limit_Qty
					,Dollar_Limit_Amt
					,Receipt_Txt
					,DW_CREATE_TS                                                                                             
                    ,DW_LOGICAL_DELETE_IND                                                                                           
                    ,DW_SOURCE_CREATE_NM                                                                                         
                    ,DW_CURRENT_VERSION_IND      
                   )
                   SELECT distinct
                     OMS_Offer_Id 
					,Discount_Id
					,Discount_Tier_Id
					,CURRENT_TIMESTAMP 
                    ,'31-DEC-9999'     
                    ,Discount_Tier_Level_Nbr
					,Discount_Tier_Amt
					,Discount_Tier_Up_To_Nbr
					,Item_Limit_Qty
					,Weight_Limit_Qty
					,Dollar_Limit_Amt
					,Receipt_Txt
					,CURRENT_TIMESTAMP
                    ,False
                    ,FileName
                    ,TRUE                                                                                                      
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
				and Discount_Id is not null
				and Discount_Tier_Id is not null
				`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
		snowflake.execute (
            {sqlText: sql_inserts  }
            );
        snowflake.execute (
            {sqlText: sql_commit  }
            ); 

                             }             
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for OMS_Offer_Benefit_Discount_Tier table ENDs *****************

$$;
