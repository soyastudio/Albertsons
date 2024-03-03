--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_To_BIM_load_OMS_Offer_Benefit runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_BENEFIT
(SRC_WRK_TBL VARCHAR(16777216), CNF_DB VARCHAR(16777216), C_PRODUCT VARCHAR(16777216), C_STAGE VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS    
$$
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_PRODUCT;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Offer_Benefit_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Offer_Benefit`;

// ************** Load for OMS_Offer_Benefit table BEGIN *****************
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
                            (SELECT DISTINCT 
                             OMS_Offer_Id          
							,Benefit_Value_Type_Cd 
							,Benefit_Value_Type_Dsc
							,Benefit_Value_Amt
							,Customer_Group_Id
							,Customer_Group_Nm     
							,lastUpdateTS
							,FileName
							,Row_number() OVER ( partition BY OMS_Offer_Id ORDER BY To_timestamp_ntz(lastUpdateTS) DESC) AS rn
                            from
                            (
                            SELECT DISTINCT 
                            payload_id as OMS_Offer_Id
						   ,payload_benefit_benefitValueType as Benefit_Value_Type_Cd
						   ,payload_benefit_benefitValueDesc as Benefit_Value_Type_Dsc
						   ,payload_benefit_benefitValue as Benefit_Value_Amt
						   ,payload_benefit_groupMemberShip_customerGroupId as Customer_Group_Id
						   ,payload_benefit_groupMemberShip_customerGroupName as Customer_Group_Nm							
                           ,lastUpdateTS
						   ,Filename						   
                          FROM ${src_wrk_tbl}						                       
                          )
                          )
                          SELECT
                           src.OMS_Offer_Id
						  ,src.Benefit_Value_Type_Cd
						  ,src.Benefit_Value_Type_Dsc
						  ,src.Benefit_Value_Amt
						  ,src.Customer_Group_Id
						  ,src.Customer_Group_Nm						  
                          ,src.DW_Logical_delete_ind
                          ,src.lastUpdateTS
						  ,src.Filename			
                          ,CASE WHEN (tgt.OMS_Offer_Id IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
                          ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind                                                                                                                                                                                                      
						  from
                          (SELECT
                           OMS_Offer_Id
						  ,Benefit_Value_Type_Cd
						  ,Benefit_Value_Type_Dsc
						  ,Benefit_Value_Amt
						  ,Customer_Group_Id
						  ,Customer_Group_Nm						  
						  ,false AS DW_Logical_delete_ind                                                                                                                                                                                                                                                                                                               
                          ,lastUpdateTS
						  ,Filename
						  FROM src_wrk_tbl_recs 
                          WHERE rn = 1 
                          AND OMS_Offer_Id is not null
                          ) src 
                          LEFT JOIN 
                          (SELECT  DISTINCT
                           tgt.OMS_Offer_Id
						  ,tgt.Benefit_Value_Type_Cd 
						  ,tgt.Benefit_Value_Type_Dsc
						  ,tgt.Benefit_Value_Amt
						   ,tgt.Customer_Group_Id
						  ,tgt.Customer_Group_Nm						  
						  ,tgt.dw_logical_delete_ind
                          ,tgt.dw_first_effective_dt                                                                                                                                                                                                                                                               
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.OMS_Offer_Id = src.OMS_Offer_Id                                                                                                                                                                  
                          WHERE  (tgt.OMS_Offer_Id is null)  
                          or(
						  NVL(src.Benefit_Value_Type_Cd,'-1') <> NVL(tgt.Benefit_Value_Type_Cd,'-1')     
                          or NVL(src.Benefit_Value_Type_Dsc,'-1') <> NVL(tgt.Benefit_Value_Type_Dsc,'-1')     
						  or NVL(src.Benefit_Value_Amt,'-1') <> NVL(tgt.Benefit_Value_Amt,'-1')
						  or NVL(src.Customer_Group_Id,'-1') <> NVL(tgt.Customer_Group_Id,'-1')
						  or NVL(src.Customer_Group_Nm,'-1') <> NVL(tgt.Customer_Group_Nm,'-1')  
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  
						  union all
						  SELECT distinct 
						  tgt.OMS_Offer_Id
						,tgt.Benefit_Value_Type_Cd
						,tgt.Benefit_Value_Type_Dsc
						,tgt.Benefit_Value_Amt
						,tgt.Customer_Group_Id
						,tgt.Customer_Group_Nm            
						,TRUE AS DW_Logical_delete_ind
						,src.lastUpdateTS
						,tgt.DW_SOURCE_CREATE_NM as FileName
						,'U' as DML_Type
						,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
						  FROM ${tgt_tbl} tgt
						  LEFT JOIN
						  (
						  SELECT  DISTINCT 
						  OMS_Offer_Id
						  ,Benefit_Value_Type_Cd
						  ,Benefit_Value_Type_Dsc
						  ,Benefit_Value_Amt
							,Customer_Group_Id
						  ,Customer_Group_Nm						  
						  ,lastUpdateTS
						  ,Filename
						 
               			  FROM  src_wrk_tbl_recs
						  ) src 
						  ON src.OMS_Offer_Id = tgt.OMS_Offer_Id
						  WHERE (tgt.OMS_Offer_Id ) in (select distinct OMS_Offer_Id
							FROM src_wrk_tbl_recs)
						  and  tgt.dw_current_version_ind = TRUE
						  AND tgt.dw_logical_delete_ind = FALSE
						  and src.OMS_Offer_Id is NULL
						  `;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of OMS_Offer_Benefit work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


var sql_begin = "BEGIN"

// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET 
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
                             DW_SOURCE_UPDATE_NM = filename
              FROM ( 
                             SELECT 
                                           OMS_Offer_Id,                              
                                           filename
										   										   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND OMS_Offer_Id is not NULL                              
                             ) src
                             WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id 
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET  Benefit_Value_Type_Dsc = src.Benefit_Value_Type_Dsc
						,Benefit_Value_Type_Cd = src.Benefit_Value_Type_Cd
						,Benefit_Value_Amt      = src.Benefit_Value_Amt
						,Customer_Group_Id      = src.Customer_Group_Id
						,Customer_Group_Nm		= src.Customer_Group_Nm
						,DW_Logical_delete_ind = src.DW_Logical_delete_ind
						,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM = src.FileName
					FROM ( SELECT OMS_Offer_Id  
									 ,Benefit_Value_Type_Cd
									 ,Benefit_Value_Type_Dsc
									 ,Benefit_Value_Amt
									,Customer_Group_Id
									 ,Customer_Group_Nm		
									 ,DW_Logical_delete_ind
									 ,lastUpdateTS
									 ,Filename 
									 FROM ${tgt_wrk_tbl}
								WHERE DML_Type = 'U'
								AND Sameday_chg_ind = 1
								AND OMS_Offer_Id IS NOT NULL
							) src
					WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id  
					AND tgt.DW_CURRENT_VERSION_IND = TRUE
					 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     OMS_Offer_Id
					 ,DW_First_Effective_Dt      
                    ,DW_Last_Effective_Dt 
                    ,Benefit_Value_Type_Cd                                       
                    ,Benefit_Value_Type_Dsc                                     
                    ,Benefit_Value_Amt
					,Customer_Group_Id
                    ,Customer_Group_Nm                                          
                    ,DW_CREATE_TS                                                                                             
                    ,DW_LOGICAL_DELETE_IND                                                                                           
                    ,DW_SOURCE_CREATE_NM                                                                                         
                    ,DW_CURRENT_VERSION_IND      
                   )
                   SELECT distinct
                     OMS_Offer_Id 
					,CURRENT_DATE
                    ,'31-DEC-9999'     
                    ,Benefit_Value_Type_Cd 
					,Benefit_Value_Type_Dsc
					,Benefit_Value_Amt
					,Customer_Group_Id
					,Customer_Group_Nm  
					,CURRENT_TIMESTAMP
                    ,DW_Logical_delete_ind
                    ,FileName
                    ,TRUE                                                                                                      
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
				and Sameday_chg_ind = 0`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
		snowflake.execute (
            {sqlText: sql_updates  }
            );
        snowflake.execute (
            {sqlText: sql_sameday  }
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

// ************** Load for OMS_Offer_Benefit table ENDs *****************

$$;
