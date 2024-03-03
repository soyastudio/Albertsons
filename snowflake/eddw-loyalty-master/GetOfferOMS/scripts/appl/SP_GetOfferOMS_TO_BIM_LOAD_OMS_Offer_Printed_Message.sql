--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Printed_Message runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_PRINTED_MESSAGE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PRODUCT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Printed_Message_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer_Printed_Message`;
	

// ************** Load for OMS_Offer_Printed_Message table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;


var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                               OMS_Offer_Id
							  ,Printed_Message_Level_Nbr
							  ,Printed_Message_Cd
							  ,lastUpdateTs
							  ,filename
							,Row_number() OVER ( partition BY OMS_Offer_Id, Printed_Message_Level_Nbr ORDER BY To_timestamp_ntz(lastUpdateTs) desc) AS rn
                            from
                            (
                            SELECT DISTINCT
							        payload_id as OMS_Offer_Id
                                   ,printed.VALUE:level::string as Printed_Message_Level_Nbr 
								   ,printed.VALUE:message::string as Printed_Message_Cd 
								   ,lastUpdateTs 
								   ,filename as filename
							FROM ${src_wrk_tbl}
							,LATERAL FLATTEN(input => payload_benefit_printedMessage_message, outer => TRUE) as printed														
							) 
                          )                          
                          
                          SELECT
						        src.OMS_Offer_Id
							   ,src.Printed_Message_Level_Nbr
							   ,src.Printed_Message_Cd
						       ,src.DW_Logical_delete_ind
							   ,src.filename
							   ,src.lastUpdateTs
                               ,CASE WHEN tgt.OMS_Offer_Id IS NULL AND tgt.Printed_Message_Level_Nbr IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 ELSE 0 END AS Sameday_chg_ind  
                          from
                          (
							select
								   OMS_Offer_Id
							      ,Printed_Message_Level_Nbr
							      ,Printed_Message_Cd
								  ,FALSE AS DW_Logical_delete_ind
								  ,filename
								  ,lastUpdateTs
							from src_wrk_tbl_recs 
							WHERE rn = 1
							AND OMS_Offer_Id IS NOT NULL						
							AND Printed_Message_Level_Nbr IS NOT NULL
						)src
							
                        LEFT JOIN 
                          (SELECT  DISTINCT
						        tgt.OMS_Offer_Id
							   ,tgt.Printed_Message_Level_Nbr
							   ,tgt.Printed_Message_Cd
							   ,tgt.dw_logical_delete_ind
							   ,tgt.dw_first_effective_dt							   
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.OMS_Offer_Id = src.OMS_Offer_Id
						  AND tgt.Printed_Message_Level_Nbr = src.Printed_Message_Level_Nbr
						  WHERE  (tgt.OMS_Offer_Id is null AND tgt.Printed_Message_Level_Nbr is null)  
                          or(
                          NVL(src.Printed_Message_Cd,'-1') <> NVL(tgt.Printed_Message_Cd,'-1')						  					  
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						  )`;        

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of OMS_Offer_Printed_Message work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                    OMS_Offer_Id
							       ,Printed_Message_Level_Nbr
                                   ,filename					   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND OMS_Offer_Id is not null							 
							 AND Printed_Message_Level_Nbr is not null
                             ) src
                             WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id
							 AND tgt.Printed_Message_Level_Nbr = src.Printed_Message_Level_Nbr
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Printed_Message_Cd                 = src.Printed_Message_Cd
					   ,DW_Logical_delete_ind 			   = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS                  = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM                = FileName
					   FROM ( SELECT 
								     OMS_Offer_Id
							        ,Printed_Message_Level_Nbr
									,Printed_Message_Cd
									,DW_Logical_delete_ind
									,filename
									,lastUpdateTs									
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND OMS_Offer_Id IS NOT NULL							   
							   AND Printed_Message_Level_Nbr IS NOT NULL
							) src
							WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id
							AND tgt.Printed_Message_Level_Nbr = src.Printed_Message_Level_Nbr
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     OMS_Offer_Id
					,Printed_Message_Level_Nbr
					,Printed_Message_Cd
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND					
                   )
                   SELECT DISTINCT
                      OMS_Offer_Id
					 ,Printed_Message_Level_Nbr
					 ,Printed_Message_Cd
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'                     
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
				and Printed_Message_Level_Nbr is not null
				and Sameday_chg_ind = 0`;
				
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit}); 
		
	}
	
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for OMS_Offer_Printed_Message table ENDs *****************
$$;
