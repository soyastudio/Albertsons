--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_BIM_load_Offer_Request_Change_Detail runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_GETOFFERREQUEST_TO_BIM_LOAD_OFFER_REQUEST_CHANGE_DETAIL"("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_PURCHASE" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
	 
    
var src_wrk_tbl = SRC_WRK_TBL;	
	var cnf_schema = C_PURCHASE;
	var wrk_schema = C_STAGE;	
	var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Offer_Request_Change_Detail_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.Offer_Request_Change_Detail`;
    

// ************** Load for Offer_Request_Change_Detail table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

var sql_command = `INSERT INTO ${tgt_wrk_tbl}
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                               Offer_Request_Id
							  ,Change_Type_Cd
							  ,Change_Type_Dsc
							  ,Change_Type_Qty
							  ,Change_Category_Cd
							  ,Change_Category_Dsc
							  ,Change_Category_Qty
							  ,Reason_Type_Cd
							  ,Reason_Type_Dsc
							  ,Reason_Comment_Txt
							  ,Change_By_Type_User_Id
							  ,Change_By_Type_First_Nm
							  ,Change_By_Type_Last_Nm
							  ,Change_By_Type_Ts
							  ,createdDate
							  ,filename
							,Row_number() OVER ( partition BY Offer_Request_Id,Change_By_Type_Ts ORDER BY To_timestamp_ntz(createdDate) desc
																						) AS rn
                            from
                            (
                            SELECT DISTINCT 	
									OfferRequestId as Offer_Request_Id
                                   ,ChangeDetailChangeTypeCd as Change_Type_Cd        	
                                   ,ChangeDetailChangeTypeDsc as Change_Type_Dsc       	
                                   ,ChangeDetailChangeTypeQty as Change_Type_Qty       	
                                   ,ChangeDetailChangeCategoryCd as Change_Category_Cd    	
                                   ,ChangeDetailChangeCategoryDsc as Change_Category_Dsc   	
                                   ,ChangeDetailChangeCategoryQty as Change_Category_Qty   	
                                   ,ChangeDetailReasonTypeCd as Reason_Type_Cd        	
                                   ,ChangeDetailReasonTypeDsc as Reason_Type_Dsc       	
                                   ,ChangeDetailCommentTxt as Reason_Comment_Txt    	
                                   ,ChangeByTypeUserId as Change_By_Type_User_Id 
                                   ,ChangeByTypeFirstNm as Change_By_Type_First_Nm
                                   ,ChangeByTypeLastNm as Change_By_Type_Last_Nm 
                                   ,ChangeByTypeChangeByDtTm as Change_By_Type_Ts
								   ,CreationDt as createdDate
								   ,filename as filename
							FROM ${src_wrk_tbl}	
							) 
                          )                          
                          
                          SELECT
						        src.Offer_Request_Id
							   ,src.Change_Type_Cd
							   ,src.Change_Type_Dsc
							   ,src.Change_Type_Qty
						   ,src.Change_Category_Cd
							   ,src.Change_Category_Dsc
							   ,src.Change_Category_Qty
							   ,src.Reason_Type_Cd
							   ,src.Reason_Type_Dsc
							   ,src.Reason_Comment_Txt
							   ,src.Change_By_Type_User_Id
							   ,src.Change_By_Type_First_Nm
							   ,src.Change_By_Type_Last_Nm
							   ,src.Change_By_Type_Ts
						       ,src.DW_Logical_delete_ind
							   ,src.filename
							   ,src.createdDate
                               ,CASE WHEN tgt.Offer_Request_Id IS NULL AND tgt.Change_By_Type_Ts IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 ELSE 0 END AS Sameday_chg_ind  
                          from
                          (
							select
								   Offer_Request_Id
							      ,Change_Type_Cd
							      ,Change_Type_Dsc
							      ,Change_Type_Qty
							      ,Change_Category_Cd
							      ,Change_Category_Dsc
							      ,Change_Category_Qty
							      ,Reason_Type_Cd
							      ,Reason_Type_Dsc
							      ,Reason_Comment_Txt
							      ,Change_By_Type_User_Id
							      ,Change_By_Type_First_Nm
							      ,Change_By_Type_Last_Nm
							      ,Change_By_Type_Ts
								  ,FALSE AS DW_Logical_delete_ind
								  ,filename
								  ,createdDate
							from src_wrk_tbl_recs 
							WHERE rn = 1
							AND Offer_Request_Id IS NOT NULL						
							AND Change_By_Type_Ts IS NOT NULL
						)src
							
                        LEFT JOIN 
                          (SELECT  DISTINCT
						        tgt.Offer_Request_Id
							   ,tgt.Change_Type_Cd
							   ,tgt.Change_Type_Dsc
							   ,tgt.Change_Type_Qty
							   ,tgt.Change_Category_Cd
							   ,tgt.Change_Category_Dsc
							   ,tgt.Change_Category_Qty
							   ,tgt.Reason_Type_Cd
							   ,tgt.Reason_Type_Dsc
							   ,tgt.Reason_Comment_Txt
							   ,tgt.Change_By_Type_User_Id
							   ,tgt.Change_By_Type_First_Nm
							   ,tgt.Change_By_Type_Last_Nm
							   ,tgt.Change_By_Type_Ts
							   ,tgt.dw_logical_delete_ind
							   ,tgt.dw_first_effective_dt							   
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.Offer_Request_Id = src.Offer_Request_Id
						  AND tgt.Change_By_Type_Ts = src.Change_By_Type_Ts
						  WHERE  (tgt.Offer_Request_Id is null and tgt.Change_By_Type_Ts is null)  
                          or(
                          NVL(src.Change_Type_Cd,'-1') <> NVL(tgt.Change_Type_Cd,'-1')
						  OR NVL(src.Change_Type_Dsc,'-1') <> NVL(tgt.Change_Type_Dsc,'-1')
						  OR NVL(src.Change_Type_Qty,'-1') <> NVL(tgt.Change_Type_Qty,'-1')
						  OR NVL(src.Change_Category_Cd,'-1') <> NVL(tgt.Change_Category_Cd,'-1')
						  OR NVL(src.Change_Category_Dsc,'-1') <> NVL(tgt.Change_Category_Dsc,'-1')
						  OR NVL(src.Change_Category_Qty,'-1') <> NVL(tgt.Change_Category_Qty,'-1')
						  OR NVL(src.Reason_Type_Cd,'-1') <> NVL(tgt.Reason_Type_Cd,'-1')
						  OR NVL(src.Reason_Type_Dsc,'-1') <> NVL(tgt.Reason_Type_Dsc,'-1')
						  OR NVL(src.Reason_Comment_Txt,'-1') <> NVL(tgt.Reason_Comment_Txt,'-1')
						  OR NVL(src.Change_By_Type_User_Id,'-1') <> NVL(tgt.Change_By_Type_User_Id,'-1')
						  OR NVL(src.Change_By_Type_First_Nm,'-1') <> NVL(tgt.Change_By_Type_First_Nm,'-1')
						  OR NVL(src.Change_By_Type_Last_Nm,'-1') <> NVL(tgt.Change_By_Type_Last_Nm,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						  )`;        

try {
		snowflake.execute ({sqlText: sql_truncate_wrk_tbl});
        snowflake.execute ({sqlText: sql_command});
        }
    catch (err)  {
        return "Creation of Offer_Request_Change_Detail work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                    Offer_Request_Id
								   ,Change_By_Type_Ts
                                   ,filename					   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND Offer_Request_Id is not null							 
							 and Change_By_Type_Ts is not null
                             ) src
                             WHERE tgt.Offer_Request_Id = src.Offer_Request_Id
							 AND tgt.Change_By_Type_Ts = src.Change_By_Type_Ts
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Change_Type_Cd                     = src.Change_Type_Cd
					   ,Change_Type_Dsc                    = src.Change_Type_Dsc
					   ,Change_Type_Qty                    = src.Change_Type_Qty
					   ,Change_Category_Cd                 = src.Change_Category_Cd
					   ,Change_Category_Dsc                = src.Change_Category_Dsc
					   ,Change_Category_Qty                = src.Change_Category_Qty
					   ,Reason_Type_Cd                     = src.Reason_Type_Cd
					   ,Reason_Type_Dsc                    = src.Reason_Type_Dsc
					   ,Reason_Comment_Txt                 = src.Reason_Comment_Txt
					   ,Change_By_Type_User_Id             = src.Change_By_Type_User_Id
					   ,Change_By_Type_First_Nm            = src.Change_By_Type_First_Nm
					   ,Change_By_Type_Last_Nm             = src.Change_By_Type_Last_Nm					   
					   ,DW_Logical_delete_ind 			   = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS                  = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM                = FileName
					   FROM ( SELECT 
								     Offer_Request_Id
							        ,Change_Type_Cd
							        ,Change_Type_Dsc
							        ,Change_Type_Qty
							        ,Change_Category_Cd
							        ,Change_Category_Dsc
							        ,Change_Category_Qty
							        ,Reason_Type_Cd
							        ,Reason_Type_Dsc
							        ,Reason_Comment_Txt
							        ,Change_By_Type_User_Id
							        ,Change_By_Type_First_Nm
							        ,Change_By_Type_Last_Nm
							        ,Change_By_Type_Ts
									,DW_Logical_delete_ind
									,filename
									,createdDate			
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Offer_Request_Id IS NOT NULL							   
							   AND Change_By_Type_Ts IS NOT NULL
							) src
							WHERE tgt.Offer_Request_Id = src.Offer_Request_Id
							AND tgt.Change_By_Type_Ts = src.Change_By_Type_Ts
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Offer_Request_Id
					,Change_Type_Cd
					,Change_Type_Dsc
					,Change_Type_Qty
					,Change_Category_Cd
					,Change_Category_Dsc
					,Change_Category_Qty
					,Reason_Type_Cd
					,Reason_Type_Dsc
					,Reason_Comment_Txt
					,Change_By_Type_User_Id
					,Change_By_Type_First_Nm
					,Change_By_Type_Last_Nm
					,Change_By_Type_Ts
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND		
                   )
                   SELECT DISTINCT
                      Offer_Request_Id
					 ,Change_Type_Cd
					 ,Change_Type_Dsc
					 ,Change_Type_Qty
					 ,Change_Category_Cd
					 ,Change_Category_Dsc
					 ,Change_Category_Qty
					 ,Reason_Type_Cd
					 ,Reason_Type_Dsc
					 ,Reason_Comment_Txt
					 ,Change_By_Type_User_Id
					 ,Change_By_Type_First_Nm
					 ,Change_By_Type_Last_Nm
					 ,Change_By_Type_Ts
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'                     
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
				FROM ${tgt_wrk_tbl}
                where Offer_Request_Id is not null
				and Change_By_Type_Ts is not null
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

// ************** Load for Offer_Request_Change_Detail table ENDs *****************

$$;
