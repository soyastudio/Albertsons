--liquibase formatted sql
--changeset SYSTEM:SP_GetUber_To_BIM_load_Partner_Grocery_Order_Header runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETUBER_TO_BIM_LOAD_PARTNER_GROCERY_ORDER_HEADER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, 
C_LOYALTY VARCHAR, C_STAGE VARCHAR, C_LOCATION VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_LOYALTY;
	var cnf_schema_lkp = C_LOCATION;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Partner_Grocery_Order_Header_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.Partner_Grocery_Order_Header`;
	var lkp_tbl = `${CNF_DB}.${cnf_schema}.Partner_Grocery_Order_Detail`;
	var lkp_tb2 = `${CNF_DB}.${cnf_schema_lkp}.FACILITY`;
	var tgt_exp_tbl = `${CNF_DB}.${C_STAGE}.Partner_Grocery_Order_Header_EXCEPTIONS`;

// ************** Load for Partner_Grocery_Order_Header table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                             Order_Id
							,Transaction_Dt
							,Store_Transaction_Ts
							,Net_Amt
							,Partner_Id
							,Partner_Nm
							,Store_Id
							,Alcoholic_Ind
							,LOADDATE
							,filename
							,row_number() over(partition by Order_Id order by Store_Transaction_Ts desc) as rn
                            from
                            (
                            SELECT DISTINCT 
                            ORDER_ID AS Order_Id
						   ,TXN_DT AS Transaction_Dt
						   ,STORE_TXN_TS AS Store_Transaction_Ts
						   ,NET_AMT AS Net_Amt
						   ,'2' AS Partner_Id
						   ,'Uber' AS Partner_Nm
						   ,STORE_ID AS Store_Id
						   ,ALCOHOLIC_IND AS Alcoholic_Ind
						   ,LOADDATE
						   ,filename						   
							FROM ${src_wrk_tbl} 
							
						   UNION ALL
							
							SELECT DISTINCT 
							       Order_Id
							      ,Transaction_Dt
								  ,Store_Transaction_Ts
								  ,Net_Amt
								  ,Partner_Id
								  ,Partner_Nm
								  ,Store_Id
								  ,Alcoholic_Ind
								  ,LOADDATE
								  ,filename
							FROM ${tgt_exp_tbl}
							where PARTNER_ID = '2'
						  ) 
                          )                          
                          
                          SELECT DISTINCT 
                            src.Order_Id
						   ,src.Partner_Grocery_Order_Customer_Integration_Id
						   ,src.Transaction_Dt
						   ,src.Store_Transaction_Ts
						   ,src.Net_Amt
						   ,src.Partner_Id
						   ,src.Partner_Nm
						   ,src.Facility_Integration_ID
						   ,src.Store_Id
                           ,src.Alcoholic_Ind						   
						   ,src.LOADDATE
						   ,src.DW_Logical_delete_ind						   
						   ,src.Filename			
                           ,CASE WHEN tgt.Order_Id IS NULL AND tgt.Partner_Grocery_Order_Customer_Integration_Id IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                           ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind  
                          from
                          (SELECT
								 s.Order_Id
								,B.Partner_Grocery_Order_Customer_Integration_Id
								,s.Transaction_Dt
								,s.Store_Transaction_Ts
								,s.Net_Amt
								,s.Partner_Id
								,s.Partner_Nm
								,C.Facility_Integration_ID
								,s.Store_Id
								,s.Alcoholic_Ind
								,s.LOADDATE
								,s.DW_Logical_delete_ind 
								,s.Filename
							FROM 
							(
							select
								Order_Id
								,Transaction_Dt
								,Store_Transaction_Ts
								,Net_Amt
								,Partner_Id
								,Partner_Nm
								,Store_Id
								,Alcoholic_Ind
								,LOADDATE
								,FALSE AS DW_Logical_delete_ind
								,filename
							from src_wrk_tbl_recs 
							WHERE rn = 1
							AND Order_Id is not null				  						  
							) s  
						   LEFT JOIN 
							(	SELECT DISTINCT Partner_Grocery_Order_Customer_Integration_Id
									  ,ORDER_ID 
								FROM ${lkp_tbl} 
								WHERE DW_CURRENT_VERSION_IND = TRUE 
								AND DW_LOGICAL_DELETE_IND = FALSE 
							) B ON S.ORDER_ID = B.ORDER_ID   
						  LEFT JOIN 
							(SELECT  Facility_integration_id,Facility_nbr from 
							( SELECT  Facility_integration_id,Facility_nbr ,row_number() over(partition by facility_nbr order by dw_create_ts desc) as rn
								FROM ${lkp_tb2} 
								WHERE DW_CURRENT_VERSION_IND = TRUE AND DW_LOGICAL_DELETE_IND = FALSE
							)where rn=1) C ON S.STORE_ID = C.Facility_nbr 
						)src
							
                        LEFT JOIN 
                          (SELECT  DISTINCT
								 tgt.Order_Id
								,tgt.Partner_Grocery_Order_Customer_Integration_Id
								,tgt.Transaction_Dt
								,tgt.Store_Transaction_Ts
								,tgt.Net_Amt
								,tgt.Partner_Id
								,tgt.Partner_Nm
								,tgt.Facility_Integration_ID
								,tgt.Store_Id
                                ,tgt.Alcoholic_Ind								
								,tgt.dw_logical_delete_ind
								,tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.Order_Id = src.Order_Id
						  AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
                          WHERE  (tgt.Order_Id is null and tgt.Partner_Grocery_Order_Customer_Integration_Id is null)  
                          or(
                           NVL(src.Transaction_Dt,'9999-12-31') <> NVL(tgt.Transaction_Dt,'9999-12-31')                                     
                          OR NVL(src.Store_Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Store_Transaction_Ts,'9999-12-31 00:00:00.000')
                          OR NVL(src.Net_Amt,'-1') <> NVL(tgt.Net_Amt,'-1')
                          OR NVL(src.Partner_Id,'-1') <> NVL(tgt.Partner_Id,'-1')  
                          OR NVL(src.Partner_Nm,'-1') <> NVL(tgt.Partner_Nm,'-1')              
                          OR NVL(src.Facility_Integration_ID,'-1') <> NVL(tgt.Facility_Integration_ID,'-1')
                          OR NVL(src.Store_Id,'-1') <> NVL(tgt.Store_Id,'-1')
						  OR NVL(src.Alcoholic_Ind,'1') <> NVL(tgt.Alcoholic_Ind,'1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `;        

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
		snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Partner_Grocery_Order_Header work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_begin = "BEGIN";

// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET 
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
                             DW_SOURCE_UPDATE_NM = filename
              FROM ( 
                             SELECT 
                                           Order_Id,                              
                                           filename,
										   Partner_Grocery_Order_Customer_Integration_Id
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND Order_Id is not NULL                              
							 AND Partner_Grocery_Order_Customer_Integration_Id is not null
                             ) src
                             WHERE tgt.Order_Id = src.Order_Id
							 AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Transaction_Dt = src.Transaction_Dt
					   ,Store_Transaction_Ts = src.Store_Transaction_Ts
					   ,Net_Amt = src.Net_Amt
					   ,Partner_Id = src.Partner_Id
					   ,Partner_Nm = src.Partner_Nm
					   ,Facility_Integration_ID = src.Facility_Integration_ID
					   ,Store_Id = src.Store_Id
					   ,Alcoholic_Ind = src.Alcoholic_Ind
					   ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM = FileName
						FROM ( SELECT 
								     Order_Id
								    ,Partner_Grocery_Order_Customer_Integration_Id
								    ,Transaction_Dt
								    ,Store_Transaction_Ts
								    ,Net_Amt
								    ,Partner_Id
								    ,Partner_Nm
								    ,Facility_Integration_ID
								    ,Store_Id 
									,Alcoholic_Ind
									,DW_Logical_delete_ind
									,FileName
									FROM ${tgt_wrk_tbl}
									WHERE DML_Type = 'U'
									AND Sameday_chg_ind = 1
									AND Order_Id IS NOT NULL									
									AND Partner_Grocery_Order_Customer_Integration_Id IS NOT NULL
									) src
							WHERE tgt.Order_Id = src.Order_Id
							AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Order_Id
					,Partner_Grocery_Order_Customer_Integration_Id
					,Transaction_Dt
					,Store_Transaction_Ts
					,Net_Amt
					,Partner_Id
					,Partner_Nm
					,Facility_Integration_ID
					,Store_Id 
					,Alcoholic_Ind
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND                                                                        
                   )
                   SELECT DISTINCT
                      Order_Id
					 ,Partner_Grocery_Order_Customer_Integration_Id
					 ,Transaction_Dt
					 ,Store_Transaction_Ts
					 ,Net_Amt
					 ,Partner_Id
					 ,Partner_Nm
					 ,Facility_Integration_ID
					 ,Store_Id 
					 ,Alcoholic_Ind
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'                     
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE                                                                                                                       
				FROM ${tgt_wrk_tbl}
                where Order_Id is not null
				and Partner_Grocery_Order_Customer_Integration_Id is not null
				and Sameday_chg_ind = 0`;
				
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl} where PARTNER_ID = '2'`;
						  /*WHERE
						  ((Order_Id,Partner_Grocery_Order_Customer_Integration_Id) in
						  (SELECT Order_Id,Partner_Grocery_Order_Customer_Integration_Id from ${tgt_tbl} WHERE DW_CURRENT_VERSION_IND = TRUE
						   and Partner_Grocery_Order_Customer_Integration_Id is not null
						  and Facility_Integration_ID is not null
						  )
						  OR
						  (Order_Id,Partner_Grocery_Order_Customer_Integration_Id) in
						  (SELECT Order_Id,Partner_Grocery_Order_Customer_Integration_Id  from ${tgt_wrk_tbl} WHERE 
						   Partner_Grocery_Order_Customer_Integration_Id is null
						   or Facility_Integration_ID is null
						  )
  						 OR 
                          			(Partner_Grocery_Order_Customer_Integration_Id is null and 
						  Order_Id in
						  (SELECT Order_Id 
                           			  from ${tgt_tbl} WHERE DW_CURRENT_VERSION_IND = TRUE
						  and Partner_Grocery_Order_Customer_Integration_Id is not null
						  ))
						)`;*/
 
	var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
	(           
	            Order_Id
		       ,Partner_Grocery_Order_Customer_Integration_Id
			   ,Transaction_Dt
			   ,Store_Transaction_Ts
			   ,Net_Amt
			   ,Partner_Id
			   ,Partner_Nm
			   ,Facility_Integration_ID
			   ,Store_Id
			   ,LOADDATE
			   ,filename
			   ,Exception_Reason
			   ,dw_create_ts
			   ,Alcoholic_Ind
			   )
		SELECT  Order_Id
		       ,Partner_Grocery_Order_Customer_Integration_Id
			   ,Transaction_Dt
			   ,Store_Transaction_Ts
			   ,Net_Amt
			   ,Partner_Id
			   ,Partner_Nm
			   ,Facility_Integration_ID
			   ,Store_Id
			   ,LOADDATE
			   ,filename
			   ,CASE WHEN Partner_Grocery_Order_Customer_Integration_Id IS NULL THEN 'Partner_Grocery_Order_Customer_Integration_Id is NULL' 
			         WHEN Facility_Integration_ID IS NULL THEN 'Facility_Integration_ID is NULL' END AS Exception_Reason
			   ,current_timestamp AS dw_create_ts
			   ,Alcoholic_Ind			
		FROM `+ tgt_wrk_tbl +`
		WHERE 
		(Facility_Integration_ID is NULL OR 
		Partner_Grocery_Order_Customer_Integration_Id is NULL
		or Order_Id is null)
		AND PARTNER_ID = '2'
	`;

var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
try {
        snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit}); 
		snowflake.execute({sqlText: truncate_exceptions});
		snowflake.execute({sqlText: sql_exceptions});
	}
	
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
        
// ************** Load for Partner_Grocery_Order_Header table ENDs *****************
 $$;
