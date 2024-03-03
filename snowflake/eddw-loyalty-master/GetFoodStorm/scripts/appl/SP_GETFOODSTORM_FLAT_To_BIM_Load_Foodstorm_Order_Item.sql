--liquibase formatted sql
--changeset SYSTEM:SP_GETFOODSTORM_FLAT_To_BIM_Load_Foodstorm_Order_Item runOnChange:true splitStatements:false OBJECT_TYPE:SP
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_APPL;

ALTER TASK GET_FOOD_STORM_TASK SUSPEND;

CREATE OR REPLACE PROCEDURE SP_GETFOODSTORM_FLAT_TO_BIM_LOAD_FOODSTORM_ORDER_ITEM
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_LOYAL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
// ************** Load for Foodstorm_Order_Item table BEGIN *****************
		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.GetFoodStorm_Flat_wrk`;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Foodstorm_Order_Item_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Foodstorm_Order_Item`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.FOODSTORM_FLAT_RERUN`;
        		
// ************************************ Truncate and Reload the work table ****************************************
 var truncate_tgt_wrk_table = `Truncate Table ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}
	
// *********************** Load for Meal_Plan_Recipe table BEGIN *****************
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
							SELECT DISTINCT 
							src.Order_Id              ,
							src.Upc_Id                ,
							src.Item_Nm               ,
							src.Department_Nm         ,
							src.Item_Price_Amt        ,
							src.Item_Qty              ,
							src.Item_Total_Amt        ,
							src.Tax_Cd                ,
							src.Tax_Rate_Pct          ,
							src.Filename ,
							src.Dw_Logical_Delete_Ind ,
							CASE 
							WHEN (
							     tgt.Order_Id IS NULL 
							     AND tgt.Upc_Id is NULL 
							     ) 
							THEN 'I' 
							ELSE 'U' 
							END AS DML_Type ,
							CASE   
							WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
							THEN 1 
							Else 0 
							END as Sameday_chg_ind
                            FROM ( SELECT
									Order_Id              ,
									Upc_Id                ,
									Item_Nm               ,
									Department_Nm         ,
									Item_Price_Amt        ,
									Item_Qty              ,
									Item_Total_Amt        ,
									Tax_Cd                ,
									Tax_Rate_Pct          ,
									Filename ,
									Dw_Logical_Delete_Ind 
								FROM ( SELECT 
										    Order_Id              ,
											Upc_Id                ,
											Item_Nm               ,
											Department_Nm         ,
											Item_Price_Amt        ,
											Item_Qty              ,
											Item_Total_Amt        ,
											Tax_Cd                ,
											Tax_Rate_Pct          ,               
											Filename ,
											FALSE AS DW_LOGICAL_DELETE_IND,
										  	Row_number() OVER (
											 PARTITION BY Order_Id,Upc_Id
											 order by DW_CreateTs DESC) as rn 
											  FROM(
											        SELECT
													Order_Id              ,
													Upc_Id                ,
													Item_Nm               ,
													Department_Nm         ,
													Item_Price_Amt        ,
													Item_Qty              ,
													Item_Total_Amt        ,
													Tax_Cd                ,
													Tax_Rate_Pct          ,
													Filename ,
                                                    DW_CreateTs 
													
													FROM
													  (
													  SELECT
													    OrderNo as Order_Id,
														try_to_numeric(UPC) as Upc_Id,
														Item as Item_Nm,
														Department as Department_Nm,
														Price as Item_Price_Amt,
														Qty as Item_Qty,
														ItemTotal as Item_Total_Amt,
														TaxCode as Tax_Cd,
														TaxRate as Tax_Rate_Pct,       
													    DW_CreateTs ,         
													    File_Name as Filename 
													  FROM 
													   ${temp_wrk_tbl} S
													  )
                                                    )
											    )  where rn=1 and Order_Id is not null and Upc_Id is not null
									    ) src
										LEFT JOIN
											( 
											SELECT DISTINCT
											        Order_Id              ,
													Upc_Id                ,
													Item_Nm               ,
													Department_Nm         ,
													Item_Price_Amt        ,
													Item_Qty              ,
													Item_Total_Amt        ,
													Tax_Cd                ,
													Tax_Rate_Pct          ,                
													DW_First_Effective_Dt ,
													DW_LOGICAL_DELETE_IND 
											FROM
											${tgt_tbl} tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE
											)as tgt 
											ON
										    src.Order_Id = tgt.Order_Id  and src.Upc_Id = tgt.Upc_Id
											WHERE (
											tgt.Order_Id IS NULL and tgt.Upc_Id IS NULL
											 )	
											 OR 
											 (
						                     NVL(src.Item_Nm,'-1') <> NVL(tgt.Item_Nm,'-1')
											 OR NVL(src.Department_Nm,'-1') <> NVL(tgt.Department_Nm,'-1')
											 OR NVL(src.Item_Price_Amt,'-1') <> NVL(tgt.Item_Price_Amt,'-1')
											 OR NVL(src.Item_Qty,'-1') <> NVL(tgt.Item_Qty,'-1')
											 OR NVL(src.Item_Total_Amt,'-1') <>NVL(tgt.Item_Total_Amt,'-1')
											 OR NVL(src.Tax_Cd,'-1') <> NVL(tgt.Tax_Cd,'-1')
											 OR NVL(src.Tax_Rate_Pct,'-1') <> NVL(tgt.Tax_Rate_Pct,'-1')
											 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
											 )`;
try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {  return `Inserting of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				} 
				
// Transaction for Updates, Insert begins           
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
						 Order_Id,
						 Upc_Id,
						 Filename
						 FROM ${tgt_wrk_tbl}
						 WHERE 
						 DML_Type = 'U' 
						 AND Sameday_chg_ind = 0
					) src
					WHERE src.Order_Id = tgt.Order_Id
					AND src.Upc_Id = tgt.Upc_Id
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;		
                   
// SCD Type1 - Processing Sameday updates.
 var sql_sameday = `UPDATE ${tgt_tbl} as tgt
                    SET Order_Id = src.Order_Id ,              
					Upc_Id = src.Upc_Id ,              
					Item_Nm = src.Item_Nm ,  
					Department_Nm = src.Department_Nm ,          
					Item_Price_Amt = src.Item_Price_Amt ,  
					Item_Qty = src.Item_Qty , 
					Item_Total_Amt = src.Item_Total_Amt ,              
					Tax_Cd = src.Tax_Cd ,      
					Tax_Rate_Pct = src.Tax_Rate_Pct ,               
					DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND ,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename					
					FROM ( 
							SELECT
							Order_Id              ,
							Upc_Id                ,
							Item_Nm               ,
							Department_Nm         ,
							Item_Price_Amt        ,
							Item_Qty              ,
							Item_Total_Amt        ,
							Tax_Cd                ,
							Tax_Rate_Pct          ,
							Filename ,
							Dw_Logical_Delete_Ind                
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
							src.Order_Id = tgt.Order_Id
							AND src.Upc_Id = tgt.Upc_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;		
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
									(
									Order_Id              ,
									Upc_Id                ,
									Dw_First_Effective_Dt ,
									Dw_Last_Effective_Dt  ,
									Item_Nm               ,
									Department_Nm         ,
									Item_Price_Amt        ,
									Item_Qty              ,
									Item_Total_Amt        ,
									Tax_Cd                ,
									Tax_Rate_Pct          ,
									Dw_Create_Ts          ,
									Dw_Logical_Delete_Ind ,
									Dw_Source_Create_Nm   ,
									Dw_Current_Version_Ind          
									)
									SELECT 
									Order_Id              ,
									Upc_Id                ,
									CURRENT_DATE ,
									'31-DEC-9999'  ,
									Item_Nm               ,
									Department_Nm         ,
									Item_Price_Amt        ,
									Item_Qty              ,
									Item_Total_Amt        ,
									Tax_Cd                ,
									Tax_Rate_Pct          ,
									CURRENT_TIMESTAMP          ,
									Dw_Logical_Delete_Ind ,
									Filename   ,
									TRUE    
									
									FROM ${tgt_wrk_tbl}
									WHERE
									Order_Id is Not Null
								    and Upc_Id is Not Null
									and Sameday_chg_ind = 0`;
    
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
    
try {
        snowflake.execute ({ sqlText: sql_begin });
		
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
		
	   return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
                     }  
                                  
                // **************        Load for Foodstorm_Order_Item Table ENDs *****************
$$;

ALTER TASK GET_FOOD_STORM_TASK RESUME;
