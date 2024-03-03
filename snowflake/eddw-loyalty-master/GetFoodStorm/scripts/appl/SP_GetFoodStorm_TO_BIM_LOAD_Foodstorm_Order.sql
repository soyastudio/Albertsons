--liquibase formatted sql
--changeset SYSTEM:SP_GetFoodStorm_TO_BIM_LOAD_Foodstorm_Order runOnChange:true splitStatements:false OBJECT_TYPE:SP
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GetFoodStorm_TO_BIM_LOAD_Foodstorm_Order
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.GetFoodStorm_Flat_wrk`;
		var cnf_schema = C_LOYAL;
		var fac_schema = 'DW_C_LOCATION';
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Foodstorm_Order_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Foodstorm_Order`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.getFOODSTORM_FLAT_RERUN`;
		var facility_tb1 = `${CNF_DB}.${fac_schema}.facility`;
		

		
// ************************************ Truncate and Reload the work table ****************************************
 var truncate_tgt_wrk_table = `Truncate Table ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}
	
// *********************** Load for Foodstorm_Order table BEGIN *****************
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}
                                      
							SELECT DISTINCT 
							                    src.Order_Id,												
												 src.Facility_Integration_Id    ,
												 src.Store_Id                ,
												 src.Partner_Id              ,
												 src.Partner_Nm              ,
												 src.Order_Type_Cd           ,
												 src.Delivery_Dt             ,
												 src.Delivery_Zip_Cd         ,
												 src.Total_Amt   ,
												 src.Tax_Amt      ,
												 src.Payment_Amt            ,
												 src.Balance_Due_Amt    ,
												 src.Tax_Exempt_Id           ,
												 src.Tax_Exempt_Ind          ,
												 src.House_Account_Ind       ,
												 src.Source_Customer_Id      ,
												 src.Customer_Loyalty_Phone_Nbr ,												 
												 src.Dw_Logical_Delete_Ind ,
												 src.filename,     
												 
							  
							CASE 
							WHEN (
							     tgt.Order_Id IS NULL 
								 
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
                             Order_Id   ,          
							 
							 Facility_Integration_Id    ,
							 Store_Id                ,
							 Partner_Id              ,
							 Partner_Nm              ,
							 Order_Type_Cd           ,
							 Delivery_Dt        ,
							 Delivery_Zip_Cd         ,
							 Total_Amt               ,
							 Tax_Amt                 ,
							 Payment_Amt             ,
							 Balance_Due_Amt         ,
							 Tax_Exempt_Id           ,
							 Tax_Exempt_Ind          ,
							 House_Account_Ind       ,
							 Source_Customer_Id      ,
							 Customer_Loyalty_Phone_Nbr    ,
							 							
							 Dw_Logical_Delete_Ind    ,
							 filename    
							   
								FROM ( SELECT
								Order_Id   ,           
							 
							 Facility_Integration_Id    ,
							 Store_Id                ,
							 Partner_Id              ,
							 Partner_Nm              ,
							 Order_Type_Cd           ,
							 Delivery_Dt        ,
							 Delivery_Zip_Cd         ,
							 Total_Amt               ,
							 Tax_Amt                 ,
							 Payment_Amt             ,
							 Balance_Due_Amt         ,
							 Tax_Exempt_Id           ,
							 Tax_Exempt_Ind          ,
							 House_Account_Ind       ,
							 Source_Customer_Id      ,
							 Customer_Loyalty_Phone_Nbr    ,
							 false as Dw_Logical_Delete_Ind    ,
							 filename   ,
							             
										  	Row_number() OVER (
											 PARTITION BY Order_Id 
											 order by (DW_CreateTs) DESC) as rn 
											  FROM(
											SELECT
												Order_Id   ,          
											 Facility_Integration_Id ,
											 Store_Id                ,
											 Partner_Id              ,
											 Partner_Nm              ,
											 Order_Type_Cd           ,
											 Delivery_Dt        ,
											 Delivery_Zip_Cd         ,
											 Total_Amt               ,
											 Tax_Amt                 ,
											 Payment_Amt             ,
											 Balance_Due_Amt         ,
											 Tax_Exempt_Id           ,
											 Tax_Exempt_Ind          ,
											 House_Account_Ind       ,
											 Source_Customer_Id      ,
											 Customer_Loyalty_Phone_Nbr    ,
											 --Dw_Logical_Delete_Ind    ,
											 filename  ,
											 DW_CREATETS
                                                    
													FROM
													  (
													  SELECT
													 orderNo as Order_Id   ,           
													 facility.Facility_Integration_Id as Facility_Integration_Id    ,
													 Store as Store_Id                ,
													 '4' as Partner_Id              ,
													 'FOODSTORM' AS Partner_Nm              ,
													 Type as Order_Type_Cd           ,
													To_date(DeliveryDate) as Delivery_Dt     ,
													 DeliveryZip as Delivery_Zip_Cd         ,
													 Total AS Total_Amt ,
													 TotalTax AS Tax_Amt       ,
													 PaymentTotal as Payment_Amt             ,
													 Balance as Balance_Due_Amt         ,
													 TaxExemptId as Tax_Exempt_Id           ,
													 TaxExempt as Tax_Exempt_Ind          ,
													 HouseAccount as House_Account_Ind       ,
													 CustomerGUID as Source_Customer_Id      ,
													 Phone_Number as Customer_Loyalty_Phone_Nbr    ,
													 Dw_CreateTs  ,
													
													 FILE_NAME as filename  
													  FROM 
													   ${temp_wrk_tbl} S
													   LEFT JOIN
												(SELECT DISTINCT Facility_Integration_Id,FACILITY_NBR,corporation_id
							 FROM <<EDM_DB_NAME>>.DW_C_LOCATION.facility
								 where DW_CURRENT_VERSION_IND = TRUE
							 AND DW_LOGICAL_DELETE_IND = FALSE							  
							) facility
							ON s.Store = facility.FACILITY_NBR
							 WHERE 
							 facility.corporation_id  = 001
										    
													  )
                                                    )
											    )  where rn=1
												
												
											)src
										
										LEFT JOIN
										
										
											( 
											SELECT DISTINCT
											        Order_Id   ,        
													Facility_Integration_Id    ,
												 Store_Id                ,
												 Partner_Id              ,
												 Partner_Nm              ,
												 Order_Type_Cd           ,
												 Delivery_Dt        ,
												 Delivery_Zip_Cd         ,
												 Total_Amt               ,
												 Tax_Amt                 ,
												 Payment_Amt             ,
												 Balance_Due_Amt         ,
												 Tax_Exempt_Id           ,
												 Tax_Exempt_Ind          ,
												 House_Account_Ind       ,
												 Source_Customer_Id      ,
												 Customer_Loyalty_Phone_Nbr    ,
												 DW_First_Effective_Dt,												 
												 Dw_Logical_Delete_Ind    
												 
											FROM
											${tgt_tbl} tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE
											)as tgt 
											ON
										    src.Order_Id = tgt.Order_Id  
											WHERE (
											tgt.Order_Id IS NULL
											 )	
											 OR
											 (
                                             nvl(src.Facility_Integration_Id,'-1') <> nvl(tgt.Facility_Integration_Id,'-1')	
                                             OR nvl(src.Store_Id,'-1') <> nvl(tgt.Store_Id,'-1')	
											 OR nvl(src.Partner_Id,'-1') <> nvl(tgt.Partner_Id,'-1')
											 OR nvl(src.Partner_Nm,'-1') <> nvl(tgt.Partner_Nm,'-1')
											 OR nvl(src.Order_Type_Cd,'-1') <> nvl(tgt.Order_Type_Cd,'-1')
											 OR NVL(to_date(src.Delivery_Dt),'9999-12-31') <> NVL(tgt.Delivery_Dt,'9999-12-31')  
											 OR nvl(src.Delivery_Zip_Cd,'-1') <> nvl(tgt.Delivery_Zip_Cd,'-1')
											 OR nvl(src.Total_Amt,'-1') <> nvl(tgt.Total_Amt,'-1')
											 OR nvl(src.Tax_Amt,'-1') <> nvl(tgt.Tax_Amt,'-1')
											 OR nvl(src.Payment_Amt,'-1') <> nvl(tgt.Payment_Amt,'-1')
											 OR nvl(src.Balance_Due_Amt,'-1') <> nvl(tgt.Balance_Due_Amt,'-1')
                                             OR nvl(src.Tax_Exempt_Id,'-1') <> nvl(tgt.Tax_Exempt_Id,'-1')
											 OR NVL(to_boolean(src.Tax_Exempt_Ind),-1) <> NVL(tgt.Tax_Exempt_Ind,-1)
											 OR NVL(to_boolean(src.House_Account_Ind),-1) <> NVL(tgt.House_Account_Ind,-1)											 
											 OR nvl(src.Source_Customer_Id,'-1') <> nvl(tgt.Source_Customer_Id,'-1')
											 OR nvl(src.Customer_Loyalty_Phone_Nbr,'-1') <> nvl(tgt.Customer_Loyalty_Phone_Nbr,'-1')
											 
											 OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND  
											 )`;
											 
try {  
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
	

  return `Inserting of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
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
						 Filename
						 FROM ${tgt_wrk_tbl}
						 WHERE 
						 DML_Type = 'U' 
						 AND Sameday_chg_ind = 0
					) src
					WHERE
					nvl(src.Order_Id,'-1') = nvl(tgt.Order_Id,'-1')	
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;		
                   
// SCD Type1 - Processing Sameday updates
 var sql_sameday = `UPDATE ${tgt_tbl} as tgt
                    SET 
					Facility_Integration_Id  = src.Facility_Integration_Id  ,
					 Store_Id        = src.Store_Id     ,
					 Partner_Id      = src.Partner_Id   ,
					 Partner_Nm      = src.Partner_Nm   ,
					 Order_Type_Cd    = src.Order_Type_Cd  ,
					 Delivery_Dt      = src.Delivery_Dt  ,
					 Delivery_Zip_Cd   = src.Delivery_Zip_Cd      ,
					 Total_Amt         = src.Total_Amt      ,
					 Tax_Amt           = src.Tax_Amt   ,
					 Payment_Amt       = src.Payment_Amt      ,
					 Balance_Due_Amt    = src.Balance_Due_Amt  ,
					 Tax_Exempt_Id      = src.Tax_Exempt_Id  ,
					 Tax_Exempt_Ind      = src.Tax_Exempt_Ind ,
					 House_Account_Ind   = src.House_Account_Ind ,
					 Source_Customer_Id    = src.Source_Customer_Id ,
					 Customer_Loyalty_Phone_Nbr = src.Customer_Loyalty_Phone_Nbr,                       
					DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND ,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = Filename					
					FROM ( 
							SELECT
							 Order_Id   ,           
							 Facility_Integration_Id    ,
							 Store_Id                ,
							 Partner_Id              ,
							 Partner_Nm              ,
							 Order_Type_Cd           ,
							 Delivery_Dt        ,
							 Delivery_Zip_Cd         ,
							 Total_Amt               ,
							 Tax_Amt                 ,
							 Payment_Amt             ,
							 Balance_Due_Amt         ,
							 Tax_Exempt_Id           ,
							 Tax_Exempt_Ind          ,
							 House_Account_Ind       ,
							 Source_Customer_Id      ,
							 Customer_Loyalty_Phone_Nbr,
							 filename    ,
							 DW_Logical_delete_ind
							  
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
							nvl(src.Order_Id,'-1') = nvl(tgt.Order_Id,'-1')
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;		
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
									(
									 Order_Id   ,           
									 Dw_First_Effective_Dt    ,
									 Dw_Last_Effective_Dt    ,
									 Facility_Integration_Id    ,
									 Store_Id                ,
									 Partner_Id              ,
									 Partner_Nm              ,
									 Order_Type_Cd           ,
									 Delivery_Dt        ,
									 Delivery_Zip_Cd         ,
									 Total_Amt               ,
									 Tax_Amt                 ,
									 Payment_Amt             ,
									 Balance_Due_Amt         ,
									 Tax_Exempt_Id           ,
									 Tax_Exempt_Ind          ,
									 House_Account_Ind       ,
									 Source_Customer_Id      ,
									 Customer_Loyalty_Phone_Nbr,
									 Dw_Create_Ts            ,
									 Dw_Logical_Delete_Ind    ,
									 Dw_Source_Create_Nm    ,
									 Dw_Current_Version_Ind           
									)
									SELECT 
									Order_Id ,              
									CURRENT_DATE ,           
									'31-DEC-9999' ,            
									Facility_Integration_Id    ,
									 Store_Id                ,
									 Partner_Id              ,
									 Partner_Nm              ,
									 Order_Type_Cd           ,
									 Delivery_Dt        ,
									 Delivery_Zip_Cd         ,
									 Total_Amt               ,
									 Tax_Amt                 ,
									 Payment_Amt             ,
									 Balance_Due_Amt         ,
									 Tax_Exempt_Id           ,
									 Tax_Exempt_Ind          ,
									 House_Account_Ind       ,
									 Source_Customer_Id      ,
									 Customer_Loyalty_Phone_Nbr,              
									CURRENT_TIMESTAMP ,                    
			                        DW_LOGICAL_DELETE_IND ,           
									filename ,            
									TRUE
									FROM ${tgt_wrk_tbl}
									WHERE
									Order_Id is Not Null                                         
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
                // **************Load for Foodstorm_Order Table ENDs ****************
$$;
