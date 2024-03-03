--liquibase formatted sql
--changeset SYSTEM:SP_GetPartnerRewardTransaction_To_BIM_load_Business_Partner_Reward_Transaction runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

 ALTER TABLE DW_C_STAGE.Business_Partner_Reward_Transaction_EXCEPTIONS
Drop COLUMN if exists Alt_Transaction_Ts ;

 ALTER TABLE DW_C_STAGE.Business_Partner_Reward_Transaction_EXCEPTIONS
ADD COLUMN Alt_Transaction_Ts    TIMESTAMP;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETPARTNERREWARDTRANSACTION_TO_BIM_LOAD_BUSINESS_PARTNER_REWARD_TRANSACTION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR, C_CUST VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	

	var src_wrk_tbl = SRC_WRK_TBL;	
	var cnf_schema = C_LOYAL;
	var wrk_schema = C_STAGE;
	var cnf_schema_cust = C_CUST;
	var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Reward_Transaction_wrk`;
    	var tgt_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner_Reward_Transaction`;
    	var lkp_tbl_Cust = `${CNF_DB}.${cnf_schema_cust}.Customer_Loyalty_Program`;	
	var lkp_tbl_Cust_RTL = `${CNF_DB}.${cnf_schema_cust}.Retail_Customer`;	
	var lkp_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner`;	
	var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Reward_Transaction_EXCEPTIONS`;

// ************** Load for Business_Partner_Reward_Transaction table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                             Partner_Participant_Id
							,Partner_Site_Id
							,Partner_Id
							,Transaction_Id
							,Transaction_Ts
							,Transaction_Type_Cd
							,Transaction_Type_Dsc
							,Transaction_Type_Short_Dsc
							,Reference_Nbr
							,Alt_Transaction_Id
							,Alt_Transaction_Type_Cd
							,Alt_Transaction_Type_Dsc
							,Alt_Transaction_Type_Short_Dsc
							,Partner_Division_Id
							,Postal_Zone_Cd
							,Customer_Division_Id
							,Old_Club_Card_Nbr
							,Status_Type_Cd
							,Status_Type_Dsc
							,Status_Type_Effective_Ts
							,Fuel_Pump_Id
							,Register_Id
							,Fuel_Grade_Cd
							,Fuel_Grade_Dsc
							,Fuel_Grade_Short_Dsc
							,Tender_Type_Cd
							,Tender_Type_Dsc
							,Tender_Type_Short_Dsc
							,Reward_Message_Id
							,Reward_Token_Offered_Qty
							,Total_Purchase_Qty
							,Purchase_UOM_Cd
							,Purchase_UOM_Nm
							,Purchase_Discount_Limit_Qty
							,Purchase_Discount_Amt
							,Total_Fuel_Purchase_Amt
							,Nonfuel_Purchase_Amt
							,Total_Purchase_Amt
							,Discount_Amt
							,Exception_Type_Dsc
							,Exception_Type_Short_Dsc
							,Exception_Transaction_Ts
							,Create_Ts
							,Create_User_Id
							,Update_Ts
							,Update_User_Id
							,CustomerId
							,CreationDt
							,Filename
							,Household_Id
							,Club_Card_Nbr
							,Customer_Phone_Nbr
							,Total_Savings_Value_Amt
							,Alt_Transaction_Ts
							,Row_number() OVER ( partition BY Partner_Participant_Id, Partner_Site_Id, Partner_Id, Transaction_Id, Status_Type_Cd, Transaction_Type_Cd ORDER BY To_timestamp_ntz(CreationDt) DESC) AS rn
                            from
                            (
                            SELECT DISTINCT 							
									  PRT.PartnerParticipantId as Partner_Participant_Id
							         ,PRT.PartnerSiteId as Partner_Site_Id
									 ,PRT.PartnerId as Partner_Id
							         ,PRT.TransactionId as Transaction_Id
									 ,PRT.TransactionTs as Transaction_Ts
									 ,PRT.TransactionTypeCd_Code as Transaction_Type_Cd									 
									 ,PRT.TransactionTypeCd_Description as Transaction_Type_Dsc
									 ,PRT.TransactionTypeCd_ShortDescription as Transaction_Type_Short_Dsc
									 ,PRT.ReferenceNbr as Reference_Nbr
									 ,PRT.AltTransactionId as Alt_Transaction_Id
									 ,PRT.AltTransactionType_Code as Alt_Transaction_Type_Cd
									 ,PRT.AltTransactionType_Description as Alt_Transaction_Type_Dsc
									 ,PRT.AltTransactionType_ShortDescription as Alt_Transaction_Type_Short_Dsc
									 ,PRT.PartnerData_DivisionId as Partner_Division_Id
									 ,PRT.PostalZoneCd as Postal_Zone_Cd
									 ,PRT.CustomerData_DivisionId as Customer_Division_Id
									 ,PRT.OldClubCardNbr as Old_Club_Card_Nbr
									 ,PRT.StatusCd as Status_Type_Cd
									 ,PRT.StatusType_Description as Status_Type_Dsc
									 ,PRT.EffectiveDtTm as Status_Type_Effective_Ts
									 ,PRT.FuelPumpId as Fuel_Pump_Id
									 ,PRT.RegisterId as Register_Id
									 ,PRT.FuelGradeCd_Code as Fuel_Grade_Cd
									 ,PRT.FuelGradeCd_Description as Fuel_Grade_Dsc
									 ,PRT.FuelGradeCd_ShortDescription as Fuel_Grade_Short_Dsc
									 ,PRT.TenderTypeCd_Code as Tender_Type_Cd
									 ,PRT.TenderTypeCd_Description as Tender_Type_Dsc
									 ,PRT.TenderTypeCd_ShortDescription as Tender_Type_Short_Dsc
									 ,PRT.RewardMsgId as Reward_Message_Id
									 ,PRT.RewardTokenOfferedQty as Reward_Token_Offered_Qty
									 ,PRT.TotalPurchQty as Total_Purchase_Qty
									 ,PRT.UOMCd as Purchase_UOM_Cd
									 ,PRT.UOMNm as Purchase_UOM_Nm
									 ,PRT.PurchDiscLimitQty as Purchase_Discount_Limit_Qty
									 ,PRT.PurchDiscLimitAmt_TransactionAmt as Purchase_Discount_Amt
									 ,PRT.TotalFuelPurchAmt_TransactionAmt as Total_Fuel_Purchase_Amt
									 ,PRT.NonFuelPurchAmt_TransactionAmt as Nonfuel_Purchase_Amt
									 ,PRT.TotalFuelPurchAmt_TransactionAmt as Total_Purchase_Amt
									 ,PRT.DiscountAmt_TransactionAmt as Discount_Amt
									 ,PRT.ExceptionTypeCd_Description as Exception_Type_Dsc
									 ,PRT.ExceptionTypeCd_ShortDescription as Exception_Type_Short_Dsc
									 ,PRT.ExceptionTxnTs as Exception_Transaction_Ts
									 ,PRT.CREATETS as Create_Ts
									 ,PRT.CreateUserId as Create_User_Id
									 ,PRT.UPDATETS as Update_Ts
									 ,PRT.UpdateUserId as Update_User_Id
									 ,PRT.CustomerId as CustomerId
									 ,PRT.CreationDt as CreationDt
									 ,PRT.Filename as Filename
									 ,PRT.HouseholdId as Household_Id
									 ,PRT.ClubCardNbr as Club_Card_Nbr
									 ,PRT.PhoneNbr as Customer_Phone_Nbr
									 ,PRT.TotalSavingsValAmt_TransactionAmt as Total_Savings_Value_Amt
									 ,PRT.ALTTRANSACTIONTS as Alt_Transaction_Ts
							FROM ${src_wrk_tbl}	PRT							
						  ) 
                          )                          
                          
                          SELECT
                                src.Business_Partner_Integration_Id
							   ,NVL(src.Retail_Customer_UUID,'-1') AS Retail_Customer_UUID
							   ,src.Partner_Participant_Id
							   ,src.Partner_Site_Id
							   ,src.Partner_Id
							   ,src.Transaction_Id
							   ,src.Transaction_Ts
							   ,src.Transaction_Type_Cd
							   ,src.Transaction_Type_Dsc
							   ,src.Transaction_Type_Short_Dsc
							   ,src.Reference_Nbr
							   ,src.Alt_Transaction_Id
							   ,src.Alt_Transaction_Type_Cd
							   ,src.Alt_Transaction_Type_Dsc
							   ,src.Alt_Transaction_Type_Short_Dsc
							   ,src.Partner_Division_Id
							   ,src.Postal_Zone_Cd
							   ,src.Customer_Division_Id
							   ,src.Old_Club_Card_Nbr
							   ,src.Status_Type_Cd
							   ,src.Status_Type_Dsc
							   ,src.Status_Type_Effective_Ts
							   ,src.Fuel_Pump_Id
							   ,src.Register_Id
							   ,src.Fuel_Grade_Cd
							   ,src.Fuel_Grade_Dsc
							   ,src.Fuel_Grade_Short_Dsc
							   ,src.Tender_Type_Cd
							   ,src.Tender_Type_Dsc
							   ,src.Tender_Type_Short_Dsc
							   ,src.Reward_Message_Id
							   ,src.Reward_Token_Offered_Qty
							   ,src.Total_Purchase_Qty
							   ,src.Purchase_UOM_Cd
							   ,src.Purchase_UOM_Nm
							   ,src.Purchase_Discount_Limit_Qty
							   ,src.Purchase_Discount_Amt
							   ,src.Total_Fuel_Purchase_Amt
							   ,src.Nonfuel_Purchase_Amt
							   ,src.Total_Purchase_Amt
							   ,src.Discount_Amt
							   ,src.Exception_Type_Dsc
							   ,src.Exception_Type_Short_Dsc
							   ,src.Exception_Transaction_Ts
							   ,src.Create_Ts
							   ,src.Create_User_Id
							   ,src.Update_Ts
							   ,src.Update_User_Id
							   ,src.CustomerId
							   ,src.CreationDt
							   ,src.DW_Logical_delete_ind
							   ,src.filename
							   ,src.Club_Card_Nbr                         
							   ,src.Household_Id                         
							   ,src.Customer_Phone_Nbr
							   ,src.Total_Savings_Value_Amt
							   ,src.Alt_Transaction_Ts
                               ,CASE WHEN tgt.Business_Partner_Integration_Id IS NULL AND tgt.Transaction_Id IS NULL AND tgt.Status_Type_Cd IS NULL   AND tgt.Transaction_Type_Cd IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind  
                          from
                          (SELECT
								   B.Business_Partner_Integration_Id
								  ,c.Retail_Customer_UUID
								  ,s.Partner_Participant_Id
								  ,s.Partner_Site_Id
								  ,s.Partner_Id
								  ,s.Transaction_Id
								  ,s.Transaction_Ts
								  ,s.Transaction_Type_Cd
								  ,s.Transaction_Type_Dsc
								  ,s.Transaction_Type_Short_Dsc
								  ,s.Reference_Nbr
								  ,s.Alt_Transaction_Id
								  ,s.Alt_Transaction_Type_Cd
								  ,s.Alt_Transaction_Type_Dsc
								  ,s.Alt_Transaction_Type_Short_Dsc
								  ,s.Partner_Division_Id
								  ,s.Postal_Zone_Cd
								  ,s.Customer_Division_Id
								  ,s.Old_Club_Card_Nbr
								  ,s.Status_Type_Cd
								  ,s.Status_Type_Dsc
								  ,s.Status_Type_Effective_Ts
								  ,s.Fuel_Pump_Id
								  ,s.Register_Id
								  ,s.Fuel_Grade_Cd
								  ,s.Fuel_Grade_Dsc
								  ,s.Fuel_Grade_Short_Dsc
								  ,s.Tender_Type_Cd
								  ,s.Tender_Type_Dsc
								  ,s.Tender_Type_Short_Dsc
								  ,s.Reward_Message_Id
								  ,s.Reward_Token_Offered_Qty
								  ,s.Total_Purchase_Qty
								  ,s.Purchase_UOM_Cd
								  ,s.Purchase_UOM_Nm
								  ,s.Purchase_Discount_Limit_Qty
								  ,s.Purchase_Discount_Amt
								  ,s.Total_Fuel_Purchase_Amt
								  ,s.Nonfuel_Purchase_Amt
								  ,s.Total_Purchase_Amt
								  ,s.Discount_Amt
								  ,s.Exception_Type_Dsc
								  ,s.Exception_Type_Short_Dsc
								  ,s.Exception_Transaction_Ts
								  ,s.Create_Ts
								  ,s.Create_User_Id
								  ,s.Update_Ts
								  ,s.Update_User_Id
								  ,s.CustomerId
								  ,s.CreationDt
								  ,s.DW_Logical_delete_ind
								  ,s.filename
								  ,s.Club_Card_Nbr                         
								  ,s.Household_Id                         
								  ,s.Customer_Phone_Nbr
								  ,s.Total_Savings_Value_Amt
								  ,s.Alt_Transaction_Ts
							FROM 
							(
							select
								   Partner_Participant_Id
								  ,Partner_Site_Id
								  ,Partner_Id
								  ,Transaction_Id
								  ,Transaction_Ts
								  ,Transaction_Type_Cd
								  ,Transaction_Type_Dsc
								  ,Transaction_Type_Short_Dsc
								  ,Reference_Nbr
								  ,Alt_Transaction_Id
								  ,Alt_Transaction_Type_Cd
								  ,Alt_Transaction_Type_Dsc
								  ,Alt_Transaction_Type_Short_Dsc
								  ,Partner_Division_Id
								  ,Postal_Zone_Cd
								  ,Customer_Division_Id
								  ,Old_Club_Card_Nbr
								  ,Status_Type_Cd
								  ,Status_Type_Dsc
								  ,Status_Type_Effective_Ts
								  ,Fuel_Pump_Id
								  ,Register_Id
								  ,Fuel_Grade_Cd
								  ,Fuel_Grade_Dsc
								  ,Fuel_Grade_Short_Dsc
								  ,Tender_Type_Cd
								  ,Tender_Type_Dsc
								  ,Tender_Type_Short_Dsc
								  ,Reward_Message_Id
								  ,Reward_Token_Offered_Qty
								  ,Total_Purchase_Qty
								  ,Purchase_UOM_Cd
								  ,Purchase_UOM_Nm
								  ,Purchase_Discount_Limit_Qty
								  ,Purchase_Discount_Amt
								  ,Total_Fuel_Purchase_Amt
								  ,Nonfuel_Purchase_Amt
								  ,Total_Purchase_Amt
								  ,Discount_Amt
								  ,Exception_Type_Dsc
								  ,Exception_Type_Short_Dsc
								  ,Exception_Transaction_Ts
								  ,Create_Ts
								  ,Create_User_Id
								  ,Update_Ts
								  ,Update_User_Id
								  ,CustomerId
								  ,CreationDt
								  ,FALSE AS DW_Logical_delete_ind
								  ,filename
								  ,Club_Card_Nbr                         
								  ,Household_Id                         
								  ,Customer_Phone_Nbr
								  ,Total_Savings_Value_Amt
								  ,Alt_Transaction_Ts
							from src_wrk_tbl_recs 
							WHERE rn = 1
							AND Transaction_Id is not null
							AND Status_Type_Cd is not null
							AND Partner_Participant_Id is not null
							AND Partner_Site_Id is not null
							AND Partner_Id is not null 
							AND Transaction_Type_Cd is not null
							) s  
						   LEFT JOIN 
							(	SELECT Business_Partner_Integration_Id
									  ,Partner_Participant_Id
								      ,Partner_Site_Id
								      ,Partner_Id 
								FROM ${lkp_tbl} 
								WHERE DW_CURRENT_VERSION_IND = TRUE 
								AND DW_LOGICAL_DELETE_IND = FALSE 
							) B  
							
							ON 		(	NVL(s.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
												AND NVL(s.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')										
												)
									
						   LEFT JOIN 
						    (	SELECT MAX(RT.Retail_Customer_UUID) as Retail_Customer_UUID
												,PG.Loyalty_Program_Card_Nbr
								FROM ${lkp_tbl_Cust} PG
								INNER JOIN ${lkp_tbl_Cust_RTL} RT ON RT.Retail_Customer_UUID = PG.Retail_Customer_UUID
								WHERE PG.DW_CURRENT_VERSION_IND = TRUE 
								AND PG.DW_LOGICAL_DELETE_IND = FALSE
								AND RT.DW_CURRENT_VERSION_IND = TRUE 
								AND RT.DW_LOGICAL_DELETE_IND = FALSE
								AND PG.loyalty_program_nm = '1'
								GROUP BY PG.Loyalty_Program_Card_Nbr
							 ) C ON S.Club_Card_Nbr = C.Loyalty_Program_Card_Nbr  
						)src
							
                        LEFT JOIN 
                          (SELECT  DISTINCT
								tgt.Business_Partner_Integration_Id
							   ,tgt.Retail_Customer_UUID
							   ,tgt.Transaction_Id
							   ,tgt.Transaction_Ts
							   ,tgt.Transaction_Type_Cd
							   ,tgt.Transaction_Type_Dsc
							   ,tgt.Transaction_Type_Short_Dsc
							   ,tgt.Reference_Nbr
							   ,tgt.Alt_Transaction_Id
							   ,tgt.Alt_Transaction_Type_Cd
							   ,tgt.Alt_Transaction_Type_Dsc
							   ,tgt.Alt_Transaction_Type_Short_Dsc
							   ,tgt.Partner_Division_Id
							   ,tgt.Postal_Zone_Cd
							   ,tgt.Customer_Division_Id
							   ,tgt.Old_Club_Card_Nbr
							   ,tgt.Status_Type_Cd
							   ,tgt.Status_Type_Dsc
							   ,tgt.Status_Type_Effective_Ts
							   ,tgt.Fuel_Pump_Id
							   ,tgt.Register_Id
							   ,tgt.Fuel_Grade_Cd
							   ,tgt.Fuel_Grade_Dsc
							   ,tgt.Fuel_Grade_Short_Dsc
							   ,tgt.Tender_Type_Cd
							   ,tgt.Tender_Type_Dsc
							   ,tgt.Tender_Type_Short_Dsc
							   ,tgt.Reward_Message_Id
							   ,tgt.Reward_Token_Offered_Qty
							   ,tgt.Total_Purchase_Qty
							   ,tgt.Purchase_UOM_Cd
							   ,tgt.Purchase_UOM_Nm
							   ,tgt.Purchase_Discount_Limit_Qty
							   ,tgt.Purchase_Discount_Amt
							   ,tgt.Total_Fuel_Purchase_Amt
							   ,tgt.Nonfuel_Purchase_Amt
							   ,tgt.Total_Purchase_Amt
							   ,tgt.Discount_Amt
							   ,tgt.Exception_Type_Dsc
							   ,tgt.Exception_Type_Short_Dsc
							   ,tgt.Exception_Transaction_Ts
							   ,tgt.Create_Ts
							   ,tgt.Create_User_Id
							   ,tgt.Update_Ts
							   ,tgt.Update_User_Id							   
							   ,tgt.dw_logical_delete_ind
							   ,tgt.dw_first_effective_dt
							   ,tgt.Club_Card_Nbr                         
							   ,tgt.Household_Id                         
							   ,tgt.Customer_Phone_Nbr
							   ,tgt.Total_Savings_Value_Amt
							   ,tgt.Alt_Transaction_Ts
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						  AND tgt.Transaction_Id = src.Transaction_Id
						  AND tgt.Status_Type_Cd =src.Status_Type_Cd
						--  AND tgt.Transaction_Ts = src.Transaction_Ts
						  AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
						  WHERE  (tgt.Business_Partner_Integration_Id is null and tgt.Transaction_Id is null and tgt.Status_Type_Cd is null and tgt.Transaction_Type_Cd is null)  
                          or(
                          NVL(src.Retail_Customer_UUID,'-1') <> NVL(tgt.Retail_Customer_UUID,'-1')
                          OR NVL(src.Transaction_Type_Dsc,'-1') <> NVL(tgt.Transaction_Type_Dsc,'-1')    
						  OR NVL(src.Transaction_Type_Short_Dsc,'-1') <> NVL(tgt.Transaction_Type_Short_Dsc,'-1')    						  
                          OR NVL(src.Reference_Nbr,'-1') <> NVL(tgt.Reference_Nbr,'-1')
                          OR NVL(src.Alt_Transaction_Id,'-1') <> NVL(tgt.Alt_Transaction_Id,'-1')
						  OR NVL(src.Alt_Transaction_Type_Cd,'-1') <> NVL(tgt.Alt_Transaction_Type_Cd,'-1')
						  OR NVL(src.Alt_Transaction_Type_Dsc,'-1') <> NVL(tgt.Alt_Transaction_Type_Dsc,'-1')
						  OR NVL(src.Alt_Transaction_Type_Short_Dsc,'-1') <> NVL(tgt.Alt_Transaction_Type_Short_Dsc,'-1')
						  OR NVL(src.Partner_Division_Id,'-1') <> NVL(tgt.Partner_Division_Id,'-1')
						  OR NVL(src.Postal_Zone_Cd,'-1') <> NVL(tgt.Postal_Zone_Cd,'-1')
						  OR NVL(src.Customer_Division_Id,'-1') <> NVL(tgt.Customer_Division_Id,'-1')
						  OR NVL(src.Status_Type_Dsc,'-1') <> NVL(tgt.Status_Type_Dsc,'-1')
						  OR NVL(src.Status_Type_Effective_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Status_Type_Effective_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Fuel_Pump_Id,'-1') <> NVL(tgt.Fuel_Pump_Id,'-1')
						  OR NVL(src.Register_Id,'-1') <> NVL(tgt.Register_Id,'-1')
						  OR NVL(src.Fuel_Grade_Cd,'-1') <> NVL(tgt.Fuel_Grade_Cd,'-1')
						  OR NVL(src.Fuel_Grade_Dsc,'-1') <> NVL(tgt.Fuel_Grade_Dsc,'-1')
						  OR NVL(src.Fuel_Grade_Short_Dsc,'-1') <> NVL(tgt.Fuel_Grade_Short_Dsc,'-1')
						  OR NVL(src.Tender_Type_Cd,'-1') <> NVL(tgt.Tender_Type_Cd,'-1')
						  OR NVL(src.Tender_Type_Dsc,'-1') <> NVL(tgt.Tender_Type_Dsc,'-1')
						  OR NVL(src.Tender_Type_Short_Dsc,'-1') <> NVL(tgt.Tender_Type_Short_Dsc,'-1')
						  OR NVL(src.Reward_Message_Id,'-1') <> NVL(tgt.Reward_Message_Id,'-1')
						  OR NVL(src.Reward_Token_Offered_Qty,'-1') <> NVL(tgt.Reward_Token_Offered_Qty,'-1')
						  OR NVL(src.Total_Purchase_Qty,'-1') <> NVL(tgt.Total_Purchase_Qty,'-1')
						  OR NVL(src.Purchase_UOM_Cd,'-1') <> NVL(tgt.Purchase_UOM_Cd,'-1')
						  OR NVL(src.Purchase_UOM_Nm,'-1') <> NVL(tgt.Purchase_UOM_Nm,'-1')
						  OR NVL(src.Purchase_Discount_Limit_Qty,'-1') <> NVL(tgt.Purchase_Discount_Limit_Qty,'-1')
						  OR NVL(src.Purchase_Discount_Amt,'-1') <> NVL(tgt.Purchase_Discount_Amt,'-1')
						  OR NVL(src.Total_Fuel_Purchase_Amt,'-1') <> NVL(tgt.Total_Fuel_Purchase_Amt,'-1')
						  OR NVL(src.Nonfuel_Purchase_Amt,'-1') <> NVL(tgt.Nonfuel_Purchase_Amt,'-1')
						  OR NVL(src.Total_Purchase_Amt,'-1') <> NVL(tgt.Total_Purchase_Amt,'-1')
						  OR NVL(src.Discount_Amt,'-1') <> NVL(tgt.Discount_Amt,'-1')
						  OR NVL(src.Exception_Type_Dsc,'-1') <> NVL(tgt.Exception_Type_Dsc,'-1')
						  OR NVL(src.Exception_Type_Short_Dsc,'-1') <> NVL(tgt.Exception_Type_Short_Dsc,'-1')
						  OR NVL(src.Exception_Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Exception_Transaction_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Create_User_Id,'-1') <> NVL(tgt.Create_User_Id,'-1')
						  OR NVL(src.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Update_User_Id,'-1') <> NVL(tgt.Update_User_Id,'-1')
						  OR NVL(src.Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Transaction_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Old_Club_Card_Nbr,'-1') <> NVL(tgt.Old_Club_Card_Nbr,'-1')
						  OR NVL(src.Club_Card_Nbr,'-1') <> NVL(tgt.Club_Card_Nbr,'-1')
						  OR NVL(src.Household_Id,'-1') <> NVL(tgt.Household_Id,'-1')
						  OR NVL(src.Customer_Phone_Nbr,'-1') <> NVL(tgt.Customer_Phone_Nbr,'-1')
						  OR NVL(src.Total_Savings_Value_Amt,'-1') <> NVL(tgt.Total_Savings_Value_Amt,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						  OR NVL(src.Alt_Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Alt_Transaction_Ts,'9999-12-31 00:00:00.000')
                          )  
						  `;        

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Business_Partner_Reward_Transaction work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                           Business_Partner_Integration_Id, 
										   Transaction_Id,
										   Status_Type_Cd,
										   Transaction_Type_Cd,
                                           filename					   
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND Business_Partner_Integration_Id is not NULL                              
							 AND Transaction_Id is not null							 
							 AND Status_Type_Cd is not null	
							 AND Transaction_Type_Cd is not null
                             ) src
                             WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							 AND tgt.Transaction_Id = src.Transaction_Id
							 AND tgt.Status_Type_Cd = src.Status_Type_Cd
							 AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Transaction_Type_Dsc               = src.Transaction_Type_Dsc          
					   ,Transaction_Type_Short_Dsc         = src.Transaction_Type_Short_Dsc    
					   ,Reference_Nbr                      = src.Reference_Nbr                 
					   ,Alt_Transaction_Id                 = src.Alt_Transaction_Id            
					   ,Alt_Transaction_Type_Cd            = src.Alt_Transaction_Type_Cd       
					   ,Alt_Transaction_Type_Dsc           = src.Alt_Transaction_Type_Dsc      
					   ,Alt_Transaction_Type_Short_Dsc     = src.Alt_Transaction_Type_Short_Dsc
					   ,Partner_Division_Id                = src.Partner_Division_Id           
					   ,Postal_Zone_Cd                     = src.Postal_Zone_Cd                
					   ,Customer_Division_Id               = src.Customer_Division_Id          
					   ,Old_Club_Card_Nbr                  = src.Old_Club_Card_Nbr             
					   ,Transaction_Ts                     = src.Transaction_Ts                
					   ,Status_Type_Dsc                    = src.Status_Type_Dsc               
					   ,Status_Type_Effective_Ts           = src.Status_Type_Effective_Ts      
					   ,Fuel_Pump_Id                       = src.Fuel_Pump_Id                  
					   ,Register_Id                        = src.Register_Id                   
					   ,Fuel_Grade_Cd                      = src.Fuel_Grade_Cd                 
					   ,Fuel_Grade_Dsc                     = src.Fuel_Grade_Dsc                
					   ,Fuel_Grade_Short_Dsc               = src.Fuel_Grade_Short_Dsc          
					   ,Tender_Type_Cd                     = src.Tender_Type_Cd                
					   ,Tender_Type_Dsc                    = src.Tender_Type_Dsc               
					   ,Tender_Type_Short_Dsc              = src.Tender_Type_Short_Dsc         
					   ,Reward_Message_Id                  = src.Reward_Message_Id             
					   ,Reward_Token_Offered_Qty           = src.Reward_Token_Offered_Qty      
					   ,Total_Purchase_Qty                 = src.Total_Purchase_Qty            
					   ,Purchase_UOM_Cd                    = src.Purchase_UOM_Cd               
					   ,Purchase_UOM_Nm                    = src.Purchase_UOM_Nm               
					   ,Purchase_Discount_Limit_Qty        = src.Purchase_Discount_Limit_Qty   
					   ,Purchase_Discount_Amt              = src.Purchase_Discount_Amt         
					   ,Total_Fuel_Purchase_Amt            = src.Total_Fuel_Purchase_Amt       
					   ,Nonfuel_Purchase_Amt               = src.Nonfuel_Purchase_Amt          
					   ,Total_Purchase_Amt                 = src.Total_Purchase_Amt            
					   ,Discount_Amt                       = src.Discount_Amt                  
					   ,Exception_Type_Dsc                 = src.Exception_Type_Dsc            
					   ,Exception_Type_Short_Dsc           = src.Exception_Type_Short_Dsc      
					   ,Exception_Transaction_Ts           = src.Exception_Transaction_Ts      
					   ,Create_Ts                          = src.Create_Ts                     
					   ,Create_User_Id                     = src.Create_User_Id                
					   ,Update_Ts                          = src.Update_Ts                     
					   ,Update_User_Id                     = src.Update_User_Id                
					   ,DW_Logical_delete_ind 			   = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS                  = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM                = FileName
					   ,Club_Card_Nbr                      = src.Club_Card_Nbr        
					   ,Household_Id                       = src.Household_Id        
					   ,Customer_Phone_Nbr                 = src.Customer_Phone_Nbr
					   ,Retail_Customer_UUID               = src.Retail_Customer_UUID
					   ,Total_Savings_Value_Amt            = src.Total_Savings_Value_Amt
					   ,Alt_Transaction_Ts				   = src.Alt_Transaction_Ts
						FROM ( SELECT 
								     Business_Partner_Integration_Id
									,Retail_Customer_UUID
									,Transaction_Id
									,Transaction_Ts
									,Transaction_Type_Cd
									,Transaction_Type_Dsc
									,Transaction_Type_Short_Dsc
									,Reference_Nbr
									,Alt_Transaction_Id
									,Alt_Transaction_Type_Cd
									,Alt_Transaction_Type_Dsc
									,Alt_Transaction_Type_Short_Dsc
									,Partner_Division_Id
									,Postal_Zone_Cd
									,Customer_Division_Id
									,Old_Club_Card_Nbr
									,Status_Type_Cd
									,Status_Type_Dsc
									,Status_Type_Effective_Ts
									,Fuel_Pump_Id
									,Register_Id
									,Fuel_Grade_Cd
									,Fuel_Grade_Dsc
									,Fuel_Grade_Short_Dsc
									,Tender_Type_Cd
									,Tender_Type_Dsc
									,Tender_Type_Short_Dsc
									,Reward_Message_Id
									,Reward_Token_Offered_Qty
									,Total_Purchase_Qty
									,Purchase_UOM_Cd
									,Purchase_UOM_Nm
									,Purchase_Discount_Limit_Qty
									,Purchase_Discount_Amt
									,Total_Fuel_Purchase_Amt
									,Nonfuel_Purchase_Amt
									,Total_Purchase_Amt
									,Discount_Amt
									,Exception_Type_Dsc
									,Exception_Type_Short_Dsc
									,Exception_Transaction_Ts
									,Create_Ts
									,Create_User_Id
									,Update_Ts
									,Update_User_Id
									,CreationDt
									,DW_Logical_delete_ind
									,filename
									,Club_Card_Nbr     
									,Household_Id     
									,Customer_Phone_Nbr
									,Total_Savings_Value_Amt
									,Alt_Transaction_Ts
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Business_Partner_Integration_Id IS NOT NULL																   
							   AND Transaction_Id IS NOT NULL	
							   AND Status_Type_Cd IS NOT NULL
							   AND Transaction_Type_Cd IS NOT NULL
							) src
							WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id							
							AND tgt.Transaction_Id = src.Transaction_Id
							AND tgt.Status_Type_Cd = src.Status_Type_Cd
							AND tgt.Transaction_Type_Cd = src.Transaction_Type_Cd
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Business_Partner_Integration_Id
					,Retail_Customer_UUID
					,Transaction_Id
					,Transaction_Ts
					,Transaction_Type_Cd
					,Transaction_Type_Dsc
					,Transaction_Type_Short_Dsc
					,Reference_Nbr
					,Alt_Transaction_Id
					,Alt_Transaction_Type_Cd
					,Alt_Transaction_Type_Dsc
					,Alt_Transaction_Type_Short_Dsc
					,Partner_Division_Id
					,Postal_Zone_Cd
					,Customer_Division_Id
					,Old_Club_Card_Nbr
					,Status_Type_Cd
					,Status_Type_Dsc
					,Status_Type_Effective_Ts
					,Fuel_Pump_Id
					,Register_Id
					,Fuel_Grade_Cd
					,Fuel_Grade_Dsc
					,Fuel_Grade_Short_Dsc
					,Tender_Type_Cd
					,Tender_Type_Dsc
					,Tender_Type_Short_Dsc
					,Reward_Message_Id
					,Reward_Token_Offered_Qty
					,Total_Purchase_Qty
					,Purchase_UOM_Cd
					,Purchase_UOM_Nm
					,Purchase_Discount_Limit_Qty
					,Purchase_Discount_Amt
					,Total_Fuel_Purchase_Amt
					,Nonfuel_Purchase_Amt
					,Total_Purchase_Amt
					,Discount_Amt
					,Exception_Type_Dsc
					,Exception_Type_Short_Dsc
					,Exception_Transaction_Ts
					,Create_Ts
					,Create_User_Id
					,Update_Ts
					,Update_User_Id
					,Total_Savings_Value_Amt
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND
					,Club_Card_Nbr                         
					,Household_Id                         
					,Customer_Phone_Nbr 
					,Alt_Transaction_Ts
                   )
                   SELECT DISTINCT
                      Business_Partner_Integration_Id
					 ,Retail_Customer_UUID
					 ,Transaction_Id
					 ,Transaction_Ts
					 ,Transaction_Type_Cd
					 ,Transaction_Type_Dsc
					 ,Transaction_Type_Short_Dsc
					 ,Reference_Nbr
					 ,Alt_Transaction_Id
					 ,Alt_Transaction_Type_Cd
					 ,Alt_Transaction_Type_Dsc
					 ,Alt_Transaction_Type_Short_Dsc
					 ,Partner_Division_Id
					 ,Postal_Zone_Cd
					 ,Customer_Division_Id
					 ,Old_Club_Card_Nbr
					 ,Status_Type_Cd
					 ,Status_Type_Dsc
					 ,Status_Type_Effective_Ts
					 ,Fuel_Pump_Id
					 ,Register_Id
					 ,Fuel_Grade_Cd
					 ,Fuel_Grade_Dsc
					 ,Fuel_Grade_Short_Dsc
					 ,Tender_Type_Cd
					 ,Tender_Type_Dsc
					 ,Tender_Type_Short_Dsc
					 ,Reward_Message_Id
					 ,Reward_Token_Offered_Qty
					 ,Total_Purchase_Qty
					 ,Purchase_UOM_Cd
					 ,Purchase_UOM_Nm
					 ,Purchase_Discount_Limit_Qty
					 ,Purchase_Discount_Amt
					 ,Total_Fuel_Purchase_Amt
					 ,Nonfuel_Purchase_Amt
					 ,Total_Purchase_Amt
					 ,Discount_Amt
					 ,Exception_Type_Dsc
					 ,Exception_Type_Short_Dsc
					 ,Exception_Transaction_Ts
					 ,Create_Ts
					 ,Create_User_Id
					 ,Update_Ts
					 ,Update_User_Id
					 ,Total_Savings_Value_Amt
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'                     
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
					 ,Club_Card_Nbr                         
					 ,Household_Id                         
					 ,Customer_Phone_Nbr
					 ,Alt_Transaction_Ts	
				FROM ${tgt_wrk_tbl}
                where Business_Partner_Integration_Id is not null				
				and Transaction_Id is not null
				and Status_Type_Cd is not null
				and Transaction_Type_Cd is not null
				and Sameday_chg_ind = 0`;
				
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;
						  
	var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
		SELECT DISTINCT 
		        Business_Partner_Integration_Id
			   ,Retail_Customer_UUID
			   ,Partner_Participant_Id
			   ,Partner_Site_Id
			   ,Partner_Id
			   ,Transaction_Id
			   ,Transaction_Ts
			   ,Transaction_Type_Cd
			   ,Transaction_Type_Dsc
			   ,Transaction_Type_Short_Dsc
			   ,Reference_Nbr
			   ,Alt_Transaction_Id
			   ,Alt_Transaction_Type_Cd
			   ,Alt_Transaction_Type_Dsc
			   ,Alt_Transaction_Type_Short_Dsc
			   ,Partner_Division_Id
			   ,Postal_Zone_Cd
			   ,Customer_Division_Id
			   ,Old_Club_Card_Nbr
			   ,Status_Type_Cd
			   ,Status_Type_Dsc
			   ,Status_Type_Effective_Ts
			   ,Fuel_Pump_Id
			   ,Register_Id
			   ,Fuel_Grade_Cd
			   ,Fuel_Grade_Dsc
			   ,Fuel_Grade_Short_Dsc
			   ,Tender_Type_Cd
			   ,Tender_Type_Dsc
			   ,Tender_Type_Short_Dsc
			   ,Reward_Message_Id
			   ,Reward_Token_Offered_Qty
			   ,Total_Purchase_Qty
			   ,Purchase_UOM_Cd
			   ,Purchase_UOM_Nm
			   ,Purchase_Discount_Limit_Qty
			   ,Purchase_Discount_Amt
			   ,Total_Fuel_Purchase_Amt
			   ,Nonfuel_Purchase_Amt
			   ,Total_Purchase_Amt
			   ,Discount_Amt
			   ,Exception_Type_Dsc
			   ,Exception_Type_Short_Dsc
			   ,Exception_Transaction_Ts
			   ,Create_Ts
			   ,Create_User_Id
			   ,Update_Ts
			   ,Update_User_Id
			   ,CustomerId
			   ,CreationDt
			   ,filename
			   ,CASE WHEN Business_Partner_Integration_Id IS NULL THEN 'Business_Partner_Integration_Id is NULL'					 
					 WHEN Transaction_Id IS NULL THEN 'Transaction_Id is NULL'
					 WHEN Status_Type_Cd IS NULL THEN 'Status_Type_Cd is NULL'	
					 WHEN Transaction_Type_Cd IS NULL THEN 'Transaction_Type_Cd is NULL'	
			         END AS Exception_Reason
			   ,CURRENT_TIMESTAMP AS dw_create_ts
			   ,Club_Card_Nbr                         
			   ,Household_Id                         
			   ,Customer_Phone_Nbr
			   ,Total_Savings_Value_Amt
			   ,Alt_Transaction_Ts
		FROM `+ tgt_wrk_tbl +`
		WHERE  Business_Partner_Integration_Id is NULL 		
		OR Transaction_Id is NULL
	    OR Status_Type_Cd is NULL
		OR Transaction_Type_Cd is NULL
	`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
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

// ************** Load for Business_Partner_Reward_Transaction table ENDs *****************

$$;
