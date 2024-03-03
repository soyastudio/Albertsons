--liquibase formatted sql
--changeset SYSTEM:SP_GetPartnerRewardReconciliation_TO_BIM_LOAD_Business_Partner_Reward_Reconciliation runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETPARTNERREWARDRECONCILIATION_TO_BIM_LOAD_BUSINESS_PARTNER_REWARD_RECONCILIATION(SRC_WRK_TBL VARCHAR, SRC_WRK_TBL1 VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
     
 
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;
var src_wrk_tbl1 = SRC_WRK_TBL1;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Reward_Reconciliation_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Reward_Reconciliation`;
var lkp_tbl =`${cnf_db}.${cnf_schema}.Business_Partner`;
var lkp_tbl_Cust = `${cnf_db}.${cnf_schema}.Business_Partner_Reward_Transaction`; 
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Reward_Reconciliation_Exceptions`;

// ************** Load for Business_Partner_Reward_Reconciliation table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
WITH src_wrk_tbl_recs as
(SELECT DISTINCT
Transaction_Id,
Alt_Transaction_Id,
--Transaction_Ts,
row_number() over (partition by Transaction_Id order by To_timestamp_ntz(CREATIONDT)desc) as Sequence_Nbr,
Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id,
--CustomerId,
FileName,
creationdt,
row_number() over (PARTITION BY  Transaction_Id,Partner_Participant_Id,Partner_Site_Id, Partner_Id order by To_timestamp_ntz(CREATIONDT) DESC)as rn
FROM
(
SELECT DISTINCT
TransactionId as Transaction_Id ,
AltTransactionId as Alt_Transaction_Id,
--TransactionTs as Transaction_Ts ,
                                                        --Sequence_Nbr ,
                                                        ReferenceNbr as Reference_Nbr,
StatusCd as Reward_Status_Cd,
                                                        StatusCd_Type as Reward_Status_Type_Cd,
                                                        StatusType_Description as Reward_Status_Dsc,
                                                        EffectiveDtTm as Reward_Status_Effective_Ts,
                                                        ReconMsgId as Reconcilation_Message_Id,
                                                        TotalPurchQty as Total_Purchase_Qty,
                                                        PurchUOMCd_Code as Purchase_UOM_Cd,
                                                        PurchUOMCd_Description as Purchase_UOM_Dsc,
                                                        PurchUOMCd_ShortDescription as Purchase_UOM_Short_Dsc,
                                                        PurchDiscLimitQty as Purchase_Discount_Limit_Qty,
                                                        TenderTypeCd_Code as Tender_Type_Cd,
                                                        TenderTypeCd_Description as Tender_Type_Dsc,
                                                        TenderTypeCd_ShortDescription as Tender_Type_Short_Dsc,
                                                        PurchDiscLimitAmt_TransactionAmt as Purchase_Discount_Amt,
                                                        RegularPriceAmt_TransactionAmt as Regular_Price_Amt,
                                                        PurchDiscLimitAmt_CurrencyCd as Currency_Cd,
                                                        PromoPriceAmt_TransactionAmt as Promotion_Price_Amt,
                                                        TotalSavingValAmt_TransactionAmt as Total_Saving_Amt,
                                                        TotalFuelPurchAmt_TransactionAmt as Total_Fuel_Purchase_Amt,
                                                        NonFuelPurchAmt_TransactionAmt as Nonfuel_Purchase_Amt,
                                                        TotalPurchaseAmt_TransactionAmt as Total_Purchase_Amt,
                                                        DiscountAmt_TransactionAmt as Discount_Purchase_Amt,
                                                        TxnFeeAmt_TransactionAmt as Transaction_Fee_Amt,
                                                        TxnNetPymtAmt_TransactionAmt as Net_Payment_Amt,
                                                        SettlementAmt_TransactionAmt as Settlement_Amt,
                                                        AccountId as Account_Id,
                                                        AccountingUnitId as Accounting_Unit_Id,
                                                        CreateTs as Create_Ts,
                                                        CreateUserId as Create_User_Id,
                                                        UpdateTs as Update_Ts,
                                                        UpdateUserId as Update_User_Id, 
TransactionTypeCd_Code as Transaction_Type_Cd,
PartnerParticipantId as Partner_Participant_Id,
PartnerSiteId as Partner_Site_Id,
PartnerId as Partner_Id, 
--CustomerId , 
FileName,
creationdt
FROM  ${src_wrk_tbl} 
) 
)
   select 
src.Transaction_Id, 
src.Alt_Transaction_Id,
src.Business_Partner_Integration_Id,
--src.Retail_Customer_UUID,
src.Transaction_Ts,
src.Sequence_Nbr ,
                                                        src.Reference_Nbr,
src.Reward_Status_Cd,
                                                        src.Reward_Status_Type_Cd,
                                                        src.Reward_Status_Dsc,
                                                        src.Reward_Status_Effective_Ts,
                                                        src.Reconcilation_Message_Id,
                                                        src.Total_Purchase_Qty,
                                                        src.Purchase_UOM_Cd,
                                                        src.Purchase_UOM_Dsc,
                                                        src.Purchase_UOM_Short_Dsc,
                                                        src.Purchase_Discount_Limit_Qty,
                                                        src.Tender_Type_Cd,
                                                        src.Tender_Type_Dsc,
                                                        src.Tender_Type_Short_Dsc,
                                                        src.Purchase_Discount_Amt,
                                                        src.Regular_Price_Amt,
                                                        src.Currency_Cd,
                                                        src.Promotion_Price_Amt,
                                                        src.Total_Saving_Amt,
                                                        src.Total_Fuel_Purchase_Amt,
                                                        src.Nonfuel_Purchase_Amt,
                                                        src.Total_Purchase_Amt,
                                                        src.Discount_Purchase_Amt,
                                                        src.Transaction_Fee_Amt,
                                                        src.Net_Payment_Amt,
                                                        src.Settlement_Amt,
                                                        src.Account_Id,
                                                        src.Accounting_Unit_Id,
                                                        src.Create_Ts,
                                                        src.Create_User_Id,
                                                        src.Update_Ts,
                                                        src.Update_User_Id,
src.Transaction_Type_Cd,
src.Partner_Participant_Id,
src.Partner_Site_Id,
src.Partner_Id,
--src.CustomerId, 
src.FileName,
src.DW_Logical_delete_ind,
src.creationdt,
CASE WHEN (tgt.Business_Partner_Integration_Id IS NULL  AND tgt.Transaction_Id IS NULL AND tgt.Transaction_Ts IS NULL AND tgt.Sequence_Nbr IS NULL) THEN 'I' ELSE 'U' END AS DML_Type,
CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from 
(select 
src1.Transaction_Id,
src1.Alt_Transaction_Id,
B.Business_Partner_Integration_Id as Business_Partner_Integration_Id, 
--C.Retail_Customer_UUID as Retail_Customer_UUID,
C.Transaction_Ts,
src1.Sequence_Nbr,
src1.Reference_Nbr,
src1.Reward_Status_Cd,
                                                        src1.Reward_Status_Type_Cd,
                                                        src1.Reward_Status_Dsc,
                                                        src1.Reward_Status_Effective_Ts,
                                                        src1.Reconcilation_Message_Id,
                                                        src1.Total_Purchase_Qty,
                                                        src1.Purchase_UOM_Cd,
                                                        src1.Purchase_UOM_Dsc,
                                                        src1.Purchase_UOM_Short_Dsc,
                                                        src1.Purchase_Discount_Limit_Qty,
                                                        src1.Tender_Type_Cd,
                                                        src1.Tender_Type_Dsc,
                                                        src1.Tender_Type_Short_Dsc,
                                                        src1.Purchase_Discount_Amt,
                                                        src1.Regular_Price_Amt,
                                                        src1.Currency_Cd,
                                                        src1.Promotion_Price_Amt,
                                                        src1.Total_Saving_Amt,
                                                        src1.Total_Fuel_Purchase_Amt,
                                                        src1.Nonfuel_Purchase_Amt,
                                                        src1.Total_Purchase_Amt,
                                                        src1.Discount_Purchase_Amt,
                                                        src1.Transaction_Fee_Amt,
                                                        src1.Net_Payment_Amt,
                                                        src1.Settlement_Amt,
                                                        src1.Account_Id,
                                                        src1.Accounting_Unit_Id,
                                                        src1.Create_Ts,
                                                        src1.Create_User_Id,
                                                        src1.Update_Ts,
                                                        src1.Update_User_Id,
src1.Transaction_Type_Cd,
src1.Partner_Participant_Id,
src1.Partner_Site_Id,
src1.Partner_Id,
--src1.CustomerId, 
src1.DW_Logical_delete_ind,
src1.FileName,
src1.creationdt
from
(
select
Transaction_Id,
Alt_Transaction_Id,
--Transaction_Ts,
Sequence_Nbr,
Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id,
--CustomerId, 
false AS DW_Logical_delete_ind,
FileName,
creationdt
FROM src_wrk_tbl_recs
where Transaction_Id is not null
   and Partner_Participant_Id is not null
AND Partner_Site_Id is not null
AND Partner_Id is not null
and rn=1
) src1
LEFT JOIN 
   ( SELECT distinct Transaction_Id
                                                                  ,max(Transaction_Ts) as Transaction_Ts

FROM ${lkp_tbl_Cust} 
WHERE DW_CURRENT_VERSION_IND = TRUE 
AND DW_LOGICAL_DELETE_IND = FALSE
Group By Transaction_Id
) C ON  src1.Transaction_Id = C.Transaction_Id
LEFT JOIN 
( SELECT distinct Business_Partner_Integration_Id
 ,Partner_Participant_Id
     ,Partner_Site_Id
     ,Partner_Id 
FROM ${lkp_tbl} 
WHERE DW_CURRENT_VERSION_IND = TRUE 
AND DW_LOGICAL_DELETE_IND = FALSE 
) B ON  ((NVL(src1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
AND NVL(src1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')
AND NVL(src1.Partner_Id,'-1') = NVL(B.Partner_Id,'-1'))
OR (NVL(src1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
AND NVL(src1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1'))) 
  
) src
LEFT JOIN (SELECT
tgt.Transaction_Id,
tgt.Alt_Transaction_Id,
tgt.Business_Partner_Integration_Id,
--tgt.Retail_Customer_UUID,
tgt.Transaction_Ts,
tgt.Sequence_Nbr, 
tgt.Reference_Nbr,
tgt.Reward_Status_Cd,
                                                        tgt.Reward_Status_Type_Cd,
                                                        tgt.Reward_Status_Dsc,
                                                        tgt.Reward_Status_Effective_Ts,
                                                        tgt.Reconcilation_Message_Id,
                                                        tgt.Total_Purchase_Qty,
                                                        tgt.Purchase_UOM_Cd,
                                                        tgt.Purchase_UOM_Dsc,
                                                        tgt.Purchase_UOM_Short_Dsc,
                                                        tgt.Purchase_Discount_Limit_Qty,
                                                        tgt.Tender_Type_Cd,
                                                        tgt.Tender_Type_Dsc,
                                                        tgt.Tender_Type_Short_Dsc,
                                                        tgt.Purchase_Discount_Amt,
                                                        tgt.Regular_Price_Amt,
                                                        tgt.Currency_Cd,
                                                        tgt.Promotion_Price_Amt,
                                                        tgt.Total_Saving_Amt,
                                                        tgt.Total_Fuel_Purchase_Amt,
                                                        tgt.Nonfuel_Purchase_Amt,
                                                        tgt.Total_Purchase_Amt,
                                                        tgt.Discount_Purchase_Amt,
                                                        tgt.Transaction_Fee_Amt,
                                                        tgt.Net_Payment_Amt,
                                                        tgt.Settlement_Amt,
                                                        tgt.Account_Id,
                                                        tgt.Accounting_Unit_Id,
                                                        tgt.Create_Ts,
                                                        tgt.Create_User_Id,
                                                        tgt.Update_Ts,
                                                        tgt.Update_User_Id,
tgt.Transaction_Type_Cd,
tgt.dw_logical_delete_ind,
tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt
ON tgt.Transaction_Id = src.Transaction_Id 
AND tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts
AND tgt.Sequence_Nbr = src.Sequence_Nbr 
where (tgt.Transaction_Id IS NULL  AND tgt.Business_Partner_Integration_Id IS NULL  AND tgt.Transaction_Ts IS NULL)
OR (NVL(src.Reference_Nbr,'-1') <> NVL(tgt.Reference_Nbr,'-1')
OR NVL(src.Reward_Status_Cd,'-1') <> NVL(tgt.Reward_Status_Cd,'-1')
                                                        OR NVL(src.Reward_Status_Dsc,'-1') <> NVL(tgt.Reward_Status_Dsc,'-1')
                                                        OR NVL(src.Reward_Status_Effective_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Reward_Status_Effective_Ts,'9999-12-31 00:00:00.000')
                                                        OR NVL(src.Reconcilation_Message_Id,'-1') <> NVL(tgt.Reconcilation_Message_Id,'-1')
                                                        OR NVL(src.Total_Purchase_Qty,'-1') <> NVL(tgt.Total_Purchase_Qty,'-1')
                                                        OR NVL(src.Purchase_UOM_Cd,'-1') <> NVL(tgt.Purchase_UOM_Cd,'-1')
                                                        OR NVL(src.Purchase_UOM_Dsc,'-1') <> NVL(tgt.Purchase_UOM_Dsc,'-1')
                                                        OR NVL(src.Purchase_UOM_Short_Dsc,'-1') <> NVL(tgt.Purchase_UOM_Short_Dsc,'-1')
                                                        OR NVL(src.Purchase_Discount_Limit_Qty,'-1') <> NVL(tgt.Purchase_Discount_Limit_Qty,'-1')
                                                        OR NVL(src.Tender_Type_Cd,'-1') <> NVL(tgt.Tender_Type_Cd,'-1')
                                                        OR NVL(src.Tender_Type_Dsc,'-1') <> NVL(tgt.Tender_Type_Dsc,'-1')
                                                        OR NVL(src.Tender_Type_Short_Dsc,'-1') <> NVL(tgt.Tender_Type_Short_Dsc,'-1')
                                                        OR NVL(src.Purchase_Discount_Amt,'-1') <> NVL(tgt.Purchase_Discount_Amt,'-1')
                                                        OR NVL(src.Regular_Price_Amt,'-1') <> NVL(tgt.Regular_Price_Amt,'-1')
                                                        OR NVL(src.Currency_Cd,'-1') <> NVL(tgt.Currency_Cd,'-1')
                                                        OR NVL(src.Promotion_Price_Amt,'-1') <> NVL(tgt.Promotion_Price_Amt,'-1')
                                                        OR NVL(src.Total_Saving_Amt,'-1') <> NVL(tgt.Total_Saving_Amt,'-1')
                                                        OR NVL(src.Total_Fuel_Purchase_Amt,'-1') <> NVL(tgt.Total_Fuel_Purchase_Amt,'-1')
                                                        OR NVL(src.Nonfuel_Purchase_Amt,'-1') <> NVL(tgt.Nonfuel_Purchase_Amt,'-1')
                                                        OR NVL(src.Total_Purchase_Amt,'-1') <> NVL(tgt.Total_Purchase_Amt,'-1')
                                                        OR NVL(src.Discount_Purchase_Amt,'-1') <> NVL(tgt.Discount_Purchase_Amt,'-1')
                                                        OR NVL(src.Transaction_Fee_Amt,'-1') <> NVL(tgt.Transaction_Fee_Amt,'-1')
                                                        OR NVL(src.Net_Payment_Amt,'-1') <> NVL(tgt.Net_Payment_Amt,'-1')
                                                        OR NVL(src.Settlement_Amt,'-1') <> NVL(tgt.Settlement_Amt,'-1')
                                                        OR NVL(src.Account_Id,'-1') <> NVL(tgt.Account_Id,'-1')
                                                        OR NVL(src.Accounting_Unit_Id,'-1') <> NVL(tgt.Accounting_Unit_Id,'-1')
                                                        OR NVL(src.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000')
                                                        OR NVL(src.Create_User_Id,'-1') <> NVL(tgt.Create_User_Id,'-1')
                                                        OR NVL(src.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000')
                                                        OR NVL(src.Update_User_Id,'-1') <> NVL(tgt.Update_User_Id,'-1')
                                                        OR NVL(src.Transaction_Type_Cd,'-1') <> NVL(tgt.Transaction_Type_Cd,'-1')
														OR NVL(src.Alt_Transaction_Id,'-1') <> NVL(tgt.Alt_Transaction_Id,'-1')											
OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)`;
try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        throw "Creation of Business_Partner_Reward_Reconciliation work table Failed with error: "+ err;   // Return a error message.
        }
//SCD Type2 transaction begins 
// Processing Updates of Type 2 SCD
var sql_begin = "BEGIN"
var sql_updates =`UPDATE ${tgt_tbl} as tgt
SET DW_Last_Effective_dt = CURRENT_DATE-1,
DW_CURRENT_VERSION_IND = FALSE,
DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Transaction_Id,
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr,
FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Transaction_Id IS NOT NULL
AND Business_Partner_Integration_Id IS NOT NULL 
--AND Retail_Customer_UUID IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL
) src
WHERE tgt.Transaction_Id = src.Transaction_Id
AND tgt.Business_Partner_Integration_Id  = src.Business_Partner_Integration_Id 
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts
AND tgt.Sequence_Nbr = src.Sequence_Nbr 
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                              
 //Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET Reference_Nbr = src.Reference_Nbr,
Reward_Status_Cd = src.Reward_Status_Cd,
                                                        Reward_Status_Type_Cd = src.Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc = src.Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts = src.Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id = src.Reconcilation_Message_Id,
                                                        Total_Purchase_Qty = src.Total_Purchase_Qty,
                                                        Purchase_UOM_Cd = src.Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc = src.Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc = src.Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty = src.Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd = src.Tender_Type_Cd,
                                                        Tender_Type_Dsc = src.Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc = src.Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt = src.Purchase_Discount_Amt,
                                                        Regular_Price_Amt = src.Regular_Price_Amt,
                                                        Currency_Cd = src.Currency_Cd,
                                                        Promotion_Price_Amt = src.Promotion_Price_Amt,
                                                        Total_Saving_Amt = src.Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt = src.Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt = src.Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt = src.Total_Purchase_Amt,
                                                        Discount_Purchase_Amt = src.Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt = src.Transaction_Fee_Amt,
                                                        Net_Payment_Amt = src.Net_Payment_Amt,
                                                        Settlement_Amt = src.Settlement_Amt,
                                                        Account_Id = src.Account_Id,
                                                        Accounting_Unit_Id = src.Accounting_Unit_Id,
                                                        Create_Ts = src.Create_Ts,
                                                        Create_User_Id = src.Create_User_Id,
                                                        Update_Ts = src.Update_Ts,
                                                        Update_User_Id = src.Update_User_Id,
                                                        Transaction_Type_Cd = src.Transaction_Type_Cd,
														Alt_Transaction_Id = src.Alt_Transaction_Id
FROM ( SELECT
Transaction_Id,
Alt_Transaction_Id, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr,
                                                        Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
FileName,
DW_Logical_delete_ind
FROM ${tgt_wrk_tbl}
       WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Transaction_Id IS NOT NULL 
AND Business_Partner_Integration_Id IS NOT NULL 
--AND Retail_Customer_UUID IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL
) src
WHERE tgt.Transaction_Id = src.Transaction_Id
AND tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id 
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts 
AND tgt.Sequence_Nbr = src.Sequence_Nbr 
AND    tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                        (Transaction_Id,
Alt_Transaction_Id,						
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr, 
DW_First_Effective_Dt,  
       DW_Last_Effective_Dt,  
  Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
DW_CREATE_TS,        
  DW_LOGICAL_DELETE_IND, 
  DW_SOURCE_CREATE_NM,   
  DW_CURRENT_VERSION_IND  
)
SELECT DISTINCT
Transaction_Id, 
Alt_Transaction_Id,
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr, 
CURRENT_DATE,
'31-DEC-9999',
Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
CURRENT_TIMESTAMP,
                                                DW_Logical_delete_ind,
FileName,
TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Transaction_Id IS NOT NULL 
AND Business_Partner_Integration_Id IS NOT NULL 
--AND Retail_Customer_UUID IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL`;
                          
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
    catch (err){
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return "Loading of Business_Partner_Reward_Reconciliation table Failed with error: "+ err;   // Return a error message.
        
       }
  
  
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;
 
var sql_exceptions =`INSERT INTO  ${tgt_exp_tbl} 
SELECT DISTINCT 
Transaction_Id,
Alt_Transaction_Id, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr,
Reference_Nbr,
Reward_Status_Cd,
                                                        Reward_Status_Type_Cd,
                                                        Reward_Status_Dsc,
                                                        Reward_Status_Effective_Ts,
                                                        Reconcilation_Message_Id,
                                                        Total_Purchase_Qty,
                                                        Purchase_UOM_Cd,
                                                        Purchase_UOM_Dsc,
                                                        Purchase_UOM_Short_Dsc,
                                                        Purchase_Discount_Limit_Qty,
                                                        Tender_Type_Cd,
                                                        Tender_Type_Dsc,
                                                        Tender_Type_Short_Dsc,
                                                        Purchase_Discount_Amt,
                                                        Regular_Price_Amt,
                                                        Currency_Cd,
                                                        Promotion_Price_Amt,
                                                        Total_Saving_Amt,
                                                        Total_Fuel_Purchase_Amt,
                                                        Nonfuel_Purchase_Amt,
                                                        Total_Purchase_Amt,
                                                        Discount_Purchase_Amt,
                                                        Transaction_Fee_Amt,
                                                        Net_Payment_Amt,
                                                        Settlement_Amt,
                                                        Account_Id,
                                                        Accounting_Unit_Id,
                                                        Create_Ts,
                                                        Create_User_Id,
                                                        Update_Ts,
                                                        Update_User_Id,
Transaction_Type_Cd,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id,
--CustomerId, 
FileName,
DW_Logical_delete_ind,
CREATIONDT,
DML_Type,
Sameday_chg_ind,
CASE WHEN Business_Partner_Integration_Id is NULL THEN 'Business_Partner_Integration_Id is NULL'
--WHEN Retail_Customer_UUID is NULL THEN 'Retail_Customer_UUID is NULL' 
ELSE NULL END AS Exception_Reason,
CURRENT_TIMESTAMP AS DW_CREATE_TS 
FROM  ${tgt_wrk_tbl}
WHERE Business_Partner_Integration_Id is NULL
--or Retail_Customer_UUID is NULL 
or Transaction_Id is NULL
or Transaction_Ts is null
or Sequence_Nbr is null
`;
try 
{
snowflake.execute (
{sqlText: sql_begin }
);
snowflake.execute(
{sqlText: truncate_exceptions}
);  
snowflake.execute (
{sqlText: sql_exceptions  }
);
snowflake.execute (
{sqlText: sql_commit  }
);
}
catch (err)  {
snowflake.execute (
{sqlText: sql_rollback  }
);
return `Insert into tgt Exception table  ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
}
  
// ************** Load for Business_Partner_Reward_Reconciliation table ENDs *****************

$$;
