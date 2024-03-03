--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "CNF_SCHEMA" VARCHAR(16777216), "WRK_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS
$$

var cnf_db = CNF_DB;
var wrk_schema = WRK_SCHEMA;
var cnf_schema = CNF_SCHEMA;
var src_wrk_tbl = SRC_WRK_TBL;
// ************** Load for Offer_Request table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_wrk";
var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request";
var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Exceptions";
var UDF_ETM = cnf_db + ".DW_APPL.returnTimeInHHMMSS_Format( EndTm )" ;
var UDF_STM = cnf_db + ".DW_APPL.returnTimeInHHMMSS_Format( StartTm )" ;

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        
        }
    catch (err)  {
        return "Truncation of Offer_Request_wrk table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_command = `INSERT INTO ` + tgt_wrk_tbl + ` 
WITH src_wrk_tbl_recs as
( SELECT DISTINCT '' AS Additional_Details_Txt
 ,AdvertisementType_Code AS Advertisement_Type_Cd
, AdvertisementType_Description AS Advertisement_Type_Dsc
, AdvertisementType_ShortDescription AS Advertisement_Type_Short_Dsc
, ApplicationId AS Application_Id
, BrandInfoTxt AS Brand_Info_Txt
, BusinessJustificationTxt AS Business_Justification_Txt
, CustomerSegmentInfoTxt AS Customer_Segment_Info_Txt
, DeliveryChannelTypeCd AS Delivery_Channel_Type_Cd
, DeliveryChannelTypeDsc AS Delivery_Channel_Type_Dsc
,OfferBankTypeCd AS Offer_Bank_Type_Cd
,OfferBankId AS Offer_Bank_Id
,OfferBankNm AS Offer_Bank_Nm
,TemplateId AS Template_Id
,TemplateNm AS Template_Nm
, OfferRequestData_DisclaimerTxt AS Disclaimer_Txt
, CASE WHEN OfferPeriodType_DisplayEndDt = '' THEN NULL ELSE OfferPeriodType_DisplayEndDt END AS Display_End_Dt
, CASE WHEN OfferPeriodType_DisplayStartDt = '' THEN NULL ELSE OfferPeriodType_DisplayStartDt END AS Display_Start_Dt
, OfferRequestData_ImageId AS Image_Id
, '9999-12-31' AS Manufacturer_Type_Create_File_Dt
, '9999-12-31 00:00:00' AS Manufacturer_Type_Create_File_Ts
, '' AS Manufacturer_Type_Destination_Id
, ManufacturerType_Description AS Manufacturer_Type_Dsc
, FileNm AS Manufacturer_Type_File_Nm
, '0' AS Manufacturer_Type_File_Sequence_Nbr
, IdNbr AS Manufacturer_Type_Id
, IdTxt AS Manufacturer_Type_Id_Txt
, ManufacturerType_ShortDescription AS Manufacturer_Type_Short_Dsc
, '' AS Manufacturer_Type_Source_Id
, B.Offer_Effective_Day_Friday_Ind
, B.Offer_Effective_Day_Monday_Ind
, B.Offer_Effective_Day_Saturday_Ind
, B.Offer_Effective_Day_Sunday_Ind
, B.Offer_Effective_Day_Thursday_Ind
, B.Offer_Effective_Day_Tuesday_Ind
, B.Offer_Effective_Day_Wednesday_Ind
, ` + UDF_ETM +`  AS Offer_Effective_End_Tm
, ` + UDF_STM +`  AS Offer_Effective_Start_Tm
, OfferEffectiveTm_TimeZoneCd AS Offer_Effective_Time_Zone_Cd
, CASE WHEN OfferEndDt = '' THEN NULL ELSE OfferEndDt END AS Offer_End_Dt
, OfferItemDsc AS Offer_Item_Dsc
, SizeDsc AS Offer_Item_Size_Dsc
, OfferNm AS Offer_Nm
, OfferRequestCommentTxt AS Offer_Request_Comment_Txt
, DepartmentNm AS Offer_Request_Department_Nm
, OfferRequestDsc AS Offer_Request_Dsc
, a.OfferRequestId AS Offer_Request_Id
, OfferRequestTypeCd AS Offer_Request_Type_Cd
, CASE WHEN OfferStartDt = '' THEN NULL ELSE OfferStartDt END AS Offer_Start_Dt
, ProductQty AS Product_Qty
, PromotionProgramType_Code AS Promotion_Program_Type_Cd
, Name AS Promotion_Program_Type_Nm
, SavingsValueTxt AS Savings_Value_Txt
, SourceSystemId AS Source_System_Id
, StoreGroupQty AS Store_Group_Qty
, ChargebackDepartmentId AS Chargeback_Department_Id
, CASE WHEN TestEndDt = '' THEN NULL ELSE TestEndDt END AS Test_End_Dt
, CASE WHEN TestStartDt = '' THEN NULL ELSE TestStartDt END AS Test_Start_Dt
, TierQty AS Tier_Qty
, TriggerId AS Trigger_Id
, UpdatedApplicationId AS Updated_Application_Id
, AllowanceType_Code AS Vendor_Promotion_Allowance_Type_Cd
, AllowanceType_Description AS Vendor_Promotion_Allowance_Type_Dsc
, AllowanceType_ShortDescription AS Vendor_Promotion_Allowance_Type_Short_Dsc
, BilledInd AS Vendor_Promotion_Billed_Ind
, BillingOptionType_Code AS Vendor_Promotion_Billing_Option_Type_Cd
, BillingOptionType_Description AS Vendor_Promotion_Billing_Option_Type_Dsc
, BillingOptionType_ShortDescription AS Vendor_Promotion_Billing_Option_Type_Short_Dsc
, NOPAAssignStatus_Description AS Vendor_Promotion_NOPA_Assign_Status_Dsc
, CASE WHEN NOPAAssignStatus_EffectiveDtTm = '' THEN NULL ELSE NOPAAssignStatus_EffectiveDtTm END AS Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, NOPAAssignStatus_StatusTypeCd_Type AS Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, CASE WHEN NOPAEndDt = '' THEN NULL ELSE NOPAEndDt END AS Vendor_Promotion_NOPA_End_Dt
, CASE WHEN NOPAStartDt = '' THEN NULL ELSE NOPAStartDt END AS Vendor_Promotion_NOPA_Start_Dt
, VersionQty AS Version_Qty
, a.creationdt
, actiontypecd
, FileName
, AllocationTypeCd_Code as Allocation_Type_Cd
, AllocationTypeCd_Description as Allocation_Type_Desc
, AllocationTypeCd_ShortDescription as Allocation_Type_Short_Desc
, TemplateStatusCd as Offer_Template_Status_Cd
, UPCQtyTxt as UPC_QTY_TXT
, EcommProgramTypeName as Ecomm_Program_Type_Nm    
, EcommProgramTypeCode as Ecomm_Program_Type_Cd  
, EcommValidWithOtherOffersInd::BOOLEAN AS Valid_With_Other_Offers_Ind 
, EcommValidForFirstTimeCustomerInd::BOOLEAN AS Valid_For_First_Time_Customer_Ind
, EcommAutoApplyPromoInd::BOOLEAN AS Auto_Apply_Promo_Ind
, EcommOfferEligibleOrderCnt AS Offer_Eligible_Order_Cnt
, PODDetailType_VendorNm AS Gaming_Vendor_Nm
, PODDetailType_Land AS Gaming_land_Nm
, PODDetailType_Space AS Gaming_Land_Space_Nm
, PODDetailType_Slot AS Gaming_Land_Space_Slot_Nm
, PromotionSubProgramCode AS Promotion_Subprogram_Type_Cd
, EcommBehaviorCd as Offer_Qualification_Behavior_Cd
,InitialSubscriptionOfferInd::BOOLEAN AS Initial_Subscription_Offer_Ind
,OfferTemplateStatusInd::BOOLEAN AS Offer_Template_Status_Ind 
,DynamicOfferInd::BOOLEAN AS Dynamic_Offer_Ind 
,DaysToRedeemOfferCnt as Days_To_Redeem_Offer_Cnt
, Row_number() OVER ( partition BY a.OfferRequestId ORDER BY To_timestamp_ntz(a.creationdt) DESC) AS rn
FROM `+ src_wrk_tbl +` A INNER JOIN 
( 
SELECT OfferRequestId, creationdt, MAX(Offer_Effective_Day_Friday_Ind)AS Offer_Effective_Day_Friday_Ind, 
MAX(Offer_Effective_Day_Monday_Ind) AS Offer_Effective_Day_Monday_Ind,
MAX(Offer_Effective_Day_Saturday_Ind) AS Offer_Effective_Day_Saturday_Ind,
 MAX(Offer_Effective_Day_Sunday_Ind) AS Offer_Effective_Day_Sunday_Ind, 
 MAX(Offer_Effective_Day_Thursday_Ind) AS Offer_Effective_Day_Thursday_Ind,
MAX(Offer_Effective_Day_Tuesday_Ind) AS Offer_Effective_Day_Tuesday_Ind,
MAX(Offer_Effective_Day_Wednesday_Ind) AS  Offer_Effective_Day_Wednesday_Ind
FROM 
(
SELECT DISTINCT OfferRequestId, creationdt,
CASE WHEN OfferEffectiveDay_Qualifier = 'Friday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Friday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Monday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Monday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Saturday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Saturday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Sunday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Sunday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Thursday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Thursday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Tuesday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Tuesday_Ind
, CASE WHEN OfferEffectiveDay_Qualifier = 'Wednesday' THEN OfferEffectiveDay ELSE NULL END AS Offer_Effective_Day_Wednesday_Ind
FROM ` + src_wrk_tbl +`
) GROUP BY OfferRequestId, creationdt
) B ON A.OfferRequestId = B.OfferRequestId AND A.creationdt = B.creationdt
)
SELECT 
src.Additional_Details_Txt
,src.Advertisement_Type_Cd
, src.Advertisement_Type_Dsc
, src.Advertisement_Type_Short_Dsc
, src.Application_Id
, src.Brand_Info_Txt
, src.Business_Justification_Txt
, src.Customer_Segment_Info_Txt
, src.Delivery_Channel_Type_Cd
, src.Delivery_Channel_Type_Dsc
,src.Offer_Bank_Type_Cd
,src.Offer_Bank_Id
,src.Offer_Bank_Nm
,src.Template_Id
,src.Template_Nm
, src.Disclaimer_Txt
, src.Display_End_Dt
, src.Display_Start_Dt
, src.Image_Id
, src.Manufacturer_Type_Create_File_Dt
, src.Manufacturer_Type_Create_File_Ts
, src.Manufacturer_Type_Destination_Id
, src.Manufacturer_Type_Dsc
, src.Manufacturer_Type_File_Nm
, src.Manufacturer_Type_File_Sequence_Nbr
, src.Manufacturer_Type_Id
, src.Manufacturer_Type_Id_Txt
, src.Manufacturer_Type_Short_Dsc
, src.Manufacturer_Type_Source_Id
, src.Offer_Effective_Day_Friday_Ind
, src.Offer_Effective_Day_Monday_Ind
, src.Offer_Effective_Day_Saturday_Ind
, src.Offer_Effective_Day_Sunday_Ind
, src.Offer_Effective_Day_Thursday_Ind
, src.Offer_Effective_Day_Tuesday_Ind
, src.Offer_Effective_Day_Wednesday_Ind
, src.Offer_Effective_End_Tm
, src.Offer_Effective_Start_Tm
, src.Offer_Effective_Time_Zone_Cd
, src.Offer_End_Dt
, src.Offer_Item_Dsc
, src.Offer_Item_Size_Dsc
, src.Offer_Nm
, src.Offer_Request_Comment_Txt
, src.Offer_Request_Department_Nm
, src.Offer_Request_Dsc
, src.Offer_Request_Id
, src.Offer_Request_Type_Cd
, src.Offer_Start_Dt
, src.Product_Qty
, src.Promotion_Program_Type_Cd
, src.Promotion_Program_Type_Nm
, src.Savings_Value_Txt
, src.Source_System_Id
, src.Store_Group_Qty
, src.Chargeback_Department_Id
, src.Test_End_Dt
, src.Test_Start_Dt
, src.Tier_Qty
, src.Trigger_Id
, src.Updated_Application_Id
, src.Vendor_Promotion_Allowance_Type_Cd
, src.Vendor_Promotion_Allowance_Type_Dsc
, src.Vendor_Promotion_Allowance_Type_Short_Dsc
, src.Vendor_Promotion_Billed_Ind
, src.Vendor_Promotion_Billing_Option_Type_Cd
, src.Vendor_Promotion_Billing_Option_Type_Dsc
, src.Vendor_Promotion_Billing_Option_Type_Short_Dsc
, src.Vendor_Promotion_NOPA_Assign_Status_Dsc
, src.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, src.Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, src.Vendor_Promotion_NOPA_End_Dt
, src.Vendor_Promotion_NOPA_Start_Dt
, src.Version_Qty
,src.dw_logical_delete_ind
,src.FileName
,src.Allocation_Type_Cd
,src.Allocation_Type_Desc
,src.Allocation_Type_Short_Desc
,src.Offer_Template_Status_Cd
,src.UPC_QTY_TXT
,src.Ecomm_Program_Type_Nm  
,src.Ecomm_Program_Type_Cd  
,src.Valid_With_Other_Offers_Ind
,src.Valid_For_First_Time_Customer_Ind
,src.Auto_Apply_Promo_Ind
,src.Offer_Eligible_Order_Cnt
,src.Gaming_Vendor_Nm
,src.Gaming_land_Nm
,src.Gaming_Land_Space_Nm
,src.Gaming_Land_Space_Slot_Nm
,src.Promotion_Subprogram_Type_Cd
,src.Offer_Qualification_Behavior_Cd
,src.Initial_Subscription_Offer_Ind
,src.Offer_Template_Status_Ind
,src.Dynamic_Offer_Ind
,src.Days_To_Redeem_Offer_Cnt
,CASE WHEN (tgt.Offer_Request_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
FROM  (SELECT Additional_Details_Txt
, Advertisement_Type_Cd
, Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc
, Application_Id
, Brand_Info_Txt
, Business_Justification_Txt
, Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc
, Offer_Bank_Type_Cd
, Offer_Bank_Id
, Offer_Bank_Nm
, Template_Id
, Template_Nm
, Disclaimer_Txt
, Display_End_Dt
, Display_Start_Dt
, Image_Id
, Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm
, Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd
, Offer_End_Dt
, Offer_Item_Dsc
, Offer_Item_Size_Dsc
, Offer_Nm
, Offer_Request_Comment_Txt
, Offer_Request_Department_Nm
, Offer_Request_Dsc
, Offer_Request_Id
, Offer_Request_Type_Cd
, Offer_Start_Dt
, Product_Qty
, Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm
, Savings_Value_Txt
, Source_System_Id
, Store_Group_Qty
, Chargeback_Department_Id
, Test_End_Dt
, Test_Start_Dt
, Tier_Qty
, Trigger_Id
, Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt
, Version_Qty
,creationdt
,FALSE AS DW_Logical_delete_ind
,FileName
,Allocation_Type_Cd
,Allocation_Type_Desc
,Allocation_Type_Short_Desc
,Offer_Template_Status_Cd
,UPC_QTY_TXT
,Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd
,Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt
,Gaming_Vendor_Nm
,Gaming_land_Nm
,Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm
,Promotion_Subprogram_Type_Cd
,Offer_Qualification_Behavior_Cd
,Initial_Subscription_Offer_Ind
,Offer_Template_Status_Ind
,Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt
FROM src_wrk_tbl_recs
WHERE  rn = 1
AND UPPER(ActionTypeCd) <> 'DELETE'
) src
LEFT JOIN (SELECT 
tgt.Additional_Details_Txt
,tgt.Advertisement_Type_Cd
, tgt.Advertisement_Type_Dsc
, tgt.Advertisement_Type_Short_Dsc
, tgt.Application_Id
, tgt.Brand_Info_Txt
, tgt.Business_Justification_Txt
, tgt.Customer_Segment_Info_Txt
, tgt.Delivery_Channel_Type_Cd
, tgt.Delivery_Channel_Type_Dsc
,tgt.Offer_Bank_Type_Cd
,tgt.Offer_Bank_Id
,tgt.Offer_Bank_Nm
,tgt.Template_Id
,tgt.Template_Nm
, tgt.Disclaimer_Txt
, tgt.Display_End_Dt
, tgt.Display_Start_Dt
   , tgt.Image_Id
, tgt.Manufacturer_Type_Create_File_Dt
, tgt.Manufacturer_Type_Create_File_Ts
, tgt.Manufacturer_Type_Destination_Id
, tgt.Manufacturer_Type_Dsc
, tgt.Manufacturer_Type_File_Nm
, tgt.Manufacturer_Type_File_Sequence_Nbr
, tgt.Manufacturer_Type_Id
, tgt.Manufacturer_Type_Id_Txt
, tgt.Manufacturer_Type_Short_Dsc
, tgt.Manufacturer_Type_Source_Id
, tgt.Offer_Effective_Day_Friday_Ind
, tgt.Offer_Effective_Day_Monday_Ind
, tgt.Offer_Effective_Day_Saturday_Ind
, tgt.Offer_Effective_Day_Sunday_Ind
, tgt.Offer_Effective_Day_Thursday_Ind
, tgt.Offer_Effective_Day_Tuesday_Ind
, tgt.Offer_Effective_Day_Wednesday_Ind
, tgt.Offer_Effective_End_Tm
, tgt.Offer_Effective_Start_Tm
, tgt.Offer_Effective_Time_Zone_Cd
, tgt.Offer_End_Dt
, tgt.Offer_Item_Dsc
, tgt.Offer_Item_Size_Dsc
, tgt.Offer_Nm
, tgt.Offer_Request_Comment_Txt
, tgt.Offer_Request_Department_Nm
, tgt.Offer_Request_Dsc
, tgt.Offer_Request_Id
, tgt.Offer_Request_Type_Cd
, tgt.Offer_Start_Dt
, tgt.Product_Qty
, tgt.Promotion_Program_Type_Cd
, tgt.Promotion_Program_Type_Nm
, tgt.Savings_Value_Txt
, tgt.Source_System_Id
, tgt.Store_Group_Qty
,tgt.Chargeback_Department_Id
, tgt.Test_End_Dt
, tgt.Test_Start_Dt
, tgt.Tier_Qty
, tgt.Trigger_Id
, tgt.Updated_Application_Id
, tgt.Vendor_Promotion_Allowance_Type_Cd
, tgt.Vendor_Promotion_Allowance_Type_Dsc
, tgt.Vendor_Promotion_Allowance_Type_Short_Dsc
, tgt.Vendor_Promotion_Billed_Ind
, tgt.Vendor_Promotion_Billing_Option_Type_Cd
, tgt.Vendor_Promotion_Billing_Option_Type_Dsc
, tgt.Vendor_Promotion_Billing_Option_Type_Short_Dsc
, tgt.Vendor_Promotion_NOPA_Assign_Status_Dsc
, tgt.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, tgt.Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, tgt.Vendor_Promotion_NOPA_End_Dt
, tgt.Vendor_Promotion_NOPA_Start_Dt
, tgt.Version_Qty
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
,tgt.Allocation_Type_Cd
,tgt.Allocation_Type_Desc
,tgt.Allocation_Type_Short_Desc
,tgt.Offer_Template_Status_Cd
,tgt.UPC_QTY_TXT
,tgt.Ecomm_Program_Type_Nm 
,tgt.Ecomm_Program_Type_Cd 
,tgt.Valid_With_Other_Offers_Ind
,tgt.Valid_For_First_Time_Customer_Ind
,tgt.Auto_Apply_Promo_Ind
,tgt.Offer_Eligible_Order_Cnt
,tgt.Gaming_Vendor_Nm
,tgt.Gaming_land_Nm
,tgt.Gaming_Land_Space_Nm
,tgt.Gaming_Land_Space_Slot_Nm
,tgt.Promotion_Subprogram_Type_Cd
,tgt.Offer_Qualification_Behavior_Cd
,tgt.Initial_Subscription_Offer_Ind
,tgt.Offer_Template_Status_Ind
,tgt.Dynamic_Offer_Ind
,tgt.Days_To_Redeem_Offer_Cnt
FROM ` + tgt_tbl + ` tgt
WHERE DW_CURRENT_VERSION_IND = TRUE
) tgt
ON tgt.Offer_Request_Id = src.Offer_Request_Id
WHERE   (tgt.Offer_Request_Id IS NULL)
OR
(       NVL(src.Additional_Details_Txt,'-1') <> NVL(tgt.Additional_Details_Txt,'-1')
OR NVL(src.Advertisement_Type_Cd,'-1') <> NVL(tgt.Advertisement_Type_Cd,'-1')
OR NVL(src.Advertisement_Type_Dsc,'-1') <> NVL(tgt.Advertisement_Type_Dsc,'-1')
OR NVL(src.Advertisement_Type_Short_Dsc,'-1') <> NVL(tgt.Advertisement_Type_Short_Dsc,'-1')
OR NVL(src.Application_Id,'-1') <> NVL(tgt.Application_Id,'-1')
OR NVL(src.Brand_Info_Txt,'-1') <> NVL(tgt.Brand_Info_Txt,'-1')
OR NVL(src.Business_Justification_Txt,'-1') <> NVL(tgt.Business_Justification_Txt,'-1')
OR NVL(src.Customer_Segment_Info_Txt,'-1') <> NVL(tgt.Customer_Segment_Info_Txt,'-1')
OR NVL(src.Delivery_Channel_Type_Cd,'-1') <> NVL(tgt.Delivery_Channel_Type_Cd,'-1')
OR NVL(src.Delivery_Channel_Type_Dsc,'-1') <> NVL(tgt.Delivery_Channel_Type_Dsc,'-1')
OR NVL(src.Offer_Bank_Type_Cd,'-1') <> NVL(tgt.Offer_Bank_Type_Cd,'-1')
OR NVL(src.Offer_Bank_Id,'-1') <> NVL(tgt.Offer_Bank_Id,'-1')
OR NVL(src.Offer_Bank_Nm,'-1') <> NVL(tgt.Offer_Bank_Nm,'-1')
OR NVL(src.Template_Id,'-1') <> NVL(tgt.Template_Id,'-1')
OR NVL(src.Template_Nm,'-1') <> NVL(tgt.Template_Nm,'-1')
OR NVL(src.Disclaimer_Txt,'-1') <> NVL(tgt.Disclaimer_Txt,'-1')
OR NVL(src.Display_End_Dt,'9999-12-31') <> NVL(tgt.Display_End_Dt,'9999-12-31')
OR NVL(src.Display_Start_Dt,'9999-12-31') <> NVL(tgt.Display_Start_Dt,'9999-12-31')
OR NVL(src.Image_Id,'-1') <> NVL(tgt.Image_Id,'-1')
OR NVL(src.Manufacturer_Type_Create_File_Dt,'9999-12-31') <> NVL(tgt.Manufacturer_Type_Create_File_Dt,'9999-12-31')
OR NVL(src.Manufacturer_Type_Create_File_Ts,'9999-12-31 00:00:00') <> NVL(tgt.Manufacturer_Type_Create_File_Ts,'9999-12-31 00:00:00')
OR NVL(src.Manufacturer_Type_Destination_Id,'-1') <> NVL(tgt.Manufacturer_Type_Destination_Id,'-1')
OR NVL(src.Manufacturer_Type_Dsc,'-1') <> NVL(tgt.Manufacturer_Type_Dsc,'-1')
OR NVL(src.Manufacturer_Type_File_Nm,'-1') <> NVL(tgt.Manufacturer_Type_File_Nm,'-1')
OR NVL(src.Manufacturer_Type_File_Sequence_Nbr,'-1') <> NVL(tgt.Manufacturer_Type_File_Sequence_Nbr,'-1')
OR NVL(src.Manufacturer_Type_Id,'-1') <> NVL(tgt.Manufacturer_Type_Id,'-1')
OR NVL(src.Manufacturer_Type_Id_Txt,'-1') <> NVL(tgt.Manufacturer_Type_Id_Txt,'-1')
OR NVL(src.Manufacturer_Type_Short_Dsc,'-1') <> NVL(tgt.Manufacturer_Type_Short_Dsc,'-1')
OR NVL(src.Manufacturer_Type_Source_Id,'-1') <> NVL(tgt.Manufacturer_Type_Source_Id,'-1')
OR NVL(src.Offer_Effective_Day_Friday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Friday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Monday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Monday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Saturday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Saturday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Sunday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Sunday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Thursday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Thursday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Tuesday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Tuesday_Ind,'-1')
OR NVL(src.Offer_Effective_Day_Wednesday_Ind,'-1') <> NVL(tgt.Offer_Effective_Day_Wednesday_Ind,'-1')
OR NVL(src.Offer_Effective_End_Tm,'00:00:00') <> NVL(tgt.Offer_Effective_End_Tm,'00:00:00')
OR NVL(src.Offer_Effective_Start_Tm,'00:00:00') <> NVL(tgt.Offer_Effective_Start_Tm,'00:00:00')
OR NVL(src.Offer_Effective_Time_Zone_Cd,'-1') <> NVL(tgt.Offer_Effective_Time_Zone_Cd,'-1')
OR NVL(src.Offer_End_Dt,'9999-12-31') <> NVL(tgt.Offer_End_Dt,'9999-12-31')
OR NVL(src.Offer_Item_Dsc,'-1') <> NVL(tgt.Offer_Item_Dsc,'-1')
OR NVL(src.Offer_Item_Size_Dsc,'-1') <> NVL(tgt.Offer_Item_Size_Dsc,'-1')
OR NVL(src.Offer_Nm,'-1') <> NVL(tgt.Offer_Nm,'-1')
OR NVL(src.Offer_Request_Comment_Txt,'-1') <> NVL(tgt.Offer_Request_Comment_Txt,'-1')
OR NVL(src.Offer_Request_Department_Nm,'-1') <> NVL(tgt.Offer_Request_Department_Nm,'-1')
OR NVL(src.Offer_Request_Dsc,'-1') <> NVL(tgt.Offer_Request_Dsc,'-1')
OR NVL(src.Offer_Request_Type_Cd,'-1') <> NVL(tgt.Offer_Request_Type_Cd,'-1')
OR NVL(src.Offer_Start_Dt,'9999-12-31') <> NVL(tgt.Offer_Start_Dt,'9999-12-31')
OR NVL(src.Product_Qty,'-1') <> NVL(tgt.Product_Qty,'-1')
OR NVL(src.Promotion_Program_Type_Cd,'-1') <> NVL(tgt.Promotion_Program_Type_Cd,'-1')
OR NVL(src.Promotion_Program_Type_Nm,'-1') <> NVL(tgt.Promotion_Program_Type_Nm,'-1')
OR NVL(src.Savings_Value_Txt,'-1') <> NVL(tgt.Savings_Value_Txt,'-1')
OR NVL(src.Source_System_Id,'-1') <> NVL(tgt.Source_System_Id,'-1')
OR NVL(src.Store_Group_Qty,'-1') <> NVL(tgt.Store_Group_Qty,'-1')
OR NVL(src.Chargeback_Department_Id,'-1') <> NVL(tgt.Chargeback_Department_Id,'-1')
OR NVL(src.Test_End_Dt,'9999-12-31') <> NVL(tgt.Test_End_Dt,'9999-12-31')
OR NVL(src.Test_Start_Dt,'9999-12-31') <> NVL(tgt.Test_Start_Dt,'9999-12-31')
OR NVL(src.Tier_Qty,'-1') <> NVL(tgt.Tier_Qty,'-1')
OR NVL(src.Trigger_Id,'-1') <> NVL(tgt.Trigger_Id,'-1')
OR NVL(src.Updated_Application_Id,'-1') <> NVL(tgt.Updated_Application_Id,'-1')
OR NVL(src.Vendor_Promotion_Allowance_Type_Cd,'-1') <> NVL(tgt.Vendor_Promotion_Allowance_Type_Cd,'-1')
OR NVL(src.Vendor_Promotion_Allowance_Type_Dsc,'-1') <> NVL(tgt.Vendor_Promotion_Allowance_Type_Dsc,'-1')
OR NVL(src.Vendor_Promotion_Allowance_Type_Short_Dsc,'-1') <> NVL(tgt.Vendor_Promotion_Allowance_Type_Short_Dsc,'-1')
OR NVL(src.Vendor_Promotion_Billed_Ind,'-1') <> NVL(tgt.Vendor_Promotion_Billed_Ind,'-1')
OR NVL(src.Vendor_Promotion_Billing_Option_Type_Cd,'-1') <> NVL(tgt.Vendor_Promotion_Billing_Option_Type_Cd,'-1')
OR NVL(src.Vendor_Promotion_Billing_Option_Type_Dsc,'-1') <> NVL(tgt.Vendor_Promotion_Billing_Option_Type_Dsc,'-1')
OR NVL(src.Vendor_Promotion_Billing_Option_Type_Short_Dsc,'-1') <> NVL(tgt.Vendor_Promotion_Billing_Option_Type_Short_Dsc,'-1')
OR NVL(src.Vendor_Promotion_NOPA_Assign_Status_Dsc,'-1') <> NVL(tgt.Vendor_Promotion_NOPA_Assign_Status_Dsc,'-1')
OR NVL(src.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts,'9999-12-31 00:00:00') <> NVL(tgt.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts,'9999-12-31 00:00:00')
OR NVL(src.Vendor_Promotion_NOPA_Assign_Status_Type_Cd,'-1') <> NVL(tgt.Vendor_Promotion_NOPA_Assign_Status_Type_Cd,'-1')
OR NVL(src.Vendor_Promotion_NOPA_End_Dt,'9999-12-31') <> NVL(tgt.Vendor_Promotion_NOPA_End_Dt,'9999-12-31')
OR NVL(src.Vendor_Promotion_NOPA_Start_Dt,'9999-12-31') <> NVL(tgt.Vendor_Promotion_NOPA_Start_Dt,'9999-12-31')
OR NVL(src.Version_Qty,'-1') <> NVL(tgt.Version_Qty,'-1')
OR NVL(src.Allocation_Type_Cd,'-1') <> NVL(tgt.Allocation_Type_Cd,'-1')
OR NVL(src.Allocation_Type_Desc,'-1') <> NVL(tgt.Allocation_Type_Desc,'-1')
OR NVL(src.Allocation_Type_Short_Desc,'-1') <> NVL(tgt.Allocation_Type_Short_Desc,'-1')
OR NVL(src.Offer_Template_Status_Cd,'-1') <> NVL(tgt.Offer_Template_Status_Cd,'-1')
OR NVL(src.UPC_QTY_TXT,'-1') <> NVL(tgt.UPC_QTY_TXT,'-1')
OR NVL(src.Ecomm_Program_Type_Nm,'-1') <> NVL(tgt.Ecomm_Program_Type_Nm,'-1')   
OR NVL(src.Ecomm_Program_Type_Cd,'-1') <> NVL(tgt.Ecomm_Program_Type_Cd,'-1')
OR NVL(src.Valid_With_Other_Offers_Ind,-1) <> NVL(tgt.Valid_With_Other_Offers_Ind,-1) 
OR NVL(src.Valid_For_First_Time_Customer_Ind,-1) <> NVL(tgt.Valid_For_First_Time_Customer_Ind,-1)
OR NVL(src.Auto_Apply_Promo_Ind,-1) <> NVL(tgt.Auto_Apply_Promo_Ind,-1)
OR NVL(src.Offer_Eligible_Order_CnT,'-1') <> NVL(tgt.Offer_Eligible_Order_CnT,'-1')
OR NVL(src.Gaming_Vendor_Nm,'-1') <> NVL(tgt.Gaming_Vendor_Nm,'-1')
OR NVL(src.Gaming_land_Nm,'-1') <> NVL(tgt.Gaming_land_Nm,'-1')
OR NVL(src.Gaming_Land_Space_Nm,'-1') <> NVL(tgt.Gaming_Land_Space_Nm,'-1')
OR NVL(src.Gaming_Land_Space_Slot_Nm,'-1') <> NVL(tgt.Gaming_Land_Space_Slot_Nm,'-1')
OR NVL(src.Promotion_Subprogram_Type_Cd,'-1') <> NVL(tgt.Promotion_Subprogram_Type_Cd,'-1')
OR NVL(src.Offer_Qualification_Behavior_Cd,'-1') <> NVL(tgt.Offer_Qualification_Behavior_Cd,'-1')
OR NVL(src.Initial_Subscription_Offer_Ind,-1) <> NVL(tgt.Initial_Subscription_Offer_Ind,-1)
OR NVL(src.Offer_Template_Status_Ind,-1) <> NVL(tgt.Offer_Template_Status_Ind,-1)
OR NVL(src.Dynamic_Offer_Ind,-1) <> NVL(tgt.Dynamic_Offer_Ind,-1)
OR NVL(src.Days_To_Redeem_Offer_Cnt,'-1') <> NVL(tgt.Days_To_Redeem_Offer_Cnt,'-1')
OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)
UNION ALL
SELECT tgt.Additional_Details_Txt
, tgt.Advertisement_Type_Cd
, tgt.Advertisement_Type_Dsc
, tgt.Advertisement_Type_Short_Dsc
, tgt.Application_Id
, tgt.Brand_Info_Txt
, tgt.Business_Justification_Txt
, tgt.Customer_Segment_Info_Txt
, tgt.Delivery_Channel_Type_Cd
, tgt.Delivery_Channel_Type_Dsc
,tgt.Offer_Bank_Type_Cd
,tgt.Offer_Bank_Id
,tgt.Offer_Bank_Nm
,tgt.Template_Id
,tgt.Template_Nm
, tgt.Disclaimer_Txt
, tgt.Display_End_Dt
, tgt.Display_Start_Dt
   , tgt.Image_Id
, tgt.Manufacturer_Type_Create_File_Dt
, tgt.Manufacturer_Type_Create_File_Ts
, tgt.Manufacturer_Type_Destination_Id
, tgt.Manufacturer_Type_Dsc
, tgt.Manufacturer_Type_File_Nm
, tgt.Manufacturer_Type_File_Sequence_Nbr
, tgt.Manufacturer_Type_Id
, tgt.Manufacturer_Type_Id_Txt
, tgt.Manufacturer_Type_Short_Dsc
, tgt.Manufacturer_Type_Source_Id
, tgt.Offer_Effective_Day_Friday_Ind
, tgt.Offer_Effective_Day_Monday_Ind
, tgt.Offer_Effective_Day_Saturday_Ind
, tgt.Offer_Effective_Day_Sunday_Ind
, tgt.Offer_Effective_Day_Thursday_Ind
, tgt.Offer_Effective_Day_Tuesday_Ind
, tgt.Offer_Effective_Day_Wednesday_Ind
, tgt.Offer_Effective_End_Tm
, tgt.Offer_Effective_Start_Tm
, tgt.Offer_Effective_Time_Zone_Cd
, tgt.Offer_End_Dt
, tgt.Offer_Item_Dsc
, tgt.Offer_Item_Size_Dsc
, tgt.Offer_Nm
, tgt.Offer_Request_Comment_Txt
, tgt.Offer_Request_Department_Nm
, tgt.Offer_Request_Dsc
, tgt.Offer_Request_Id
, tgt.Offer_Request_Type_Cd
, tgt.Offer_Start_Dt
, tgt.Product_Qty
, tgt.Promotion_Program_Type_Cd
, tgt.Promotion_Program_Type_Nm
, tgt.Savings_Value_Txt
, tgt.Source_System_Id
, tgt.Store_Group_Qty
,tgt.Chargeback_Department_Id
, tgt.Test_End_Dt
, tgt.Test_Start_Dt
, tgt.Tier_Qty
, tgt.Trigger_Id
, tgt.Updated_Application_Id
, tgt.Vendor_Promotion_Allowance_Type_Cd
, tgt.Vendor_Promotion_Allowance_Type_Dsc
, tgt.Vendor_Promotion_Allowance_Type_Short_Dsc
, tgt.Vendor_Promotion_Billed_Ind
, tgt.Vendor_Promotion_Billing_Option_Type_Cd
, tgt.Vendor_Promotion_Billing_Option_Type_Dsc
, tgt.Vendor_Promotion_Billing_Option_Type_Short_Dsc
, tgt.Vendor_Promotion_NOPA_Assign_Status_Dsc
, tgt.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, tgt.Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, tgt.Vendor_Promotion_NOPA_End_Dt
, tgt.Vendor_Promotion_NOPA_Start_Dt
, tgt.Version_Qty
,TRUE AS DW_Logical_delete_ind
,src.Filename
,tgt.Allocation_Type_Cd
,tgt.Allocation_Type_Desc
,tgt.Allocation_Type_Short_Desc
,tgt.Offer_Template_Status_Cd
,tgt.UPC_QTY_TXT
,tgt.Ecomm_Program_Type_Nm
,tgt.Ecomm_Program_Type_Cd
,tgt.Valid_With_Other_Offers_Ind
,tgt.Valid_For_First_Time_Customer_Ind
,tgt.Auto_Apply_Promo_Ind
,tgt.Offer_Eligible_Order_Cnt
,tgt.Gaming_Vendor_Nm
,tgt.Gaming_land_Nm
,tgt.Gaming_Land_Space_Nm
,tgt.Gaming_Land_Space_Slot_Nm
,tgt.Promotion_Subprogram_Type_Cd
,tgt.Offer_Qualification_Behavior_Cd
,tgt.Initial_Subscription_Offer_Ind
,tgt.Offer_Template_Status_Ind
,tgt.Dynamic_Offer_Ind
,tgt.Days_To_Redeem_Offer_Cnt
,'U' as DML_Type
,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
FROM ` + tgt_tbl + ` tgt
 inner join src_wrk_tbl_recs src on src.Offer_Request_Id = tgt.Offer_Request_Id
WHERE DW_CURRENT_VERSION_IND = TRUE
and rn = 1
AND upper(ActionTypeCd) = 'DELETE'
AND DW_LOGICAL_DELETE_IND = FALSE
AND (tgt.Offer_Request_Id) in
(
SELECT DISTINCT Offer_Request_Id
FROM src_wrk_tbl_recs src
WHERE rn = 1
AND upper(ActionTypeCd) = 'DELETE'
AND Offer_Request_Id is not null
)
`;
try {        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
var sql_updates = `// Processing Updates of Type 2 SCD
UPDATE ` + tgt_tbl + ` as tgt
SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT Offer_Request_Id
              ,FileName
FROM `+ tgt_wrk_tbl +`
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Offer_Request_Id is not NULL
) src
WHERE tgt.Offer_Request_Id = src.Offer_Request_Id
   AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
    var sql_sameday = `// Processing Sameday updates
UPDATE ` + tgt_tbl + ` as tgt
SET Additional_Details_Txt = src.Additional_Details_Txt
 ,Advertisement_Type_Cd = src.Advertisement_Type_Cd
, Advertisement_Type_Dsc = src.Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc = src.Advertisement_Type_Short_Dsc
, Application_Id = src.Application_Id
, Brand_Info_Txt = src.Brand_Info_Txt
, Business_Justification_Txt = src.Business_Justification_Txt
, Customer_Segment_Info_Txt = src.Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd = src.Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc = src.Delivery_Channel_Type_Dsc
,Offer_Bank_Type_Cd = src.Offer_Bank_Type_Cd
,Offer_Bank_Id = src.Offer_Bank_Id
,Offer_Bank_Nm = src.Offer_Bank_Nm
,Template_Id = src.Template_Id
,Template_Nm = src.Template_Nm
, Disclaimer_Txt = src.Disclaimer_Txt
, Display_End_Dt = src.Display_End_Dt
, Display_Start_Dt = src.Display_Start_Dt
   , Image_Id = src.Image_Id
, Manufacturer_Type_Create_File_Dt = src.Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts = src.Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id = src.Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc = src.Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm = src.Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr = src.Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id = src.Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt = src.Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc = src.Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id = src.Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind = src.Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind = src.Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind = src.Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind = src.Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind = src.Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind = src.Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind = src.Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm = src.Offer_Effective_End_Tm
, Offer_Effective_Start_Tm = src.Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd = src.Offer_Effective_Time_Zone_Cd
, Offer_End_Dt = src.Offer_End_Dt
, Offer_Item_Dsc = src.Offer_Item_Dsc
, Offer_Item_Size_Dsc = src.Offer_Item_Size_Dsc
, Offer_Nm = src.Offer_Nm
, Offer_Request_Comment_Txt = src.Offer_Request_Comment_Txt
, Offer_Request_Department_Nm = src.Offer_Request_Department_Nm
, Offer_Request_Dsc = src.Offer_Request_Dsc
, Offer_Request_Type_Cd = src.Offer_Request_Type_Cd
, Offer_Start_Dt = src.Offer_Start_Dt
, Product_Qty = src.Product_Qty
, Promotion_Program_Type_Cd = src.Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm = src.Promotion_Program_Type_Nm
, Savings_Value_Txt = src.Savings_Value_Txt
, Source_System_Id = src.Source_System_Id
, Store_Group_Qty = src.Store_Group_Qty
,Chargeback_Department_Id == src.Chargeback_Department_Id
, Test_End_Dt = src.Test_End_Dt
, Test_Start_Dt = src.Test_Start_Dt
, Tier_Qty = src.Tier_Qty
, Trigger_Id = src.Trigger_Id
, Updated_Application_Id = src.Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd = src.Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc = src.Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc = src.Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind = src.Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd = src.Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc = src.Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc = src.Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc = src.Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts = src.Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd = src.Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt = src.Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt = src.Vendor_Promotion_NOPA_Start_Dt
, Version_Qty = src.Version_Qty
,DW_Logical_delete_ind = src.DW_Logical_delete_ind
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
,Allocation_Type_Cd = src.Allocation_Type_Cd
,Allocation_Type_Desc = src.Allocation_Type_Desc
,Allocation_Type_Short_Desc = src.Allocation_Type_Short_Desc
,Offer_Template_Status_Cd = src.Offer_Template_Status_Cd
,UPC_QTY_TXT = src.UPC_QTY_TXT
,Ecomm_Program_Type_Nm = src.Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd = src.Ecomm_Program_Type_Cd 
,Valid_With_Other_Offers_Ind = src.Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind = src.Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind = src.Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt = src.Offer_Eligible_Order_Cnt
,Gaming_Vendor_Nm = src.Gaming_Vendor_Nm
,Gaming_land_Nm = src.Gaming_land_Nm
,Gaming_Land_Space_Nm = src.Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm = src.Gaming_Land_Space_Slot_Nm
,Promotion_Subprogram_Type_Cd = src.Promotion_Subprogram_Type_Cd
,Offer_Qualification_Behavior_Cd = src.Offer_Qualification_Behavior_Cd
,Initial_Subscription_Offer_Ind = src.Initial_Subscription_Offer_Ind
,Offer_Template_Status_Ind= src.Offer_Template_Status_Ind
,Dynamic_Offer_Ind = src.Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt = src.Days_To_Redeem_Offer_Cnt
FROM ( SELECT Additional_Details_Txt
, Advertisement_Type_Cd
, Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc
, Application_Id
, Brand_Info_Txt
, Business_Justification_Txt
, Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc
,Offer_Bank_Type_Cd
,Offer_Bank_Id
,Offer_Bank_Nm
,Template_Id
,Template_Nm
, Disclaimer_Txt
, Display_End_Dt
, Display_Start_Dt
   , Image_Id
, Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm
, Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd
, Offer_End_Dt
, Offer_Item_Dsc
, Offer_Item_Size_Dsc
, Offer_Nm
, Offer_Request_Comment_Txt
, Offer_Request_Department_Nm
, Offer_Request_Dsc
, Offer_Request_Id
, Offer_Request_Type_Cd
, Offer_Start_Dt
, Product_Qty
, Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm
, Savings_Value_Txt
, Source_System_Id
, Store_Group_Qty
,Chargeback_Department_Id
, Test_End_Dt
, Test_Start_Dt
, Tier_Qty
, Trigger_Id
, Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt
, Version_Qty
,DW_Logical_delete_ind
,FileName
,Allocation_Type_Cd 
,Allocation_Type_Desc 
,Allocation_Type_Short_Desc 
,Offer_Template_Status_Cd
,UPC_QTY_TXT
,Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd 
,Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt 
,Gaming_Vendor_Nm
,Gaming_land_Nm
,Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm
,Promotion_Subprogram_Type_Cd
,Offer_Qualification_Behavior_Cd
,Initial_Subscription_Offer_Ind
,Offer_Template_Status_Ind
,Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt
FROM `+ tgt_wrk_tbl +`
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Offer_Request_Id is not NULL
) src
WHERE tgt.Offer_Request_Id = src.Offer_Request_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ` + tgt_tbl + `
( Additional_Details_Txt
,Advertisement_Type_Cd
, Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc
, Application_Id
, Brand_Info_Txt
, Business_Justification_Txt
, Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc
,Offer_Bank_Type_Cd
,Offer_Bank_Id
,Offer_Bank_Nm
,Template_Id
,Template_Nm
, Disclaimer_Txt
, Display_End_Dt
, Display_Start_Dt
, Image_Id
, Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm
, Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd
, Offer_End_Dt
, Offer_Item_Dsc
, Offer_Item_Size_Dsc
, Offer_Nm
, Offer_Request_Comment_Txt
, Offer_Request_Department_Nm
, Offer_Request_Dsc
, Offer_Request_Id
, Offer_Request_Type_Cd
, Offer_Start_Dt
, Product_Qty
, Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm
, Savings_Value_Txt
, Source_System_Id
, Store_Group_Qty
,Chargeback_Department_Id
, Test_End_Dt
, Test_Start_Dt
, Tier_Qty
, Trigger_Id
, Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt
, Version_Qty
, DW_First_Effective_Dt    
, DW_Last_Effective_Dt    
, DW_CREATE_TS
, DW_LOGICAL_DELETE_IND
, DW_SOURCE_CREATE_NM
, DW_CURRENT_VERSION_IND
,Allocation_Type_Cd 
,Allocation_Type_Desc 
,Allocation_Type_Short_Desc 
,Offer_Template_Status_Cd
,UPC_QTY_TXT
,Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd 
,Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt
,Gaming_Vendor_Nm
,Gaming_land_Nm
,Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm 
,Promotion_Subprogram_Type_Cd
,Offer_Qualification_Behavior_Cd
,Initial_Subscription_Offer_Ind
,Offer_Template_Status_Ind
,Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt
)
SELECT Additional_Details_Txt
,Advertisement_Type_Cd
, Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc
, Application_Id
, Brand_Info_Txt
, Business_Justification_Txt
, Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc
,Offer_Bank_Type_Cd
,Offer_Bank_Id
,Offer_Bank_Nm
,Template_Id
,Template_Nm
, Disclaimer_Txt
, Display_End_Dt
, Display_Start_Dt
   , Image_Id
, Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm
, Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd
, Offer_End_Dt
, Offer_Item_Dsc
, Offer_Item_Size_Dsc
, Offer_Nm
, Offer_Request_Comment_Txt
, Offer_Request_Department_Nm
, Offer_Request_Dsc
, Offer_Request_Id
, Offer_Request_Type_Cd
, Offer_Start_Dt
, Product_Qty
, Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm
, Savings_Value_Txt
, Source_System_Id
, Store_Group_Qty
,Chargeback_Department_Id
, Test_End_Dt
, Test_Start_Dt
, Tier_Qty
, Trigger_Id
, Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt
, Version_Qty
, CURRENT_DATE
, '31-DEC-9999'
, CURRENT_TIMESTAMP
, DW_Logical_delete_ind
, FileName
, TRUE
,Allocation_Type_Cd 
,Allocation_Type_Desc 
,Allocation_Type_Short_Desc 
,Offer_Template_Status_Cd
,UPC_QTY_TXT
,Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd 
,Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt 
,Gaming_Vendor_Nm
,Gaming_land_Nm
,Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm 
,Promotion_Subprogram_Type_Cd
,Offer_Qualification_Behavior_Cd
,Initial_Subscription_Offer_Ind
,Offer_Template_Status_Ind
,Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt
FROM `+ tgt_wrk_tbl +`
WHERE Sameday_chg_ind = 0
AND Offer_Request_Id is not null
`;
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
        return "Loading of Offer_Request table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
                               select
 Additional_Details_Txt
,Advertisement_Type_Cd
, Advertisement_Type_Dsc
, Advertisement_Type_Short_Dsc
, Application_Id
, Brand_Info_Txt
, Business_Justification_Txt
, Customer_Segment_Info_Txt
, Delivery_Channel_Type_Cd
, Delivery_Channel_Type_Dsc
,Offer_Bank_Type_Cd
,Offer_Bank_Id
,Offer_Bank_Nm
,Template_Id
,Template_Nm
, Disclaimer_Txt
, Display_End_Dt
, Display_Start_Dt
   , Image_Id
, Manufacturer_Type_Create_File_Dt
, Manufacturer_Type_Create_File_Ts
, Manufacturer_Type_Destination_Id
, Manufacturer_Type_Dsc
, Manufacturer_Type_File_Nm
, Manufacturer_Type_File_Sequence_Nbr
, Manufacturer_Type_Id
, Manufacturer_Type_Id_Txt
, Manufacturer_Type_Short_Dsc
, Manufacturer_Type_Source_Id
, Offer_Effective_Day_Friday_Ind
, Offer_Effective_Day_Monday_Ind
, Offer_Effective_Day_Saturday_Ind
, Offer_Effective_Day_Sunday_Ind
, Offer_Effective_Day_Thursday_Ind
, Offer_Effective_Day_Tuesday_Ind
, Offer_Effective_Day_Wednesday_Ind
, Offer_Effective_End_Tm
, Offer_Effective_Start_Tm
, Offer_Effective_Time_Zone_Cd
, Offer_End_Dt
, Offer_Item_Dsc
, Offer_Item_Size_Dsc
, Offer_Nm
, Offer_Request_Comment_Txt
, Offer_Request_Department_Nm
, Offer_Request_Dsc
, Offer_Request_Id
, Offer_Request_Type_Cd
, Offer_Start_Dt
, Product_Qty
, Promotion_Program_Type_Cd
, Promotion_Program_Type_Nm
, Savings_Value_Txt
, Source_System_Id
, Store_Group_Qty
,Chargeback_Department_Id
, Test_End_Dt
, Test_Start_Dt
, Tier_Qty
, Trigger_Id
, Updated_Application_Id
, Vendor_Promotion_Allowance_Type_Cd
, Vendor_Promotion_Allowance_Type_Dsc
, Vendor_Promotion_Allowance_Type_Short_Dsc
, Vendor_Promotion_Billed_Ind
, Vendor_Promotion_Billing_Option_Type_Cd
, Vendor_Promotion_Billing_Option_Type_Dsc
, Vendor_Promotion_Billing_Option_Type_Short_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Dsc
, Vendor_Promotion_NOPA_Assign_Status_Effective_Ts
, Vendor_Promotion_NOPA_Assign_Status_Type_Cd
, Vendor_Promotion_NOPA_End_Dt
, Vendor_Promotion_NOPA_Start_Dt
, Version_Qty
, FileName
, DW_Logical_delete_ind
   , DML_Type
   , Sameday_chg_ind
   ,CASE WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
ELSE NULL END AS Exception_Reason
   ,CURRENT_TIMESTAMP AS DW_CREATE_TS
,Allocation_Type_Cd 
,Allocation_Type_Desc 
,Allocation_Type_Short_Desc 
,Offer_Template_Status_Cd
,UPC_QTY_TXT
,Ecomm_Program_Type_Nm  
,Ecomm_Program_Type_Cd
,Valid_With_Other_Offers_Ind
,Valid_For_First_Time_Customer_Ind
,Auto_Apply_Promo_Ind
,Offer_Eligible_Order_Cnt
,Gaming_Vendor_Nm
,Gaming_land_Nm
,Gaming_Land_Space_Nm
,Gaming_Land_Space_Slot_Nm
,Promotion_Subprogram_Type_Cd 
,Offer_Qualification_Behavior_Cd 
,Initial_Subscription_Offer_Ind 
,Offer_Template_Status_Ind
,Dynamic_Offer_Ind
,Days_To_Redeem_Offer_Cnt
FROM `+ tgt_wrk_tbl +`
WHERE Offer_Request_Id  IS NULL
`;
    try {
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
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
// ************** Load for Offer_Request table table ENDs *****************


$$;
