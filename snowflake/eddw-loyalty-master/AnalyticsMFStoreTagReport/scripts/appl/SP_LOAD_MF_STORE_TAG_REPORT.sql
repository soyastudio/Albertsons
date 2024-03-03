--liquibase formatted sql
--changeset SYSTEM:SP_LOAD_MF_STORE_TAG_REPORT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_LOAD_MF_STORE_TAG_REPORT()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

// ************** Load for MF_STORE_TAG_REPORT table BEGIN *****************
var cnf_db = "EDM_CONFIRMED_PRD";
var stg_schema = "DW_C_STAGE";
var cnf_schema = "DW_C_PRODUCT";
var cnf_schema_purchasing = "DW_C_PURCHASING";
var app_schema = "DW_APPL";

var tgt_tbl = cnf_db + "." + cnf_schema+ ".MF_STORE_TAG_REPORT";
var tgt_wrk_tbl = cnf_db + "." + stg_schema + ".MF_STORE_TAG_REPORT_WRK";
var src_tbl = cnf_db + "." + app_schema + ".MF_STORE_TAG_REPORT_Stream";
var src_wrk_tbl = cnf_db + "." + stg_schema + ".MF_STORE_TAG_REPORT_Stream_WRK";

var OMS_OFFER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER";
var OMS_OFFER_REGION_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_REGION";
var OFFER_REQUEST_OFFER_SPECIFICATION_tbl = cnf_db + "." + cnf_schema_purchasing + ".OFFER_REQUEST_OFFER_SPECIFICATION";
var OFFER_REQUEST_PRODUCT_GROUP_tbl = cnf_db + "." + cnf_schema_purchasing + ".OFFER_REQUEST_PRODUCT_GROUP";
var OFFER_REQUEST_PRODUCT_GROUP_TIER_tbl = cnf_db + "." + cnf_schema_purchasing + ".OFFER_REQUEST_PRODUCT_GROUP_TIER";
var OMS_OFFER_QUALIFICATION_PRODUCT_GROUP_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_QUALIFICATION_PRODUCT_GROUP";
var OMS_PRODUCT_GROUP_tbl = cnf_db + "." + cnf_schema + ".OMS_PRODUCT_GROUP";
var OMS_PRODUCT_GROUP_UPC_tbl = cnf_db + "." + cnf_schema + ".OMS_PRODUCT_GROUP_UPC";
var OMS_OFFER_BENEFIT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT";
var OMS_OFFER_BENEFIT_DISCOUNT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT";
var OMS_OFFER_BENEFIT_DISCOUNT_TIER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT_TIER";

// Truncating Target work table
var trunc_src_tmp_wrk_tbl = `Truncate table `+ tgt_wrk_tbl +` `;
try {
snowflake.execute (
{sqlText: trunc_src_tmp_wrk_tbl  }
);
}
catch (err)  {
throw "Truncating of Target Work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
}

// Inserting into Target Work table
var cr_src_tmp_wrk_tbl = `INSERT INTO ` + tgt_wrk_tbl + `
(
Offer_Status_Dsc  ,
OMS_Offer_Region_Nm  ,
Chargeback_Vendor_Nm ,
Headline_Txt  ,
Product_Group_Nm  ,
OFFER_PROTOTYPE_CD ,
Aggregator_Offer_Id ,
OMS_Offer_Id ,
UPC_Cd ,
UPC_Dsc ,
Display_Effective_Start_Dt ,
Display_Effective_End_Dt ,
Offer_Prototype_Dsc ,
Benefit_Value_Type_Dsc ,
Discount_Tier_Amt ,
Discount_Tier_id ,
Item_Limit_Qty ,
Weight_Limit_Qty ,
Price_Title_Txt ,
Brand_Size_Dsc ,
Usage_Limit_Type_Per_User_Dsc ,
Store_Tag_Amt ,
Store_Tag_Comments_Txt ,
Pod_Offer_Detail_Dsc ,
Tier_Level_Amt ,
Unit_Of_Measure_Dsc ,
Min_Qty_To_Buy ,
Program_Cd ,
External_Offer_Id
)

SELECT * FROM (Select Distinct 
OMO.Offer_Status_Dsc ,
OMR.OMS_Offer_Region_Nm  ,
OMO.Chargeback_Vendor_Nm ,
OMO.Headline_Txt ,
OPG.Product_Group_Nm  ,
OMO.OFFER_PROTOTYPE_CD ,
OMO.Aggregator_Offer_Id  ,
OMO.OMS_Offer_Id ,
OUPC.UPC_Cd ,
OUPC.UPC_Dsc ,
OMO.Display_Effective_Start_Dt ,
OMO.Display_Effective_End_Dt ,
OMO.Offer_Prototype_Dsc ,
OBD.Benefit_Value_Type_Dsc ,
OBDT.Discount_Tier_Amt ,
OBDT.Discount_Tier_id ,
OBDT.Item_Limit_Qty ,
OBDT.Weight_Limit_Qty ,
OMO.Price_Title_Txt ,
OMO.Brand_Size_Dsc ,
OMO.Usage_Limit_Type_Per_User_Dsc ,
OMO.Store_Tag_Amt ,
OMO.Store_Tag_Comments_Txt ,
OROS.Pod_Offer_Detail_Dsc ,
OPGT.Tier_Level_Amt ,
ORPG.Unit_Of_Measure_Dsc ,
(case when upper(OMO.OFFER_PROTOTYPE_CD) = 'ITEM_DISCOUNT'
AND OPG.Product_Group_Nm is NOT NULL Then '1'
Else 
Case WHEN ORPG.Unit_Of_Measure_Dsc = 'Items' 
OR ORPG.Unit_Of_Measure_Dsc = 'Per Pound' 
then IFF(OPGT.Tier_Level_Amt is not null,to_number(OPGT.Tier_Level_Amt), NULL)
ELSE Null 
END
END) AS Min_Qty_To_Buy ,
OMO.Program_Cd ,
OMO.External_Offer_Id 
From ` + OMS_OFFER_tbl + ` OMO
LEFT JOIN ` + OMS_OFFER_REGION_tbl + ` OMR on OMR.Oms_Offer_id = OMO.Oms_Offer_id and OMR.DW_CURRENT_VERSION_IND = TRUE and OMR.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OFFER_REQUEST_OFFER_SPECIFICATION_tbl + ` OROS on OROS.Offer_Request_id = OMO.Offer_Request_id and OROS.DW_CURRENT_VERSION_IND = TRUE and OROS.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OFFER_REQUEST_PRODUCT_GROUP_tbl + ` ORPG On ORPG.Offer_Request_id = OMO.Offer_Request_id and ORPG.DW_CURRENT_VERSION_IND = TRUE and ORPG.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OFFER_REQUEST_PRODUCT_GROUP_TIER_tbl + ` OPGT On OPGT.Offer_Request_id = ORPG.Offer_Request_id and OPGT.DW_CURRENT_VERSION_IND = TRUE and OPGT.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_OFFER_QUALIFICATION_PRODUCT_GROUP_tbl + ` OQPG On OMO.Oms_Offer_id = OQPG.Oms_Offer_id and OQPG.DW_CURRENT_VERSION_IND = TRUE and OQPG.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_PRODUCT_GROUP_tbl + ` OPG On OPG.Product_Group_id = OQPG.Product_Group_id and OPG.DW_CURRENT_VERSION_IND = TRUE and OPG.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_PRODUCT_GROUP_UPC_tbl + ` OUPC On OUPC.Product_Group_id = OPG.Product_Group_id and OUPC.DW_CURRENT_VERSION_IND = TRUE and OUPC.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_OFFER_BENEFIT_tbl + ` OFB on OFB.Oms_Offer_id = OMO.Oms_Offer_id and OFB.DW_CURRENT_VERSION_IND = TRUE and OFB.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_OFFER_BENEFIT_DISCOUNT_tbl + ` OBD on OBD.Oms_Offer_id = OFB.Oms_Offer_id and OBD.DW_CURRENT_VERSION_IND = TRUE and OBD.DW_LOGICAL_DELETE_IND = FALSE
LEFT JOIN ` + OMS_OFFER_BENEFIT_DISCOUNT_TIER_tbl + ` OBDT on OBDT.Oms_Offer_id = OBD.Oms_Offer_id and OBDT.DW_CURRENT_VERSION_IND = TRUE and OBDT.DW_LOGICAL_DELETE_IND = FALSE
where OMO.DW_CURRENT_VERSION_IND = TRUE and OMO.DW_LOGICAL_DELETE_IND = FALSE
AND OMO.Program_Cd = 'MF'
AND OBDT.Discount_Tier_id = 1
AND OMO.Aggregator_Offer_Id != '(mfOfferID)'
AND OMO.OMS_Offer_Id IN
(
SELECT Payload_id from ` + src_tbl + ` where UPPER(METADATA$ACTION) = 'INSERT'
   )
)
`; 

 try {
snowflake.execute (
{sqlText: cr_src_tmp_wrk_tbl  }
)
}
catch (err)  {
throw "Creation of  MF_STORE_TAG_REPORT work table Failed with error: " + err;   // Return a error message.
}
var sql_begin = "BEGIN";

// Processing deletes
var sql_deletes = `delete from ` + tgt_tbl + `
where (OMS_Offer_ID)
in (select distinct OMS_Offer_ID
from ` + tgt_wrk_tbl + `);`;

// Processing Inserts
var sql_inserts = `INSERT INTO ` + tgt_tbl + `
(
Offer_Status_Dsc ,
OMS_Offer_Region_Nm ,
Chargeback_Vendor_Nm ,
Headline_Txt ,
Product_Group_Nm ,
OFFER_PROTOTYPE_CD ,
Aggregator_Offer_Id ,
OMS_Offer_id ,
UPC_Cd ,
UPC_Dsc ,
Display_Effective_Start_Dt ,
Display_Effective_End_Dt ,
Offer_Prototype_Dsc  ,
Benefit_Value_Type_Dsc ,
Discount_Tier_Amt ,
Discount_Tier_id ,
Item_Limit_Qty ,
Weight_Limit_Qty ,
Price_Title_Txt ,
Brand_Size_Dsc ,
Usage_Limit_Type_Per_User_Dsc  ,
Store_Tag_Amt ,
Store_Tag_Comments_Txt ,
Pod_Offer_Detail_Dsc ,
Tier_Level_Amt ,
Unit_Of_Measure_Dsc ,
Min_Qty_To_Buy ,
Program_Cd ,
External_Offer_Id 
)
SELECT 
Offer_Status_Dsc ,
OMS_Offer_Region_Nm ,
Chargeback_Vendor_Nm ,
Headline_Txt ,
Product_Group_Nm ,
OFFER_PROTOTYPE_CD ,
Aggregator_Offer_Id ,
OMS_Offer_id ,
UPC_Cd ,
UPC_Dsc ,
Display_Effective_Start_Dt ,
Display_Effective_End_Dt ,
Offer_Prototype_Dsc ,
Benefit_Value_Type_Dsc ,
Discount_Tier_Amt ,
Discount_Tier_id ,
Item_Limit_Qty ,
Weight_Limit_Qty ,
Price_Title_Txt ,
Brand_Size_Dsc ,
Usage_Limit_Type_Per_User_Dsc ,
Store_Tag_Amt ,
Store_Tag_Comments_Txt ,
Pod_Offer_Detail_Dsc ,
Tier_Level_Amt ,
Unit_Of_Measure_Dsc ,
Min_Qty_To_Buy ,
Program_Cd ,
External_Offer_Id 

FROM ` + tgt_wrk_tbl + `;`;
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
snowflake.execute (
{sqlText: sql_begin}
);
snowflake.execute (
{sqlText: sql_deletes}
);
snowflake.execute (
{sqlText: sql_inserts}
);
snowflake.execute (
{sqlText: sql_commit}
);    
}
catch (err) {
snowflake.execute (
{sqlText: sql_rollback}
);
throw "Loading of MF_STORE_TAG_REPORT " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}
// **************        Load for MF_STORE_TAG_REPORT ENDs *****************
return "Done"


$$;