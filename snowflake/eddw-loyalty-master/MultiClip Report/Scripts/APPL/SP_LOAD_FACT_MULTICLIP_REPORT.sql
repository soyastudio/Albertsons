--liquibase formatted sql
--changeset SYSTEM:SP_F_Grocery_Reward_Offer_Redemption runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_MULTICLIP_REPORT()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
// ************** Load for FACT_MULTICLIP_REPORT table BEGIN *****************

var cnf_db = "EDM_CONFIRMED_PRD";
var stg_schema = "DW_C_STAGE";
var cnf_schema = "DW_C_PRODUCT";
var cnf_schema_retail = "DW_C_RETAILSALE";
var app_schema = "DW_APPL";
var loyalty_schema = "DW_C_LOYALTY";
var purchase_schema = "DW_C_PURCHASING";


var tgt_tbl = cnf_db + "." + cnf_schema + ".F_Multiclip";
var src_wrk_tbl = cnf_db + "." + stg_schema + ".FACT_MULTICLIP_REPORT_WRK";
var HHID_wrk_tbl = cnf_db + "." + stg_schema + ".HHID_WRK";
var src_tbl = cnf_db + "." + app_schema + ".FACT_MULTICLIP_REPORT_Stream";

var OMS_OFFER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER";
var EPE_TRANSACTION_HEADER_tbl = cnf_db + "." + cnf_schema_retail + ".EPE_TRANSACTION_HEADER";
var Epe_Transaction_header_Saving_Clips_tbl = cnf_db + "." + cnf_schema_retail + ".Epe_Transaction_header_Saving_Clips";
var Epe_Transaction_Item_Saving_Clips_tbl = cnf_db + "." + cnf_schema_retail +".Epe_Transaction_Item_Saving_Clips";
var CLIP_HEADER_tbl = cnf_db + "." + loyalty_schema + ".CLIP_HEADER";
var CLIP_DETAILS_tbl = cnf_db + "." + loyalty_schema + ".CLIP_DETAILS";
var Offer_Request_Requirement_Type_tbl = cnf_db + "." + purchase_schema + ".Offer_Request_Requirement_Type";
var OMS_OFFER_BENEFIT_DISCOUNT_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT";
var OMS_OFFER_BENEFIT_DISCOUNT_tier_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER_BENEFIT_DISCOUNT_tier";

// Inserting into Stream work table

var sql_inst_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tbl +` AS
							SELECT * FROM `+ src_tbl +` where METADATA$ACTION = 'INSERT'`;
try {
snowflake.execute (
{sqlText: sql_inst_src_wrk_tbl  });
}

catch (err)  {
throw "Creation of source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
}

var sql_HHID_wrk_tbl = `CREATE OR REPLACE TABLE `+ HHID_wrk_tbl +` AS
						SELECT DISTINCT HOUSEHOLD_ID FROM `+ CLIP_HEADER_tbl +` HDR
						JOIN `+ src_wrk_tbl +` SRC ON HDR.CLIP_SEQUENCE_ID = SRC.CLIP_SEQUENCE_ID
						WHERE HDR.dw_current_version_ind = true and HDR.dw_logical_delete_ind = false`;
try {
snowflake.execute (
{sqlText: sql_HHID_wrk_tbl  });
}

catch (err)  {
throw "Creation of HHID Work table "+ HHID_wrk_tbl +" Failed with error: " + err;   // Return a error message.
}

var sql_delete = `DELETE FROM `+ tgt_tbl +` WHERE HHID IN (select distinct HOUSEHOLD_ID from `+ HHID_wrk_tbl +`)`;
							
try {
snowflake.execute (
{sqlText: sql_delete  });
}

catch (err)  {
throw "Deletion of target table "+ tgt_tbl +" Failed with error: " + err;   // Return a error message.
}

var sql_begin = "BEGIN";

// Processing Inserts

var sql_inserts = `INSERT INTO ` + tgt_tbl + `
select ch.Household_Id as HHID
,Banner_Nm as Banner
,Clip_Source_Cd
,cd.OFFER_ID as Offer_ID
,Clip_Type_Cd as Clip_Type_Cd
,Offer_Nm as Offer_Description
,offer_Prototype_Cd as Offer_Type
,Benefit_Value_Type_Dsc as DISCOUNT_TYPE
,Usage_Limit_Type_Per_User_Dsc as Limit_Type
,Multi_Clip_Limit_Cnt as MultiClip_Limit
,REQUIRED_QTY as Required_Rewards
,Discount_Tier_Amt as Offer_$_Value
,epe.CLIP_ID
,CD.Clip_Dt as Clip_Date
,current_timestamp as dw_create_ts
from `+CLIP_HEADER_tbl+` ch
left join `+CLIP_DETAILS_tbl+` cd on ch.CLIP_SEQUENCE_ID = cd.CLIP_SEQUENCE_ID and cd.dw_current_version_ind = true and cd.dw_logical_delete_ind = false
left join `+OMS_OFFER_tbl+` off on cd.OFFER_ID = off.oms_offer_id and off.dw_current_version_ind = true and off.dw_logical_delete_ind = false
left join `+OMS_OFFER_BENEFIT_DISCOUNT_tbl+` obd on obd.oms_offer_id = off.oms_offer_id and obd.DISCOUNT_ID = 1
												and obd.dw_current_version_ind = true and obd.dw_logical_delete_ind = false
left join `+OMS_OFFER_BENEFIT_DISCOUNT_tier_tbl+` tier on tier.oms_offer_id = off.oms_offer_id and tier.DISCOUNT_TIER_ID = 1
												and tier.dw_current_version_ind = true and tier.dw_logical_delete_ind = false
left join `+Offer_Request_Requirement_Type_tbl+` req on req.offer_request_id = off.offer_request_id 
                                            and req.dw_current_version_ind = true and req.dw_logical_delete_ind = false
left join 
 (
 select distinct hdr.HOUSEHOLD_ID as HOUSEHOLD_ID,Hdr_Svn.offer_id as offer_id, Hdr_Svn.CLIP_ID as CLIP_ID from 
 `+EPE_TRANSACTION_HEADER_tbl+` hdr
left join 

(select transaction_integration_id,Offer_Id,a.value::string as CLIP_ID from `+Epe_Transaction_header_Saving_Clips_tbl+`   
  ,LATERAL FLATTEN(input => case when CLIP_ID like '%[%]%' then parse_json(CLIP_ID) else to_array(CLIP_ID) end , outer => TRUE) as a 
 where dw_current_version_ind = true and dw_logical_delete_ind = false
 
 union ALL
 
 select transaction_integration_id,Offer_Id,a.value::string as CLIP_ID from `+Epe_Transaction_Item_Saving_Clips_tbl+`   
  ,LATERAL FLATTEN(input => case when CLIP_ID like '%[%]%' then parse_json(CLIP_ID) else to_array(CLIP_ID) end , outer => TRUE) as a 
 where dw_current_version_ind = true and dw_logical_delete_ind = false
 
 )Hdr_Svn
 on Hdr_Svn.transaction_integration_id = hdr.transaction_integration_id
where hdr.dw_current_version_ind = true and hdr.dw_logical_delete_ind = false
and hdr.STATUS_CD = 'COMPLETED'
) epe on epe.HOUSEHOLD_ID = ch.HOUSEHOLD_ID and epe.offer_id = cd.offer_id and epe.CLIP_ID = cd.CLIP_ID
where ch.dw_current_version_ind = true and ch.dw_logical_delete_ind = false  
and off.PROGRAM_CD = 'GR'
AND off.Usage_Limit_Type_Per_User_Dsc = 'Once per Clip'
and off.POD_USAGE_LIMIT_TYPE_PER_USER_DSC = 'MULTI_CLIP'
and ch.HOUSEHOLD_ID in (select distinct HOUSEHOLD_ID from `+ HHID_wrk_tbl +`)
order by ch.Household_Id asc`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"

try {
snowflake.execute (
{sqlText: sql_begin}
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
throw "Loading of FACT_MULTICLIP_REPORT " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}

// **************        Load for FACT_MULTICLIP_REPORT ENDs *****************

$$;
