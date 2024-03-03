--liquibase formatted sql
--changeset SYSTEM:SP_LOAD_OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY runOnChange:true splitStatements:false OBJECT_TYPE:SP
USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_LOAD_OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

// ************** Load for OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY table BEGIN *****************
var cnf_db = "<<EDM_DB_NAME>>";
var stg_schema = "DW_C_STAGE";
var stg_schema_1 = "DW_STAGE";
var cnf_schema = "DW_C_PRODUCT";
var cnf_schema_retail = "DW_C_RETAILSALE";
var app_schema = "DW_APPL";
var loyalty_schema = "DW_C_LOYALTY";

var tgt_tbl = cnf_db + "." + loyalty_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY";
var tgt_wrk_tbl = cnf_db + "." + stg_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_WRK";
var src_tbl = cnf_db + "." + app_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_HEADER_SAVINGS";
var src_tbl_item = cnf_db + "." + app_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_ITEM_SAVINGS";
var src_wrk_tbl = cnf_db + "." + stg_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_HEADER_SAVINGS_WRK";
var src_wrk_tbl_item = cnf_db + "." + stg_schema + ".OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY_Stream_ITEM_SAVINGS_WRK";

var OMS_OFFER_tbl = cnf_db + "." + cnf_schema + ".OMS_OFFER";
var EPE_TRANSACTION_HEADER_tbl = cnf_db + "." + cnf_schema_retail + ".EPE_TRANSACTION_HEADER";
var EPE_TRANSACTION_HEADER_SAVINGS_tbl = cnf_db + "." + cnf_schema_retail + ".EPE_TRANSACTION_HEADER_SAVINGS";
var EPE_TRANSACTION_HEADER_SAVING_POINTS_tbl = cnf_db + "." + cnf_schema_retail + ".EPE_TRANSACTION_HEADER_SAVING_POINTS";
var EPE_TRANSACTION_ITEM_SAVINGS_tbl = cnf_db + "." + cnf_schema_retail +".EPE_TRANSACTION_ITEM_SAVINGS";
var CLIP_HEADER_tbl = cnf_db + "." + loyalty_schema + ".CLIP_HEADER";
var CLIP_DETAILS_tbl = cnf_db + "." + loyalty_schema + ".CLIP_DETAILS";


// Truncating Stream work table
var sql_trunc_src_wrk_tbl = `Truncate table `+ src_wrk_tbl +` `;
try {
snowflake.execute (
{sqlText: sql_trunc_src_wrk_tbl  }
);
}
catch (err)  {
throw "Truncating of Stream Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
}

// Inserting into Stream work table
var sql_inst_src_wrk_tbl = `INSERT INTO `+ src_wrk_tbl +`
SELECT * FROM `+ src_tbl +``;
try {
snowflake.execute (
{sqlText: sql_inst_src_wrk_tbl  }
);
}
catch (err)  {
throw "Inserting of Stream Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
}

// Truncating item Stream work table
var sql_trunc_src_wrk_tbl_item = `Truncate table `+ src_wrk_tbl_item +` `;
try {
snowflake.execute (
{sqlText: sql_trunc_src_wrk_tbl_item  }
);
}
catch (err)  {
throw "Truncating of Stream Work table "+ src_wrk_tbl_item +" Failed with error: " + err;   // Return a error message.
}

// Inserting into item Stream work table
var sql_inst_src_wrk_tbl_item = `INSERT INTO `+ src_wrk_tbl_item +`
SELECT * FROM `+ src_tbl_item +``;
try {
snowflake.execute (
{sqlText: sql_inst_src_wrk_tbl_item  }
);
}
catch (err)  {
throw "Inserting of Stream Work table "+ src_wrk_tbl_item +" Failed with error: " + err;   // Return a error message.
}

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
(STORE
,LANE
,TRANSACTION_NUMBER
,HHID
,REDEMPTION_COUNT
,REDEMPTION_AMOUNT
,POINTS_AMOUNT
,POINTS_PROGRAMNAME
,TRANSACTION_DATE
,OFFER_ID
,OFFER_START_DATE
,OFFER_END_DATE
,CLIPS_TOTALCOLUMN
,DW_CREATE_TS
)
SELECT * FROM (Select distinct x.Store_Nbr STORE,
x.Terminal_Nbr LANE,
x.Register_Transaction_Sequence_Nbr TRANSACTION_NUMBER,
x.Household_ID HHID,
x.REDEMPTION_TOTALCOLUMN REDEMPTION_COUNT,
round(x.Redemption_Amount,2) REDEMPTION_AMOUNT,
POINTS_AMOUNT POINTS_AMOUNT,
x.POINTS_PROGRAM_NM POINTS_PROGRAMNAME,
x.Transaction_Time TRANSACTION_DATE,
cast (X.EXTERNAL_OFFER_ID as VARCHAR) OFFER_ID,
oms.EFFECTIVE_START_DT OFFER_START_DATE,
oms.EFFECTIVE_END_DT OFFER_END_DATE,
x.Count_Clip CLIPS_TOTALCOLUMN,
CURRENT_TIMESTAMP DW_CREATE_TS
from (
(with
EPE_Redemption
as (Select Store_Nbr,TRANSACTION_INTEGRATION_ID,Terminal_Nbr,Register_Transaction_Sequence_Nbr,Household_ID,
sum(coalesce(REDEMPTION_CNT,0)) REDEMPTION_TOTALCOLUMN,
sum(coalesce(REDEMPTION_AMT,0)) Redemption_Amount,Transaction_Time,EXTERNAL_OFFER_ID,offer_ID from
    (Select h.Store_Nbr,h.TRANSACTION_INTEGRATION_ID,
h.Terminal_Nbr,
h.Register_Transaction_Sequence_Nbr,
h.Household_ID,
REDEMPTION_CNT,
REDEMPTION_AMT,
h.Transaction_Ts Transaction_Time,
hs.EXTERNAL_OFFER_ID,
hs.offer_ID
from ` + EPE_TRANSACTION_HEADER_tbl + ` h
inner join ` + src_wrk_tbl + ` hs on h.TRANSACTION_INTEGRATION_ID= hs.TRANSACTION_INTEGRATION_ID
where h.dw_current_version_ind=true and h.dw_logical_delete_ind =false
and h.status_cd = 'COMPLETED' 
and hs.dw_current_version_ind=true and hs.dw_logical_delete_ind =false 
QUALIFY (ROW_NUMBER() OVER (PARTITION BY Register_Transaction_Sequence_Nbr,EXTERNAL_OFFER_ID ORDER  BY Transaction_Time DESC)=1)
   Union all--436034
  Select h.Store_Nbr,h.TRANSACTION_INTEGRATION_ID,
h.Terminal_Nbr,
h.Register_Transaction_Sequence_Nbr,
h.Household_ID,
hs.DISCOUNT_QTY,
hs.DISCOUNT_AMT,
h.Transaction_Ts Transaction_Time,
hs.EXTERNAL_OFFER_ID,
hs.offer_ID
from ` + EPE_TRANSACTION_HEADER_tbl + ` h
inner join ` + src_wrk_tbl_item + ` hs on h.TRANSACTION_INTEGRATION_ID= hs.TRANSACTION_INTEGRATION_ID
where h.dw_current_version_ind=true and h.dw_logical_delete_ind =false
and h.status_cd = 'COMPLETED'
and hs.dw_current_version_ind=true and hs.dw_logical_delete_ind =false and hs.discount_level_txt = 'Item Level'
QUALIFY (ROW_NUMBER() OVER (PARTITION BY Register_Transaction_Sequence_Nbr,EXTERNAL_OFFER_ID ORDER  BY Transaction_Time DESC)=1)
     
)group by Store_Nbr,
Terminal_Nbr,
Register_Transaction_Sequence_Nbr,
Household_ID,Transaction_Time,EXTERNAL_OFFER_ID,TRANSACTION_INTEGRATION_ID,offer_ID)
,EPE_Points as
(Select Hs.Store_Nbr,hs.TRANSACTION_INTEGRATION_ID,hs.Terminal_Nbr,hs.Register_Transaction_Sequence_Nbr,hs.Household_ID,
hs.REDEMPTION_TOTALCOLUMN,
hs.Redemption_Amount,hs.Transaction_Time,hs.EXTERNAL_OFFER_ID,
to_number(sum(coalesce(hsp.POINTS_EARNED_NBR,0)),38,2) Points_Amount,
coalesce(hsp.POINTS_PROGRAM_NM,'') POINTS_PROGRAM_NM
from EPE_Redemption hs left join ` + EPE_TRANSACTION_HEADER_SAVING_POINTS_tbl + `  hsp on
hsp.TRANSACTION_INTEGRATION_ID=hs.TRANSACTION_INTEGRATION_ID and hsp.offer_id= hs.offer_id
Where (hsp.dw_current_version_ind=true OR hsp.dw_current_version_ind is NULL)
and (hsp.dw_logical_delete_ind =false OR hsp.dw_logical_delete_ind IS NULL )
group by Store_Nbr,Terminal_Nbr,Register_Transaction_Sequence_Nbr,Household_ID,Transaction_Time,EXTERNAL_OFFER_ID,hs.TRANSACTION_INTEGRATION_ID,REDEMPTION_TOTALCOLUMN,Redemption_Amount,POINTS_PROGRAM_NM
)
Select distinct Store_Nbr,
Terminal_Nbr,
Register_Transaction_Sequence_Nbr,
h.Household_ID,
REDEMPTION_TOTALCOLUMN,
Redemption_Amount,
Points_Amount,
POINTS_PROGRAM_NM,
h.Transaction_Time,
count(distinct c.OFFER_ID) Count_Clip,
h.EXTERNAL_OFFER_ID,
h.TRANSACTION_INTEGRATION_ID
from EPE_Points h  
left join ` + CLIP_HEADER_tbl + ` CH on CH.HOUSEHOLD_ID=h.HOUSEHOLD_ID
left join ` + CLIP_DETAILS_tbl + ` c on c.CLIP_SEQUENCE_ID = CH.CLIP_SEQUENCE_ID
group by  Store_Nbr,
Terminal_Nbr,
Register_Transaction_Sequence_Nbr,
h.Household_ID,
REDEMPTION_TOTALCOLUMN,
Redemption_Amount,
h.POINTS_PROGRAM_NM,
Transaction_Time,Points_Amount,
h.EXTERNAL_OFFER_ID,h.TRANSACTION_INTEGRATION_ID
)
)x join ` + OMS_OFFER_tbl + ` oms
on x.EXTERNAL_OFFER_ID= oms.EXTERNAL_OFFER_ID
and oms.dw_current_version_ind=true and oms.dw_logical_delete_ind =false
)
`;
try {
snowflake.execute (
{sqlText: cr_src_tmp_wrk_tbl  }
)
}
catch (err)  {
throw "Creation of OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY work table Failed with error: " + err;   // Return a error message.
}
var sql_begin = "BEGIN";
// Processing Inserts
var sql_inserts = `INSERT INTO ` + tgt_tbl + `
(STORE
,LANE
,TRANSACTION_NUMBER
,HHID
,REDEMPTION_COUNT
,REDEMPTION_AMOUNT
,POINTS_AMOUNT
,POINTS_PROGRAMNAME
,TRANSACTION_DATE
,OFFER_ID
,OFFER_START_DATE
,OFFER_END_DATE
,CLIPS_TOTALCOLUMN
,DW_CREATE_TS
)
SELECT
STORE,
LANE,
TRANSACTION_NUMBER,
HHID,
REDEMPTION_COUNT,
REDEMPTION_AMOUNT,
POINTS_AMOUNT,
POINTS_PROGRAMNAME,
TRANSACTION_DATE,
OFFER_ID,
OFFER_START_DATE,
OFFER_END_DATE,
CLIPS_TOTALCOLUMN,
DW_CREATE_TS
FROM ` + tgt_wrk_tbl + `;`;
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
throw "Loading of Fact_Offer_Request " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}
// **************        Load for OFFER_REDEMPTION_REPORTING_DETAILED_SUMMARY ENDs *****************
return "Done"

$$;
