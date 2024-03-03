--liquibase formatted sql
--changeset SYSTEM:SP_F_Grocery_Reward_Offer_Redemption runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_F_GROCERY_REWARD_OFFER_REDEMPTION
("SRC_WRK_TBL" VARCHAR(16777216), "ANL_DB" VARCHAR(16777216), "ANL_SCHEMA" VARCHAR(16777216), "WRK_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

// ************** Load for F_Grocery_Reward_Offer_Redemption_REPORT table BEGIN *****************

var src_wrk_tbl = SRC_WRK_TBL;
var anl_db = ANL_DB;
var anl_schema = ANL_SCHEMA;
var wrk_schema = WRK_SCHEMA;
		
var cnf_db = "EDM_CONFIRMED_PRD";
var cnf_schema_retail = "DW_C_RETAILSALE";
var loyalty_schema = "DW_C_LOYALTY";
var loc_schema = "DW_C_LOCATION";

var tgt_tbl = anl_db + "." + anl_schema + ".F_Grocery_Reward_Offer_Redemption";
var tgt_wrk_tbl = anl_db + "." + wrk_schema + ".F_Grocery_Reward_Offer_Redemption_Report_WRK";

var EPE_TRANSACTION_HEADER_tbl = cnf_db + "." + cnf_schema_retail + ".EPE_TRANSACTION_HEADER";
var Epe_Transaction_header_Saving_Clips_tbl = cnf_db + "." + cnf_schema_retail + ".Epe_Transaction_header_Saving_Clips";
var Epe_Transaction_Item_Saving_Clips_tbl = cnf_db + "." + cnf_schema_retail +".Epe_Transaction_Item_Saving_Clips";
var CLIP_HEADER_tbl = cnf_db + "." + loyalty_schema + ".CLIP_HEADER";
var CLIP_DETAILS_tbl = cnf_db + "." + loyalty_schema + ".CLIP_DETAILS";
var D1_BANNER_tbl = anl_db + "." + anl_schema + ".D1_BANNER";
var D0_Fiscal_Day_tbl = anl_db + "." + anl_schema + ".D0_Fiscal_Day";
var D1_offer_tbl = anl_db + "." + anl_schema + ".D1_offer";
var D1_Clip_tbl = anl_db + "." + anl_schema + ".D1_Clip";
var RETAIL_STORE_tbl = cnf_db + "." + loc_schema + ".RETAIL_STORE";
var D1_retail_customer_household_tbl = anl_db + "." + anl_schema + ".D1_retail_customer_household";
var d1_retail_store_tbl = anl_db + "." + anl_schema + ".d1_retail_store";

var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
with cte AS
(
select distinct epe.transaction_id::number as transaction_id
,fis.Fiscal_Day_ID as transaction_day_id
,ofr.offer_d1_sk as offer_d1_sk
,rch.Retail_Customer_Household_D1_Sk as Retail_Customer_Household_D1_Sk
,nvl(bnr.Banner_D1_Sk,-1) as Banner_D1_Sk
,epe.household_id as household_id
,date(epe.TRANSACTION_TS) as Transaction_Dt
,case when Clip_Source_Cd in ('chkios','chkand','appan_','appios','emmd','11223_','mobile','app','appandroid','112233445566778000',
										'mobil_','mobile-android-shop','mobile-ios-shop') then 'UMA'
                                             when Clip_Source_Cd in ('emjou','chkweb','dlink','flipp','emju','web-p_','web-portal') then 'WEB' 
                                             when Clip_Source_Cd in ('OCGP-_','RXWA','United','q-app','239eb_','???') then 'Other'
                                             when Clip_Source_Cd in ('SVSC','SVCT','ccarw_','svct') then 'CCA' end as Platform
,case when Clip_Source_Cd in ('chkweb','chkios','chkan','web-p_','mobil_','web-portal','mobile-android-shop','mobile-ios-shop') then 'Cart' 
											else 'Gallery' end as Clip_Location_Source
,epe.offer_id
,CLIP_TYPE_CD
,epe.CLIP_ID
from `+D1_offer_tbl+` ofr 
join `+CLIP_DETAILS_tbl+` clip on ofr.offer_id = clip.offer_id and ofr.dw_logical_delete_ind = false
join `+CLIP_HEADER_tbl+` ch on ch.CLIP_SEQUENCE_ID = clip.CLIP_SEQUENCE_ID and ch.dw_current_version_ind = true 
and ch.dw_logical_delete_ind = false  
left join `+D1_BANNER_tbl+` bnr on replace(replace(replace(case when upper(bnr.banner_nm) like '%ANDRONIC%' THEN 'ANDRONICOS' ELSE 
								upper(bnr.banner_nm) END,'-',''),' ',''),'''','') = 
								replace(replace(replace(case when upper(ch.banner_nm) = 'ACMEMARKETS' then 'ACME' when 
								upper(ch.banner_nm) = 'KINGSFOODMARKETS' then 'KINGS' ELSE upper(ch.banner_nm) END,'-',''),' ',''),'''','')
								and bnr.dw_logical_delete_ind = false
left join (
  
select distinct hdr.household_id, nvl(try_to_numeric(HDR.ORDER_ID), hdr.TRANSACTION_INTEGRATION_ID) AS transaction_id, 
hdr.TRANSACTION_TS, Hdr_Svn.offer_id as offer_id, Hdr_Svn.CLIP_ID as CLIP_ID 
from `+EPE_TRANSACTION_HEADER_tbl+` hdr
left join 
(select transaction_integration_id,Offer_Id,a.value::string as CLIP_ID from `+Epe_Transaction_header_Saving_Clips_tbl+`   
  ,LATERAL FLATTEN(input => case when CLIP_ID like '%[%]%' then parse_json(CLIP_ID) else to_array(CLIP_ID) end , outer => TRUE) as a 
 where offer_id in (select distinct offer_id from `+ src_wrk_tbl +`) and dw_current_version_ind = true and dw_logical_delete_ind = false
 union ALL
 select transaction_integration_id,Offer_Id,a.value::string as CLIP_ID from `+Epe_Transaction_Item_Saving_Clips_tbl+`   
  ,LATERAL FLATTEN(input => case when CLIP_ID like '%[%]%' then parse_json(CLIP_ID) else to_array(CLIP_ID) end , outer => TRUE) as a 
 where offer_id in (select distinct offer_id from `+ src_wrk_tbl +`) and dw_current_version_ind = true and dw_logical_delete_ind = false
 )Hdr_Svn
 on Hdr_Svn.transaction_integration_id = hdr.transaction_integration_id
where hdr.dw_current_version_ind = true and hdr.dw_logical_delete_ind = false
and hdr.STATUS_CD = 'COMPLETED'
) epe on epe.offer_id = clip.offer_id and epe.CLIP_ID = clip.CLIP_ID
left join `+D0_Fiscal_Day_tbl+` fis on fis.CALENDAR_DT = date(epe.TRANSACTION_TS) 
left join 

	( select HOUSEHOLD_ID, RETAIL_CUSTOMER_HOUSEHOLD_D1_SK, row_number() over(partition by HOUSEHOLD_ID order by dw_last_update_ts desc) as  rn
		from `+D1_retail_customer_household_tbl+` rch where dw_logical_delete_ind = false qualify rn=1 ) rch 
	on epe.household_id = rch.HOUSEHOLD_ID 
where clip.dw_current_version_ind = true and clip.dw_logical_delete_ind = false  
and epe.offer_id in (select distinct offer_id from `+ src_wrk_tbl +`)
)
,cte1 as
(
select cte.*,Clip_D1_Sk
  from cte 
  left join `+D1_Clip_tbl+` on nvl(cte.Platform,'-1') = nvl(D1_Clip.CLIP_PLATFORM_CD,'-1') 
  and nvl(cte.Clip_Location_Source,'-1') = nvl(D1_Clip.CLIP_SOURCE_NM,'-1') and cte.CLIP_TYPE_CD = D1_Clip.CLIP_TYPE_CD
) 
,Redemption_Qty as
(
select Transaction_Id, Transaction_Day_Id, Clip_D1_Sk, offer_d1_sk, Banner_D1_Sk, count(distinct clip_id) as Redemption_Qty
  from cte1 where Clip_Type_Cd = 'C' 
  group by Transaction_Id, Transaction_Day_Id, Clip_D1_Sk, offer_d1_sk, Banner_D1_Sk
)
select src.transaction_id
,src.transaction_day_id
,src.offer_d1_sk
,src.Retail_Customer_Household_D1_Sk
,src.Banner_D1_Sk
,src.household_id
,src.Transaction_Dt
,src.Clip_D1_Sk
,src.Redemption_Qty
,case when tgt.Transaction_Id is null and tgt.Transaction_Day_Id is null and tgt.Offer_D1_Sk is null and tgt.Clip_D1_Sk is null 
							and tgt.Banner_D1_Sk is null
							then 'I' ELSE 'U' END as DML_Type
from (
select distinct cte1.transaction_id
			   ,cte1.transaction_day_id
			   ,cte1.offer_d1_sk
			   ,Retail_Customer_Household_D1_Sk
			   ,cte1.Banner_D1_Sk
			   ,cte1.household_id
			   ,Transaction_Dt
			   ,cte1.Clip_D1_Sk
			   ,Redemption_Qty
from cte1
left join Redemption_Qty on cte1.Transaction_Id = Redemption_Qty.Transaction_Id
and cte1.Transaction_Day_Id = Redemption_Qty.Transaction_Day_Id
and cte1.Clip_D1_Sk = Redemption_Qty.Clip_D1_Sk
and cte1.offer_d1_sk = Redemption_Qty.offer_d1_sk
and cte1.Banner_D1_Sk = Redemption_Qty.Banner_D1_Sk

)src
left join 
(
	SELECT 
   	     transaction_id
		,transaction_day_id
		,offer_d1_sk
		,Retail_Customer_Household_D1_Sk
		,Banner_D1_Sk
		,household_id
		,Transaction_Dt
		,Clip_D1_Sk
		,Redemption_Qty
    FROM   ` + tgt_tbl + `
) tgt on src.transaction_id = tgt.transaction_id and src.transaction_day_id = tgt.transaction_day_id and src.offer_d1_sk = tgt.offer_d1_sk
         and src.Clip_D1_Sk = tgt.Clip_D1_Sk and src.Banner_D1_Sk = tgt.Banner_D1_Sk
where (tgt.Clip_D1_Sk is null and tgt.Offer_D1_Sk is null and tgt.transaction_day_id is null and tgt.transaction_id is null and tgt.Banner_D1_Sk
		is null) OR
(
nvl(tgt.Transaction_Dt, '9999-12-31') <> nvl(src.Transaction_Dt, '9999-12-31') OR
nvl(tgt.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK, '-1') <> nvl(src.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK, '-1') OR
nvl(tgt.HOUSEHOLD_ID, '-1') <> nvl(src.HOUSEHOLD_ID,'-1') OR
nvl(tgt.Redemption_Qty, '-1') <> nvl(src.Redemption_Qty, '-1') 
)
`;

try {
      snowflake.execute (
          {sqlText: cr_src_wrk_tbl  }
      )
  }
  catch (err)  {
      return "Creation of F_Grocery_Reward_Offer_Redemption_Report_WRK table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
      }

var sql_updates = // Processing Updates of Type 1 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET    
						 Transaction_Dt           		 = src.Transaction_Dt 
					    ,Redemption_Qty     			 = src.Redemption_Qty
						,HOUSEHOLD_ID      				 = src.HOUSEHOLD_ID
						,RETAIL_CUSTOMER_HOUSEHOLD_D1_SK = src.RETAIL_CUSTOMER_HOUSEHOLD_D1_SK
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   
									 transaction_id
									,transaction_day_id
									,offer_d1_sk
									,Retail_Customer_Household_D1_Sk
									,Banner_D1_Sk
									,household_id
									,Transaction_Dt
									,Clip_D1_Sk
									,Redemption_Qty                           
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Clip_D1_Sk = tgt.Clip_D1_Sk and src.Offer_D1_Sk = tgt.Offer_D1_Sk 
							and src.transaction_day_id = tgt.transaction_day_id
							and src.transaction_id = tgt.transaction_id
							and src.Banner_D1_Sk = tgt.Banner_D1_Sk`;  


var sql_begin = "BEGIN";

// Processing Inserts

var sql_inserts = `INSERT INTO ` + tgt_tbl + `
(
 Transaction_Id
,Transaction_Day_Id
,Offer_D1_Sk
,Clip_D1_Sk
,Retail_Customer_Household_D1_Sk
,Banner_D1_Sk
,Household_Id
,Transaction_Dt
,Redemption_Qty
,DW_CREATE_TS
,Dw_Last_Update_Ts
)
select distinct Transaction_Id
,Transaction_Day_Id
,Offer_D1_Sk
,Clip_D1_Sk
,Retail_Customer_Household_D1_Sk
,Banner_D1_Sk
,Household_Id
,Transaction_Dt
,Redemption_Qty
,current_timestamp() AS DW_CREATE_TS 
,'9999-12-31 00:00:00.000 -0600' AS Dw_Last_Update_Ts
from `+ tgt_wrk_tbl +`
WHERE DML_Type = 'I'
AND Transaction_Id IS NOT NULL
AND Transaction_Day_Id IS NOT NULL 
AND Offer_D1_Sk IS NOT NULL
AND Clip_D1_Sk IS NOT NULL
AND Banner_D1_Sk IS NOT NULL`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"

try {
            snowflake.execute (
                {sqlText: sql_begin}
            );
			snowflake.execute (
				{sqlText: sql_updates}
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
return "Loading of F_Grocery_Reward_Offer_Redemption_Report " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}

// **************        Load for F_Grocery_Reward_Offer_Redemption_REPORT ENDs *****************
return "Done"
$$;
