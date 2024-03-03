--liquibase formatted sql
--changeset SYSTEM:SP_F_Grocery_Reward_Offer_Clips runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_F_GROCERY_REWARD_OFFER_CLIPS
("SRC_WRK_TBL" VARCHAR(16777216), "ANL_DB" VARCHAR(16777216), "ANL_SCHEMA" VARCHAR(16777216), "WRK_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
// ************** Load for F_Grocery_Reward_Offer_Clips_REPORT table BEGIN *****************

var src_wrk_tbl = SRC_WRK_TBL;
var anl_db = ANL_DB;
var anl_schema = ANL_SCHEMA;
var wrk_schema = WRK_SCHEMA;
		
var cnf_db = "EDM_CONFIRMED_PRD";
var cnf_schema_retail = "DW_C_RETAILSALE";
var loyalty_schema = "DW_C_LOYALTY";
var loc_schema = "DW_C_LOCATION";

var tgt_tbl = anl_db + "." + anl_schema + ".F_Grocery_Reward_Offer_Clips";
var tgt_wrk_tbl = anl_db + "." + wrk_schema + ".F_Grocery_Reward_Offer_Clips_Report_WRK";

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
var d1_retail_store_tbl = anl_db + "." + anl_schema + ".d1_retail_store";

var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
with cte as
(
select distinct case when Clip_Source_Cd in ('chkios','chkand','appan_','appios','emmd','11223_','mobile','app','appandroid','112233445566778000',
								'mobil_','mobile-android-shop','mobile-ios-shop') then 'UMA'
                                             when Clip_Source_Cd in ('emjou','chkweb','dlink','flipp','emju','web-p_','web-portal') then 'WEB' 
                                             when Clip_Source_Cd in ('OCGP-_','RXWA','United','q-app','239eb_','???') then 'Other'
                                             when Clip_Source_Cd in ('SVSC','SVCT','ccarw_','svct') then 'CCA' end as Platform
,case when Clip_Source_Cd in ('chkweb','chkios','chkan','web-p_','mobil_','web-portal','mobile-android-shop','mobile-ios-shop') then 'Cart' 
												else 'Gallery' end as Clip_Location_Source
,Clip_Type_Cd
,Fiscal_Day_Id as Clip_Day_Id
,nvl(bnr.BANNER_D1_SK,-1) as BANNER_D1_SK
,Clip_Dt
,ofr.offer_id
,ofr.Offer_D1_Sk as Offer_D1_Sk
,clip.CLIP_ID as clips_clip_id
,epe.CLIP_ID as epe_clip_id
from `+D1_offer_tbl+` ofr 
join `+CLIP_DETAILS_tbl+` clip on ofr.offer_id = clip.offer_id and ofr.dw_logical_delete_ind = false
join `+CLIP_HEADER_tbl+` ch on ch.CLIP_SEQUENCE_ID = clip.CLIP_SEQUENCE_ID and ch.dw_current_version_ind = true and ch.dw_logical_delete_ind = false

left join `+D1_BANNER_tbl+` bnr on replace(replace(replace(case when upper(bnr.banner_nm) like '%ANDRONIC%' THEN 'ANDRONICOS' ELSE 
								upper(bnr.banner_nm) END,'-',''),' ',''),'''','') = 
								replace(replace(replace(case when upper(ch.banner_nm) = 'ACMEMARKETS' then 'ACME' when 
								upper(ch.banner_nm) = 'KINGSFOODMARKETS' then 'KINGS' ELSE upper(ch.banner_nm) END,'-',''),' ',''),'''','')
								and bnr.dw_logical_delete_ind = false
								
left join `+D0_Fiscal_Day_tbl+` on CALENDAR_DT = Clip_Dt

left join (  
select distinct Hdr_Svn.offer_id as offer_id, Hdr_Svn.CLIP_ID as CLIP_ID from `+EPE_TRANSACTION_HEADER_tbl+` hdr
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
where clip.dw_current_version_ind = true and clip.dw_logical_delete_ind = false  
and clip.offer_id in (select distinct offer_id from `+ src_wrk_tbl +`)
)
,cte1 as
(
  select cte.*,Clip_D1_Sk
  from cte 
  left join `+D1_Clip_tbl+` on nvl(cte.Platform,'-1') = nvl(D1_Clip.CLIP_PLATFORM_CD,'-1') 
  and nvl(cte.Clip_Location_Source,'-1') = nvl(D1_Clip.CLIP_SOURCE_NM,'-1') and cte.CLIP_TYPE_CD = D1_Clip.CLIP_TYPE_CD
)
,Clip_Qty as
(
  select cte1.offer_id, cte1.Clip_D1_Sk, cte1.Clip_Day_Id, cte1.Banner_D1_Sk, count(distinct clips_clip_id) as Clip_Qty 
  from cte1 
  where Clip_Type_Cd = 'C' 
  group by cte1.offer_id, cte1.Clip_D1_Sk, cte1.Clip_Day_Id, cte1.Banner_D1_Sk
) 
,Clip_Redeemed_Qty as
(
  select offer_id, Clip_D1_Sk, Clip_Day_Id, Banner_D1_Sk, count(distinct epe_clip_id) as Clip_Redeemed_Qty
  from cte1 where Clip_Type_Cd = 'C' 
  group by offer_id, Clip_D1_Sk, Clip_Day_Id, Banner_D1_Sk
)

select src.Clip_D1_Sk
,src.Offer_D1_Sk
,src.Clip_Day_Id
,src.Banner_D1_Sk
,src.Clip_Dt
,src.Clip_Qty
,src.Clip_Redeemed_Qty
,case when tgt.Offer_D1_Sk is null and tgt.Clip_D1_Sk is null and tgt.Clip_Day_Id is null and tgt.Banner_D1_Sk is null then 'I' ELSE 'U' END as DML_Type
from (
select distinct 
		cte1.Clip_D1_Sk
		,cte1.Offer_D1_Sk
		,cte1.Clip_Day_Id
		,cte1.Banner_D1_Sk
		,Clip_Dt
		,Clip_Qty
		,Clip_Redeemed_Qty
from cte1
left join Clip_Qty on cte1.offer_id = Clip_Qty.offer_id and cte1.Clip_D1_Sk = Clip_Qty.Clip_D1_Sk 
									and cte1.Clip_Day_Id = Clip_Qty.Clip_Day_Id and cte1.Banner_D1_Sk = Clip_Qty.Banner_D1_Sk
left join Clip_Redeemed_Qty on cte1.offer_id = Clip_Redeemed_Qty.offer_id and cte1.Clip_D1_Sk = Clip_Redeemed_Qty.Clip_D1_Sk 
								and cte1.Clip_Day_Id = Clip_Redeemed_Qty.Clip_Day_Id and cte1.Banner_D1_Sk = Clip_Redeemed_Qty.Banner_D1_Sk
)src
left join 
(
	SELECT 
   	    Clip_D1_Sk
		,Offer_D1_Sk
		,Clip_Day_Id
		,Banner_D1_Sk
		,Clip_Dt
		,Clip_Qty
		,Clip_Redeemed_Qty
    FROM   ` + tgt_tbl + `
) tgt on src.Clip_D1_Sk = tgt.Clip_D1_Sk and src.Offer_D1_Sk = tgt.Offer_D1_Sk and src.Clip_Day_Id = tgt.Clip_Day_Id 
and src.Banner_D1_Sk = tgt.Banner_D1_Sk
where (tgt.Clip_D1_Sk is null and tgt.Offer_D1_Sk is null and tgt.Clip_Day_Id is null and tgt.Banner_D1_Sk is null) OR
(
nvl(tgt.Clip_Dt, '9999-12-31') <> nvl(src.Clip_Dt, '9999-12-31') OR
nvl(tgt.Clip_Qty, '-1') <> nvl(src.Clip_Qty, '-1') OR
nvl(tgt.Clip_Redeemed_Qty, '-1') <> nvl(src.Clip_Redeemed_Qty, '-1') 
)
`;

try {
      snowflake.execute (
          {sqlText: cr_src_wrk_tbl  }
      )
  }
  catch (err)  {
      return "Creation of F_Grocery_Reward_Offer_Clips_Report_WRK table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
      }

var sql_updates = // Processing Updates of Type 1 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET    
						 Clip_Dt           = src.Clip_Dt 
						,Clip_Qty          = src.Clip_Qty
						,Clip_Redeemed_Qty = src.Clip_Redeemed_Qty
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   
									 Clip_D1_Sk
									,Offer_D1_Sk
									,Clip_Day_Id
									,Banner_D1_Sk
									,Clip_Dt
									,Clip_Qty
									,Clip_Redeemed_Qty                           
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Clip_D1_Sk = tgt.Clip_D1_Sk and src.Offer_D1_Sk = tgt.Offer_D1_Sk and src.Clip_Day_Id = tgt.Clip_Day_Id
							and src.Banner_D1_Sk = tgt.Banner_D1_Sk`;  


var sql_begin = "BEGIN";

// Processing Inserts

var sql_inserts = `INSERT INTO ` + tgt_tbl + `
(
Clip_D1_Sk
,Offer_D1_Sk
,Clip_Day_Id
,Banner_D1_Sk
,Clip_Dt
,Clip_Qty
,Clip_Redeemed_Qty 
,DW_CREATE_TS
,Dw_Last_Update_Ts
)
select distinct Clip_D1_Sk
,Offer_D1_Sk
,Clip_Day_Id
,Banner_D1_Sk
,Clip_Dt
,Clip_Qty
,Clip_Redeemed_Qty 
,current_timestamp() AS DW_CREATE_TS 
,'9999-12-31 00:00:00.000 -0600' AS Dw_Last_Update_Ts
from `+ tgt_wrk_tbl +`
WHERE DML_Type = 'I'
AND Clip_D1_Sk IS NOT NULL
AND Offer_D1_Sk IS NOT NULL
AND Clip_Day_Id IS NOT NULL
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
return "Loading of F_Grocery_Reward_Offer_Clips_Report " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
}

// **************        Load for F_Grocery_Reward_Offer_Clips_REPORT ENDs *****************
return "Done"
$$;
