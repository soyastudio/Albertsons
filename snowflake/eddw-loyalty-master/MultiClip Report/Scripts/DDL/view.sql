--liquibase formatted sql
--changeset SYSTEM:FACT_MULTICLIP runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view FACT_MULTICLIP(
	HHID,
	BANNER,
	PLATFORM_WEB_UMA,
	CLIP_LOCATION_SOURCE_CART_GALLERY,
	OFFER_ID,
	OFFER_DESCRIPTION,
	OFFER_TYPE,
	DISCOUNT_TYPE,
	LIMIT_TYPE,
	MULTICLIP_LIMIT,
	REQUIRED_REWARDS,
	OFFER_$_VALUE,
	TTL_UNCLAIM_QTY_UNCLIP,
	CLIP_QUANTITY,
	CLIP_DATE,
	TTL_REDEEMED_OFFERS_QTY,
	TTL_REDEEMED_OFFER_$_VALUE
) as
with cte as 
(
select HHID
,Banner
,case when Clip_Source_Cd in ('chkios','chkand','appan_','appios','emmd','11223_','mobile','app','appandroid','112233445566778000',
									'mobil_','mobile-android-shop','mobile-ios-shop') then 'UMA'
                                             when Clip_Source_Cd in ('emjou','chkweb','dlink','flipp','emju','web-p_','web-portal') then 'WEB' 
                                             when Clip_Source_Cd in ('OCGP-_','RXWA','United','q-app','239eb_','???') then 'Other'
                                             when Clip_Source_Cd in ('SVSC','SVCT','ccarw_','svct') then 'CCA' end as Platform_Web_UMA
,case when Clip_Source_Cd in ('chkweb','chkios','chkan','web-p_','mobil_','web-portal','mobile-android-shop','mobile-ios-shop') then 'Cart' 
										else 'Gallery' end as Clip_Location_Source_Cart_Gallery
,Offer_ID
,Clip_Type_Cd
,Offer_Description
,Offer_Type
,DISCOUNT_TYPE
,Limit_Type
,MultiClip_Limit
,Required_Rewards
,Offer_$_Value
,CLIP_ID
,Clip_Date
from EDM_CONFIRMED_PRD.dw_c_product.F_Multiclip
)
,Unclip_qty as 
(
select HHID,Offer_ID, count(*) as Ttl_Unclaim_Qty_Unclip
  from cte where Clip_Type_Cd = 'U'
  group by HHID,Offer_ID
)
,Clip_qty as
(
  select HHID,Offer_ID, count(*) as Clip_Quantity
  from cte where Clip_Type_Cd = 'C'
  group by HHID,Offer_ID
)
,Ttl_Redeemed as 
(
  select HHID,Offer_ID, count(distinct clip_id) as Ttl_Redeemed_Offers_Qty
  from cte 
  group by HHID,Offer_ID
)
,Final_Result as
(
select distinct cte.HHID
,Banner
,Platform_Web_UMA
,Clip_Location_Source_Cart_Gallery
,cte.Offer_ID
,Offer_Description
,Offer_Type
,DISCOUNT_TYPE
,Limit_Type
,MultiClip_Limit
,Required_Rewards
,Offer_$_Value
,Unclip_qty.Ttl_Unclaim_Qty_Unclip
,Clip_qty.Clip_Quantity
,Clip_Date
,Ttl_Redeemed.Ttl_Redeemed_Offers_Qty
,Ttl_Redeemed.Ttl_Redeemed_Offers_Qty * nvl(Offer_$_Value,0) as Ttl_Redeemed_Offer_$_Value
from cte 
left join Unclip_qty on cte.HHID = Unclip_qty.HHID and cte.Offer_ID = Unclip_qty.Offer_ID left join Clip_qty on cte.HHID = Clip_qty.HHID and cte.Offer_ID = Clip_qty.Offer_ID left join Ttl_Redeemed on cte.HHID = Ttl_Redeemed.HHID and cte.Offer_ID = Ttl_Redeemed.Offer_ID
) 
select * from Final_Result where Clip_Quantity>1;
