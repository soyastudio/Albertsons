--liquibase formatted sql
--changeset SYSTEM:GRreport runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view F_Grocery_Reward_Offer_Clips
AS
select * from EDM_ANALYTICS_PRD.dw_reference.F_Grocery_Reward_Offer_Clips;

create or replace view D1_Clip
AS
select * from EDM_ANALYTICS_PRD.dw_reference.D1_Clip;

create or replace view D1_Offer
AS
select * from EDM_ANALYTICS_PRD.dw_reference.D1_Offer;

create or replace view Fact_Grocery_Reward_Offer_Clips_WOD
as
select distinct clip.* from EDM_ANALYTICS_PRD.dw_reference.F_Grocery_Reward_Offer_Clips clip
left join  EDM_ANALYTICS_PRD.dw_reference.d1_offer ofr
on clip.OFFER_D1_SK = ofr.OFFER_D1_SK
where ofr.OFFER_TYPE_CD = 'WOD_OR_POD';

create or replace view F_Grocery_Reward_Offer_Redemption
AS
select * from EDM_ANALYTICS_PRD.dw_reference.F_Grocery_Reward_Offer_Redemption;
