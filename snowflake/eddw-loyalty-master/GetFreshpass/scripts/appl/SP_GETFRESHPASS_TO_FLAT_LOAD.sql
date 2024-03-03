--liquibase formatted sql
--changeset SYSTEM:SP_GETFRESHPASS_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;
 
 
// ********************R PIPELINE***************************
 
 
//Stored Procedure to load in Flat table
CREATE OR REPLACE PROCEDURE sp_GetFreshpass_To_FLAT_load()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 	
$$	
var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1); 
	env = env.split('_'); 
	env = env[env.length - 1]; 
	var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
	var env_schema_nm = 'DW_R_MASTERDATA'; 
	var env_db_nm = `EDM_REFINED_${env}`;
 
	try { 
    var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
    var metaparams = {};
    while (rs.next()){
      metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
    }
    var ref_db = metaparams['REF_DB']; 
    var ref_schema = metaparams['R_LOYAL']; 
    var app_schema = metaparams['APPL']; 
    var wrk_schema = metaparams['R_STAGE']; 
	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_Freshpass';
	var bod_nm = 'GetFreshpass';
    var src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} as
                            select * from ${src_tbl} where METADATA$ACTION ='INSERT'
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
    } catch (err) { 
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
							            SELECT * FROM ${src_wrk_tbl}`;
	var insert_into_flat_dml =`INSERT INTO ${tgt_flat_tbl}
WITH LVL_1_FLATTEN_EP  as
(
  select 
    src.SRC_JSON:"@"::string AS BODNm
   ,src.FILENAME AS FILENAME
   ,'Effective' as Subscription_paln_type
   ,src.src_json:banner::string as banner
   ,src.src_json:eventId::string as eventId
   ,src.src_json:eventTime::string as eventTime
   ,src.src_json:eventType::string as eventType
   ,src.src_json:gid::string as gid
   ,src.src_json:reenrolledUser::string as reenrolledUser
   ,src.src_json:source::string as source  
   ,src.src_json:uuid::string as Customer_UUID 
   ,src.src_json:benefits::array as benefits  
   ,benefits_values.value::string as benefits_values
   ,src.src_json:effectivePlan:currency::string as currency
   ,src.src_json:effectivePlan:discountType::string as discountType    
   ,src.src_json:effectivePlan:deliveryFeeWaived::string as deliveryFeeWaived
   ,src.src_json:effectivePlan:discountDuration::string as discountDuration
   ,src.src_json:effectivePlan:enrollmentDate::string as enrollmentDate
   ,src.src_json:effectivePlan:expiryDate::string as expiryDate
   ,src.src_json:effectivePlan:fee::string as fee
   ,src.src_json:effectivePlan:fuelSurchargeWaived::string as fuelSurchargeWaived
   ,src.src_json:effectivePlan:isDiscountedPrice::string as isDiscountedPrice
   ,src.src_json:effectivePlan:regularPlanPrice::string as regularPlanPrice
   ,src.src_json:effectivePlan:renewalDate::string as renewalDate
   ,src.src_json:effectivePlan:serviceFeeWaived::string as serviceFeeWaived
   ,src.src_json:effectivePlan:signupDate::string as signupDate
   ,src.src_json:effectivePlan:discountEndDate::string as discountEndDate
   ,src.src_json:effectivePlan:status::string as status
   ,src.src_json:effectivePlan:subscriptionCode::string as subscriptionCode
   ,src.src_json:effectivePlan:subscriptionType::string as subscriptionType
   ,src.src_json:effectivePlan:cancellationReason::string as cancellationReason
   ,src.src_json:effectivePlan:taxAmount::string as taxAmount
   ,src.src_json:effectivePlan:totalCharges::string as totalCharges
   ,src.src_json:effectivePlan:trialDuration::string as trialDuration
   ,src.src_json:effectivePlan:campaignEndDate::string as campaignenddate
   ,src.src_json:effectivePlan:campaignStartDate::string as campaignstartdate
   ,src.src_json:effectivePlan:isExtendedTrialDuration::string as isextendedtrialduration
   ,src.src_json:tender:maskedNumber::string as maskedNumber 
   ,src.src_json:tender:paymentDate::string as paymentDate 
   ,src.src_json:tender:type::string as type 
   ,src.src_json:cancellationGracePeriod::string as cancellationGracePeriod
   ,src.src_json:hhid::string as hhid
   ,src.src_json:orderCount::string as orderCount
   ,src.src_json:cycleSavings.deliverySavings::string as CycleSavings_DeliverySavings
   ,src.src_json:cycleSavings.perkSavings::string as CycleSavings_PerkSavings
   ,src.src_json:cycleSavings.totalSavings::string as CycleSavings_TotalSavings
   ,src.src_json:lifeSavings.deliverySavings::string as LifeSavings_DeliverySavings
   ,src.src_json:lifeSavings.perkSavings::string as LifeSavings_PerkSavings
   ,src.src_json:lifeSavings.totalSavings::string as LifeSavings_TotalSavings
   ,src.src_json:userComment::string as usercomment
   ,src.src_json:sourceClient::string as Source_Channel_Nm
   ,src.src_json:cycleSavings.deliveryOrderCount::string as Cycle_Delivery_Order_Cnt
   ,src.src_json:cycleSavings.dugOrderCount::string as Cycle_Dug_Order_Cnt
   ,src.src_json:cycleSavings.rewardsEarned::string as Cycle_Rewards_Earned_Qty
   ,src.src_json:lifeSavings.deliveryOrderCount::string as Life_Delivery_Order_Cnt
   ,src.src_json:lifeSavings.dugOrderCount::string as Life_Dug_Order_Cnt
   ,src.src_json:lifeSavings.rewardsEarned::string as Life_Rewards_Earned_Qty
   ,src.src_json:lifeSavings.rewardPoints::string as Life_Reward_Points_Qty
   ,src.src_json:cycleSavings.rewardPoints::string as Cycle_Reward_Points_Qty
   ,src.src_json:effectivePlan:paymentTransactionId::string as paymentTransactionId
 
  from ${src_wrk_tbl} src,lateral flatten(input => src.src_json:benefits,outer=>true) as benefits_values
),
LVL_1_FLATTEN_P  as
(
  select 
    src.SRC_JSON:"@"::string AS BODNm
   ,src.FILENAME AS FILENAME
   ,'Pending' as Subscription_paln_type
   ,src.src_json:banner::string as banner
   ,src.src_json:eventId::string as eventId
   ,src.src_json:eventTime::string as eventTime
   ,src.src_json:eventType::string as eventType
   ,src.src_json:gid::string as gid
   ,src.src_json:reenrolledUser::string as reenrolledUser
   ,src.src_json:source::string as source  
   ,src.src_json:uuid::string as Customer_UUID 
   ,src.src_json:benefits::array as benefits  
   ,benefits_values.value::string as benefits_values
   ,src.src_json:pendingPlan:currency::string as currency
   ,src.src_json:pendingPlan:discountType::string as discountType    
   ,src.src_json:pendingPlan:deliveryFeeWaived::string as deliveryFeeWaived
   ,src.src_json:pendingPlan:discountDuration::string as discountDuration
   ,src.src_json:pendingPlan:enrollmentDate::string as enrollmentDate
   ,src.src_json:pendingPlan:expiryDate::string as expiryDate
   ,src.src_json:pendingPlan:fee::string as fee
   ,src.src_json:pendingPlan:fuelSurchargeWaived::string as fuelSurchargeWaived
   ,src.src_json:pendingPlan:isDiscountedPrice::string as isDiscountedPrice
   ,src.src_json:pendingPlan:regularPlanPrice::string as regularPlanPrice
   ,src.src_json:pendingPlan:renewalDate::string as renewalDate
   ,src.src_json:pendingPlan:serviceFeeWaived::string as serviceFeeWaived
   ,src.src_json:pendingPlan:signupDate::string as signupDate
   ,src.src_json:pendingPlan:discountEndDate::string as discountEndDate
   ,src.src_json:pendingPlan:status::string as status
   ,src.src_json:pendingPlan:subscriptionCode::string as subscriptionCode
   ,src.src_json:pendingPlan:subscriptionType::string as subscriptionType
   ,src.src_json:pendingPlan:cancellationReason::string as cancellationReason
   ,src.src_json:pendingPlan:taxAmount::string as taxAmount
   ,src.src_json:pendingPlan:totalCharges::string as totalCharges
   ,src.src_json:pendingPlan:trialDuration::string as trialDuration
   ,src.src_json:pendingPlan:campaignEndDate::string as campaignenddate
   ,src.src_json:pendingPlan:campaignStartDate::string as campaignstartdate
   ,src.src_json:pendingPlan:isExtendedTrialDuration::string as isextendedtrialduration
   ,src.src_json:tender:maskedNumber::string as maskedNumber 
   ,src.src_json:tender:paymentDate::string as paymentDate 
   ,src.src_json:tender:type::string as type 
   ,src.src_json:cancellationGracePeriod::string as cancellationGracePeriod
   ,src.src_json:hhid::string as hhid
   ,src.src_json:orderCount::string as orderCount
   ,src.src_json:CycleSavings.deliverySavings::string as CycleSavings_DeliverySavings
   ,src.src_json:CycleSavings.perkSavings::string as CycleSavings_PerkSavings
   ,src.src_json:CycleSavings.totalSavings::string as CycleSavings_TotalSavings
   ,src.src_json:lifeSavings.deliverySavings::string as LifeSavings_DeliverySavings
   ,src.src_json:lifeSavings.perkSavings::string as LifeSavings_PerkSavings
   ,src.src_json:lifeSavings.totalSavings::string as LifeSavings_TotalSavings
   ,src.src_json:userComment::string as usercomment
   ,src.src_json:sourceClient::string as Source_Channel_Nm
   ,src.src_json:cycleSavings.deliveryOrderCount::string as Cycle_Delivery_Order_Cnt
   ,src.src_json:cycleSavings.dugOrderCount::string as Cycle_Dug_Order_Cnt
   ,src.src_json:cycleSavings.rewardsEarned::string as Cycle_Rewards_Earned_Qty
   ,src.src_json:lifeSavings.deliveryOrderCount::string as Life_Delivery_Order_Cnt
   ,src.src_json:lifeSavings.dugOrderCount::string as Life_Dug_Order_Cnt
   ,src.src_json:lifeSavings.rewardsEarned::string as Life_Rewards_Earned_Qty
   ,src.src_json:lifeSavings.rewardPoints::string as Life_Reward_Points_Qty
   ,src.src_json:cycleSavings.rewardPoints::string as Cycle_Reward_Points_Qty
   ,src.src_json:pendingPlan:paymentTransactionId::string as paymentTransactionId
  from ${src_wrk_tbl} src,lateral flatten(input => src.src_json:benefits,outer=>true) as benefits_values
)
SELECT * FROM (SELECT FILENAME,BODNm,Subscription_paln_type,reenrolledUser,signupDate,discountEndDate,regularPlanPrice,fee,enrollmentDate,fuelSurchargeWaived,renewalDate,isDiscountedPrice,serviceFeeWaived,lower(subscriptionCode) subscriptionCode,expiryDate,deliveryFeeWaived,trialDuration,subscriptionType,cancellationReason,discountDuration,totalCharges,currency,discountType,taxAmount,status,maskedNumber,type,paymentDate,eventId,gid,eventTime,banner,eventType,source,Customer_UUID
,benefits_values,cancellationGracePeriod,campaignenddate,campaignstartdate,isextendedtrialduration,hhid,orderCount,CycleSavings_DeliverySavings,CycleSavings_PerkSavings,CycleSavings_TotalSavings,LifeSavings_DeliverySavings,LifeSavings_PerkSavings,LifeSavings_TotalSavings,usercomment
,current_timestamp() as DW_CREATE_TS,Source_Channel_Nm,Cycle_Delivery_Order_Cnt,Cycle_Dug_Order_Cnt,Cycle_Rewards_Earned_Qty,Life_Delivery_Order_Cnt,Life_Dug_Order_Cnt,Life_Rewards_Earned_Qty,Life_Reward_Points_Qty,Cycle_Reward_Points_Qty,paymentTransactionId
from LVL_1_FLATTEN_EP
UNION ALL
SELECT FILENAME,BODNm,Subscription_paln_type,reenrolledUser,signupDate,discountEndDate,regularPlanPrice,fee,enrollmentDate,fuelSurchargeWaived,renewalDate,isDiscountedPrice,serviceFeeWaived,lower(subscriptionCode) subscriptionCode,expiryDate,deliveryFeeWaived,trialDuration,subscriptionType,cancellationReason,discountDuration,totalCharges,currency,discountType,taxAmount,status,maskedNumber,type,paymentDate,eventId,gid,eventTime,banner,eventType,source,Customer_UUID
,benefits_values,cancellationGracePeriod,campaignenddate,campaignstartdate,isextendedtrialduration,hhid,orderCount,CycleSavings_DeliverySavings,CycleSavings_PerkSavings,CycleSavings_TotalSavings,LifeSavings_DeliverySavings,LifeSavings_PerkSavings,LifeSavings_TotalSavings,usercomment
,current_timestamp() as DW_CREATE_TS,Source_Channel_Nm,Cycle_Delivery_Order_Cnt,Cycle_Dug_Order_Cnt,Cycle_Rewards_Earned_Qty,Life_Delivery_Order_Cnt,Life_Dug_Order_Cnt,Life_Rewards_Earned_Qty,Life_Reward_Points_Qty,Cycle_Reward_Points_Qty,paymentTransactionId
from LVL_1_FLATTEN_P)
where
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(NVL(nvl(nvl(nvl(status,signupDate),discountEndDate),regularPlanPrice),fee),enrollmentDate),fuelSurchargeWaived),renewalDate),isDiscountedPrice),serviceFeeWaived),subscriptionCode)
,expiryDate),deliveryFeeWaived),trialDuration),subscriptionType),cancellationReason),discountDuration),totalCharges),currency),discountType),taxAmount),campaignenddate),campaignstartdate),isextendedtrialduration),paymentTransactionId) is not null;`
 
try {
    snowflake.execute({ sqlText: insert_into_flat_dml});
}
catch (err)  { 
    snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
    throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
}
$$;
