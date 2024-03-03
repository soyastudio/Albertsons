--liquibase formatted sql
--changeset SYSTEM:SP_JSON_EPE_OMS_OFFER runOnChange:true splitStatements:false OBJECT_TYPE:SP



USE DATABASE <<EDM_DB_NAME_OUT>>;
USE SCHEMA DW_APPL;


  Create or replace transient table  EDM_CONFIRMED_OUT_PRD.dw_stage.EPE_OMS_OFFER_JSON_TEMP
as Select *,md5(payload) as payload_md5 from  EDM_CONFIRMED_OUT_PRD.dw_dcat.epe_oms_offer_json where 1=2;

Create or replace transient table  EDM_CONFIRMED_OUT_PRD.dw_stage.EPE_OMS_OFFER_JSON_EXCEPTION
as Select *,'Offer already exists in the table' as Reason from  EDM_CONFIRMED_OUT_PRD.dw_dcat.epe_oms_offer_json where 1=2;

CREATE OR REPLACE PROCEDURE SP_JSON_EPE_OMS_OFFER()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS  
$$
      
    // Globa Variables
    var cnf_out_db = "EDM_CONFIRMED_OUT_PRD";
    var wrk_schema = "DW_STAGE";
    var cnf_out_schema = "DW_DCAT";
    var src_db = "EDM_CONFIRMED_PRD";
    var src_ref_db = "EDM_REFINED_PRD";
     var src_ref_schema = "DW_R_PRODUCT";
    var appl_db="DW_APPL";
    var src_schema = "DW_C_PRODUCT";
    var src_Loc = "DW_C_LOCATION";
    var src_tbl = src_db + "." + appl_db + ".OFFEROMS_EPE_FLAT_R_STREAM";
    //var src_tbl = src_ref_db + "." + appl_db + ".GetOfferOMS_Flat_C_Stream";
    var src_wrk_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFEROMS_O_WRK";
    var src_rerun_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFEROMS_O_RERUN";
    var tgt_tbl = cnf_out_db + "." + cnf_out_schema + ".EPE_OMS_OFFER_JSON";
    var lkp_store= src_db + "." + src_schema +".STOREGROUP_FLAT";
    var lkp_Product=src_db + "." + src_schema +".PRODUCTGROUP_FLAT";
    var lkp_OMSOFFER=src_db + "." + src_schema +".OFFEROMS_FLAT";
    var lkp_ref_Product=src_ref_db + "." + src_ref_schema +".PRODUCTGROUP_FLAT";
    var lkp_Retail_Store=src_db + "." +src_Loc + ".RETAIL_STORE";
	var lkp_Facility=src_db + "." +src_Loc + ".FACILITY";
    var lkp_Product_UPC =src_db + "." + src_schema + ".OMS_PRODUCT_GROUP_UPC";
	var src_temp_tbl =  cnf_out_db + "." + wrk_schema + ".EPE_OMS_OFFER_JSON_TEMP";
	var src_excep_tbl =  cnf_out_db + "." + wrk_schema + ".EPE_OMS_OFFER_JSON_EXCEPTION";
  
//check if rerun queue table exists otherwise create it
    
var sql_crt_rerun_tbl = `CREATE TRANSIENT TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS 
SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;
                                                                                                                   
    try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
	// Empty the work table
     
    var sql_empty_wrk_tbl = `truncate table `+ src_wrk_tbl + ``;

    try {
        snowflake.execute (
            {sqlText: sql_empty_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
    // persist stream data in work table for the current transaction, includes data from previous failed run
    
    var sql_crt_src_wrk_tbl = `INSERT INTO `+ src_wrk_tbl +` 
                               SELECT * FROM `+ src_tbl +` where METADATA$ACTION = 'INSERT' AND UPPER(SOURCEACTION) IN ('DEPLOY','REMOVE')  
								AND PAYLOAD_PROGRAMCODE IN ('MF','GR','SC','BPD','SPD')
  
                                UNION ALL 
                                SELECT * FROM `+ src_rerun_tbl +`
                                `;
    try {
       
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
     // Empty the rerun queue table
     
    var sql_empty_rerun_tbl = `truncate table `+ src_rerun_tbl + ``;

    try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
    // query to load rerun queue table when encountered a failure
              
    var sql_ins_rerun_tbl = `INSERT INTO `+ src_rerun_tbl+`  SELECT * FROM `+ src_wrk_tbl+``;
   // Empty the temp table
     
    var sql_empty_temp_tbl = `truncate table `+ src_temp_tbl + ``;

      //query to load the Json table	
    var sql_json = `INSERT INTO `+ src_temp_tbl + ` 
	           (
                                      topic, -- ''EDDW_C02_EPE_OFFER''
                                      key, -- offerid, startdate, enddate
                                      payload,
									  dw_create_ts,
                                      payload_md5
                                      
                                    )
                   SELECT *,current_timestamp,md5(payload)  FROM ( 
                  SELECT DISTINCT 'EDDW_C02_EPE_OFFER' AS topic,
                                    (OFFERID::string)||'|'||(OFFERSTARTDATE::string)||'|'||(OFFERENDDATE::string)||'|' || RN AS key,
                                    to_json(object_construct(
'productGroups',CASE when BENEFITPRODUCTGROUPS is null then Array_construct(object_construct()) ELSE BENEFITPRODUCTGROUPS END ,
'source', SOURCE,
'programCode', benefitprogramCode,
'promoType', benefitecomm,
'offerId', OFFERID,
'offerStartDate', OFFERSTARTDATE,
'offerEndDate', OFFERENDDATE,
'linkPLUNumber',0,
'offerTestingStartDate', OFFERTESTINGSTARTDATE,
'offerTestingEndDate', OFFERTESTINGENDDATE,
'offerName', OFFERNAME,
'offerDescription', OFFERDESCRIPTION,
'category', CATEGORY,
'programType',PAYLOAD_PROGRAMTYPE,
'pageNo',PAGENUM,
'pageCount',TOTALPAGES,
'createdDate',createdDate,
'sourceAction', SOURCEACTION,
'qualificationBehavior',PAYLOAD_QUALIFICATIONBEHAVIOR,
'isDynamicOffer',PAYLOAD_ISDYNAMICOFFER,
'initialSubscriptionOffer',PAYLOAD_INITIALSUBSCRIPTIONOFFER,
'subProgramCode',PAYLOAD_SUBPROGRAMCODE,
'autoApplyPromoCode',autoApplyPromoCode,
'priority', object_construct(
'footerPriority', priorityfooterpriority),
'deferEndOfSaleIndicator', DEFERENDOFSALEINDICATIOR,
'offerLimit',
object_construct('code',OfferLimitType,
'limit',limit,
'periodType',periodType,
'periodQuantity',periodQuantity) ,
'offerExternalId', Offer_External_Id,
'location',object_construct('stores',OMS_Stores,'terminalTypes',terminalTypes),
'conditions',object_construct_keep_null(
'conditionJoinOperator','AND',
'eCommPromoCode',PAYLOAD_ECOMMPROMOCODE,
'validWithOtherOffer',validWithOtherOffer,
'orderCount',PAYLOAD_ORDERCOUNT,
'firstTimeCustomerOnly',firstTimeCustomerOnly,
'notCombinableWith',PAYLOAD_NOTCOMBINABLEWITH,
'fulfillmentChannel',parse_json(FULFILLMENTCHANNEL_1),
'customerCondition',object_construct('exclude', case when CustomerGroupnmexclude is null then array_construct(object_construct('customerGroupNm','')) else 
 CustomerGroupnmexclude end,
'include', case when CustomerGroupnm is null then array_construct(object_construct('customerGroupNm','')) else CustomerGroupnm end),

'dayCondition',customerCondition ,
'triggerConditions',array_construct(triggerConditions),
'timeConditions',array_construct(
object_construct('startTm',startTm,
'endTm',endTm
)),
'pointsProgram',pointsGroupName,
'productConditions',ProductGroup ,

'disqualifierProductGroup',disqualifierProductGroup1,
'tenders',Tendertype),


'notifications',object_construct(
'accumulationPrintedMessage', object_construct('text',NotificationPrintedMessage)),

'benefit', object_construct(
'cashierMessage',object_construct('tiers',BENEFITCASHIERMESSAGE),


'discount',object_construct('discountLevel',discountLevel,
'discountProducts'
, benefitdiscountProductstiers



),


'groupMembership',object_construct('tiers',array_construct(object_construct('consumerGroupNm',
GROUPMEMBERSHIP))),


'pointsProgram',Scorecards,
'printedMessage' ,object_construct('tiers',PrintedMessages)
)


) )AS payload
      FROM
         (SELECT A.* ,C.CustomerGroupnm, CGE.CustomerGroupnmexclude, ccndn.customerCondition ,GRPNM.pointsGroupName,E.ProductGroup,
                                BCM.BENEFITCASHIERMESSAGE,DCL.discountLevel,Hier.benefitdiscountProductstiers,
                                BPP.Scorecards,FFC.FULFILLMENTCHANNEL_1,
                                BPM.PrintedMessages,
                                BPM.NotificationPrintedMessage,
                                TTYP.Tendertype,
         F.OMS_Stores,BPGR.BENEFITPRODUCTGROUPS,BPGR.PAGENUM,BPGR.TOTALPAGES,to_char(to_timestamp_ntz(A.LASTUPDATETS),'YYYY-MM-DD HH:MI:SS')::string AS createdDate,rn
         
          
		  FROM (SELECT     DISTINCT        FILENAME,
                            NVL(PAYLOAD_PROVIDERNAME::string,'') AS source,
                           CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,
                           CASE WHEN len(payload_externalOfferId) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(payload_externalOfferId::string,'') END AS Offer_External_Id,
                          NVL(PAYLOAD_COPIENTCATEGORY::string,'') AS CATEGORY,
		          'programType',PAYLOAD_PROGRAMTYPE,
                          'sourceAction',SOURCEACTION,
                          'qualificationBehavior',PAYLOAD_QUALIFICATIONBEHAVIOR,
						  PAYLOAD_ISDYNAMICOFFER::BOOLEAN as PAYLOAD_ISDYNAMICOFFER,
						  PAYLOAD_INITIALSUBSCRIPTIONOFFER::boolean as PAYLOAD_INITIALSUBSCRIPTIONOFFER,
                          'subProgramCode',PAYLOAD_SUBPROGRAMCODE,
                          'autoApplyPromoCode',case when PAYLOAD_AUTOAPPLYPROMOCODE::string=true THEN true ELSE false END as autoApplyPromoCode,
                          'validWithOtherOffer',case when PAYLOAD_VALIDWITHOTHEROFFER::string=true THEN true ELSE false END as validWithOtherOffer ,
                          'orderCount',PAYLOAD_ORDERCOUNT,
                          'eCommPromoCode',PAYLOAD_ECOMMPROMOCODE,
                          'firstTimeCustomerOnly',case when PAYLOAD_FIRSTTIMECUSTOMERONLY::string=true THEN true ELSE false END as firstTimeCustomerOnly ,
                          'notCombinableWith',PAYLOAD_NOTCOMBINABLEWITH,
                          CASE WHEN payload_effectiveStartDate IS NULL THEN '' ELSE to_char(to_timestamp_ntz(payload_effectiveStartDate),'YYYY-MM-DD HH:MI:SS')::string END AS offerStartDate,
                          CASE WHEN payload_effectiveEndDate IS NULL THEN '' ELSE to_char(to_timestamp_ntz(payload_effectiveEndDate),'YYYY-MM-DD HH:MI:SS')::string END AS offerEndDate,
                           NVL(payload_description::string,'') AS OFFERDESCRIPTION,
                           NVL(payload_offerName::string,'') AS OFFERNAME,
                            CASE WHEN payload_testEffectiveStartDate IS NULL THEN '' ELSE to_char(to_timestamp_ntz(payload_testEffectiveStartDate),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGSTARTDATE,
                            CASE WHEN payload_testEffectiveEndDate IS NULL THEN '' ELSE to_char(to_timestamp_ntz(payload_testEffectiveEndDate),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGENDDATE,
                            NVL(PAYLOAD_PRIORITY::string,'') AS priorityfooterpriority, 
                            case when payload_deferEvaluationUntilEOS = TRUE THEN TRUE ELSE FALSE END AS DEFERENDOFSALEINDICATIOR,
	CASE WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Unlimited' THEN 'No Limit' 
     WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Once per Offer' THEN 'Once Per Offer'
	 WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Once per Week' THEN 'Once Per Week'
	 WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Once per Day' THEN 'Once Per Day'
                                WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Once per Transaction' THEN 'Once Per Transaction'
                                WHEN PAYLOAD_USAGELIMITTYPEPERUSER='Custom' THEN 'Custom' ELSE 
                                                                            PAYLOAD_USAGELIMITTYPEPERUSER  END as OfferLimitType,                                                                                                                                                                                                                                                                                                                                                                                                                             
                                                                             CASE
                                WHEN PAYLOAD_CUSTOMTYPE='Days Since Start Of Incentive' THEN 'Days Since Start of Incentive' ELSE ''  END 
								as periodType,
                                                                                                                nvl(PAYLOAD_CUSTOMPERIOD::NUMBER(38,2),0.00) as periodQuantity,
                            nvl(PAYLOAD_BENEFIT_GROUPMEMBERSHIP_CUSTOMERGROUPNAME::string,'') as GROUPMEMBERSHIP,
                            nvl(PAYLOAD_QUALIFICATIONPRODUCTDISQUALIFIER::string ,'') as PRODUCTDISQUALIFIER,
                                                                                                    PAYLOAD_TERMINALS::array  AS terminalTypes,
                            NVL(PAYLOAD_PROGRAMCODE::string,'') AS benefitprogramCode,
							NVL(PAYLOAD_ECOMMPROMOTYPE::string,'') AS benefitecomm,
                                                                                                     LASTUPDATETS,
                            NVL(PAYLOAD_QUALIFICATIONTIME_START::string,'') AS startTm,
                            NVL(PAYLOAD_QUALIFICATIONTIME_END::string,'') AS endTm,
                                                                                                                NVL(PAYLOAD_QUALIFICATIONTRIGGERCODES.Value:code::string,'') AS triggerConditions,
                                                                                                                NVL(QUALIFICATIONPRODUCTDISQUALIFIERNAME::string ,'') AS disqualifierProductGroup1,   
                            NVL(PAYLOAD_BENEFIT_PRINTEDMESSAGE_MESSAGE::string,'') AS accumulationPrintedMessage,
                            NVL(PAYLOAD_USAGELIMITPERUSER::string,0) AS limit,
                            PAYLOAD_BENEFIT_PRINTEDMESSAGE_ISAPPLICABLEFORNOTIFICATIONS
                            
                           from `+ src_wrk_tbl +` 
                                                                                                   ,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONTRIGGERCODES, outer => TRUE) as PAYLOAD_QUALIFICATIONTRIGGERCODES
                                                                                                   ) A
                           join 
 
                     (SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,
                     ROW_NUMBER() OVER (PARTITION BY PAYLOAD_ID  ORDER BY TO_TIMESTAMP_LTZ(LASTUPDATETS) DESC) AS rn
                      FROM ` + src_wrk_tbl + `) B
                ON A.offerId = B.offerId
                AND A.LASTUPDATETS = B.LASTUPDATETS
                AND B.RN = 1 
				
    left join
(SELECT  DISTINCT OFFERID, LASTUPDATETS,array_agg(CustomerGroupnm) within group (order by CustomerGroupnm) AS CustomerGroupnm
from
(SELECT  DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId
,LASTUPDATETS,
    object_construct('customerGroupNm', conditionscustomerGroupNm.Value:name::string) as CustomerGroupnm
from ` + src_wrk_tbl + `
,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONCUSTOMERGROUPS, outer => TRUE) as conditionscustomerGroupNm
where NVL(conditionscustomerGroupNm.Value:excludeUsers::String,'false') ='false'
)
GROUP by OFFERID, LASTUPDATETS) C
ON A.offerId=C.offerId and A.LASTUPDATETS=C.LASTUPDATETS

left join
(SELECT  DISTINCT OFFERID, LASTUPDATETS
,array_agg(CustomerGroupnmexclude) within group (order by CustomerGroupnmexclude) AS CustomerGroupnmexclude from 
(SELECT  DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId
,LASTUPDATETS,
    object_construct('customerGroupNm',conditionscustomerGroupNm.Value:name::string)  as CustomerGroupnmexclude
from ` + src_wrk_tbl + `
,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONCUSTOMERGROUPS, outer => TRUE) as conditionscustomerGroupNm
where NVL(conditionscustomerGroupNm.Value:excludeUsers::String,'false') ='true'
)
GROUP by OFFERID, LASTUPDATETS) CGE
ON A.offerId=CGE.offerId and A.LASTUPDATETS=CGE.LASTUPDATETS
Join 

(SELECT  DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId
,LASTUPDATETS,
                object_construct('MONDAY',case when PAYLOAD_QUALIFICATIONDAY_MONDAY = TRUE THEN TRUE ELSE FALSE END,
                                     'TUESDAY',case when PAYLOAD_QUALIFICATIONDAY_TUESDAY = TRUE THEN TRUE ELSE FALSE END,
                                     'WEDNESDAY',case when PAYLOAD_QUALIFICATIONDAY_WEDNESDAY=TRUE THEN TRUE ELSE FALSE END,
'THURSDAY',case when PAYLOAD_QUALIFICATIONDAY_THURSDAY=TRUE THEN TRUE ELSE FALSE END,
                                      'FRIDAY',case when PAYLOAD_QUALIFICATIONDAY_FRIDAY=TRUE THEN TRUE ELSE FALSE END,
                                       'SATURDAY',case when PAYLOAD_QUALIFICATIONDAY_SATURDAY=TRUE THEN TRUE ELSE FALSE END,
                                       'SUNDAY',case when PAYLOAD_QUALIFICATIONDAY_SUNDAY=TRUE THEN TRUE ELSE FALSE END) as customerCondition
from ` + src_wrk_tbl + `
)ccndn
ON A.offerId=ccndn.offerId and A.LASTUPDATETS=ccndn.LASTUPDATETS

join

(Select offerId,LASTUPDATETS,pointsGroupNames as pointsGroupName from  (select  DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN 
NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,
object_construct('pointsGroupName',NVL(conditionspointsprogram.Value:name::string,''),
                 'tiers',array_agg(distinct object_construct('pointQty',NVL(conditionspointsProgramtierspointQty.Value:quantity::NUMBER(38,2),
				 0.00)))
				 within group (order by object_construct('pointQty',NVL(conditionspointsProgramtierspointQty.Value:quantity::NUMBER(38,2),
				 0.00))))                
                 
                 as pointsGroupNames
from ` + src_wrk_tbl + `
,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONPOINTSGROUPS, outer => TRUE) as conditionspointsprogram
,LATERAL FLATTEN(input => conditionspointsprogram.value:tiers, outer => TRUE) as conditionspointsProgramtierspointQty 
group by PAYLOAD_ID, payload_redemptionSystemId, LASTUPDATETS, conditionspointsprogram.Value
)group by offerId,LASTUPDATETS,pointsGroupNames
) GRPNM
ON A.offerId=GRPNM.offerId and A.LASTUPDATETS=GRPNM.LASTUPDATETS

JOIN
(
SELECT offerId,LASTUPDATETS,array_agg(ProdGroup) within group (order by ProdGroup,ProdGroup:tiers) AS PRODUCTGROUP
from (
SELECT a.offerId
,a.LASTUPDATETS,
object_construct( 'productGroupName',nvl(pg.payload_name,'') ,
'excGroupName', excGroupName ,
'minProductQuantity', minProductQuantity ,
'minPurchaseAmount',minPurchaseAmount ,
'productConditionOperator',productConditionOperator,
'productConditionType', productConditionType ,
'tiers', array_agg(distinct object_construct( 'productAmt', productAmt)
) within group (order by object_construct( 'productAmt', productAmt))
) as ProdGroup
 from 
(SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId
,LASTUPDATETS,conditionsproductConditions.Value:id as pg_id,nvl(conditionsproductConditions.Value:excludedProductGroupName::string,'') 
as excGroupName,nvl(conditionsproductConditions.Value:amount::number,0) as minProductQuantity,
CASE WHEN conditionsproductConditions.Value:quantityUnitType::string='DOLLARS' then 
nvl(conditionsproductConditions.Value:minPurchaseAmount::NUMBER(38,2),0.00) else 0.00 END as minPurchaseAmount,
nvl(conditionsproductConditions.Value:conjunction::string,'') as productConditionOperator,
nvl(conditionsproductConditions.Value:quantityUnitDesc::string,'') as productConditionType,
nvl(conditionsproductConditionstiers.Value:amount::NUMBER(38,2),0.00) as productAmt
from `+ src_wrk_tbl +` 
,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE) as conditionsproductConditions
,LATERAL FLATTEN(input => conditionsproductConditions.value:tiers, outer => TRUE) as conditionsproductConditionstiers
)a
left join (select distinct payload_id, payload_name from `+ lkp_Product +`) 
pg on a.pg_id = pg.payload_id
group by offerId,LASTUPDATETS,pg.payload_name,
excGroupName,minProductQuantity,minPurchaseAmount,productConditionOperator,productConditionType
)group by offerId,LASTUPDATETS 
) E
  ON A.offerId=E.offerId and A.LASTUPDATETS=E.LASTUPDATETS
  
    
  JOIN 
  (SELECT offerId,LASTUPDATETS,array_agg(BENEFITCASHIERMESSAGES ) within group (order by TRIM(REGEXP_REPLACE(BENEFITCASHIERMESSAGES:messageLine1Txt, 
  '[^[:digit:]]', ' '))
  asc ) as BENEFITCASHIERMESSAGE from (
    SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,
               object_construct(    'displayImmediatelyInd',case when ISDISPLAYIMMEDIATE::string=true THEN true ELSE false END,
'beepDurationSec', nvl(BENEFITCASHIERMESSAGETIERS.Value:beepDuration::string,0) ,
                                     'beepType', nvl(BENEFITCASHIERMESSAGETIERS.Value:beepType::string,'') ,
                                     'messageLine1Txt', nvl(BENEFITCASHIERMESSAGETIERS.Value:line1::string,'') ,
                                     'messageLine2Txt', nvl(BENEFITCASHIERMESSAGETIERS.Value:line2::string ,'') 
                                                                                            ) as BENEFITCASHIERMESSAGES 
                from `+ src_wrk_tbl +`
                       ,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_CASHIERMESSAGE_CASHIERMESSAGETIERS, outer => TRUE )  as BENEFITCASHIERMESSAGETIERS ) group by offerId,LASTUPDATETS)BCM
                        
               ON A.offerId = BCM.offerId
             AND A.LASTUPDATETS = BCM.LASTUPDATETS
			 
  JOIN 
  
  (SELECT offerId,LASTUPDATETS,array_agg(Tendertype) within group (order by Tendertype) as Tendertype from (
    SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,
   object_construct(    'excludedAmt',0.00,
'tenderNm', nvl(PAYLOAD_QUALIFICATIONTENDERTYPES.Value:tenderType::string,'') ,
'tiers', array_agg(distinct object_construct('tenderAmt',nvl(tenderTiers.value:value::string,0)))
                    within group(order by object_construct('tenderAmt',nvl(tenderTiers.value:value::string,0)))
) as Tendertype 
                from `+ src_wrk_tbl +`
				,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONTENDERTYPES, outer => TRUE )  as PAYLOAD_QUALIFICATIONTENDERTYPES
				,LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONTENDERTYPES.value:tenderTiers, outer => TRUE )  as tenderTiers
				group by PAYLOAD_ID, payload_redemptionSystemId, LASTUPDATETS, PAYLOAD_QUALIFICATIONTENDERTYPES.Value
					   ) group by offerId,LASTUPDATETS
				   )TTYP
                        
               ON A.offerId = TTYP.offerId
             AND A.LASTUPDATETS = TTYP.LASTUPDATETS
     
      JOIN 
 
 
              
              (
    SELECT offerId,LASTUPDATETS, discountLvl as discountLevel from (
    SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS, 
      nvl(BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:discountDesc::string,'') as discountLvl
              from `+ src_wrk_tbl +`
                      ,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_DISCOUNT, outer => TRUE) as BENEFITDISCOUNTDISCOUNTPRODUCTS ) 
                                                                                  group by offerId,LASTUPDATETS, discountLvl)DCL
                        
               ON A.offerId = DCL.offerId
             AND A.LASTUPDATETS = DCL.LASTUPDATETS
      
     JOIN 
  (
  SELECT offerId,LASTUPDATETS,array_agg(benefitdiscountProductstier) within group (order by benefitdiscountProductstier)  as benefitdiscountProductstiers 
from (select a.offerId,a.LASTUPDATETS,object_construct('allowNegative',allowNegative,
'discountType' ,discountType,
'excGroupName',excGroupName,
'flexNegative', flexNegative,
'productGroupName',nvl(pg.payload_name,''),
'tiers',array_agg(distinct construt) 
within group (order by construt:discountValue asc) ) as benefitdiscountProductstier from 
(SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId
,LASTUPDATETS,BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:includeProductGroupId as pg_id,
case when BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:advanced:allowNegative::string='true' THEN true ELSE false END as allowNegative,
CASE WHEN BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:benefitValueDesc='Cents Off' THEN 'Fixed Amount Off'
WHEN BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:benefitValueDesc='Cents Off (Per Lb)' THEN 'Fixed Amount Off (Weight/Volume)'
WHEN BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:benefitValueDesc='Price Point (Per Lb)' THEN 'Price Point (Weight/Volume)'
ELSE
nvl(BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:benefitValueDesc::string,'') END as discountType,
nvl(BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:excludeProductGroupName::string,'') as excGroupName,
case when BENEFITDISCOUNTDISCOUNTPRODUCTS.Value:advanced:flexNegative::string=true THEN true ELSE false END as flexNegative,
object_construct('discountValue',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:amount::NUMBER(38,2),0.00),
'dollarLimit',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:dollarLimit::NUMBER(38,2),0.00),
'itemLimit',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:itemLimit::NUMBER(38,2),0.00),
'receiptText',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:receiptText::string ,''),
'weightLimit',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:weightLimit::NUMBER(38,2),0.00),
'percentOffLimitLevel1Amt',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:upTo::NUMBER(38,2),0.00),
'amountLevel2',NVL(BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS.Value:upTo::NUMBER(38,2),0.00),
'amountLevel3',0 ,'amountTypeLevel2','' ,'percentOffLimitLevel2Amt',0) as construt
 from `+ src_wrk_tbl +`
,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_DISCOUNT, outer => TRUE) as BENEFITDISCOUNTDISCOUNTPRODUCTS
,LATERAL FLATTEN(input => BENEFITDISCOUNTDISCOUNTPRODUCTS.value:discountTier, outer => TRUE) as BENEFITDISCOUNTDISCOUNTPRODUCTSTIERS 
)a
left join (select distinct payload_id, payload_name from `+ lkp_Product +`) pg on a.pg_id = pg.payload_id
group by a.offerId,a.LASTUPDATETS,a.allowNegative,a.discountType,a.excGroupName,a.flexNegative,pg.payload_name
)A group by offerId, LASTUPDATETS 
  ) Hier on a.offerId = HIER.offerId AND A.LASTUPDATETS = HIER.LASTUPDATETS
                                                   
   JOIN (SELECT offerId,LASTUPDATETS,array_agg(Scorecards) within group (order by Scorecards) as Scorecards from 
    (SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,
                LASTUPDATETS, 
    
    object_construct('pointsGroupName',NVL(BENEFITpointsprogram.Value:pointsProgram::string ,''),
'pointsLimit',0,
'scoreCard',      object_construct('enabled',case when BENEFITpointsprogram.Value:scoreCard is null or BENEFITpointsprogram.Value:scoreCard = '' 
                                   then false else true end,'lineText',
                               NVL(BENEFITpointsprogram.Value:scoreCardText::string,'' ),
                               'name',
                          NVL(BENEFITpointsprogram.Value:scoreCard::string,'')),
'tiers',array_agg(distinct object_construct('tierPointNbr',NVL(BENEFITpointsprogramquantity.Value:quantity::NUMBER(38,2),0.00)))
                     within group (order by object_construct('tierPointNbr',NVL(BENEFITpointsprogramquantity.Value:quantity::NUMBER(38,2),0.00)))
                                                                                                  ) as Scorecards
      from `+ src_wrk_tbl +`,
                  LATERAL FLATTEN(input => PAYLOAD_BENEFIT_POINTS, outer => TRUE) as BENEFITpointsprogram,
                  LATERAL FLATTEN(input => PAYLOAD_QUALIFICATIONPOINTSGROUPS, outer => TRUE) as benefitpointsprogrampointsGroupName,
      LATERAL FLATTEN(input => BENEFITpointsprogram.value:pointsTier, outer => TRUE) as BENEFITpointsprogramquantity,
      LATERAL FLATTEN(input => BENEFITpointsprogram.value:tiers, outer => TRUE) as BENEFITpointsprogramtierPointNbr
	  group by PAYLOAD_ID,payload_redemptionSystemId,LASTUPDATETS,BENEFITpointsprogram.Value)
                  group by offerId,LASTUPDATETS
				  )BPP
       ON A.offerId = BPP.offerId
        AND A.LASTUPDATETS = BPP.LASTUPDATETS  
   JOIN (SELECT offerId,LASTUPDATETS,
    CASE WHEN FULFILLMENTCHANNEL:delivery = false  and FULFILLMENTCHANNEL:dug =false and    FULFILLMENTCHANNEL:inStorePurchase=false  
and FULFILLMENTCHANNEL:wug = false THEN parse_json('NULL') ELSE TO_CHAR( FULFILLMENTCHANNEL)::variant  END as FULFILLMENTCHANNEL_1 from
   (    SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,
               object_construct(

'delivery', case when  PAYLOAD_FULFILLMENTCHANNEL:delivery::string='true' THEN true ELSE false END ,
'dug', case when  PAYLOAD_FULFILLMENTCHANNEL:dug::string='true' THEN true ELSE false END ,
'inStorePurchase', case when  PAYLOAD_FULFILLMENTCHANNEL:inStorePurchase::string='true' THEN true ELSE false END ,
'wug', case when  PAYLOAD_FULFILLMENTCHANNEL:wug::string='true' THEN true ELSE false END ) as FULFILLMENTCHANNEL
  from `+ src_wrk_tbl +`
  ,LATERAL FLATTEN(input => PAYLOAD_FULFILLMENTCHANNEL, outer => TRUE )  as FULFILLMENTCHANNEL)                                                                                     
group by offerId,LASTUPDATETS,FULFILLMENTCHANNEL
				  )FFC
       ON A.offerId = FFC.offerId
        AND A.LASTUPDATETS = FFC.LASTUPDATETS  
    
    JOIN (
select offerId, array_agg( distinct PrintedMessages) within group (order by TRIM(REGEXP_REPLACE(PrintedMessages:printedMessageTxt, '[^[:digit:]]', ' '))) 
as PrintedMessages,NotificationPrintedMessage,LASTUPDATETS
FROM (
SELECT offerId,LASTUPDATETS, object_construct( 'printedMessageTxt', PrintedMessages )  PrintedMessages ,
nvl(NotificationPrintedMessage,'') NotificationPrintedMessage from(
SELECT DISTINCT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,LASTUPDATETS,

CASE WHEN BENEFITprintedMessageprintedMessageTxt.Value:message::string not like '%ACCUMULATION_PRINTED_MSG:%'then
BENEFITprintedMessageprintedMessageTxt.Value:message::string END PrintedMessages ,

CASE WHEN BENEFITprintedMessageprintedMessageTxt.Value:message::string like '%ACCUMULATION_PRINTED_MSG:%'then
replace(BENEFITprintedMessageprintedMessageTxt.Value:message,'ACCUMULATION_PRINTED_MSG:','')::string END NotificationPrintedMessage
from `+ src_wrk_tbl +`
,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_PRINTEDMESSAGE_MESSAGE, outer => TRUE) as BENEFITprintedMessageprintedMessageTxt

)group by offerId,LASTUPDATETS,PrintedMessages,NotificationPrintedMessage
) group by offerId,NotificationPrintedMessage,LASTUPDATETS

) BPM
      ON A.offerId = BPM.offerId
      AND A.LASTUPDATETS = BPM.LASTUPDATETS 
    
 LEFT   JOIN
     (
       SELECT offerId,Lastupdatets,array_agg(OMS_Stores) within group (order by OMS_Stores) AS OMS_Stores from
(
  SELECT OFFERID, MAX(Lastupdatets) as Lastupdatets ,OMS_Stores
  FROM(
SELECT DISTINCT offerId, OS.Lastupdatets ,os.Store_Group_Id,
object_construct( 'storeNumber', LPAD(f.facility_nbr, 4,'0'),'corporationId',f.CORPORATION_ID::string,
'divisionId',f.DIVISION_ID::string ) as OMS_Stores from
(
		  
Select distinct CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS 
 offerId,Lastupdatets,QSG.VALUE:id::STRING AS Store_Group_Id
FROM `+ src_wrk_tbl +`
,lateral flatten(input => payload_qualificationStoreGroups_redemptionStoreGroups, outer => TRUE)  as QSG
  union all
  Select distinct CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS 
 offerId,Lastupdatets,TSG.VALUE::STRING AS Store_Group_Id
FROM `+ src_wrk_tbl +`
,lateral flatten(input => payload_testStoreGroups, outer => TRUE)  as TSG
 
) OS
left join `+ lkp_store +` SG on OS.Store_Group_Id=SG.payload_id 
JOIN `+ lkp_Retail_Store +` F on LPAD(payload_stores,4,'0') = LPAD(f.facility_nbr, 4,'0') WHERE 1=1
AND f.DW_LOGICAL_DELETE_IND = 'False' and f.DW_CURRENT_VERSION_IND ='True'  AND CORPORATION_ID ='001'
     ) GROUP BY OFFERID,OMS_Stores

) c group by offerId,Lastupdatets
     )F
    ON A.offerId = F.offerId
      AND A.LASTUPDATETS = F.LASTUPDATETS 
    
    LEFT JOIN 

      (
      
With Benefit_Prouct_Group_CTE as (  
SELECT OMS.*, PG.PAYLOAD_PRODUCTGROUPIDS_UPCIDS::string as extProductId,PG.MFC,PG.dept,PG.UPC,
NVL(PG.PAYLOAD_NAME::string,'') as productGroupName,
PG.PAYLOAD_PRODUCTGROUPIDS_UPCIDS ,PG.PAGENUM,PG.TOTALPAGES FROM (
SELECT OFFERID, MAX(Lastupdatets) Lastupdatets ,PRODUCT_GROUP_ID FROM (

SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,Lastupdatets, 
  PRODUCTGROUPS_STORES.value:id::string AS Product_Group_Id
from `+ src_wrk_tbl +`  ,LATERAL FLATTEN(input =>PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE ) as PRODUCTGROUPS_STORES
union
SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,Lastupdatets, 
  PRODUCTGROUPS_STORES.value:excludedProductGroupId::string AS Product_Group_Id
from `+ src_wrk_tbl +`  ,LATERAL FLATTEN(input =>PAYLOAD_QUALIFICATIONPRODUCTGROUPS, outer => TRUE ) as PRODUCTGROUPS_STORES
union
SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,Lastupdatets, 
  PAYLOAD_QUALIFICATIONPRODUCTDISQUALIFIER AS Product_Group_Id
from `+ src_wrk_tbl +`  
union
SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,Lastupdatets, 
  PRODUCTGROUPS_STORES.value:includeProductGroupId::string AS Product_Group_Id
from `+ src_wrk_tbl +`  ,LATERAL FLATTEN(input =>payload_benefit_discount, outer => TRUE ) as PRODUCTGROUPS_STORES
union
SELECT CASE WHEN len(PAYLOAD_ID) = 0 THEN NVL(payload_redemptionSystemId::string,'') ELSE NVL(PAYLOAD_ID::string,'') END AS offerId,Lastupdatets, 
  PRODUCTGROUPS_STORES.value:excludeProductGroupId::string AS Product_Group_Id
from `+ src_wrk_tbl +`  ,LATERAL FLATTEN(input =>payload_benefit_discount, outer => TRUE ) as PRODUCTGROUPS_STORES


) GROUP BY OFFERID, PRODUCT_GROUP_ID
) OMS
       
        
        
left join
(
SELECT entityId, PAYLOAD_NAME,PAGENUM,TOTALPAGES, MFC.value::string as MFC, dept.value::string as dept, UPC.value::string as UPC ,PAYLOAD_PRODUCTGROUPIDS_UPCIDS
FROM `+ lkp_Product +`
,LATERAL FLATTEN (input => PAYLOAD_PRODUCTGROUPIDS_MANUFACTUREIDS, outer => TRUE)as MFC
,lateral flatten(input => PAYLOAD_PRODUCTGROUPIDS_DEPARTMENTSECTIONIDS, outer => TRUE)as dept
,lateral flatten(input => PAYLOAD_PRODUCTGROUPIDS_UPCIDS, outer => TRUE)as UPC 

) PG ON PG.entityId=OMS.Product_Group_Id

) 
  Select OFFERID,Lastupdatets,PAGENUM,TOTALPAGES, array_agg(Benefitgroup)within group (order by Benefitgroup) as BENEFITPRODUCTGROUPS  from(
select OFFERID, Lastupdatets,PAGENUM,TOTALPAGES,
object_construct
(
'extProductId' , UPC,
'productGroupName' ,CASE WHEN UPC IS NULL Then NULL ELSE PRODUCTGROUPNAME END,
  'type',CASE WHEN UPC IS NULL Then NULL ELSE  'Items (UPC)' END
)Benefitgroup from Benefit_Prouct_Group_CTE
where UPC is not null 
UNION 
select OFFERID, Lastupdatets,PAGENUM,TOTALPAGES,
object_construct
(
'extProductId' , MFC,
'productGroupName' , CASE WHEN MFC IS NULL THEN NULL ELSE PRODUCTGROUPNAME END ,
  'type',CASE WHEN MFC IS NULL THEN NULL ELSE 'Manufacturer Family Code' END 
) Benefitgroup from Benefit_Prouct_Group_CTE where MFC is not null 
UNION
select OFFERID, Lastupdatets,PAGENUM,TOTALPAGES,
object_construct
(
'extProductId' , dept,
'productGroupName' ,CASE WHEN dept is null THEN NULL ELSE  PRODUCTGROUPNAME END,
  'type',CASE WHEN dept is null THEN NULL ELSE 'Departments' END
)  Benefitgroup from Benefit_Prouct_Group_CTE where dept is not null )
group by OFFERID, Lastupdatets,PAGENUM,TOTALPAGES

       ) BPGR
                        
               ON A.offerId = BPGR.offerId
             AND A.LASTUPDATETS = BPGR.LASTUPDATETS
    
 
  )
         ) src
     /*    where md5(src.payload) not in (select md5(payload) from ( 
select tgt.payload, row_number() over(partition by left(key,charindex('|',key)-1) order by tgt.DW_CREATE_TS desc) as rn
from `+ tgt_tbl + ` tgt join `+ src_wrk_tbl +`
on PAYLOAD_ID = left(key,charindex('|',key)-1) qualify rn = 1))*/
              
    `;
 var sql_copy_excep = `insert into `+ src_excep_tbl + ` Select TOPIC,KEY,PAYLOAD,DW_CREATE_TS,'Offer Details already Exist' from   `+ src_temp_tbl  + ` src 
					    where src.payload_md5 in (select md5(payload) from ( 
						select tgt.payload, row_number() over(partition by left(tgt.key,charindex('|',tgt.key)-1) order by tgt.DW_CREATE_TS desc) as rn
						from `+ tgt_tbl + ` tgt join `+ src_temp_tbl +` src
						on left(src.key,charindex('|',src.key)-1)  = left(tgt.key,charindex('|',tgt.key)-1) qualify rn = 1))`;

   
	var sql_copy_tgt = `insert into `+ tgt_tbl + ` Select TOPIC,KEY,PAYLOAD,DW_CREATE_TS from  `+ src_temp_tbl  + ` src 
					    where src.payload_md5 not in (select md5(payload) from ( 
						select tgt.payload, row_number() over(partition by left(tgt.key,charindex('|',tgt.key)-1) order by tgt.DW_CREATE_TS desc) as rn
						from `+ tgt_tbl + ` tgt join `+ src_temp_tbl +` src
						on left(src.key,charindex('|',src.key)-1) = left(tgt.key,charindex('|',tgt.key)-1) qualify rn = 1 ))`;																																							  
	      var sql_begin = "BEGIN"
          var sql_commit = "COMMIT"
          var sql_rollback = "ROLLBACK"
         
         try {
        snowflake.execute (
            {sqlText: sql_begin  }
        );
		snowflake.execute (
            {sqlText: sql_empty_temp_tbl  }
        );		  
        snowflake.execute (
            {sqlText: sql_json  }
        );
		snowflake.execute (
            {sqlText: sql_copy_excep  }
        );
		snowflake.execute (
            {sqlText: sql_copy_tgt  }
        );
	   snowflake.execute (
            {sqlText: sql_commit  }
        );    
        }
        catch (err) {
            snowflake.execute (
                {sqlText: sql_ins_rerun_tbl  }
            );
           return "Loading of Json table  table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        } 
                    
       
      $$
;

