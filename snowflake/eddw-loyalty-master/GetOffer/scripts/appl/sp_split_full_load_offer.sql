--liquibase formatted sql
--changeset SYSTEM:sp_split_full_load_offer runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_OUT>>.DW_DCAT.SP_SPLIT_FULL_LOAD_OFFER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Global Variables
    var cnf_out_db = "<<EDM_DB_NAME_OUT>>";
    var cnf_db = "EDM_REFINED_PRD.DW_R_STAGE";
    var wrk_schema = "DW_DCAT";
    var cnf_out_schema = "DW_DCAT";
    var src_db = "EDM_REFINED_PRD";
    var src_schema = "DW_R_PRODUCT";
    var src_tbl = src_db + "." + src_schema + ".GETOFFER_FLAT";
    var tgt_tbl = cnf_out_db + "." + cnf_out_schema + ".EPE_OFFER_JSON";

    var strt = 0;
    var ends = 10000;

    var split_tbl = cnf_out_db + "." + wrk_schema + ".GETOFFER_FLAT_FL_SPLIT";


    var log_tbl = cnf_db + ".Full_load_Log";
    
    var sql_cnt = `SELECT count(*) FROM `+ src_tbl;
    var cnt = 0;
    try {
      ret_obj = snowflake.execute (
            {sqlText: sql_cnt }
            );
            ret_obj.next();
            cnt = ret_obj.getColumnValue(1);
        }
    catch (err)  {
        return "Fetching count from source table failed with error: " + err;   // Return a error message.
        }
    
    var sql_crt = `CREATE OR REPLACE TABLE `+ split_tbl + `
                    AS 
                    SELECT a.incentiveid
                  ,filename
            ,engineTypeNm
            ,deployInd
            ,deferDeployInd
            ,a.lastUpdateTs
            ,a.dw_create_ts
            ,general_identification_offerNm
            ,general_identification_offerDsc
            ,general_identification_categoryDsc
            ,general_identification_offerExternalId
            ,general_identification_vendorCouponCd
            ,general_priority_priorityNm
            ,general_priority_footerPriorityMsgTxt
            ,general_dates_testing_testingStartDt
            ,general_dates_testing_testingEndDt
            ,general_dates_eligibility_eligibilityStartDt
            ,general_dates_eligibility_eligibilityEndDt
            ,general_dates_production_productionStartDt
            ,general_dates_production_productionEndDt
            ,general_limits_eligibility_eligibilityFrequencyNm
            ,general_limits_eligibility_limitNbr
            ,general_limits_eligibility_periodQty
            ,general_limits_eligibility_quantityType
            ,general_limits_reward_rewardsFrequencyNm
            ,general_limits_reward_limitNbr
            ,general_limits_reward_periodQty
            ,general_limits_reward_quantityType
            ,general_inboundoutbound_chargebackVendorId
            ,general_inboundoutbound_creationSourceNm
            ,general_inboundoutbound_outboundSourceNm
            ,general_employees_employeeEligibilityTypNm
            ,general_advanced_eosDeferInd
            ,general_advanced_issuanceInd
            ,general_advanced_manufacturerCouponInd
            ,general_advanced_reporting_impressionInd
            ,general_advanced_reporting_redemptionInd
            ,notifications_printedmessage_printedmessageTxt
            ,notifications_cashiermessage_messageLine1Txt
            ,notifications_cashiermessage_messageLine2Txt
            ,notifications_cashiermessage_BeepType
            ,notifications_cashiermessage_BeepDurationSec
            ,notifications_cashiermessage_displayImmediatelyInd
            ,notifications_accumulationPrintedMessage_AcumulationPrintedMessageInd
            ,locations_storegroups_excluded_StoreGroupNm
            ,locations_storegroups_included
            ,locations_terminals_included
            ,locations_terminals_excluded
            ,conditions_Enterpriseinstantwin_prizenbr
            ,conditions_Enterpriseinstantwin_prizefrequencynbr
            ,conditions_day_mondayind
            ,conditions_day_tuesdayind
            ,conditions_day_wednesdayind
            ,conditions_day_thursdayind
            ,conditions_day_fridayind
            ,conditions_day_saturdayind
            ,conditions_day_sundayind
            ,conditions_DisqualifierProductGroupNm
            ,conditions_storedvalue_programNm
            ,conditions_storedvalue_tiers
            ,conditions_tenders
            ,conditions_times
            ,conditions_triggercodes
            ,conditions_customers_included
            ,conditions_customers_excluded
            ,conditions_products
            ,conditions_points_programNm 
            ,conditions_points_tiers
            ,conditions_cardtypes
            ,conditions_attributes
            ,rewards_discount_productgroup_discountType
            ,rewards_discount_productgroup_incProductGroupNm
            ,rewards_discount_productgroup_excProductGroupNm
            ,rewards_discount_chargebackdepartment_departmentNm
            ,rewards_discount_chargebackdepartment_departmentId
            ,rewards_discount_scorecard_scorecardNm
            ,rewards_discount_scorecard_scorecardEnableInd
            ,rewards_discount_scorecard_scorecardLineTxt
            ,rewards_discount_advanced_computeDiscountInd
            ,rewards_discount_advanced_bestDealInd
            ,rewards_discount_advanced_allowNegativeInd
            ,rewards_discount_advanced_flexNegativeInd
            ,rewards_discount_distribution_distributionType
            ,rewards_discount_distribution_tiers
            ,rewards_cashiermessage_tiers
            ,rewards_cashiermessage_DisplayImmediatelyInd
            ,rewards_points
            ,rewards_printedmessage_tiers
            ,rewards_groupmembership_tiers
            ,rewards_frankingmessage_tiers
            ,rewards_storedvalue_programnm
            ,rewards_storedvalue_scorecard_scorecardenabledInd
            ,rewards_storedvalue_scorecard_scorecardNm
            ,rewards_storedvalue_scorecard_scorecardTxt
            ,rewards_storedvalue_tiers
            ,productgroup_productGroupNm
            ,productgroup_productGroupDsc
            ,a.productgroup_LastUpdateTs
            ,product_groups_products
            ,a.storegroup_LastUpdateTs
            ,storegroup_StoreGroupNm
            ,storegroups_stores
            ,row_number() over( order by a.incentiveid) as rn
            FROM `+ src_tbl + `  a
            join
            (
            select incentiveid,LASTUPDATETS,STOREGROUP_LASTUPDATETS,productgroup_LastUpdateTs
            from
            (select incentiveid,LASTUPDATETS,STOREGROUP_LASTUPDATETS,productgroup_LastUpdateTs
            ,row_number() over( partition by incentiveid order by to_timestamp_ltz(LASTUPDATETS), to_timestamp(STOREGROUP_LASTUPDATETS), to_timestamp(productgroup_LastUpdateTs) desc) as rn
            from `+ src_tbl + `
            ) 
            where rn = 1) b
            ON a.incentiveid = b.incentiveid
            AND a.LASTUPDATETS = b.LASTUPDATETS
            AND A.STOREGROUP_LASTUPDATETS=B.STOREGROUP_LASTUPDATETS
            AND A.productgroup_LastUpdateTs=B.productgroup_LastUpdateTs`;
                  
  
    
    try {
          snowflake.execute (
              {sqlText: sql_crt  }
              );
          }
      catch (err)  {
          return "Creation of table Failed with error: " + err;   // Return a error message.
          }
    

    var loop_cnt = Math.ceil(cnt/ends) + 1;
  
    for (i = 1; i <= loop_cnt; i++) {
    
    
    var sql_json = `INSERT INTO ` + tgt_tbl + ` 
                                    (
                                      
                                      payload,
                                      key, -- offerid, startdate, enddate
                                      topic -- ''EDDW_C02_EPE_OFFER''
                                      
                                    )
                                    WITH split_table AS
                    (SELECT incentiveid
                  ,filename
            ,engineTypeNm
            ,deployInd
            ,deferDeployInd
            ,lastUpdateTs
            ,dw_create_ts
            ,general_identification_offerNm
            ,general_identification_offerDsc
            ,general_identification_categoryDsc
            ,general_identification_offerExternalId
            ,general_identification_vendorCouponCd
            ,general_priority_priorityNm
            ,general_priority_footerPriorityMsgTxt
            ,general_dates_testing_testingStartDt
            ,general_dates_testing_testingEndDt
            ,general_dates_eligibility_eligibilityStartDt
            ,general_dates_eligibility_eligibilityEndDt
            ,general_dates_production_productionStartDt
            ,general_dates_production_productionEndDt
            ,general_limits_eligibility_eligibilityFrequencyNm
            ,general_limits_eligibility_limitNbr
            ,general_limits_eligibility_periodQty
            ,general_limits_eligibility_quantityType
            ,general_limits_reward_rewardsFrequencyNm
            ,general_limits_reward_limitNbr
            ,general_limits_reward_periodQty
            ,general_limits_reward_quantityType
            ,general_inboundoutbound_chargebackVendorId
            ,general_inboundoutbound_creationSourceNm
            ,general_inboundoutbound_outboundSourceNm
            ,general_employees_employeeEligibilityTypNm
            ,general_advanced_eosDeferInd
            ,general_advanced_issuanceInd
            ,general_advanced_manufacturerCouponInd
            ,general_advanced_reporting_impressionInd
            ,general_advanced_reporting_redemptionInd
            ,notifications_printedmessage_printedmessageTxt
            ,notifications_cashiermessage_messageLine1Txt
            ,notifications_cashiermessage_messageLine2Txt
            ,notifications_cashiermessage_BeepType
            ,notifications_cashiermessage_BeepDurationSec
            ,notifications_cashiermessage_displayImmediatelyInd
            ,notifications_accumulationPrintedMessage_AcumulationPrintedMessageInd
            ,locations_storegroups_excluded_StoreGroupNm
            ,locations_storegroups_included
            ,locations_terminals_included
            ,locations_terminals_excluded
            ,conditions_Enterpriseinstantwin_prizenbr
            ,conditions_Enterpriseinstantwin_prizefrequencynbr
            ,conditions_day_mondayind
            ,conditions_day_tuesdayind
            ,conditions_day_wednesdayind
            ,conditions_day_thursdayind
            ,conditions_day_fridayind
            ,conditions_day_saturdayind
            ,conditions_day_sundayind
            ,conditions_DisqualifierProductGroupNm
            ,conditions_storedvalue_programNm
            ,conditions_storedvalue_tiers
            ,conditions_tenders
            ,conditions_times
            ,conditions_triggercodes
            ,conditions_customers_included
            ,conditions_customers_excluded
            ,conditions_products
            ,conditions_points_programNm 
            ,conditions_points_tiers
            ,conditions_cardtypes
            ,conditions_attributes
            ,rewards_discount_productgroup_discountType
            ,rewards_discount_productgroup_incProductGroupNm
            ,rewards_discount_productgroup_excProductGroupNm
            ,rewards_discount_chargebackdepartment_departmentNm
            ,rewards_discount_chargebackdepartment_departmentId
            ,rewards_discount_scorecard_scorecardNm
            ,rewards_discount_scorecard_scorecardEnableInd
            ,rewards_discount_scorecard_scorecardLineTxt
            ,rewards_discount_advanced_computeDiscountInd
            ,rewards_discount_advanced_bestDealInd
            ,rewards_discount_advanced_allowNegativeInd
            ,rewards_discount_advanced_flexNegativeInd
            ,rewards_discount_distribution_distributionType
            ,rewards_discount_distribution_tiers
            ,rewards_cashiermessage_tiers
            ,rewards_cashiermessage_DisplayImmediatelyInd
            ,rewards_points
            ,rewards_printedmessage_tiers
            ,rewards_groupmembership_tiers
            ,rewards_frankingmessage_tiers
            ,rewards_storedvalue_programnm
            ,rewards_storedvalue_scorecard_scorecardenabledInd
            ,rewards_storedvalue_scorecard_scorecardNm
            ,rewards_storedvalue_scorecard_scorecardTxt
            ,rewards_storedvalue_tiers
            ,productgroup_productGroupNm
            ,productgroup_productGroupDsc
            ,productgroup_LastUpdateTs
            ,product_groups_products
            ,storegroup_LastUpdateTs
            ,storegroup_StoreGroupNm
            ,storegroups_stores
            ,rn
            FROM `+ split_tbl + `
            WHERE rn > ` + strt + `and rn <= ` + ends + `
           
            )
                        SELECT 
                                    distinct to_json(object_construct(
                    				'source',  SOURCE,		
                    				'offerId', INCENTIVEID,
                                    'offerStartDate', OFFERSTARTDATE,
							        'offerEndDate', OFFERENDDATE,
                                    'linkPLUNumber',0,
                                    'offerTestingStartDate', OFFERTESTINGSTARTDATE,
							        'offerTestingEndDate', OFFERTESTINGENDDATE,
							        'offerName', OFFERNAME,
							        'offerDescription', OFFERDESCRIPTION,
                                    'category', OFFERCATEGORY,
                                    'offerPriority', OFFERPRIORITY,
                                    'programCode', PROGRAMCODE,
                                    'deferEndOfSaleIndicator', DEFERENDOFSALEINDICATIOR,
                                    'offerLimit', OFFERLIMIT,
                                    'offerExternalId', Offer_External_Id,
                                    'location',object_construct('stores', STOREIDS,
                                                                'terminalTypes',TERMINALTYPES),
                                    'conditions',object_construct('conditionJoinOperator','AND',
                                                                   'customerCondition',object_construct('exclude', CONDITIONS_CUSTOMERS_EXCLUDED,
                                                                                                      'include', CONDITIONS_CUSTOMERS_INCLUDED),
                                                                   'productConditions',productCondition,
                                                                   'pointsProgram', object_construct('pointsGroupName',CONDITIONS_POINTS_PROGRAMNM,
                                                                                                     'tiers', CONDITIONS_POINTS_TIERS),
                                                                   'triggerConditions',triggerCondition,
                                                                   'timeConditions', CONDITIONS_TIMES,
                                                                   'dayCondition', dayCondition),
                                    'benefit', object_construct('discount',object_construct('discountLevel',discountLevel,
                                                                                            'specialPricing', specialpricing,
                                                                                            'discountProducts', discountProducts,
                                                                                            'scoreCard',scorecard),
                                                                'pointsProgram',pointsprogram), 
                                   'productGroups',PRODUCTGROUPS
                                   
                                    
            )) AS payload,
            (INCENTIVEID::string)||'|'||(OFFERSTARTDATEKEY::string)||'|'||(OFFERENDDATEKEY::string) AS key,
            'EDDW_C02_EPE_OFFER' AS topic
      FROM
         (SELECT A.*, C.PRODUCTGROUPS, D.STOREIDS, E.TERMINALTYPES, F.PRODUCTCONDITION,
                 G.TRIGGERCONDITION, J.POINTSPROGRAM, K.DISCOUNTPRODUCTS, K.SPECIALPRICING
          FROM (
                 SELECT     DISTINCT NVL(INCENTIVEID::string,'') AS INCENTIVEID,
                            FILENAME,
                            NVL(ENGINETYPENM::string,'') AS SOURCE,
                            NVL(CAST(GENERAL_DATES_PRODUCTION_PRODUCTIONSTARTDT AS DATE)::string,'') AS OFFERSTARTDATEKEY,
                            NVL(CAST(GENERAL_DATES_PRODUCTION_PRODUCTIONENDDT AS DATE)::string,'') AS OFFERENDDATEKEY,
                            CASE WHEN GENERAL_DATES_PRODUCTION_PRODUCTIONSTARTDT IS NULL THEN '' ELSE to_char(to_timestamp_ntz(GENERAL_DATES_PRODUCTION_PRODUCTIONSTARTDT),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERSTARTDATE,
                            CASE WHEN GENERAL_DATES_PRODUCTION_PRODUCTIONENDDT IS NULL THEN '' ELSE to_char(to_timestamp_ntz(GENERAL_DATES_PRODUCTION_PRODUCTIONENDDT),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERENDDATE,
                            CASE WHEN GENERAL_DATES_TESTING_TESTINGSTARTDT IS NULL THEN '' ELSE to_char(to_timestamp_ntz(GENERAL_DATES_TESTING_TESTINGSTARTDT),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGSTARTDATE,
                            CASE WHEN GENERAL_DATES_TESTING_TESTINGENDDT IS NULL THEN '' ELSE to_char(to_timestamp_ntz(GENERAL_DATES_TESTING_TESTINGENDDT),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGENDDATE,
                            NVL(GENERAL_IDENTIFICATION_OFFERNM::string,'') AS OFFERNAME,
                            NVL(GENERAL_IDENTIFICATION_OFFERDSC::string,'') AS OFFERDESCRIPTION,
                            NVL(GENERAL_IDENTIFICATION_CATEGORYDSC::string,'') AS OFFERCATEGORY,
                            NVL(GENERAL_PRIORITY_PRIORITYNM::string,'') AS OFFERPRIORITY,
                            case when general_advanced_eosDeferInd = TRUE THEN TRUE ELSE FALSE END AS DEFERENDOFSALEINDICATIOR,
                            CASE WHEN General_advanced_manufacturercouponind = TRUE THEN 'MF' 
                                 ELSE 'SC' END AS PROGRAMCODE,
                            CASE WHEN len(GENERAL_IDENTIFICATION_OFFEREXTERNALID) = 0 THEN NVL(INCENTIVEID::string,'')
                                                                                      ELSE NVL(GENERAL_IDENTIFICATION_OFFEREXTERNALID::string,'') END AS Offer_External_Id,
                            object_construct('code',NVL(general_limits_reward_rewardsFrequencyNm::string,''),
                                             'limit', NVL(general_limits_reward_limitnbr::string,0),
                                             'periodType',NVL(general_limits_reward_QuantityType::string,''),
                                             'periodQuantity',NVL(general_limits_reward_periodqty::string,0)) AS OFFERLIMIT,
                            CONDITIONS_CUSTOMERS_INCLUDED, 
                            CONDITIONS_CUSTOMERS_EXCLUDED,
                            NVL(CONDITIONS_POINTS_PROGRAMNM::string,'') AS CONDITIONS_POINTS_PROGRAMNM,
                            CASE WHEN array_size(CONDITIONS_POINTS_TIERS) IS NULL THEN array_construct(object_construct('pointQty',0))
                                                                                  ELSE CONDITIONS_POINTS_TIERS END AS CONDITIONS_POINTS_TIERS,
                            CASE WHEN array_size(CONDITIONS_TIMES) IS NULL THEN array_construct(object_construct('startTm','','endTm','')) 
                                                                           ELSE CONDITIONS_TIMES END AS CONDITIONS_TIMES,
                            object_construct('MONDAY',case when CONDITIONS_DAY_MONDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'TUESDAY',case when CONDITIONS_DAY_TUESDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'WEDNESDAY',case when CONDITIONS_DAY_WEDNESDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'THURSDAY',case when CONDITIONS_DAY_WEDNESDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'FRIDAY',case when CONDITIONS_DAY_FRIDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'SATURDAY',case when CONDITIONS_DAY_SATURDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'SUNDAY',case when CONDITIONS_DAY_SATURDAYIND = TRUE THEN TRUE ELSE FALSE END) AS dayCondition,
                            object_construct('name',NVL(rewards_discount_scorecard_scorecardnm::string,''),
                                             'lineText', NVL(rewards_discount_scorecard_scorecardlinetxt::string,''),
                                             'enabled', case when rewards_discount_scorecard_scorecardenableind = TRUE THEN TRUE ELSE FALSE END) AS scorecard,
                            NVL(rewards_discount_productgroup_discounttype::string,'') AS discountLevel,
                            NVL(rewards_discount_advanced_allownegativeind::string,'') AS allowNegative,
                            NVL(rewards_discount_distribution_distributiontype::string,'') AS discountType,
                            NVL(rewards_discount_advanced_flexnegativeind::string,'') AS flexNegative,
                            NVL(rewards_discount_productgroup_incproductgroupnm::string,'') AS discount_incGroupName,
                            NVL(rewards_discount_productgroup_excproductgroupnm::string,'') AS discount_excGroupName,
                            LASTUPDATETS,
                            dw_create_ts
                          
                FROM split_table
               -- WHERE  METADATA$ACTION = 'INSERT'
               )A      
                JOIN (SELECT DISTINCT INCENTIVEID, array_agg(prodgroup) within group (order by prodgroup) AS PRODUCTGROUPS 
                      FROM
                        (SELECT DISTINCT INCENTIVEID, 
                                         object_construct('productGroupName',NVL(PRODUCTGROUP_PRODUCTGROUPNM::string,''),
                                                          'extProductId',NVL(pg.value:extProductId::string,'')
                                                          ,'type',NVL(pg.value:productTypeNm::string,'')) AS prodgroup
                    FROM split_table
                    ,LATERAL FLATTEN(input => PRODUCT_GROUPS_PRODUCTS, outer => true) as  pg order by prodgroup)
                    GROUP BY INCENTIVEID) C 
                ON A.INCENTIVEID = C.INCENTIVEID 
                JOIN (SELECT DISTINCT INCENTIVEID, array_agg(STOREIDS) within group (order by STOREIDS) AS STOREIDS
                           FROM (SELECT DISTINCT INCENTIVEID, object_construct('storeNumber',NVL(SUBSTRING(sg.value:extLocationCd,4,4)::string,''), 
                                                                               'divisionId',NVL(SUBSTRING(sg.value:extLocationCd,2,2)::string,''), 
                                                                               'corporationId','001') AS STOREIDS
                            FROM split_table 
                            ,LATERAL FLATTEN(input => STOREGROUPS_STORES, outer => true) as  sg) 
                            GROUP BY INCENTIVEID) D
                ON A.INCENTIVEID = D.INCENTIVEID
                JOIN (SELECT DISTINCT INCENTIVEID, array_agg(terminaltypes) within group (order by terminaltypes) AS TERMINALTYPES 
                      FROM
                        (SELECT DISTINCT INCENTIVEID, 
                                         NVL(tt.value:terminalNm::string,'') AS terminaltypes
                        FROM split_table 
                        ,LATERAL FLATTEN(input => LOCATIONS_TERMINALS_INCLUDED, outer => true) AS  tt)
                        GROUP BY INCENTIVEID) E 
                ON A.INCENTIVEID = E.INCENTIVEID
                JOIN (SELECT DISTINCT INCENTIVEID, array_agg(productCondition) within group (order by productCondition) AS PRODUCTCONDITION
                      FROM 
                        (SELECT DISTINCT INCENTIVEID,
                                                       object_construct('productGroupName', NVL(pc.value:incProductGroupNm::string,''),
                                                                        'excGroupName', NVL(pc.value:excProductGroupNm::string,''),
                                                                        'productConditionType', NVL(pc.value:amountType::string,''),
                                                                        'productConditionOperator',NVL(pc.value:productComboConditionOper::string,''),
                                                                        'uniqueProductCondition', case when pc.value:uniqueProductInd = TRUE THEN TRUE ELSE FALSE END,
                                                                        'minPurchaseAmount', NVL(pc.value:minPurchaseAmt::string,0),
                                                                        'minProductQuantity', NVL(pc.value:accumulation:minAmt::string,0) ,-- NVL(pc.value:accumulation::object,''),
                                                                        'tiers', pc.value:tiers::array
                                                                        ) AS productCondition
                                         
                        FROM split_table
                        ,LATERAL FLATTEN(input => CONDITIONS_PRODUCTS, outer => true) AS pc)
                        GROUP BY INCENTIVEID ) F
                ON A.INCENTIVEID = F.INCENTIVEID
                JOIN (SELECT DISTINCT INCENTIVEID, 
                                                    array_agg(triggercodes) within group (order by triggercodes) AS triggerCondition
                      FROM
                        (SELECT DISTINCT INCENTIVEID, 
                                         NVL(ct.value:TriggerCd::string,'') AS triggercodes
                        FROM split_table 
                        ,LATERAL FLATTEN(input => CONDITIONS_TRIGGERCODES, outer => true) AS  ct)
                        GROUP BY INCENTIVEID) G
                ON A.INCENTIVEID = G.INCENTIVEID    
                JOIN (SELECT DISTINCT INCENTIVEID, array_agg(pointsprogram) within group (order by pointsprogram) AS pointsprogram
                      FROM 
                        (SELECT DISTINCT INCENTIVEID , 
                                                      object_construct('pointsGroupName', NVL(rp.value:programNm::string,''),
                                                                        'pointsLimit',NVL(rp.value:Maximumadjustment:maxPointAllowNbr::string,0),
                                                                        'scoreCard', object_construct('name',NVL(rp.value:scorecard:programNm::string,''),
                                                                                                      'lineText',NVL(rp.value:scorecard:scorecardLineTxt::string,''),
                                                                                                      'enabled',case when rp.value:scorecard:scorecardEnableInd = TRUE THEN TRUE ELSE FALSE END),
                                                                        'tiers', case when array_size(rp.value:tiers) is null then array_construct(object_construct('tierPointNbr',0)) else rp.value:tiers::array end
                                                                        ) AS pointsprogram          
                        FROM split_table
                        ,LATERAL FLATTEN(input => REWARDS_POINTS, outer => true) AS rp)
                      
                        GROUP BY INCENTIVEID) J
                ON A.INCENTIVEID = J.INCENTIVEID
                JOIN (SELECT DISTINCT A.INCENTIVEID, CASE WHEN specialpricing[0]:amount = 0 THEN
                                       array_construct(object_construct('productGroupName', NVL(rewards_discount_productgroup_incproductgroupnm::string,''),
                                                                        'excGroupName', NVL(rewards_discount_productgroup_excproductgroupnm::string,''),
                                                                        'allowNegative', case when rewards_discount_advanced_allownegativeind = TRUE THEN TRUE ELSE FALSE END,
                                                                        'flexNegative',case when rewards_discount_advanced_flexnegativeind = TRUE THEN TRUE ELSE FALSE END ,
                                                                        'couponFactor', 0,
                                                                        'discountType',NVL(rewards_discount_distribution_distributiontype::string,''),
                                                                        'tiers', TIERS)) 
                                                                        ELSE
                                       array_construct(object_construct('incGroupName', NVL(rewards_discount_productgroup_incproductgroupnm::string,''),
                                                                        'excGroupName', NVL(rewards_discount_productgroup_excproductgroupnm::string,''),
                                                                        'allowNegative', case when rewards_discount_advanced_allownegativeind = TRUE THEN TRUE ELSE FALSE END,
                                                                        'flexNegative',case when rewards_discount_advanced_flexnegativeind = TRUE THEN TRUE ELSE FALSE END ,
                                                                        'couponFactor', 0,
                                                                        'discountType',NVL(rewards_discount_distribution_distributiontype::string,''),
                                                                        'tiers', array_construct(object_construct('discountValue', '',
                                                                                                                   'dollarLimit',0,
                                                                                                                   'itemLimit', 0,
                                                                                                                   'receiptText', '',
                                                                                                                   'weightLimit','')))) END AS DISCOUNTPRODUCTS,
                                      CASE WHEN specialpricing[0]:amount = 0 THEN
                                      array_construct(object_construct('amount', 0,
                                                                       'seqNumber',0))
                                                      ELSE SPECIALPRICING END AS SPECIALPRICING
                                                                  
                        FROM split_table A
                        JOIN  (SELECT DISTINCT INCENTIVEID,
                                                              array_agg( object_construct('discountValue', NVL(rpd.value:tierAmt::string,0),
                                                                                         'dollarLimit',NVL(rpd.value:tierMaxDollarItemLimitAmt::string,0),
                                                                                         'itemLimit', NVL(rpd.value:tierItemLimitNbr::string,0),
                                                                                         'receiptText', NVL(rpd.value:tierReceiptTxt::string,''),
                                                                                         'weightLimit',NVL(rpd.value:weightvolumelimit:maxWeightLimitVal::string,0))) within group (order by rpd.index asc) AS TIERS,
                                                             array_agg(DISTINCT object_construct('amount', NVL(rpricess.value:specialPriceAmt::string,0),
                                                                                        'seqNumber',rpricess.index::number)) AS SPECIALPRICING
                                                           
                              FROM split_table
                              ,LATERAL FLATTEN(input => REWARDS_DISCOUNT_DISTRIBUTION_TIERS, outer => true)  rpd
                              ,LATERAL FLATTEN(input => rpd.VALUE:prices, outer => true ) AS rpricess
                              GROUP BY INCENTIVEID) B
                       ON A.INCENTIVEID = B.INCENTIVEID) K
                ON A.INCENTIVEID = K.INCENTIVEID     
         )
    `;
         
         
    var sql_log = `Insert into `+ log_tbl + `
               select '`+ tgt_tbl +`' as table_nm,` + i + ` as loop_cntr,` + strt + ` as Start_range, `+ ends + ` as end_range, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) as Finsihed_Ts`;


   
    try {
        snowflake.execute (
            {sqlText: sql_json  }
            );
        }
    catch (err)  {
        return "Insertion of data Failed with error: " + err;   // Return a error message.
        }
    try {
        snowflake.execute (
            {sqlText: sql_log  }
            );
        }
    catch (err)  {
        return "Creation of log table Failed with error: " + err;   // Return a error message.
        } 
    strt = strt + 10000;
    ends = ends + 10000;
        }
         
       
$$;
