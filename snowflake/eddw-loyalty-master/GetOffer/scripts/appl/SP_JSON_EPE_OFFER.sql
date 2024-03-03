--liquibase formatted sql
--changeset SYSTEM:SP_JSON_EPE_OFFER runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_OUT>>.DW_DCAT.SP_JSON_EPE_OFFER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // Globa Variables
    var cnf_out_db = "<<EDM_DB_NAME_OUT>>";
    var wrk_schema = "DW_DCAT";
    var cnf_out_schema = "DW_DCAT";
    var src_db = "<<EDM_DB_NAME_OUT>>";
    var src_schema = "DW_DCAT";
    var src_tbl = src_db + "." + src_schema + ".GETOFFER_FLAT_O_STREAM";
    var src_wrk_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_O_WRK";
    var src_rerun_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_O_RERUN";
    var tgt_tbl = cnf_out_db + "." + cnf_out_schema + ".EPE_OFFER_JSON";

    
    
  
 //check if rerun queue table exists otherwise create it

    var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS 
								SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;
								
    try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
    // persist stream data in work table for the current transaction, includes data from previous failed run
    var sql_crt_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
                                SELECT * FROM `+ src_tbl +` 
                                UNION ALL 
                                SELECT * FROM `+ src_rerun_tbl +``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
     // Empty the rerun queue table
     
    var sql_empty_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 
							   AS SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;
    try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
    // query to load rerun queue table when encountered a failure
	
    var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl+`  as SELECT * FROM `+ src_wrk_tbl+``;
    
    var sql_json = `INSERT INTO `+ tgt_tbl + ` 
                                    (
                                      topic, -- ''EDDW_C02_EPE_OFFER''
                                      key, -- offerid, startdate, enddate
                                      payload
                                    )
                  SELECT * FROM ( 
                  SELECT 'EDDW_C02_EPE_OFFER' AS topic,
                                    (INCENTIVEID::string)||'|'||(OFFERSTARTDATEKEY::string)||'|'||(OFFERENDDATEKEY::string) AS key,
                                    to_json(object_construct(
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
                                    'priority', object_construct('name', OFFERPRIORITY,
                                                                 'footerPriority', FOOTERPRIORITYMSGTXT),
                                    'vendorCouponCode', vendorcouponcode,
                                    'programCode', PROGRAMCODE,
                                    'deferEndOfSaleIndicator', DEFERENDOFSALEINDICATIOR,
                                    'offerLimit', OFFERLIMIT,
                                    'offerExternalId', Offer_External_Id,
                                    'notifications',NOTIFICATIONS,
                                    'location',object_construct('stores', STOREIDS,
                                                                'terminalTypes',TERMINALTYPES),
                                    'conditions',object_construct('conditionJoinOperator','AND',
                                                                    'disqualifierProductGroup',DISQUALIFIERPRODUCTGROUPNM,
                                                                   'customerCondition',object_construct('exclude', CONDITIONS_CUSTOMERS_EXCLUDED,
                                                                                                      'include', CONDITIONS_CUSTOMERS_INCLUDED),
                                                                   'productConditions',productCondition,
                                                                   'pointsProgram', object_construct('pointsGroupName',CONDITIONS_POINTS_PROGRAMNM,
                                                                                                     'tiers', CONDITIONS_POINTS_TIERS),
                                                                   'triggerConditions',triggerCondition,
                                                                   'timeConditions', CONDITIONS_TIMES,
                                                                   'tenders',CONDITIONS_TENDERS,
                                                                   'dayCondition', dayCondition),
                                    'benefit', object_construct('discount',object_construct('discountLevel',discountLevel,
                                                                                            'specialPricing', specialpricing,
                                                                                            'discountProducts', discountProducts,
                                                                                            'scoreCard',scorecard),
                                                                'printedMessage',REWARDPRINTEDMESSAGE,
                                                                'cashierMessage',REWARDCASHIERMESSAGE,
                                                                'frankingMessage',REWARDS_FRANKINGMESSAGE,
                                                                'pointsProgram',pointsprogram,
                                                                'groupMembership',object_construct('tiers',GROUPMEMBERSHIP_TIERS)),  
                                   'productGroups',PRODUCTGROUPS
                                   
                                    
            )) AS payload
      FROM
         (SELECT A.*, C.PRODUCTGROUPS, D.STOREIDS, E.TERMINALTYPES, F.PRODUCTCONDITION,
                 G.TRIGGERCONDITION, J.POINTSPROGRAM, K.DISCOUNTPRODUCTS, K.SPECIALPRICING,
				 object_construct('tiers',RCT.REWARDS_CASHIERMESSAGE_TIERS) AS REWARDCASHIERMESSAGE
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
                            NVL(GENERAL_PRIORITY_FOOTERPRIORITYMSGTXT::string,'') AS FOOTERPRIORITYMSGTXT,
                            GENERAL_IDENTIFICATION_VENDORCOUPONCD as vendorcouponcode,
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
                                             'THURSDAY',case when CONDITIONS_DAY_THURSDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'FRIDAY',case when CONDITIONS_DAY_FRIDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'SATURDAY',case when CONDITIONS_DAY_SATURDAYIND = TRUE THEN TRUE ELSE FALSE END,
                                             'SUNDAY',case when CONDITIONS_DAY_SUNDAYIND = TRUE THEN TRUE ELSE FALSE END) AS dayCondition,
                            object_construct('name',NVL(rewards_discount_scorecard_scorecardnm::string,''),
                                             'lineText', NVL(rewards_discount_scorecard_scorecardlinetxt::string,''),
                                             'enabled', case when rewards_discount_scorecard_scorecardenableind = TRUE THEN TRUE ELSE FALSE END) AS scorecard,
                            object_construct('tiers',REWARDS_PRINTEDMESSAGE_TIERS) AS REWARDPRINTEDMESSAGE,    
                            /*object_construct('tiers',REWARDS_CASHIERMESSAGE_TIERS) AS REWARDCASHIERMESSAGE,*/
                            object_construct('tiers',REWARDS_FRANKINGMESSAGE_TIERS) AS REWARDS_FRANKINGMESSAGE,
                            object_construct('printedMessage',object_construct('text',NOTIFICATIONS_PRINTEDMESSAGE_PRINTEDMESSAGETXT),
                                             'cashierMessage',object_construct('beepDurationSec', NOTIFICATIONS_CASHIERMESSAGE_BEEPDURATIONSEC,
                                                           'beepType', NOTIFICATIONS_CASHIERMESSAGE_BEEPTYPE,
                                                           'displayImmediatelyInd',NVL(NOTIFICATIONS_CASHIERMESSAGE_DISPLAYIMMEDIATELYIND::string,'False'),
                                                           'messageLine1Txt',NOTIFICATIONS_CASHIERMESSAGE_MESSAGELINE1TXT,
                                                           'messageLine2Txt',NOTIFICATIONS_CASHIERMESSAGE_MESSAGELINE2TXT),
                                            'accumulationPrintedMessage',object_construct('text',NOTIFICATIONS_ACCUMULATIONPRINTEDMESSAGE_ACUMULATIONPRINTEDMESSAGEIND)) AS NOTIFICATIONS,
                                            NVL(CONDITIONS_DISQUALIFIERPRODUCTGROUPNM::string,'') AS DISQUALIFIERPRODUCTGROUPNM,
                            CONDITIONS_TENDERS,             
                            NVL(rewards_discount_productgroup_discounttype::string,'') AS discountLevel,
                            NVL(rewards_discount_advanced_allownegativeind::string,'') AS allowNegative,
                            NVL(rewards_discount_distribution_distributiontype::string,'') AS discountType,
                            NVL(rewards_discount_advanced_flexnegativeind::string,'') AS flexNegative,
                            NVL(rewards_discount_productgroup_incproductgroupnm::string,'') AS discount_incGroupName,
                            NVL(rewards_discount_productgroup_excproductgroupnm::string,'') AS discount_excGroupName,
                            CASE WHEN REWARDS_GROUPMEMBERSHIP_TIERS is NULL THEN array_construct(object_construct('consumerGroupNm','')) ELSE REWARDS_GROUPMEMBERSHIP_TIERS END as GROUPMEMBERSHIP_TIERS,
                            LASTUPDATETS,
                            dw_create_ts
                          
                FROM ` + src_wrk_tbl + `
               WHERE  METADATA$ACTION = 'INSERT'
               )A
                JOIN 
                     (SELECT INCENTIVEID,LASTUPDATETS, ROW_NUMBER() OVER (PARTITION BY INCENTIVEID 
                                                                       ORDER BY TO_TIMESTAMP_LTZ(LASTUPDATETS) DESC) AS rn
                      FROM ` + src_wrk_tbl + `) B
                ON A.INCENTIVEID = B.INCENTIVEID
                AND A.LASTUPDATETS = B.LASTUPDATETS
                AND B.RN = 1       
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,array_agg(prodgroup) within group (order by prodgroup) AS PRODUCTGROUPS 
                      FROM
                        (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,
                                         object_construct('productGroupName',NVL(PRODUCTGROUP_PRODUCTGROUPNM::string,''),
                                                          'extProductId',NVL(pg.value:extProductId::string,'')
                                                          ,'type',NVL(pg.value:productTypeNm::string,'')) AS prodgroup
                    FROM ` + src_wrk_tbl + `
                    ,LATERAL FLATTEN(input => PRODUCT_GROUPS_PRODUCTS, outer => true) as  pg order by prodgroup)
                    GROUP BY INCENTIVEID,LASTUPDATETS) C 
                ON A.INCENTIVEID = C.INCENTIVEID 
                AND A.LASTUPDATETS =C.LASTUPDATETS
				
				JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,array_agg(REWARDS_CASHIERMESSAGE_TIERS) within group (order by REWARDS_CASHIERMESSAGE_TIERS) AS REWARDS_CASHIERMESSAGE_TIERS 
                      FROM
                        (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,
                                   object_construct('displayImmediatelyInd',CASE WHEN REWARDS_CASHIERMESSAGE_DISPLAYIMMEDIATELYIND = true THEN true  ELSE false END,
													'beepDurationSec',NVL(pg.value:beepDurationSec::string,0),
                                                    'beepType',NVL(pg.value:beepType::string,''),
                                                    'messageLine1Txt',NVL(pg.value:messageLine1Txt::string,''),
													'messageLine2Txt',NVL(pg.value:messageLine2Txt::string,'')) AS REWARDS_CASHIERMESSAGE_TIERS
                    FROM ` + src_wrk_tbl + `
                    ,LATERAL FLATTEN(input => REWARDS_CASHIERMESSAGE_TIERS, outer => true) as  pg order by REWARDS_CASHIERMESSAGE_TIERS)
                    GROUP BY INCENTIVEID,LASTUPDATETS) RCT 
                ON A.INCENTIVEID = RCT.INCENTIVEID 
                AND A.LASTUPDATETS =RCT.LASTUPDATETS
				
				
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,array_agg(STOREIDS) within group (order by STOREIDS) AS STOREIDS
                           FROM (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, object_construct('storeNumber',NVL(SUBSTRING(sg.value:extLocationCd,4,4)::string,''), 
                                                                               'divisionId',NVL(SUBSTRING(sg.value:extLocationCd,2,2)::string,''), 
                                                                               'corporationId','001') AS STOREIDS
                            FROM ` + src_wrk_tbl + ` 
                            ,LATERAL FLATTEN(input => STOREGROUPS_STORES, outer => true) as  sg) 
                            GROUP BY INCENTIVEID,LASTUPDATETS) D
                ON A.INCENTIVEID = D.INCENTIVEID
                AND A.LASTUPDATETS = D.LASTUPDATETS
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, array_agg(terminaltypes) within group (order by terminaltypes) AS TERMINALTYPES 
                      FROM
                        (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,
                                         NVL(tt.value:terminalNm::string,'') AS terminaltypes
                        FROM ` + src_wrk_tbl + ` 
                        ,LATERAL FLATTEN(input => LOCATIONS_TERMINALS_INCLUDED, outer => true) AS  tt)
                        GROUP BY INCENTIVEID,LASTUPDATETS) E 
                ON A.INCENTIVEID = E.INCENTIVEID
                AND A.LASTUPDATETS = E.LASTUPDATETS
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, array_agg(productCondition) within group (order by productCondition) AS PRODUCTCONDITION
                      FROM 
                        (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,
                                                       object_construct('productGroupName', NVL(pc.value:incProductGroupNm::string,''),
                                                                        'excGroupName', NVL(pc.value:excProductGroupNm::string,''),
                                                                        'productConditionType', NVL(pc.value:amountType::string,''),
                                                                        'productConditionOperator',NVL(pc.value:productComboConditionOper::string,''),
                                                                        'uniqueProductCondition', case when pc.value:uniqueProductInd = TRUE THEN TRUE ELSE FALSE END,
                                                                        'minPurchaseAmount', NVL(pc.value:minPurchaseAmt::string,0),
                                                                        'minProductQuantity', NVL(pc.value:accumulation:minAmt::string,0) ,-- NVL(pc.value:accumulation::object,''),
                                                                        'tiers', case when array_size(pc.value:tiers::array) is null then array_construct(object_construct('productAmt',0)) else pc.value:tiers::array end
                                                                        ) AS productCondition
                                         
                        FROM ` + src_wrk_tbl + ` 
                        ,LATERAL FLATTEN(input => CONDITIONS_PRODUCTS, outer => true) AS pc)
                        GROUP BY INCENTIVEID,LASTUPDATETS ) F
                ON A.INCENTIVEID = F.INCENTIVEID
                AND A.LASTUPDATETS = F.LASTUPDATETS
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, 
                                                    array_agg(triggercodes) within group (order by triggercodes) AS triggerCondition
                      FROM
                        (SELECT DISTINCT INCENTIVEID, LASTUPDATETS,
                                         NVL(ct.value:TriggerCd::string,'') AS triggercodes
                        FROM ` + src_wrk_tbl + ` 
                        ,LATERAL FLATTEN(input => CONDITIONS_TRIGGERCODES, outer => true) AS  ct)
                        GROUP BY INCENTIVEID,LASTUPDATETS) G
                ON A.INCENTIVEID = G.INCENTIVEID  
                AND A.LASTUPDATETS = G.LASTUPDATETS  
                JOIN (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, array_agg(pointsprogram) within group (order by pointsprogram) AS pointsprogram
                      FROM 
                        (SELECT DISTINCT INCENTIVEID ,LASTUPDATETS, 
                                                       object_construct('pointsGroupName', NVL(rp.value:programNm::string,''),
                                                                        'pointsLimit',NVL(rp.value:Maximumadjustment:maxPointAllowNbr::string,0),
                                                                        'scoreCard', object_construct('name',NVL(rp.value:scorecard:programNm::string,''),
                                                                                                      'lineText',NVL(rp.value:scorecard:scorecardLineTxt::string,''),
                                                                                                      'enabled',case when rp.value:scorecard:scorecardEnableInd = TRUE THEN TRUE ELSE FALSE END),
                                                                        'tiers', case when array_size(rp.value:tiers) is null then array_construct(object_construct('tierPointNbr',0)) else rp.value:tiers::array end
                                                                        ) AS pointsprogram                
                        FROM ` + src_wrk_tbl + `
                        ,LATERAL FLATTEN(input => REWARDS_POINTS, outer => true) AS rp)
                        GROUP BY INCENTIVEID,LASTUPDATETS) J
                ON A.INCENTIVEID = J.INCENTIVEID
                AND A.LASTUPDATETS = J.LASTUPDATETS
                JOIN (SELECT DISTINCT A.INCENTIVEID, A.LASTUPDATETS,  CASE WHEN specialpricing[0]:amount = 0 THEN
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
                                                                                                                   'weightLimit','',
                                                                                                                   'amountLevel3',0,
                                                                                                                   'amountTypeLevel2','',
                                                                                                                   'percentOffLimitLevel1Amt',0,
                                                                                                                   'percentOffLimitLevel2Amt',0,
                                                                                                                   'amountLevel2',0)
                                                                                                                   ))) END AS DISCOUNTPRODUCTS,
                                      CASE WHEN specialpricing[0]:amount = 0 THEN
                                      array_construct(object_construct('amount', 0,
                                                                       'seqNumber',0))
                                                      ELSE SPECIALPRICING END AS SPECIALPRICING
                                                                  
                        FROM ` + src_wrk_tbl + ` A
                        JOIN  (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, 
                                                              array_agg( object_construct('discountValue', NVL(rpd.value:tierAmt::string,0),
                                                                                         'dollarLimit',NVL(rpd.value:tierMaxDollarItemLimitAmt::string,0),
                                                                                         'itemLimit', NVL(rpd.value:tierItemLimitNbr::string,0),
                                                                                         'receiptText', NVL(rpd.value:tierReceiptTxt::string,''),
                                                                                         'weightLimit',NVL(rpd.value:weightvolumelimit:maxWeightLimitVal::string,0),
                                                                                          'amountLevel3',NVL(rpd.value:amountlevel3::string,0),
                                                                                         'amountTypeLevel2',NVL(rpd.value:discountAmountTypelevel2::string,''),
                                                                                         'percentOffLimitLevel1Amt',NVL(rpd.value:percentOffLimitLevel1Amt::string,0),
                                                                                         'percentOffLimitLevel2Amt',NVL(rpd.value:percentOffLimitLevel2Amt::string,0),
                                                                                         'amountLevel2',NVL(rpd.value:tierLevel2Amt::string,0)))  within group (order by rpd.index asc) AS TIERS,
                                                             array_agg(DISTINCT object_construct('amount', NVL(rpricess.value:specialPriceAmt::string,0),
                                                                                        'seqNumber',rpricess.index::number)) AS SPECIALPRICING
                                                           
                              FROM (SELECT DISTINCT INCENTIVEID, LASTUPDATETS, REWARDS_DISCOUNT_DISTRIBUTION_TIERS FROM ` + src_wrk_tbl + ` ) 
                              ,LATERAL FLATTEN(input => REWARDS_DISCOUNT_DISTRIBUTION_TIERS, outer => true)  rpd
                              ,LATERAL FLATTEN(input => rpd.VALUE:prices, outer => true ) AS rpricess
                              GROUP BY INCENTIVEID,LASTUPDATETS) B
                       ON A.INCENTIVEID = B.INCENTIVEID
                       AND A.LASTUPDATETS = B.LASTUPDATETS) K
                ON A.INCENTIVEID = K.INCENTIVEID
                AND A.LASTUPDATETS = K.LASTUPDATETS     
         ) 
         ) src
         where md5(src.payload) not in (select md5(payload) from `+ tgt_tbl + `)
         
    `;
   
          var sql_begin = "BEGIN"
          var sql_commit = "COMMIT"
          var sql_rollback = "ROLLBACK"
         
         try {
        snowflake.execute (
            {sqlText: sql_begin  }
        );
        snowflake.execute (
            {sqlText: sql_json  }
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
         
       
$$;
