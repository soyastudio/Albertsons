--liquibase formatted sql
--changeset SYSTEM:SP_JSON_EPE_PROMOTION runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_OUT>>;
use schema <<EDM_DB_NAME_OUT>>.DW_DCAT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_OUT>>.DW_DCAT.SP_JSON_EPE_PROMOTION()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
	
	// Globa Variables (including child sp''s variables)
	var cnf_out_db = "<<EDM_DB_NAME_OUT>>";
    var wrk_schema = "DW_DCAT";
    var cnf_out_schema = "DW_DCAT";
    var src_db = "<<EDM_DB_NAME_OUT>>";
    var src_schema = "DW_DCAT";
    var src_tbl = src_db + "." + src_schema + ".EPE_OFFER_O_STREAM";
    var src_wrk_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_WRK";
    var src_rerun_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_RERUN";
    var tgt_tbl = cnf_out_db + "." + cnf_out_schema + ".EPE_OFFER_JSON";
    
    
  
    //check if rerun queue table exists otherwise create it
    
     var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS 
								SELECT * FROM `+ src_tbl +` where 1=2 `;
								

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
				WHERE METADATA$ACTION = 'INSERT'
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
							   AS SELECT * FROM `+ src_tbl +` where 1=2 `;
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



 SELECT      'EDDW_C02_EPE_OFFER' AS topic,
                                    (OFFERID::string)||'|'||(OFFERSTARTDATEKEY::string)||'|'||(OFFERENDDATEKEY::string) AS key,
                                    to_json(object_construct(
                    				'source',  SOURCE,		
                    				'offerId', OFFERID,
                                    'offerStartDate', OFFERSTARTDATE,
							        'offerEndDate', OFFERENDDATE,
                                    'offerTestingStartDate', OFFERTESTINGSTARTDATE,
							        'offerTestingEndDate', OFFERTESTINGENDDATE,
							        'offerName', OFFERNAME,
                                    'linkPLUNumber',LINKPLUNUMBER,
							        'offerDescription', OFFERDESCRIPTION,
                                    'category', OFFERCATEGORY,
                                    'offerPriority', OFFERPRIORITY,
                                    'programCode', PROGRAMCODE,
                                    'deferEndOfSaleIndicator', DEFERENDOFSALEINDICATIOR,
                                    'offerLimit', OFFERLIMIT,
                                    'offerExternalId', Offer_External_Id,
                                    'location', object_construct('stores', STOREIDS,
                                                                 'terminalTypes', TERMINALTYPES),
                                    'conditions',object_construct('conditionJoinOperator',NVL(OFFER_CONDITION_JOIN_OPERATOR::string,''),
                                                                   'customerCondition',customerCondition,
                                                                   'productConditions',productCondition,
                                                                   'pointsProgram', object_construct('pointsGroupName', CP_POINTSPROGRAM_GROUPNAME,
                                                                                                     'tiers', CP_POINTSPROGRAM_TIERS),
                                                                   'triggerConditions',triggerCondition,
                                                                   'timeConditions', timeCondition,
                                                                   'dayCondition', dayCondition),
                                    'benefit', object_construct('discount',discount,
                                                                'pointsProgram',pointsprogram),
                                    'productGroups',PRODUCTGROUPS
                  
            )) AS payload
  FROM 
  (
     SELECT A.*, B.STOREIDS, C.TERMINALTYPES,I.OFFER_CONDITION_JOIN_OPERATOR,
            I.CP_POINTSPROGRAM_TIERS,I.dayCondition,I.customerCondition,I.TRIGGERCONDITION,I.timeCondition,I.productCondition,
            K.PRODUCTGROUPS, P.DISCOUNT, P.POINTSPROGRAM-- for aggreation json object 
     FROM
        ( 
         SELECT     DISTINCT NVL(OFFERID::string,'') AS OFFERID,
                    NVL(SOURCE::string,'') AS SOURCE,
                    NVL(to_char(OFFERSTARTDATE::timestamp_ntz, 'YYYY-MM-DD HH:MI:SS')::string,'') AS OFFERSTARTDATE,
                    NVL(to_char(OFFERENDDATE::timestamp_ntz, 'YYYY-MM-DD HH:MI:SS')::string,'') AS OFFERENDDATE,
                    NVL(OFFERSTARTDATE::string,'') AS OFFERSTARTDATEKEY,
                    NVL(OFFERENDDATE::string,'') AS OFFERENDDATEKEY,
                    CASE WHEN OFFERTESTINGSTARTDATE IS NULL THEN '' ELSE to_char(to_timestamp_ntz(OFFERTESTINGSTARTDATE),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGSTARTDATE,
                    CASE WHEN OFFERTESTINGENDDATE IS NULL THEN '' ELSE to_char(to_timestamp_ntz(OFFERTESTINGENDDATE),'YYYY-MM-DD HH:MI:SS')::string END AS OFFERTESTINGENDDATE,
                    NVL(OFFERNAME::string,'') AS OFFERNAME,
                    NVL(OFFERDESCRIPTION::string,'') AS OFFERDESCRIPTION,
                    NVL(LINKPLUNBR::string,0) AS LINKPLUNUMBER,
                    NVL(OFFERCATEGORY::string,'') AS OFFERCATEGORY,
                    NVL(OFFERPRIORITY_NM::string,'') AS OFFERPRIORITY,
                    case when DEFERENDOFSALEINDICATIOR = TRUE THEN TRUE ELSE FALSE END AS DEFERENDOFSALEINDICATIOR,
                    'SC' AS PROGRAMCODE,
                    NVL(OFFERID::string,'') AS Offer_External_Id,
                    object_construct('code',NVL(OFFERLIMIT_CODE::string,''),
                                     'limit', NVL(OFFERLIMIT_LIMIT::string,0),
                                     'periodType',NVL(OFFERLIMIT_PERIODTYPE::string,''),
                                     'periodQuantity',NVL(OFFERLIMIT_QUANTITY::string,0)) AS OFFERLIMIT,
                    NVL(CP_POINTSPROGRAM_GROUPNAME::string,'') AS CP_POINTSPROGRAM_GROUPNAME

        FROM ` + src_wrk_tbl + `
        WHERE  METADATA$ACTION = 'INSERT'        
        ) A -- MAIN DATA, FIRST LAYER
   
     JOIN -- Location information
       (
        SELECT     DISTINCT NVL(OFFERID::string,'') AS OFFERID,
                   array_agg(object_construct('storeNumber', NVL(STORENUMBER::string,''),
                                              'corporationId', NVL(CORPORATIONID::string,''),
                                              'divisionId', NVL(DIVISIONID::string,''))) AS STOREIDS
        FROM 
            (SELECT DISTINCT OFFERID, STORENUMBER, CORPORATIONID, DIVISIONID
             FROM ` + src_wrk_tbl + `)
        GROUP BY OFFERID
        ) B -- LOCATION DATA
    ON A.OFFERID = B.OFFERID
    JOIN 
        (
         SELECT    DISTINCT NVL(OFFERID::string,'') AS OFFERID,
                  array_agg(NVL(TERMINALTYPES::string,'')) AS TERMINALTYPES
         FROM 
            (SELECT DISTINCT OFFERID, TERMINALTYPES
            FROM ` + src_wrk_tbl + `)
        GROUP BY OFFERID
        ) C
    ON A.OFFERID = C.OFFERID
    JOIN 
        (
         SELECT DISTINCT NVL(D.OFFERID::string,'') AS OFFERID,  OFFER_CONDITION_JOIN_OPERATOR, CP_POINTSPROGRAM_TIERS, 
                                                                                   object_construct('MONDAY',case when DAYCONDITION_MON = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'TUESDAY',case when DAYCONDITION_TUE = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'WEDNESDAY',case when DAYCONDITION_WED = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'THURSDAY',case when DAYCONDITION_THU = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'FRIDAY',case when DAYCONDITION_FRI = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'SATURDAY',case when DAYCONDITION_SAT = TRUE THEN TRUE ELSE FALSE END,
                                                                                                    'SUNDAY',case when DAYCONDITION_SUN = TRUE THEN TRUE ELSE FALSE END) AS dayCondition,
                                                                                  object_construct('include', INCLUDE,
                                                                                                   'exclude', EXCLUDE) AS customerCondition,
                                                                                                   TRIGGERCONDITION,
                                                                                                   timeCondition,
                                                                                                   productCondition                                                                                      
        FROM ` + src_wrk_tbl + ` D
        JOIN 
            ( SELECT DISTINCT OFFERID, array_agg(object_construct('pointQty',NVL(CP_POINTSLIMIT::string,0))) AS CP_POINTSPROGRAM_TIERS
                                                                                                    
                  FROM (SELECT DISTINCT OFFERID, CP_POINTSPROGRAM_GROUPNAME	,CP_POINTSLIMIT,CP_POINT_TIER_ID
                    FROM ` + src_wrk_tbl + `)
                  GROUP BY OFFERID) E1
        ON D.OFFERID = E1.OFFERID
        JOIN( 
              SELECT DISTINCT OFFERID,  array_agg(object_construct('customerGroupNm',INCLUDE)) AS INCLUDE, 
                                        array_agg(EXCLUDE) AS EXCLUDE
                  FROM (SELECT DISTINCT OFFERID, CASE WHEN CUSTOMER_GROUP_IND = TRUE THEN CUSTOMER_GROUP_NM END AS INCLUDE,
                                                 CASE WHEN CUSTOMER_GROUP_IND = FALSE THEN CUSTOMER_GROUP_NM END AS EXCLUDE
                  FROM ` + src_wrk_tbl + `)
                  GROUP BY OFFERID ) E
        ON D.OFFERID = E.OFFERID
        JOIN 
            (SELECT DISTINCT OFFERID, array_agg(NVL(TRIGGERCODE::string,'')) AS triggerCondition
                FROM (SELECT DISTINCT OFFERID, TRIGGERCODE 
                FROM ` + src_wrk_tbl + `)
                GROUP BY OFFERID) F
        ON D.OFFERID = F.OFFERID
        JOIN (SELECT DISTINCT OFFERID, array_agg(object_construct('startTm',NVL(TIMECONDITION_STARTTIME::string,''), 
                                                                  'endTm',NVL(TIMECONDITION_ENDTIME::string,''))) AS timeCondition 
                FROM (SELECT DISTINCT OFFERID,TIMECONDITION_STARTTIME, TIMECONDITION_ENDTIME
                FROM ` + src_wrk_tbl + `)
                GROUP BY OFFERID) G
        ON D.OFFERID = G.OFFERID
        JOIN (SELECT DISTINCT OFFERID,array_agg(object_construct('productGroupName',NVL(CP_PRODUCT_GROUPNAME::string,''),
                                                                  'excGroupName','',
                                                                  'productConditionType', NVL(CP_PRODUCTCONDITIONTYPE::string,''),
                                                                  'productConditionOperator',NVL(CP_PRODUCT_COMBO_CONDITION_OPER_CD::string,''),
                                                                  'uniqueProductCondition', case when CP_UNIQUEPRODUCTCONDITION = TRUE THEN TRUE ELSE FALSE END,
                                                                  'minPurchaseAmount',NVL(CP_MINPURCHASEAMOUNT::string,0),
                                                                  'minProductQuantity',NVL(CP_MINPRODUCTQUANTITY::string,0), 
                                                                  'tiers', array_construct(object_construct('productAmt', NVL(CP_TIER_PRODUCT_QTY::string,0))))) AS productCondition
                                                                  
                      FROM ( SELECT DISTINCT OFFERID, CP_PRODUCT_GROUPNAME, CP_EXCLUDEINDICATOR,CP_PRODUCTCONDITIONTYPE,
                                        CP_PRODUCT_COMBO_CONDITION_OPER_CD,CP_UNIQUEPRODUCTCONDITION,CP_MINPURCHASEAMOUNT,CP_MINPRODUCTQUANTITY,
                                        CP_PRODUCT_TIER_ID, CP_TIER_PRODUCT_QTY 
                             FROM ` + src_wrk_tbl + `) 
                             GROUP BY OFFERID) H2
        ON D.OFFERID = H2.OFFERID) I
    ON A.OFFERID = I.OFFERID
    JOIN 
        (SELECT DISTINCT OFFERID, array_agg(object_construct('productGroupName',NVL(PRODUCTGROUPNAME::string,''),
                                                             'extProductId', NVL(EXTPRODUCTID::string,''),
                                                             'type', NVL(PRODUCTTYPE::string,''))) AS PRODUCTGROUPS
         FROM (SELECT DISTINCT OFFERID, PRODUCTGROUPNAME, EXTPRODUCTID,PRODUCTTYPE
                  FROM ` + src_wrk_tbl + `)
                  GROUP BY OFFERID) K
    ON A.OFFERID = K.OFFERID
    JOIN 
        (SELECT DISTINCT NVL(M.OFFERID::string,'') AS OFFERID, object_construct('discountLevel',NVL(DISCOUNTLEVEL::string,''),
                                                                                'specialPricing',specialPricing,
                                                                                'discountProducts',discountProducts,
                                                                                'scoreCard',object_construct('name', NVL(SCORECARD_NM::string,''),
                                                                                                            'lineText',NVL(SCORECARD_LINETXT::string,''),
                                                                                                            'enabled',case when SCORECARD_ENABLED = TRUE THEN TRUE ELSE FALSE END)) AS discount,
                                                      pointsProgram
                FROM ` + src_wrk_tbl + ` M
                JOIN (
                      SELECT DISTINCT OFFERID, array_agg(object_construct('seqNumber', NVL(SPECIALPRICING_SEQNUMBER::string,0),
                                                                          'amount',NVL(SPECIALPRICING_AMOUNT::string,0))) AS specialPricing
            
                      FROM (  SELECT DISTINCT OFFERID, SPECIALPRICING_SEQNUMBER, SPECIALPRICING_AMOUNT
                        FROM ` + src_wrk_tbl + `)
                      GROUP BY OFFERID) N
                ON M.OFFERID = N.OFFERID
                JOIN (
                      SELECT  DISTINCT OFFERID, array_agg(object_construct('productGroupName', NVL(DP_GROUPNAME::string,''),
                                                                           'couponFactor', NVL(DP_COUPONFACTOR::string,0),
                                                                           'excGroupName','', -- NVL(DP_EXCLUDEINDICATOR::string,''),
                                                                           'allowNegative', case when DP_ALLOWNEGATIVE = TRUE THEN TRUE ELSE FALSE END,
                                                                           'flexNegative', case when DP_FLEXNEGATIVE = TRUE THEN TRUE ELSE FALSE END,
                                                                           'discountType', NVL(DP_DISCOUNTTYPE::string,''),
                                                                           'tiers',array_construct(object_construct('discountValue', NVL(DP_DISCOUNTVALUE::string,0),
                                                                                                   'dollarLimit', NVL(DP_DOLLARLIMIT::string,0),
                                                                                                   'weightLimit', NVL(DP_WEIGHTLIMIT::string,0),
                                                                                                   'itemLimit', NVL(DP_ITEMLIMIT::string,0),
                                                                                                   'receiptText', NVL(DP_RECEIPTTEXT::string,''))))) AS discountProducts
                      FROM (SELECT DISTINCT OFFERID, DP_GROUPNAME, DP_COUPONFACTOR, DP_EXCLUDEINDICATOR, DP_ALLOWNEGATIVE, DP_FLEXNEGATIVE,DR_PRODUCT_TIER_ID,
                                            DP_DISCOUNTTYPE, DP_DISCOUNTVALUE, DP_DOLLARLIMIT, DP_WEIGHTLIMIT, DP_ITEMLIMIT, DP_RECEIPTTEXT 
                            FROM ` + src_wrk_tbl + `)
                            GROUP BY OFFERID) O
                ON M.OFFERID = O.OFFERID
            JOIN (
                  SELECT DISTINCT OFFERID, array_agg(object_construct('pointsGroupName', NVL(RW_POINTSPROGRAM_GROUPNAME::string,''),
                                                                      'pointsLimit', NVL(RW_POINTSLIMIT::string, 0),
                                                                      'scoreCard', object_construct('name','',
                                                                                                    'lineText','',
                                                                                                    'enabled', FALSE),
                                                                      'tiers', array_construct(object_construct('tierPointNbr',NVL(RW_POINTSPROGRAM_POINTS::string,0))))) AS POINTSPROGRAM
                    FROM (SELECT DISTINCT OFFERID,RW_POINTSPROGRAM_GROUPNAME,RW_POINTSLIMIT,RW_POINTSPROGRAM_POINTS 
                        FROM ` + src_wrk_tbl + `)
                    GROUP BY OFFERID) Q
           ON M.OFFERID = Q.OFFERID) P
      ON A.OFFERID = P.OFFERID
         )
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
