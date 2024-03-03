--liquibase formatted sql
--changeset SYSTEM:SP_LOAD_EPE_PROMOTION runOnChange:true splitStatements:false OBJECT_TYPE:SP

USE DATABASE EDM_CONFIRMED_OUT_PRD;
USE SCHEMA DW_DCAT;

CREATE OR REPLACE PROCEDURE SP_LOAD_EPE_PROMOTION()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

    var tgt_db = "EDM_CONFIRMED_OUT_PRD";
    var tgt_schema = "DW_DCAT";
    var src_db = "EDM_CONFIRMED_PRD";
    var src_schema = "DW_C_PRODUCT";
    var src_loc_schema = "DW_C_LOCATION";
	var tgt_exp_schema = "DW_STAGE";
    var tgt_new_img = tgt_db + "." + "DW_DCAT" + ".EPE_OFFER_NEW_IMG";
    var tgt_tbl = tgt_db + "." + tgt_schema + ".EPE_OFFER";
    var tgt_tmp_tbl = tgt_db + "." + "DW_DCAT" + ".EPE_OFFER_TMP_TBL";
    var tgt_excep_tbl = tgt_db + "." + tgt_schema + ".EPE_OFFER_EXCEPTIONS";
    
    // Creating New IMG 
    sql_create_new_img = `CREATE OR REPLACE TABLE `+ tgt_new_img +` as 
  
        (
        
        SELECT DISTINCT    CAST(CONCAT(PROMOTION_STORE.FACILITY_INTEGRATION_ID, NVL(LINK_PLU_NBR,0), 
                                        to_number(to_char(PROMOTION_START_DT,'YYYYMMDD'),'99999999'),
                                        to_number(to_char(PROMOTION_END_DT,'YYYYMMDD'),'99999999')) AS INTEGER) AS OFFERID,
        'CMS' AS SOURCE, 
        NULL AS  PROGRAMCODE,
        NVL(PROMOTION_STORE.LINK_PLU_NBR,0) AS LINKPLUNBR,
        PROMOTION_STORE.PROMOTION_START_DT AS OFFERSTARTDATE,
        PROMOTION_STORE.PROMOTION_END_DT AS OFFERENDDATE,
        NULL AS OFFERCATEGORY,
        NULL AS OFFERTESTINGSTARTDATE,
        NULL AS OFFERTESTINGENDDATE,
        'ONCE PER TRANSACTION' AS OFFERLIMIT_CODE, 
        PROMOTION_STORE.COUPON_LIMIT_QTY AS OFFERLIMIT_LIMIT,
        NULL AS OFFERLIMIT_PERIODTYPE,
        NULL AS OFFERLIMIT_QUANTITY,
        NULL AS DEFERENDOFSALEINDICATIOR,
        FACILITY.FACILITY_NBR AS STORENUMBER,
        FACILITY.CORPORATION_ID AS CORPORATIONID, 
        FACILITY.DIVISION_ID AS DIVISIONID,
        'ALL TERMINAL' AS TERMINALTYPES,
        'CMS COUPON' AS OFFERNAME,
        'CMS DESCRIPTION' AS OFFERDESCRIPTION,
        'HIGH' AS OFFERPRIORITY_NM,
        'AND' AS OFFER_CONDITION_JOIN_OPERATOR,
        'ANY CARDHOLDER' AS CUSTOMER_GROUP_NM,
        TRUE AS CUSTOMER_GROUP_IND, 
        CONCAT('PROD_GROUP_',CAST(CONCAT(PROMOTION_STORE.FACILITY_INTEGRATION_ID, NVL(LINK_PLU_NBR,0), 
		to_number(to_char(PROMOTION_START_DT,'YYYYMMDD'),'99999999'),
                                        to_number(to_char(PROMOTION_END_DT,'YYYYMMDD'),'99999999')) AS INTEGER)) AS CP_PRODUCT_GROUPNAME,
        FALSE AS CP_EXCLUDEINDICATOR,
        CASE WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'CE' THEN 'Items'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'PE' THEN 'Items'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'NE' THEN 'Items'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'CW' THEN 'Weight/Volume'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'PW' THEN 'Weight/Volume'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'NW' THEN 'Weight/Volume'
             ELSE NULL 
             END AS CP_PRODUCTCONDITIONTYPE,
        'AND' AS CP_PRODUCT_COMBO_CONDITION_OPER_CD,
        FALSE AS CP_UNIQUEPRODUCTCONDITION,
        NULL AS CP_MINPURCHASEAMOUNT,
        PROMOTION_STORE.MINIMUM_PURCHASE_QTY AS CP_MINPRODUCTQUANTITY,
        NULL AS CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
        NULL AS CP_PRODUCT_TIER_ID,
        NULL AS CP_TIER_PRODUCT_QTY,
        NULL AS CP_POINTSPROGRAM_GROUPNAME,
        NULL AS CP_POINTSLIMIT,
        NULL AS CP_POINT_TIER_ID,
        NULL AS TRIGGERCODE,
        NULL AS TIMECONDITION_STARTTIME,
        NULL AS TIMECONDITION_ENDTIME,
        NULL AS DAYCONDITION_MON,
        NULL AS DAYCONDITION_TUE,
        NULL AS DAYCONDITION_WED,
        NULL AS DAYCONDITION_THU,
        NULL AS DAYCONDITION_FRI,
        NULL AS DAYCONDITION_SAT,
        NULL AS DAYCONDITION_SUN,
        'Item Level' AS DISCOUNTLEVEL,
        CONCAT('PROD_GROUP_',CAST(CONCAT(PROMOTION_STORE.FACILITY_INTEGRATION_ID, NVL(LINK_PLU_NBR,0), 
                                        to_number(to_char(PROMOTION_START_DT,'YYYYMMDD'),'99999999'),
                                        to_number(to_char(PROMOTION_END_DT,'YYYYMMDD'),'99999999')) AS INTEGER)) AS DP_GROUPNAME,
        CASE WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'CE' THEN 'Cents Off'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'PE' THEN 'Percent Off'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'NE' THEN 'Price Point'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'CW' THEN 'Cents Off'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'PW' THEN 'Percent Off'
             WHEN PROMOTION_STORE.COUPON_METHOD_CD = 'NW' THEN 'Price Point'
             ELSE NULL 
             END AS DP_DISCOUNTTYPE,
        PROMOTION_STORE.COUPON_AMT AS DP_DISCOUNTVALUE,
        FALSE AS DP_EXCLUDEINDICATOR,
        PROMOTION_STORE.ORIGINAL_COUPON_FCTR AS DP_COUPONFACTOR,
        NULL AS DP_DOLLARLIMIT,
        NULL AS DP_WEIGHTLIMIT,
        NULL AS DP_ITEMLIMIT,
        NULL AS DP_RECEIPTTEXT,
        FALSE AS DP_ALLOWNEGATIVE,
        FALSE AS DP_FLEXNEGATIVE,
        NULL AS SPECIALPRICING_SEQNUMBER, 
        NULL AS SPECIALPRICING_AMOUNT,
        NULL AS SCORECARD_NM,
        NULL AS SCORECARD_ENABLED,
        NULL AS SCORECARD_LINETXT,
        NULL AS DR_PRODUCT_TIER_ID,
        NULL AS RW_POINTSPROGRAM_GROUPNAME,
        NULL AS RW_POINTSLIMIT,
        NULL AS RW_POINTSPROGRAM_POINTS,
        CONCAT('PROD_GROUP_',CAST(CONCAT(PROMOTION_STORE.FACILITY_INTEGRATION_ID, NVL(LINK_PLU_NBR,0), 
                                        to_number(to_char(PROMOTION_START_DT,'YYYYMMDD'),'99999999'),
                                        to_number(to_char(PROMOTION_END_DT,'YYYYMMDD'),'99999999')) AS INTEGER)) AS  PRODUCTGROUPNAME,
        PROMOTION_STORE.UPC_NBR AS EXTPRODUCTID,
        CASE WHEN LENGTH(PROMOTION_STORE.UPC_NBR) > 7 THEN 'UPC'
             ELSE 'PLU' 
             END AS PRODUCTTYPE,
        PROMOTION_STORE.DW_SOURCE_CREATE_NM AS DW_SOURCE_CREATE_NM,
        PROMOTION_STORE.DW_CREATE_TS AS DW_CREATE_TS,
        PROMOTION_STORE.DW_LAST_UPDATE_TS AS DW_LAST_UPDATE_TS,
        PROMOTION_STORE.DW_CURRENT_VERSION_IND AS DW_CURRENT_VERSION_IND,
        PROMOTION_STORE.DW_LOGICAL_DELETE_IND AS DW_LOGICAL_DELETE_IND,
        NULL AS EXCEP_FLAG
        
        FROM `+ src_db + `.` + src_schema + `.PROMOTION_STORE 
        LEFT JOIN ` + src_db + `.` + src_loc_schema + `.FACILITY
        ON PROMOTION_STORE.FACILITY_INTEGRATION_ID = FACILITY.FACILITY_INTEGRATION_ID 
          AND PROMOTION_STORE.DW_CURRENT_VERSION_IND = TRUE
          AND PROMOTION_STORE.DW_LOGICAL_DELETE_IND = FALSE
          AND FACILITY.DW_CURRENT_VERSION_IND = TRUE
          AND FACILITY.DW_LOGICAL_DELETE_IND = FALSE
        WHERE PROMOTION_STORE.DW_CURRENT_VERSION_IND = TRUE
          AND PROMOTION_STORE.DW_LOGICAL_DELETE_IND = FALSE
          AND PROMOTION_STORE.PROMOTION_START_DT IS NOT NULL
          AND PROMOTION_STORE.PROMOTION_END_DT IS NOT NULL
		  AND PROMOTION_STORE.MINIMUM_PURCHASE_QTY >= 2
          AND PROMOTION_STORE.PROMOTION_END_DT > DATEADD(DAY, -30,GETDATE()))
        `; 
       
       
        try {
                snowflake.execute (
                    {sqlText: sql_create_new_img }
                    );
            }
        catch (err)  {
        throw "Creation of Item new image table "+ tgt_new_img +" Failed with error: " + err;   // Return a error message.
        }; 
      
          
    // Creating TMP_TBL with Insert and Delete status              
    var sql_crt_tmp_tbl = `CREATE OR REPLACE TABLE `+ tgt_tmp_tbl +` AS 
                  
               SELECT   CASE WHEN NEW.OFFERID IS NULL THEN OLD.OFFERID ELSE NEW.OFFERID END AS OFFERID,
                        CASE WHEN NEW.OFFERID IS NULL THEN 'Delete' ELSE 'Insert' END AS ACTIONITEM
                
                FROM 
                    (SELECT DISTINCT OFFERID
                      FROM `+ tgt_new_img+ ` 
                      WHERE EXCEP_FLAG IS NULL) NEW
                FULL OUTER JOIN 
                    (
                     SELECT DISTINCT OFFERID
                        FROM `+ tgt_tbl +`   
                        WHERE DW_CURRENT_VERSION_IND = TRUE
                        AND DW_LOGICAL_DELETE_IND = FALSE) OLD
                ON NEW.OFFERID = OLD.OFFERID
                WHERE NEW.OFFERID IS NULL OR OLD.OFFERID IS NULL
                `;
         
        try {
        snowflake.execute (
           {sqlText: sql_crt_tmp_tbl }
          );
        }
    catch (err)  {
     throw "Creation of temp table with deletes and inserts "+ tgt_tmp_tbl +" Failed with error: " + err;   // Return a error message.
     };
     
                    
    // Define the rows with Update Status                  
    var sql_ins_tmp_tbl  = `INSERT INTO `+tgt_tmp_tbl +`
                            (OFFERID, 
                            ACTIONITEM)
           
                            SELECT      DISTINCT CASE WHEN OLD.OFFERID IS NULL THEN NEW.OFFERID ELSE OLD.OFFERID END AS OFFERID,
                                        'Update' AS ACTIONITEM
            
                            FROM (SELECT DISTINCT A.OFFERID, A.HASH_AGG 
                            FROM (SELECT OFFERID, 
                                   HASH_AGG(NVL(CAST(SOURCE AS VARCHAR(100)),'0'),NVL(CAST(OFFERID AS NUMBER(38,0)),'0'), NVL(CAST(PROGRAMCODE AS VARCHAR(16777216)),'O'),
                                            NVL(CAST(LINKPLUNBR AS NUMBER(12,0)),'0') ,NVL(CAST(OFFERSTARTDATE AS DATE),'0'),
                                            NVL(CAST(OFFERENDDATE AS DATE),'0'),NVL(CAST(OFFERCATEGORY AS VARCHAR(255)),'0'),NVL(CAST(OFFERTESTINGSTARTDATE AS DATE),'0'),
                                            NVL(CAST(OFFERTESTINGENDDATE AS DATE),'0'),NVL(CAST(OFFERLIMIT_CODE AS VARCHAR(16777216)),'0'),
                                            NVL(CAST(OFFERLIMIT_LIMIT AS NUMBER(38,0)),'0'),NVL(CAST(OFFERLIMIT_PERIODTYPE AS VARCHAR(16777216)),'0'),NVL(CAST(OFFERLIMIT_QUANTITY AS NUMBER(38,0)),'0'),
                                            NVL(CAST(DEFERENDOFSALEINDICATIOR AS BOOLEAN),'0'),NVL(CAST(STORENUMBER AS VARCHAR(16777216)),'0'),
                                            NVL(CAST(CORPORATIONID AS VARCHAR(3)),'0'),NVL(CAST(DIVISIONID AS VARCHAR(10)),'0'),NVL(CAST(TERMINALTYPES AS VARCHAR(1000)),'0'),
                                            NVL(CAST(OFFERNAME AS VARCHAR(100)),'0'),NVL(CAST(OFFERDESCRIPTION AS VARCHAR(16777216)),'0'),
                                            NVL(CAST(OFFERPRIORITY_NM AS VARCHAR(50)),'0'),NVL(CAST(OFFER_CONDITION_JOIN_OPERATOR AS VARCHAR(10)),'0'),NVL(CAST(CUSTOMER_GROUP_NM AS VARCHAR(255)),'0'),
                                            NVL(CAST(CP_PRODUCT_GROUPNAME AS VARCHAR(200)),'0'),NVL(CAST(CP_PRODUCTCONDITIONTYPE AS VARCHAR(20)),'0'), 
                                            NVL(CAST(CP_PRODUCT_COMBO_CONDITION_OPER_CD AS VARCHAR(20)),'0'),NVL(CAST(CP_UNIQUEPRODUCTCONDITION AS BOOLEAN),'0'),
                                            NVL(CAST(CP_MINPURCHASEAMOUNT AS NUMBER(38,2)),'0'),NVL(CAST(CP_MINPRODUCTQUANTITY AS NUMBER(38,2)),'0'),NVL(CAST(CP_POINTSPROGRAM_GROUPNAME AS VARCHAR(255)) ,'0'),
                                            NVL(CAST(CP_POINTSLIMIT AS NUMBER(38,0)),'0'),NVL(CAST(TRIGGERCODE AS VARCHAR(16777216)),'0'),
                                            NVL(CAST(TIMECONDITION_STARTTIME AS TIME(9)),'0'),NVL(CAST(TIMECONDITION_ENDTIME AS TIME(9)),'0'),NVL(CAST(DAYCONDITION_MON AS BOOLEAN),'0'),
                                            NVL(CAST(DAYCONDITION_TUE AS BOOLEAN),'0'),NVL(CAST(DAYCONDITION_WED AS BOOLEAN),'0'), NVL(CAST(DAYCONDITION_THU AS BOOLEAN),'0'),NVL(CAST(DAYCONDITION_FRI AS BOOLEAN),'0'),
                                            NVL(CAST(DAYCONDITION_SAT AS BOOLEAN),'0'),NVL(CAST(DAYCONDITION_SUN AS BOOLEAN),'0') ,NVL(CAST(DISCOUNTLEVEL AS VARCHAR(16777216)),'0'),
                                            NVL(CAST(DP_GROUPNAME AS VARCHAR(100)),'0'),NVL(CAST(DP_DISCOUNTTYPE AS VARCHAR(16777216)),'0'),NVL(CAST(DP_DISCOUNTVALUE AS NUMBER(38,3)),'0'),
                                            NVL(CAST(DP_COUPONFACTOR AS NUMBER(2,0)),'0'),NVL(CAST(DP_DOLLARLIMIT AS NUMBER(38,3)),'0'),NVL(CAST(DP_WEIGHTLIMIT AS NUMBER(38,3)),'0'),
                                            NVL(CAST(DP_ITEMLIMIT AS NUMBER(38,0)),'0'),NVL(CAST(DP_RECEIPTTEXT AS VARCHAR(100)),'0'),NVL(CAST(DP_ALLOWNEGATIVE AS BOOLEAN),'0'),
                                            NVL(CAST(DP_FLEXNEGATIVE AS BOOLEAN),'0'),NVL(CAST(SPECIALPRICING_SEQNUMBER AS NUMBER(38,0)),'0'),NVL(CAST(SPECIALPRICING_AMOUNT AS NUMBER(38,3)),'0'),
                                            NVL(CAST(SCORECARD_NM AS VARCHAR(100)),'0'),NVL(CAST(SCORECARD_ENABLED AS BOOLEAN),'0'),
                                            NVL(CAST(SCORECARD_LINETXT AS VARCHAR(100)),'0'),NVL(CAST(RW_POINTSPROGRAM_GROUPNAME AS VARCHAR(255)),'0'),NVL(CAST(RW_POINTSLIMIT AS NUMBER(38,0)),'0'),
                                            NVL(CAST(RW_POINTSPROGRAM_POINTS AS NUMBER(38,0)),'0'),NVL(CAST(PRODUCTGROUPNAME AS VARCHAR(200)),'0'),
                                            NVL(CAST(EXTPRODUCTID AS VARCHAR(16777216)),'0'),NVL(CAST(PRODUCTTYPE AS VARCHAR(16777216)),'0')
                                           ) 
                                            AS HASH_AGG FROM `+ tgt_new_img+ `
                                                        GROUP BY OFFERID) A
                                                 JOIN (SELECT OFFERID FROM `+ tgt_new_img+ `
                                                                      WHERE OFFERID NOT IN (SELECT OFFERID FROM `+tgt_tmp_tbl +`
                                                                                                           WHERE ACTIONITEM = 'Insert') 
                                                                                  AND EXCEP_FLAG IS NULL) B
                                                 ON A.OFFERID = B.OFFERID ) NEW
               JOIN
                     (SELECT DISTINCT C.OFFERID, C.HASH_AGG 
                      FROM (SELECT OFFERID, 
                                   HASH_AGG(NVL(SOURCE,'0'),NVL(OFFERID,'0'),NVL(PROGRAMCODE,'O'),NVL(LINKPLUNBR,'0'),NVL(OFFERSTARTDATE,'0'),
                                            NVL(OFFERENDDATE,'0'),NVL(OFFERCATEGORY,'0'),NVL(OFFERTESTINGSTARTDATE,'0'),NVL(OFFERTESTINGENDDATE,'0'),NVL(OFFERLIMIT_CODE,'0'),
                                            NVL(OFFERLIMIT_LIMIT,'0'),NVL(OFFERLIMIT_PERIODTYPE,'0'),NVL(OFFERLIMIT_QUANTITY,'0'),NVL(DEFERENDOFSALEINDICATIOR,'0'),NVL(STORENUMBER,'0'),
                                            NVL(CORPORATIONID,'0'),NVL(DIVISIONID,'0'),NVL(TERMINALTYPES,'0'),NVL(OFFERNAME,'0'),NVL(OFFERDESCRIPTION,'0'),
                                            NVL(OFFERPRIORITY_NM,'0'),NVL(OFFER_CONDITION_JOIN_OPERATOR,'0'),NVL(CUSTOMER_GROUP_NM,'0'),NVL(CP_PRODUCT_GROUPNAME,'0'),
                                            NVL(CP_PRODUCTCONDITIONTYPE,'0'), NVL(CP_PRODUCT_COMBO_CONDITION_OPER_CD,'0'),NVL(CP_UNIQUEPRODUCTCONDITION,'0') ,
                                            NVL(CP_MINPURCHASEAMOUNT,'0'),NVL(CP_MINPRODUCTQUANTITY,'0'),NVL(CP_POINTSPROGRAM_GROUPNAME,'0'),NVL(CP_POINTSLIMIT,'0'),NVL(TRIGGERCODE,'0'),
                                            NVL(TIMECONDITION_STARTTIME,'0'),NVL(TIMECONDITION_ENDTIME,'0'),NVL(DAYCONDITION_MON,'0'),NVL(DAYCONDITION_TUE,'0'),NVL(DAYCONDITION_WED,'0'),
                                            NVL(DAYCONDITION_THU,'0'),NVL(DAYCONDITION_FRI,'0'),NVL(DAYCONDITION_SAT,'0'),NVL(DAYCONDITION_SUN,'0') ,NVL(DISCOUNTLEVEL,'0'),
                                            NVL(DP_GROUPNAME,'0'),NVL(DP_DISCOUNTTYPE,'0'),NVL(DP_DISCOUNTVALUE,'0'),NVL(DP_COUPONFACTOR,'0'),
                                            NVL(DP_DOLLARLIMIT,'0'),NVL(DP_WEIGHTLIMIT,'0'),NVL(DP_ITEMLIMIT,'0'),NVL(DP_RECEIPTTEXT,'0'),NVL(DP_ALLOWNEGATIVE,'0'),
                                            NVL(DP_FLEXNEGATIVE,'0'),NVL(SPECIALPRICING_SEQNUMBER,'0'),NVL(SPECIALPRICING_AMOUNT,'0'),NVL(SCORECARD_NM,'0') ,NVL(SCORECARD_ENABLED,'0'),
                                            NVL(SCORECARD_LINETXT,'0'),NVL(RW_POINTSPROGRAM_GROUPNAME,'0'),NVL(RW_POINTSLIMIT,'0'),NVL(RW_POINTSPROGRAM_POINTS,'0'),NVL(PRODUCTGROUPNAME,'0'),
                                            NVL(EXTPRODUCTID,'0') ,NVL(PRODUCTTYPE,'0')
                                           ) 
                                            AS HASH_AGG FROM `+ tgt_tbl +`
											WHERE DW_LOGICAL_DELETE_IND = FALSE
                                                        GROUP BY OFFERID) C
                                               JOIN (SELECT OFFERID FROM `+ tgt_tbl +`
                                                                    WHERE OFFERID NOT IN (SELECT OFFERID FROM `+tgt_tmp_tbl +`
                                                                                                         WHERE ACTIONITEM = 'Delete')                                                 
                                                                                AND DW_CURRENT_VERSION_IND = TRUE
                                                                                AND DW_LOGICAL_DELETE_IND = FALSE) D
                                               ON C.OFFERID = D.OFFERID) OLD
              ON NEW.OFFERID = OLD.OFFERID
              AND NEW.HASH_AGG <> OLD.HASH_AGG 
                      `;
                      
        try {
                snowflake.execute (
                    {sqlText: sql_ins_tmp_tbl }
                    );
        }
        catch (err)  {
        throw "Inserting updates data into temp table "+ tgt_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        };		
    
            
    //Insert rows with Insert status in tmp_tbl into Target_Table
    var sql_tgt_insert = `INSERT INTO `+ tgt_tbl + ` 
         
                (   SOURCE , 
                    OFFERID,
                    PROGRAMCODE,
                    LINKPLUNBR,
                    OFFERSTARTDATE,
                    OFFERENDDATE,
                    OFFERCATEGORY,
                    OFFERTESTINGSTARTDATE,
                    OFFERTESTINGENDDATE,
                    OFFERLIMIT_CODE,
                    OFFERLIMIT_LIMIT,
                    OFFERLIMIT_PERIODTYPE,
                    OFFERLIMIT_QUANTITY,
                    DEFERENDOFSALEINDICATIOR,
                    STORENUMBER,
                    CORPORATIONID,
                    DIVISIONID,
                    TERMINALTYPES,
                    OFFERNAME,
                    OFFERDESCRIPTION,
                    OFFERPRIORITY_NM,
                    OFFER_CONDITION_JOIN_OPERATOR,
                    CUSTOMER_GROUP_NM, 
                    CUSTOMER_GROUP_IND, 
                    CP_PRODUCT_GROUPNAME,
                    CP_EXCLUDEINDICATOR,
                    CP_PRODUCTCONDITIONTYPE,
                    CP_PRODUCT_COMBO_CONDITION_OPER_CD,
                    CP_UNIQUEPRODUCTCONDITION,
                    CP_MINPURCHASEAMOUNT,
                    CP_MINPRODUCTQUANTITY,
                    CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
                    CP_PRODUCT_TIER_ID,
                    CP_TIER_PRODUCT_QTY,
                    CP_POINTSPROGRAM_GROUPNAME,
                    CP_POINTSLIMIT,
                    CP_POINT_TIER_ID,
                    TRIGGERCODE,
                    TIMECONDITION_STARTTIME,
                    TIMECONDITION_ENDTIME,
                    DAYCONDITION_MON,
                    DAYCONDITION_TUE,
                    DAYCONDITION_WED,
                    DAYCONDITION_THU,
                    DAYCONDITION_FRI,
                    DAYCONDITION_SAT,
                    DAYCONDITION_SUN,
                    DISCOUNTLEVEL,
                    DP_GROUPNAME,
                    DP_DISCOUNTTYPE,
                    DP_DISCOUNTVALUE,
                    DP_EXCLUDEINDICATOR,
                    DP_COUPONFACTOR,
                    DP_DOLLARLIMIT,
                    DP_WEIGHTLIMIT,
                    DP_ITEMLIMIT,
                    DP_RECEIPTTEXT,
                    DP_ALLOWNEGATIVE,
                    DP_FLEXNEGATIVE,
                    SPECIALPRICING_SEQNUMBER, 
                    SPECIALPRICING_AMOUNT,
                    SCORECARD_NM,
                    SCORECARD_ENABLED,
                    SCORECARD_LINETXT,
                    DR_PRODUCT_TIER_ID,
                    RW_POINTSPROGRAM_GROUPNAME,
                    RW_POINTSLIMIT,
                    RW_POINTSPROGRAM_POINTS,
                    PRODUCTGROUPNAME,
                    EXTPRODUCTID,
                    PRODUCTTYPE,
                    DW_SOURCE_CREATE_NM,
                    DW_CREATE_TS,
                    DW_LAST_UPDATE_TS,
                    DW_CURRENT_VERSION_IND,
                    DW_LOGICAL_DELETE_IND,
                    ACTIONITEM
                    )
                    
                    SELECT 
                          SOURCE , 
                          NEW.OFFERID,
                          PROGRAMCODE,
                          LINKPLUNBR,
                          OFFERSTARTDATE,
                          OFFERENDDATE,
                          OFFERCATEGORY,
                          OFFERTESTINGSTARTDATE,
                          OFFERTESTINGENDDATE,
                          OFFERLIMIT_CODE,
                          OFFERLIMIT_LIMIT,
                          OFFERLIMIT_PERIODTYPE,
                          OFFERLIMIT_QUANTITY,
                          DEFERENDOFSALEINDICATIOR,
                          STORENUMBER,
                          CORPORATIONID,
                          DIVISIONID,
                          TERMINALTYPES,
                          OFFERNAME,
                          OFFERDESCRIPTION,
                          OFFERPRIORITY_NM,
                          OFFER_CONDITION_JOIN_OPERATOR,
                          CUSTOMER_GROUP_NM, 
                          CUSTOMER_GROUP_IND, 
                          CP_PRODUCT_GROUPNAME,
                          CP_EXCLUDEINDICATOR,
                          CP_PRODUCTCONDITIONTYPE,
                          CP_PRODUCT_COMBO_CONDITION_OPER_CD,
                          CP_UNIQUEPRODUCTCONDITION,
                          CP_MINPURCHASEAMOUNT,
                          CP_MINPRODUCTQUANTITY,
                          CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
                          CP_PRODUCT_TIER_ID,
                          CP_TIER_PRODUCT_QTY,
                          CP_POINTSPROGRAM_GROUPNAME,
                          CP_POINTSLIMIT,
                          CP_POINT_TIER_ID,
                          TRIGGERCODE,
                          TIMECONDITION_STARTTIME,
                          TIMECONDITION_ENDTIME,
                          DAYCONDITION_MON,
                          DAYCONDITION_TUE,
                          DAYCONDITION_WED,
                          DAYCONDITION_THU,
                          DAYCONDITION_FRI,
                          DAYCONDITION_SAT,
                          DAYCONDITION_SUN,
                          DISCOUNTLEVEL,
                          DP_GROUPNAME,
                          DP_DISCOUNTTYPE,
                          DP_DISCOUNTVALUE,
                          DP_EXCLUDEINDICATOR,
                          DP_COUPONFACTOR,
                          DP_DOLLARLIMIT,
                          DP_WEIGHTLIMIT,
                          DP_ITEMLIMIT,
                          DP_RECEIPTTEXT,
                          DP_ALLOWNEGATIVE,
                          DP_FLEXNEGATIVE,
                          SPECIALPRICING_SEQNUMBER, 
                          SPECIALPRICING_AMOUNT,
                          SCORECARD_NM,
                          SCORECARD_ENABLED,
                          SCORECARD_LINETXT,
                          DR_PRODUCT_TIER_ID,
                          RW_POINTSPROGRAM_GROUPNAME,
                          RW_POINTSLIMIT,
                          RW_POINTSPROGRAM_POINTS,
                          PRODUCTGROUPNAME,
                          EXTPRODUCTID,
                          PRODUCTTYPE,
                          DW_SOURCE_CREATE_NM,
                          DW_CREATE_TS,
                          NEW.DW_LAST_UPDATE_TS,
                          DW_CURRENT_VERSION_IND,
                          DW_LOGICAL_DELETE_IND,
                          TMP.ACTIONITEM
                          
                          FROM  (      SELECT * FROM `+ tgt_tmp_tbl +`
                                                WHERE ACTIONITEM = 'Insert') TMP
                          JOIN `+ tgt_new_img + ` NEW
                          ON TMP.OFFERID = NEW.OFFERID
                          AND NEW.EXCEP_FLAG IS NULL
                  `;    
              
    // Processing update, updating DW_CURRENT_VERSION_IND = FALSE
    
        var sql_tgt_delete = `UPDATE  `+ tgt_tbl +` tgt
                        SET  tgt.DW_LOGICAL_DELETE_IND =TRUE,tgt.OFFERENDDATE=current_date()-1,tgt.dw_create_ts=current_timestamp()
                             
                                 WHERE OFFERID IN (SELECT OFFERID FROM `+ tgt_tmp_tbl +`
                                                                  WHERE ACTIONITEM = 'Delete' )
                               
                  `;
                 
				 
    //Processing Updates first step delete all rows that will be updated from target table
    var sql_tgt_upd_step1 = `DELETE FROM `+ tgt_tbl +` tgt
                                   WHERE OFFERID IN (SELECT OFFERID FROM `+ tgt_tmp_tbl +`
                                                                    WHERE ACTIONITEM = 'Update' )
                                   AND tgt.DW_CURRENT_VERSION_IND = TRUE
                                   AND tgt.DW_LOGICAL_DELETE_IND = FALSE
                   
                  `;
    
    //Processing Updates second step insert rows into target table
                  
    var sql_tgt_upd_step2 =  `INSERT INTO `+ tgt_tbl + ` 
                                 (SOURCE , 
                                  OFFERID,
                                  PROGRAMCODE,
                                  LINKPLUNBR,
                                  OFFERSTARTDATE,
                                  OFFERENDDATE,
                                  OFFERCATEGORY,
                                  OFFERTESTINGSTARTDATE,
                                  OFFERTESTINGENDDATE,
                                  OFFERLIMIT_CODE,
                                  OFFERLIMIT_LIMIT,
                                  OFFERLIMIT_PERIODTYPE,
                                  OFFERLIMIT_QUANTITY,
                                  DEFERENDOFSALEINDICATIOR,
                                  STORENUMBER,
                                  CORPORATIONID,
                                  DIVISIONID,
                                  TERMINALTYPES,
                                  OFFERNAME,
                                  OFFERDESCRIPTION,
                                  OFFERPRIORITY_NM,
                                  OFFER_CONDITION_JOIN_OPERATOR,
                                  CUSTOMER_GROUP_NM, 
                                  CUSTOMER_GROUP_IND, 
                                  CP_PRODUCT_GROUPNAME,
                                  CP_EXCLUDEINDICATOR,
                                  CP_PRODUCTCONDITIONTYPE,
                                  CP_PRODUCT_COMBO_CONDITION_OPER_CD,
                                  CP_UNIQUEPRODUCTCONDITION,
                                  CP_MINPURCHASEAMOUNT,
                                  CP_MINPRODUCTQUANTITY,
                                  CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
                                  CP_PRODUCT_TIER_ID,
                                  CP_TIER_PRODUCT_QTY,
                                  CP_POINTSPROGRAM_GROUPNAME,
                                  CP_POINTSLIMIT,
                                  CP_POINT_TIER_ID,
                                  TRIGGERCODE,
                                  TIMECONDITION_STARTTIME,
                                  TIMECONDITION_ENDTIME,
                                  DAYCONDITION_MON,
                                  DAYCONDITION_TUE,
                                  DAYCONDITION_WED,
                                  DAYCONDITION_THU,
                                  DAYCONDITION_FRI,
                                  DAYCONDITION_SAT,
                                  DAYCONDITION_SUN,
                                  DISCOUNTLEVEL,
                                  DP_GROUPNAME,
                                  DP_DISCOUNTTYPE,
                                  DP_DISCOUNTVALUE,
                                  DP_EXCLUDEINDICATOR,
                                  DP_COUPONFACTOR,
                                  DP_DOLLARLIMIT,
                                  DP_WEIGHTLIMIT,
                                  DP_ITEMLIMIT,
                                  DP_RECEIPTTEXT,
                                  DP_ALLOWNEGATIVE,
                                  DP_FLEXNEGATIVE,
                                  SPECIALPRICING_SEQNUMBER, 
                                  SPECIALPRICING_AMOUNT,
                                  SCORECARD_NM,
                                  SCORECARD_ENABLED,
                                  SCORECARD_LINETXT,
                                  DR_PRODUCT_TIER_ID,
                                  RW_POINTSPROGRAM_GROUPNAME,
                                  RW_POINTSLIMIT,
                                  RW_POINTSPROGRAM_POINTS,
                                  PRODUCTGROUPNAME,
                                  EXTPRODUCTID,
                                  PRODUCTTYPE,
                                  DW_SOURCE_CREATE_NM,
                                  DW_CREATE_TS,
                                  DW_LAST_UPDATE_TS,
                                  DW_CURRENT_VERSION_IND,
                                  DW_LOGICAL_DELETE_IND,
                                  ACTIONITEM
                                  )
                                  SELECT  SOURCE , 
                                          NEW.OFFERID,
                                          PROGRAMCODE,
                                          LINKPLUNBR,
                                          OFFERSTARTDATE,
                                          OFFERENDDATE,
                                          OFFERCATEGORY,
                                          OFFERTESTINGSTARTDATE,
                                          OFFERTESTINGENDDATE,
                                          OFFERLIMIT_CODE,
                                          OFFERLIMIT_LIMIT,
                                          OFFERLIMIT_PERIODTYPE,
                                          OFFERLIMIT_QUANTITY,
                                          DEFERENDOFSALEINDICATIOR,
                                          STORENUMBER,
                                          CORPORATIONID,
                                          DIVISIONID,
                                          TERMINALTYPES,
                                          OFFERNAME,
                                          OFFERDESCRIPTION,
                                          OFFERPRIORITY_NM,
                                          OFFER_CONDITION_JOIN_OPERATOR,
                                          CUSTOMER_GROUP_NM, 
                                          CUSTOMER_GROUP_IND, 
                                          CP_PRODUCT_GROUPNAME,
                                          CP_EXCLUDEINDICATOR,
                                          CP_PRODUCTCONDITIONTYPE,
                                          CP_PRODUCT_COMBO_CONDITION_OPER_CD,
                                          CP_UNIQUEPRODUCTCONDITION,
                                          CP_MINPURCHASEAMOUNT,
                                          CP_MINPRODUCTQUANTITY,
                                          CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
                                          CP_PRODUCT_TIER_ID,
                                          CP_TIER_PRODUCT_QTY,
                                          CP_POINTSPROGRAM_GROUPNAME,
                                          CP_POINTSLIMIT,
                                          CP_POINT_TIER_ID,
                                          TRIGGERCODE,
                                          TIMECONDITION_STARTTIME,
                                          TIMECONDITION_ENDTIME,
                                          DAYCONDITION_MON,
                                          DAYCONDITION_TUE,
                                          DAYCONDITION_WED,
                                          DAYCONDITION_THU,
                                          DAYCONDITION_FRI,
                                          DAYCONDITION_SAT,
                                          DAYCONDITION_SUN,
                                          DISCOUNTLEVEL,
                                          DP_GROUPNAME,
                                          DP_DISCOUNTTYPE,
                                          DP_DISCOUNTVALUE,
                                          DP_EXCLUDEINDICATOR,
                                          DP_COUPONFACTOR,
                                          DP_DOLLARLIMIT,
                                          DP_WEIGHTLIMIT,
                                          DP_ITEMLIMIT,
                                          DP_RECEIPTTEXT,
                                          DP_ALLOWNEGATIVE,
                                          DP_FLEXNEGATIVE,
                                          SPECIALPRICING_SEQNUMBER, 
                                          SPECIALPRICING_AMOUNT,
                                          SCORECARD_NM,
                                          SCORECARD_ENABLED,
                                          SCORECARD_LINETXT,
                                          DR_PRODUCT_TIER_ID,
                                          RW_POINTSPROGRAM_GROUPNAME,
                                          RW_POINTSLIMIT,
                                          RW_POINTSPROGRAM_POINTS,
                                          PRODUCTGROUPNAME,
                                          EXTPRODUCTID,
                                          PRODUCTTYPE,
                                          DW_SOURCE_CREATE_NM,
                                          DW_CREATE_TS,
                                          CURRENT_TIMESTAMP,
                                          TRUE,
                                          FALSE,
                                          TMP.ACTIONITEM
                          
                          FROM (    SELECT * FROM `+ tgt_tmp_tbl +`
                                             WHERE ACTIONITEM = 'Update') TMP
                          JOIN `+ tgt_new_img + ` NEW
                          ON TMP.OFFERID = NEW.OFFERID
                          AND NEW.EXCEP_FLAG is NULL
                   `;    
                   
    var sql_begin = "BEGIN"
	var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
                   
        try {
              snowflake.execute (
                  {sqlText: sql_begin  }
            );
              snowflake.execute (
                  {sqlText: sql_tgt_insert  }
            );
              snowflake.execute (
                  {sqlText: sql_tgt_delete  }
            );
              snowflake.execute (
                  {sqlText: sql_tgt_upd_step1  }
            );
              snowflake.execute (
                  {sqlText: sql_tgt_upd_step2  }
            );
              snowflake.execute (
                  {sqlText: sql_commit  }
            );
        }
        catch (err)  {
            snowflake.execute (
            {sqlText: sql_rollback  }
            );
        throw "Inserts/Updates/Delete transaction on Target Item table "+ tgt_table +" Failed with error: " + err;   // Return a error message.
        }
    // **************	Load for EPE_OFFER table ENDs ***************** 
    
                   
    //Processing Exceptions
                   
    var sql_excep = `INSERT INTO `+ tgt_excep_tbl + `
                   
                   SELECT SOURCE , 
                          OFFERID,
                          PROGRAMCODE,
                          LINKPLUNBR,
                          OFFERSTARTDATE,
                          OFFERENDDATE,
                          OFFERCATEGORY,
                          OFFERTESTINGSTARTDATE,
                          OFFERTESTINGENDDATE,
                          OFFERLIMIT_CODE,
                          OFFERLIMIT_LIMIT,
                          OFFERLIMIT_PERIODTYPE,
                          OFFERLIMIT_QUANTITY,
                          DEFERENDOFSALEINDICATIOR,
                          STORENUMBER,
                          CORPORATIONID,
                          DIVISIONID,
                          TERMINALTYPES,
                          OFFERNAME,
                          OFFERDESCRIPTION,
                          OFFERPRIORITY_NM,
                          OFFER_CONDITION_JOIN_OPERATOR,
                          CUSTOMER_GROUP_NM, 
                          CUSTOMER_GROUP_IND, 
                          CP_PRODUCT_GROUPNAME,
                          CP_EXCLUDEINDICATOR,
                          CP_PRODUCTCONDITIONTYPE,
                          CP_PRODUCT_COMBO_CONDITION_OPER_CD,
                          CP_UNIQUEPRODUCTCONDITION,
                          CP_MINPURCHASEAMOUNT,
                          CP_MINPRODUCTQUANTITY,
                          CP_CONDITIONAL_PRODUCT_SEQUENCE_ID,
                          CP_PRODUCT_TIER_ID,
                          CP_TIER_PRODUCT_QTY,
                          CP_POINTSPROGRAM_GROUPNAME,
                          CP_POINTSLIMIT,
                          CP_POINT_TIER_ID,
                          TRIGGERCODE,
                          TIMECONDITION_STARTTIME,
                          TIMECONDITION_ENDTIME,
                          DAYCONDITION_MON,
                          DAYCONDITION_TUE,
                          DAYCONDITION_WED,
                          DAYCONDITION_THU,
                          DAYCONDITION_FRI,
                          DAYCONDITION_SAT,
                          DAYCONDITION_SUN,
                          DISCOUNTLEVEL,
                          DP_GROUPNAME,
                          DP_DISCOUNTTYPE,
                          DP_DISCOUNTVALUE,
                          DP_EXCLUDEINDICATOR,
                          DP_COUPONFACTOR,
                          DP_DOLLARLIMIT,
                          DP_WEIGHTLIMIT,
                          DP_ITEMLIMIT,
                          DP_RECEIPTTEXT,
                          DP_ALLOWNEGATIVE,
                          DP_FLEXNEGATIVE,
                          SPECIALPRICING_SEQNUMBER, 
                          SPECIALPRICING_AMOUNT,
                          SCORECARD_NM,
                          SCORECARD_ENABLED,
                          SCORECARD_LINETXT,
                          DR_PRODUCT_TIER_ID,
                          RW_POINTSPROGRAM_GROUPNAME,
                          RW_POINTSLIMIT,
                          RW_POINTSPROGRAM_POINTS,
                          PRODUCTGROUPNAME,
                          EXTPRODUCTID,
                          PRODUCTTYPE,
                          DW_SOURCE_CREATE_NM,
                          DW_CREATE_TS,
                          DW_LAST_UPDATE_TS,
                          DW_CURRENT_VERSION_IND,
                          DW_LOGICAL_DELETE_IND,
                          EXCEP_FLAG
                          
                          FROM `+ tgt_new_img +`
                          WHERE EXCEP_FLAG IS NOT NULL
                   `;
     try {
        snowflake.execute (
            {sqlText: sql_excep  }
            );
        }
	catch (err)  {
        throw "Inserting exceptions into EPE_OFFER Exceptions table "+ tgt_excep_tbl +" Failed with error: " + err;   // Return a error message.
        }
                 
// **************	Load for EPE_OFFER table ENDs *****************
               
   $$;
