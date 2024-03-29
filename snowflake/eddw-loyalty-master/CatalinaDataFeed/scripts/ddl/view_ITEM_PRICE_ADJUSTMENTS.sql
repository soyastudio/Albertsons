--liquidbase formatted sql
--changeset SYSTEM:ITEM_PRICE_ADJUSTMENTS runOnChange: true splitStatements:false OBJECT_TYPE:VIEW

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<VW_DEPLOY_SCHEMA>>;

CREATE OR REPLACE SECURE VIEW <<TGT_EDM_DB_NAME>>.<<VW_DEPLOY_SCHEMA>>.ITEM_PRICE_ADJUSTMENTS COPY GRANTS as

select 
    RS.BANNER_NM as RETAILER_ID
    ,TH.FACILITY_NBR as RETAIL_STORE_ID 
    ,TH.Transaction_DT as TRANSACTION_DATE
    ,TH.TRANSACTION_TS as TRANSACTION_TIME
    ,TH.REGISTER_NBR as LANE_ID
    ,RT.REGISTER_DEPARTMENT_NM as REGISTER_NAME
    ,TH.TRANSACTION_ID as TRANSACTION_ID
    ,TI.UPC_NBR as TRADE_ITEM_ID
    ,TIM.OMS_OFFER_ID as ADJUSTMENT_ID
    ,OO.PROGRAM_CD as ADJUSTMENT_TYPE_CODE
    ,TI.ITEM_QTY AS APPLICABLE_UNIT_QUANTITY
    ,TIM.MARKDOWN_AMT AS ADJUSTMENT_UNIT_AMOUNT
        
FROM <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_HDR TH
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_ITEM TI
ON TH.TRANSACTION_HDR_INTEGRATION_ID = TI.TRANSACTION_HDR_INTEGRATION_ID
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.D1_RETAIL_STORE RS
ON TH.FACILITY_NBR = RS.RETAIL_STORE_FACILITY_NBR
AND RS.DW_LOGICAL_DELETE_IND = TRUE
JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_ITEM_MARKDOWN TIM
ON TI.TRANSACTION_HDR_INTEGRATION_ID = TIM.TRANSACTION_HDR_INTEGRATION_ID
AND TI.ITEM_SEQUENCE_NBR = TIM.ITEM_SEQUENCE_NBR
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.OMS_OFFER OO 
ON TIM.OMS_OFFER_ID = OO.OMS_OFFER_ID
AND OO.DW_CURRENT_VERSION_IND = TRUE
LEFT JOIN <<SRC_EDM_DB_NAME>>.<<SRC_EDM_EDW_SCHEMA_NAME>>.REGISTER_TYPE RT
ON TH.REGISTER_NBR = RT.REGISTER_NBR

where TH.TRANSACTION_DT BETWEEN TO_DATE('2022-11-01') AND TO_DATE('2022-11-30');
