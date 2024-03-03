--liquidbase formatted sql
--changeset SYSTEM:CUSTOMER_IDS runOnChange: true splitStatements:false OBJECT_TYPE:VIEW

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<VW_DEPLOY_SCHEMA>>;

CREATE OR REPLACE SECURE VIEW <<TGT_EDM_DB_NAME>>.<<VW_DEPLOY_SCHEMA>>.CUSTOMER_IDS COPY GRANTS as
select  
    DISTINCT(TH.TRANSACTION_ID)
    ,RS.BANNER_NM as RETAILER_ID
    ,TH.FACILITY_NBR as RETAIL_STORE_ID
    ,TH.TRANSACTION_DT as TRANSACTION_DATE
    ,TH.TRANSACTION_TS as TRANSACTION_TIME
    ,TH.REGISTER_NBR as LANE_ID
    ,CASE WHEN TH.RETAIL_CUSTOMER_UUID = '-1' THEN 1
    ELSE 2 END as CUSTOMER_ID_TYPE_CODE
    ,TH.RETAIL_CUSTOMER_UUID as CUSTOMER_ID
    ,TH.LOYALTY_PROGRAM_CARD_NBR as LOYALTY_CARD_NUMBER
    
from
    <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.TRANSACTION_HDR TH
left join
    <<SRC_EDM_DB_NAME>>.<<SRC_EDM_SCHEMA_NAME>>.D1_RETAIL_STORE RS on TH.FACILITY_NBR = RS.RETAIL_STORE_FACILITY_NBR
    
where TH.TRANSACTION_DT BETWEEN TO_DATE('2022-11-01') AND TO_DATE('2022-11-30') AND RS.DW_LOGICAL_DELETE_IND = FALSE;
