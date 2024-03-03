--liquibase formatted sql
--changeset SYSTEM:TXN_FACTS_DIGITAL_NEPTUNE_TABLE_NEW runOnChange:true splitStatements:false OBJECT_TYPE:TASK
use database <<EDM_DB_NAME>>;
USE SCHEMA DW_APPL;				
				   
CREATE OR REPLACE TASK DIGITAL_REDEMPTION_REPROCESS_NEPTUNE_TO_KAFKAOUTQUEUE_O_TASK
WAREHOUSE = '<<EDM_DB_WAREHOUSE>>'
SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'
  QUERY_TAG = '{"APPCODE":"OCOM"}'
AS

CALL SP_Digital_Redemption_Neptune_To_KafkaOutQueue_reprocess('<<EDM_DB_VIEWS>>.DW_VIEWS.EPE_TRANSACTION_HEADER_SAVINGS','<<EDM_DB_NAME>>','<<EDM_DB_VIEWS>>','DW_C_STAGE','1')   ;
 ALTER TASK DIGITAL_REDEMPTION_REPROCESS_NEPTUNE_TO_KAFKAOUTQUEUE_O_TASK resume;
