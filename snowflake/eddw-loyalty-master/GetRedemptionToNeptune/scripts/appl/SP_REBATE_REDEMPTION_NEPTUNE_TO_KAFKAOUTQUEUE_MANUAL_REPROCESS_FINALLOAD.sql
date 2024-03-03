--liquibase formatted sql
--changeset SYSTEM:SP_REBATE_REDEMPTION_NEPTUNE_TO_KAFKAOUTQUEUE_MANUAL_REPROCESS_FINALLOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database <<EDM_DB_NAME_OUT>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_REBATE_REDEMPTION_NEPTUNE_TO_KAFKAOUTQUEUE_MANUAL_REPROCESS_FINALLOAD(CNF_DB VARCHAR, STARTDAY VARCHAR, ENDDAY VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var STARTDAY = STARTDAY;
  var ENDDAY = ENDDAY;
  var cnf_db = CNF_DB;
  var tgt_tbl = `${cnf_db}.DW_DCAT.KAFKAOUTQUEUE`;
  var src_tbl = `${cnf_db}.DW_STAGE.KAFKAOUTQUEUE_MANUAL_REPROCESS`;
  
  var sql_insert_tgttbl =  `insert into ${tgt_tbl} (
  MSG_SEQ
  ,TOPIC
  ,KEY
  ,PAYLOAD
  ,STATUS
  ,CREATETIME
  ,DW_SOURCE_CREATE_NM
  )
  SELECT * from ${src_tbl} WHERE CREATETIME >= (CURRENT_DATE - ${STARTDAY}) AND CREATETIME <= (CURRENT_DATE - ${ENDDAY})`;

  try { 
  snowflake.execute({ sqlText: sql_insert_tgttbl });       
  }
  catch (err)  {	    
  throw `Data Insertion of table ${tgt_tbl} failed with error: ${err}`;   // Return a error message.
  }
$$;
