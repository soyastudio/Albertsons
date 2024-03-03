--liquibase formatted sql
--changeset SYSTEM:SP_GETADOBECLICKHITDATA_CLICK_STREAM_LOG runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETADOBECLICKHITDATA_CLICK_STREAM_LOG(CNF_DB VARCHAR, WRK_SCH VARCHAR, MSG VARCHAR, STATUS VARCHAR, SP_NAME VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


var do_log = true;
var status = STATUS;
var msg = MSG;
var sp_name = SP_NAME;
var db_nm = CNF_DB;
var sch_nm = WRK_SCH;
var log_tbl = 'Click_Stream_Log_Table';

//if the value is anything other than true, do not log
if (do_log==true){ 
var ins_log = `insert into ${db_nm}.${sch_nm}.${log_tbl} (STATUS, LOG_MSG, SP_NAME) values ('${status}','${msg}','${sp_name}')`;
    try{	
	snowflake.execute({ sqlText: ins_log });
    } catch (ERROR){
        throw ERROR;
		//return ins_log;
    }
 }
 
$$;