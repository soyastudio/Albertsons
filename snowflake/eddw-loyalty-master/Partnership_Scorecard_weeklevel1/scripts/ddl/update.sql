--liquibase formatted sql
--changeset SYSTEM:update runOnChange:true splitStatements:false OBJECT_TYPE:table

/*USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_RETAIL_EXP;


TRUNCATE TABLE <<EDM_DB_NAME>>.DW_RETAIL_EXP.F_ORDER_TRANSACTION;*/

USE DATABASE <<BIZ>>;
USE SCHEMA FIN;


update <<BIZ>>.FIN.GW_REG99_TXNS_2020
set txn_tm =  DateAdd(Minute,1,txn_tm)
;
