--liquibase formatted sql
--changeset SYSTEM:EDM_Environment_Variable_ runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_REFINED_<<parm_ENV>>;
use schema DW_R_MASTERDATA;

delete from EDM_Environment_Variable_<<parm_ENV>> where PARAM_KEY='R_USER_ACT';

insert into EDM_Environment_Variable_<<parm_ENV>> 
(PARAM_KEY,param_value,DW_CREATE_TS)
values
('R_USER_ACT','DW_R_USER_ACTIVITY',current_timestamp);
