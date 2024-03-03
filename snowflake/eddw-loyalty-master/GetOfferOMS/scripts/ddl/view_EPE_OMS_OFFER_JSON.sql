--liquibase formatted sql
--changeset SYSTEM:EPE_OMS_OFFER_JSON runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_OMS_OFFER_JSON(
	TOPIC COMMENT 'To load Topic Name',
	KEY COMMENT 'To load Key value which contains offer details',
	PAYLOAD COMMENT 'Contains all columns and values details of offer',
	DW_CREATE_TS COMMENT 'shows as what time offer created'
) COMMENT='View for OMS Final Json table on EPE'
 as SELECT 
	TOPIC ,
	KEY ,
	PAYLOAD ,
	DW_CREATE_TS FROM <<EDM_DB_NAME_OUT>>.DW_DCAT.EPE_OMS_OFFER_JSON
	 ;
