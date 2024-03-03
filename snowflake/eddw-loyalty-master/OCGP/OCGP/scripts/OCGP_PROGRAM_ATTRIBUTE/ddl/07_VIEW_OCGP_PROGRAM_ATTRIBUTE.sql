--liquibase formatted sql
--changeset SYSTEM:OCGP_PROGRAM_ATTRIBUTE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

USE DATABASE <<EDM_DB_NAME_VIEW>>;
USE SCHEMA DW_VIEWS;

CREATE OR REPLACE VIEW OCGP_PROGRAM_ATTRIBUTE(
											PROGRAM_CD COMMENT 'unique code for the program. used by OCRP to manage points / rewards for a program bucket ',
											SOURCE_NM COMMENT 'action which results in earning rewards - optin, grocery purchase etc. used by systems who implements limits on rewards earning. currently used by OCRP ',
											DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
											DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
											EARN_FREQUENCY_CD COMMENT 'The interval of earning rewards for a program. Weekly, Monthly etc',
											EARN_LIMIT_TXT COMMENT 'how much earn is allowed for a particular frequency. the frequency defined in column "earn_frequency" column',
											DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
											DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
											DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
											DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
											DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
											DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
											)COMMENT='VIEW FOR OCGP_PROGRAM_ATTRIBUTE'
											AS SELECT
											PROGRAM_CD,
											SOURCE_NM,
											DW_FIRST_EFFECTIVE_DT,
											DW_LAST_EFFECTIVE_DT,
											EARN_FREQUENCY_CD,
											EARN_LIMIT_TXT,
											DW_CREATE_TS,
											DW_LAST_UPDATE_TS,
											DW_LOGICAL_DELETE_IND,
											DW_SOURCE_CREATE_NM,
											DW_SOURCE_UPDATE_NM,
											DW_CURRENT_VERSION_IND 
											FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.OCGP_PROGRAM_ATTRIBUTE;
