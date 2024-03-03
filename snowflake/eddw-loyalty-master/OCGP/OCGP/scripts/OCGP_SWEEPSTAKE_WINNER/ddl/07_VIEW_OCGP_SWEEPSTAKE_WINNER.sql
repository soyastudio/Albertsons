--liquibase formatted sql
--changeset SYSTEM:OCGP_SWEEPSTAKE_WINNER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

use database <<EDM_DB_NAME_VIEW>>;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW OCGP_SWEEPSTAKE_WINNER(
											PRIZE_ID COMMENT 'Unique Identifier of the prize',
											PROGRAM_CD COMMENT 'unique code for the program. used by OCRP to manage points / rewards for a program bucket',
											RETAIL_CUSTOMER_UUID COMMENT 'This ID represents an Universal Unique Identifier for a Retail Customer',
											SWEEPSTAKE_DRAW_DT COMMENT 'date of sweepstake draw',
											DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
											DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is 12/31/9999 for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
											DISPLAY_NOTIFICATION_IND COMMENT 'win notification displayed to the user or not ',
											CLAIM_EXPIRATION_DT COMMENT 'date of expiration to claim this win',
											WINNING_DETAIL_TXT COMMENT 'description of win',
											WIN_STATUS_CD COMMENT 'Potential winner or Final Winner',
											WINNER_EMAIL_ADDRESS_TXT COMMENT 'email of the winner. used for information on ui',
											DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
											DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
											DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
											DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert',
											DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete',
											DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
											) COMMENT='VIEW FOR OCGP_SWEEPSTAKE_WINNER'
											AS
											SELECT
											PRIZE_ID,
											PROGRAM_CD,
											RETAIL_CUSTOMER_UUID,
											SWEEPSTAKE_DRAW_DT,
											DW_FIRST_EFFECTIVE_DT,
											DW_LAST_EFFECTIVE_DT,
											DISPLAY_NOTIFICATION_IND,
											CLAIM_EXPIRATION_DT,
											WINNING_DETAIL_TXT,
											WIN_STATUS_CD,
											WINNER_EMAIL_ADDRESS_TXT,
											DW_CREATE_TS,
											DW_LAST_UPDATE_TS,
											DW_LOGICAL_DELETE_IND,
											DW_SOURCE_CREATE_NM,
											DW_SOURCE_UPDATE_NM,
											DW_CURRENT_VERSION_IND
											FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.OCGP_SWEEPSTAKE_WINNER;
