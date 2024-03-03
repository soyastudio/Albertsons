--liquibase formatted sql
--changeset SYSTEM:Ocgp_Prize runOnChange:true splitStatements:false OBJECT_TYPE:view

USE DATABASE <<EDM_DB_NAME_VIEW>>;
USE SCHEMA DW_VIEWS;

CREATE OR REPLACE VIEW Ocgp_Prize(
									PRIZE_ID COMMENT 'Unique Identifier of the prize',
									PROGRAM_CD COMMENT 'unique code for the program. used by OCRP to manage points / rewards for a program bucket ',
									DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
									DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
									PRIZE_NM COMMENT 'Name of the prize',
									PRIZE_DSC COMMENT 'Description of what the prize is',
									PRIZE_TYPE_CD COMMENT 'type of prize - vendor sponsored, b4u offers, freshpass etc',
									VENDOR_NM COMMENT 'Describes who the vendor is if the prize is vendor prize',
									PRIZE_EXPIRATION_DT COMMENT 'date when this prize expires',
									EARN_POINTS_QTY COMMENT 'points earned if this prize is redeemed',
									BURN_POINTS_QTY COMMENT 'points needed to redeem this prize',
									PRIZE_RANKING_NBR COMMENT 'where this prize ranks to be displayed on list of prizes on UI',
									SWEEPSTAKE_DRAW_DT COMMENT 'draw date of sweepstake if the prize is sweepstake',
									DISCLAIMER_TXT COMMENT 'disclaimer displayed on UI',
									DIGITAL_PRIZE_IND COMMENT 'is this a digital prize. for instance - starbucks $25 recharge card',
									BURN_PROGRAM_NM COMMENT 'which program this prize belongs to from which the points earn transactions will be maintained',
									EARN_PROGRAM_NM COMMENT 'which program this prize belongs to from which the points burn transactions will be maintained',
									PRIZE_DETAIL_TXT COMMENT 'extra details about this prize',
									INITIAL_STOCK_QTY COMMENT 'how many prizes did we started wtih',
									AVAILABLE_STOCK_QTY COMMENT 'how much is available at this point of time',
									DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
									DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
									DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
									DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
									DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
									DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
									) COMMENT='VIEW FOR Ocgp_Prize'
									AS
									SELECT
									PRIZE_ID,
									PROGRAM_CD,
									DW_FIRST_EFFECTIVE_DT,
									DW_LAST_EFFECTIVE_DT,
									PRIZE_NM,
									PRIZE_DSC,
									PRIZE_TYPE_CD,
									VENDOR_NM,
									PRIZE_EXPIRATION_DT,
									EARN_POINTS_QTY,
									BURN_POINTS_QTY,
									PRIZE_RANKING_NBR,
									SWEEPSTAKE_DRAW_DT,
									DISCLAIMER_TXT,
									DIGITAL_PRIZE_IND,
									BURN_PROGRAM_NM,
									EARN_PROGRAM_NM,
									PRIZE_DETAIL_TXT,
									INITIAL_STOCK_QTY,
									AVAILABLE_STOCK_QTY,
									DW_CREATE_TS,
									DW_LAST_UPDATE_TS,
									DW_LOGICAL_DELETE_IND,
									DW_SOURCE_CREATE_NM,
									DW_SOURCE_UPDATE_NM,
									DW_CURRENT_VERSION_IND
									FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Ocgp_Prize;
