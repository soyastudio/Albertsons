--liquibase formatted sql
--changeset SYSTEM:GR Report runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database EDM_ANALYTICS_PRD;
use schema EDM_ANALYTICS_PRD.dw_reference;

CREATE TABLE if not exists F_Grocery_Reward_Offer_Clips
(
	Offer_D1_Sk NUMBER NOT NULL ,
	Clip_D1_Sk NUMBER NOT NULL  COMMENT 'Surrogate key for each clip identifier',
	Clip_Day_Id NUMBER(8) NOT NULL  COMMENT 'Fiscal Day Identifier; Format: YYYYMMDD; e.g. 20210101',
	Banner_D1_Sk NUMBER NOT NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Clip_Dt DATE  NULL ,
	Clip_Qty NUMBER  NULL ,
	Clip_Redeemed_Qty NUMBER  NULL ,
	Dw_Create_Ts TIMESTAMP NOT NULL ,
	Dw_Last_Update_Ts TIMESTAMP NOT NULL ,
	PRIMARY KEY (Offer_D1_Sk, Clip_D1_Sk, Clip_Day_Id,Banner_D1_Sk)
);

CREATE TABLE if not exists F_Grocery_Reward_Offer_Redemption
(
	Transaction_Id NUMBER NOT NULL  COMMENT 'Unique identifier for each transaction',
	Transaction_Day_Id NUMBER(8) NOT NULL  COMMENT 'Fiscal Day Identifier; Format: YYYYMMDD; e.g. 20210101',
	Offer_D1_Sk NUMBER NOT NULL ,
	Clip_D1_Sk NUMBER NOT NULL  COMMENT 'Surrogate key for each clip identifier',
	Retail_Customer_Household_D1_Sk NUMBER NULL ,
	Banner_D1_Sk NUMBER not NULL  COMMENT 'A Surrogate Key - system-generated unique identifier that is used instead of the natural key.',
	Household_Id VARCHAR(20) NULL  COMMENT 'Unique Identifier of houehold who made the transaction',
	Transaction_Dt DATE NULL  COMMENT 'Date of transaction',
	Redemption_Qty NUMBER NULL ,
	Dw_Create_Ts TIMESTAMP NOT NULL ,
	Dw_Last_Update_Ts TIMESTAMP NOT NULL ,
	PRIMARY KEY (Transaction_Id, Transaction_Day_Id, Offer_D1_Sk, Clip_D1_Sk,Banner_D1_Sk)
);

CREATE TABLE if not exists D1_Offer
(
	Offer_D1_Sk NUMBER NOT NULL autoincrement start 1 increment 1,
	Offer_Id VARCHAR(20) NOT NULL  COMMENT 'Unique identifier for the offer clipped',
	Offer_Nm VARCHAR NULL ,
	Offer_Program_Cd VARCHAR(20) NULL ,
	Offer_Type_Cd VARCHAR(20) NULL  COMMENT 'Type of offer clipped',
	Offer_Status_Cd VARCHAR NULL ,
	Offer_Benefit_Value_Type_Dsc VARCHAR NULL ,
	Offer_Dollar_Value_Amt NUMBER(8,3) NULL  COMMENT 'Dollar value of clipped offer',
	Offer_Reward_Value_Qty NUMBER NULL  COMMENT 'Reward value of offer clipped',
	Dw_Create_Ts TIMESTAMP NOT NULL  COMMENT 'timestamp when record was created.',
	Dw_Last_Update_Ts TIMESTAMP NOT NULL  COMMENT 'timestamp when record was updated.',
	Dw_Logical_Delete_Ind BOOLEAN NOT NULL  COMMENT 'logical indicated for deleted records, used when record was deleted from the source system.',
	PRIMARY KEY (Offer_D1_Sk)
);


CREATE TABLE if not exists D1_Clip
(
	Clip_D1_Sk NUMBER NOT NULL autoincrement start 1 increment 1 COMMENT 'Surrogate key for each clip identifier',
	Clip_Platform_Cd VARCHAR(20) NULL  COMMENT 'Platform through which coupon has been clipped',
	Clip_Source_Nm VARCHAR(20) NULL  COMMENT 'Source or channel through which coupon has been clipped',
	Clip_Type_Cd VARCHAR(20) NULL  COMMENT 'Type of the clip. Clipped vs Unclipped',
	Dw_Create_Ts TIMESTAMP NOT NULL  COMMENT 'timestamp when record was created.',
	Dw_Last_Update_Ts TIMESTAMP NOT NULL  COMMENT 'timestamp when record was updated.',
	Dw_Logical_Delete_Ind BOOLEAN NOT NULL  COMMENT 'logical indicated for deleted records, used when record was deleted from the source system.',
	PRIMARY KEY (Clip_D1_Sk)
);


alter table EDM_CONFIRMED_PRD.dw_c_loyalty.clip_details set change_tracking = TRUE;
alter table EDM_CONFIRMED_PRD.dw_c_retailsale.Epe_Transaction_header_Saving_Clips set change_tracking = TRUE;
alter table EDM_CONFIRMED_PRD.dw_c_product.oms_offer set change_tracking = TRUE;
