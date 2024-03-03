--liquibase formatted sql
--changeset SYSTEM:EPISODIC_OFFER_CLIPPED runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA <<EDM_DB_NAME>>.DW_C_LOYALTY;

CREATE OR REPLACE TABLE EPISODIC_OFFER_CLIPPED
(
	Program_Id VARCHAR NOT NULL  COMMENT 'Unique Identifier of the episodic program',
	Offer_Clipped_Id NUMBER NOT NULL  COMMENT 'Unique transaction identifier of each offer clipped',
	Dw_First_Effective_Dt DATE NOT NULL  COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	Dw_Last_Effective_Dt DATE NOT NULL  COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	Household_Id NUMBER NULL  COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	Oms_Offer_Id VARCHAR NULL  COMMENT 'Offer identifier of the clipped offer from oms system',
	Facility_Integration_Id NUMBER NULL  COMMENT 'Surrogate Key for Facility based on FacilityID & DivisionID for each facility from GetAccountingFacilityBOD',
	Retail_Store_Id VARCHAR NULL  COMMENT 'User''s preferred store identifier',
	Banner_Nm VARCHAR NULL  COMMENT 'User''s active banner name',
	Division_Nm VARCHAR NULL  COMMENT 'Users default division name',
	Source_Nm VARCHAR NULL  COMMENT 'Source name where the action was triggered',
	App_User_Ind BOOLEAN NULL  COMMENT 'Indicator if the user is an app user',
	Created_Ts TIMESTAMP NULL  COMMENT 'Timestamp when the record was created in source system',
	Extract_Ts TIMESTAMP NULL  COMMENT 'Timestamp when the record was queried',
	Dw_Create_Ts TIMESTAMP NULL  COMMENT 'The timestamp the record was inserted.',
	Dw_Last_Update_Ts TIMESTAMP NULL  COMMENT 'When a record is updated  this would be the current timestamp',
	Dw_Logical_Delete_Ind BOOLEAN NULL  COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	Dw_Source_Create_Nm VARCHAR(255) NULL  COMMENT 'The Bod (data source) name of this insert.',
	Dw_Source_Update_Nm VARCHAR(255) NULL  COMMENT 'The Bod (data source) name of this update or delete.',
	Dw_Current_Version_Ind BOOLEAN NULL  COMMENT 'set to yes when the current record is deleted,Â  the Last Effective date on this record is still set to beÂ  current date -1 d'
);

ALTER TABLE EPISODIC_OFFER_CLIPPED
	ADD CONSTRAINT XPKepisodic_clipped_offer PRIMARY KEY (Program_Id, Offer_Clipped_Id, Dw_First_Effective_Dt, Dw_Last_Effective_Dt);
