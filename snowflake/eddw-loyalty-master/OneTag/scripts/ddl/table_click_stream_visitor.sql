--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_VISITOR runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_C_USER_ACTIVITY;

CREATE OR REPLACE TABLE CUSTOMER_SESSION_VISITOR
(
    VISITOR_INTEGRATION_ID NUMBER NOT NULL  COMMENT 'Used to identify unique visitors. EDDL will populate this value from the absVisitorId cookie that gets populated for all users.',
    DW_First_Effective_Ts TIMESTAMP NOT NULL  COMMENT 'The date that this address instance became effective.',
	DW_Last_Effective_Ts TIMESTAMP NOT NULL  COMMENT 'The last date that this address instance was effective. Thid date for the current instance will be 9999/12/31.',
	VISITOR_ID VARCHAR NOT NULL  COMMENT 'Used to identify unique visitors. EDDL will populate this value from the absVisitorId cookie that gets populated for all users.',
	HOUSEHOLD_ID NUMBER NULL  COMMENT 'data to identify multiple users at same household ',
	CLUB_CARD_NBR NUMBER NULL  COMMENT 'loyalty card ID assigned to the user',
	RETAIL_CUSTOMER_UUID VARCHAR  NULL  COMMENT 'Unique corporate ID assigned for each user after signed in',
    USER_TYPE_CD VARCHAR NULL  COMMENT 'The type of user based on their previous interaction with banner. web only - not used.options: R (Registered user without orders placed), GGuest user),C (Registered user with orders placed)',
	FRESHPASS_SUBSCRIPTION_STATUS_DSC VARCHAR NULL  COMMENT 'for freshpass only: fires on order confirmation for freshpass,standonle signup for freshpass,delivery_subscription_confirmation',
	FRESHPASS_SUBSCRIPTION_DT DATE NULL  COMMENT 'Date on which Freshpass subscription was enrolled or modified',
	TOTAL_ORDER_CNT INTEGER NULL  COMMENT 'amount of orders users have made on their account',
	EMAIL_OPTIN_IND boolean NULL  COMMENT 'Indicator to signify if user like to recieve email communication or not',
	SMS_OPTIN_IND boolean NULL  COMMENT 'Indicator to signify if user like to recieve sms communication or not',
	CAMERA_PREFERENCE_CD VARCHAR NULL  COMMENT 'Code to signify the users choice on camera access',
	NOTIFICATION_PREFERENCE_CD VARCHAR NULL  COMMENT 'Code to signify the users choice on notification',
	LOCATION_SHARING_PREFERENCE_CD VARCHAR NULL  COMMENT 'Code to signify the users choice on location sharing option',
	COOKIE_PREFERENCE_TXT VARCHAR NULL  COMMENT 'cookie categories the user has consented to via OneTrust: More information.C0001 - strictly necessary C0003 - Functional web only',
    RETAIL_CUSTOMER_UUID_VALID_IND boolean NULL  COMMENT 'Indicator to know if Retail customer uuid is matching enterprise retail customer table',
	DW_CREATE_TS TIMESTAMP NULL  COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS TIMESTAMP NULL  COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN NULL  COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) NULL  COMMENT 'Name of source system or user created the record',
	DW_SOURCE_UPDATE_NM VARCHAR(255) NULL  COMMENT 'Name of source system or user updated the record',
	DW_CURRENT_VERSION_IND BOOLEAN NULL  COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
    DW_Checksum_Value_Txt VARCHAR(16777216) COMMENT 'Concatenated value of all the columns in the record used to capture SCD2 compare logic for updates.',
	primary key (VISITOR_INTEGRATION_ID,DW_First_Effective_Ts,DW_Last_Effective_Ts)
);