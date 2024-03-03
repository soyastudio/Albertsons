--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_VISITOR runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_<<ENV>>;
use schema DW_VIEWS;

create or replace VIEW CUSTOMER_SESSION_VISITOR
(
	VISITOR_INTEGRATION_ID COMMENT 'Used to identify unique visitors. EDDL will populate this value from the absVisitorId cookie that gets populated for all users.',
    DW_First_Effective_Ts COMMENT 'The date that this address instance became effective.',
	DW_Last_Effective_Ts COMMENT 'The last date that this address instance was effective. Thid date for the current instance will be 9999/12/31.',
	VISITOR_ID COMMENT 'Used to identify unique visitors. EDDL will populate this value from the absVisitorId cookie that gets populated for all users.',
	HOUSEHOLD_ID COMMENT 'data to identify multiple users at same household ',
	CLUB_CARD_NBR COMMENT 'loyalty card ID assigned to the user',
	RETAIL_CUSTOMER_UUID COMMENT 'Unique corporate ID assigned for each user after signed in',
    USER_TYPE_CD COMMENT 'The type of user based on their previous interaction with banner. web only - not used.options: R (Registered user without orders placed), GGuest user),C (Registered user with orders placed)',
	FRESHPASS_SUBSCRIPTION_STATUS_DSC COMMENT 'for freshpass only: fires on order confirmation for freshpass,standonle signup for freshpass,delivery_subscription_confirmation',
	FRESHPASS_SUBSCRIPTION_DT COMMENT 'Date on which Freshpass subscription was enrolled or modified',
	TOTAL_ORDER_CNT COMMENT 'amount of orders users have made on their account',
	EMAIL_OPTIN_IND COMMENT 'Indicator to signify if user like to recieve email communication or not',
	SMS_OPTIN_IND COMMENT 'Indicator to signify if user like to recieve sms communication or not',
	CAMERA_PREFERENCE_CD COMMENT 'Code to signify the users choice on camera access',
	NOTIFICATION_PREFERENCE_CD COMMENT 'Code to signify the users choice on notification',
	LOCATION_SHARING_PREFERENCE_CD COMMENT 'Code to signify the users choice on location sharing option',
	COOKIE_PREFERENCE_TXT COMMENT 'cookie categories the user has consented to via OneTrust: More information.C0001 - strictly necessary C0003 - Functional web only',
    RETAIL_CUSTOMER_UUID_VALID_IND COMMENT 'Indicator to know if Retail customer uuid is matching enterprise retail customer table',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'Name of source system or user created the record',
	DW_SOURCE_UPDATE_NM COMMENT 'Name of source system or user updated the record',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
    DW_Checksum_Value_Txt COMMENT 'Concatenated value of all the columns in the record used to capture SCD2 compare logic for updates.'
)COPY GRANTS COMMENT='View for CUSTOMER_SESSION_VISITOR'
 AS
SELECT
	VISITOR_INTEGRATION_ID,
    Dw_First_Effective_Ts,
    Dw_Last_Effective_Ts,
    VISITOR_ID,
    HOUSEHOLD_ID,
    CLUB_CARD_NBR,
    RETAIL_CUSTOMER_UUID,
    USER_TYPE_CD,
    FRESHPASS_SUBSCRIPTION_STATUS_DSC,
    FRESHPASS_SUBSCRIPTION_DT,
    TOTAL_ORDER_CNT,
    EMAIL_OPTIN_IND,
    SMS_OPTIN_IND,
    CAMERA_PREFERENCE_CD,
    NOTIFICATION_PREFERENCE_CD,
    LOCATION_SHARING_PREFERENCE_CD,
	COOKIE_PREFERENCE_TXT,
    RETAIL_CUSTOMER_UUID_VALID_IND,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND,
    DW_Checksum_Value_Txt
FROM EDM_CONFIRMED_<<ENV>>.DW_C_USER_ACTIVITY.CUSTOMER_SESSION_VISITOR;