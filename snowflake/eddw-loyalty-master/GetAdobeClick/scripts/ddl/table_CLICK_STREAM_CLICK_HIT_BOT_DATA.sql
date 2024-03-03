--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CLICK_HIT_BOT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_CLICK_HIT_BOT_DATA (
	LOGIC_ID NUMBER(38,0) autoincrement COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	CLICK_STREAM_INTEGRATION_ID VARCHAR(16777216) COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	VISIT_ID VARCHAR(16777216) COMMENT 'Unique identifier for a visit as identified by Adobe',
	VISITOR_ID VARCHAR(16777216) COMMENT 'Unique identifier for a visitor as identified by Adobe',
	IP VARCHAR(16777216) COMMENT 'The users IP Address i.e. 65.208.210.98 - Expires After Visit',
	GUID VARCHAR(16777216) COMMENT 'GUID# of the Retail_Customer',
	GROSS_ORDER_FLAG NUMBER(38,0) COMMENT 'The Gross Orders’ metric shows the number of times a visitor submitted an order. (There are numerous nuances to this metric not the least of which is that order cancellations are tracked as a separate event)',
	COUPON_CLIP_FLAG BOOLEAN COMMENT 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.',
	CART_ADDITION_FLAG BOOLEAN COMMENT 'The Cart Additions’ metric shows the number of times a visitor added an item to cart. (There are numerous nuances to this metric not the least of which is that incrementing the number of units for a given product in the cart is tracked as a separate event)',
	VISITOR_IP VARCHAR(500) COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe and users IP Address',
	VISITOR_GUID VARCHAR(500) COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe and GUID# of the Retail_Customer',
	VISITOR_IP_GUID VARCHAR(500) COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe, users IP Address and GUID# of the Retail_Customer',
	BOT_FLAG VARCHAR(255) COMMENT 'Identified as BOT flag',
	DW_CREATETS TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	ATTRIBUTE1 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE2 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRUBUTE3 VARCHAR(16777216) COMMENT 'Future enhancement',
	ATTRIBUTE4 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	ATTRIBUTE5 TIMESTAMP_LTZ(9) COMMENT 'Future enhancement',
	DW_CREATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) DEFAULT CAST(CONVERT_TIMEZONE('UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) COMMENT 'When a record is updated this would be the current timestamp',
	DW_SOURCE_CREATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) DEFAULT CURRENT_USER() COMMENT 'The data source name of this update or delete'
);