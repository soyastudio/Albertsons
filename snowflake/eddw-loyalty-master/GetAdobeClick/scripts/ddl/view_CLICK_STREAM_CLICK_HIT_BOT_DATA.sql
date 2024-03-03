--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CLICK_HIT_BOT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_STREAM_CLICK_HIT_BOT_DATA
(
logic_id	                COMMENT 'Unique Key generated for each record in the Adobe transaction data'
, click_stream_integration_id	COMMENT 'Unique Key generated for each record in the Adobe transaction data'
, visit_id	                COMMENT 'Unique identifier for a visit as identified by Adobe'
, visitor_id	                COMMENT 'Unique identifier for a visitor as identified by Adobe'
, ip	                        COMMENT 'The users IP Address i.e. 65.208.210.98 - Expires After Visit'
, guid	                    	COMMENT 'GUID# of the Retail_Customer'
, gross_order_flag	        COMMENT 'The Gross Orders’ metric shows the number of times a visitor submitted an order. (There are numerous nuances to this metric not the least of which is that order cancellations are tracked as a separate event)'
, coupon_clip_flag	        COMMENT 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.'
, cart_addition_flag	        COMMENT 'The Cart Additions’ metric shows the number of times a visitor added an item to cart. (There are numerous nuances to this metric not the least of which is that incrementing the number of units for a given product in the cart is tracked as a separate event)'
, visitor_ip	                COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe and users IP Address'
, visitor_guid	            	COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe and GUID# of the Retail_Customer'
, visitor_ip_guid	        COMMENT 'Combination of Unique identifier for a visitor as identified by Adobe, users IP Address and GUID# of the Retail_Customer'
, bot_flag	                COMMENT 'Identified as BOT flag'  
, dw_createts	                comment 'When a record is created this would be the current timestamp'
, attribute1	                COMMENT 'Future enhancement'
, attribute2	                COMMENT 'Future enhancement'
, attrubute3	                COMMENT 'Future enhancement'
, attribute4	                COMMENT 'Future enhancement'
, attribute5	                COMMENT 'Future enhancement'
, dw_create_ts 			COMMENT 'When a record is created this would be the current timestamp'
, dw_last_update_ts 		COMMENT 'When a record is updated this would be the current timestamp'
, dw_source_create_nm 		COMMENT 'The data source name of this insert'
, dw_source_update_nm 		COMMENT 'The data source name of this update or delete'                                
)
COPY GRANTS
comment = 'VIEW For click_stream_click_hit_bot_data' 
AS
SELECT
  logic_id
, click_stream_integration_id
, visit_id
, visitor_id
, ip
, guid
, gross_order_flag
, coupon_clip_flag
, cart_addition_flag
, visitor_ip
, visitor_guid
, visitor_ip_guid
, bot_flag
, dw_createts
, attribute1
, attribute2
, attrubute3
, attribute4
, attribute5
, dw_create_ts
, dw_last_update_ts
, dw_source_create_nm
, dw_source_update_nm
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_STREAM_CLICK_HIT_BOT_DATA;