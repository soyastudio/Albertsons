--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_METRICS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_METRICS (
	CLICK_STREAM_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	HIT_ID_HIGH VARCHAR(16777216) COMMENT 'Used in combination with hitid_low to uniquely identify a hit.',
	HIT_ID_LOW VARCHAR(16777216) COMMENT 'Used in combination with hitid_high to uniquely identify a hit.',
	VISIT_PAGE_NBR VARCHAR(16777216) COMMENT 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.',
	VISIT_NBR NUMBER(38,0) COMMENT 'Variable used in the Visit number dimension. Starts at 1 and increments each time a new visit starts per visitor.',
	APPLICATION_REGISTRATION_IND BOOLEAN COMMENT 'The App Registrations metric shows the number of visitors successfully register for Loyalty Shop on App',
	BASE_PRICE_IND BOOLEAN COMMENT 'Web Only: WYSWYG - Unit Original Price',
	BOX_TOPS_GET_STARTED_IND BOOLEAN COMMENT 'The BoxTops Get Started’ metric shows the number of times a visitor clicked on Get Started button.',
	CHECKOUT_STEP1_IND BOOLEAN COMMENT 'The Checkout: Step 1’ metric shows the number of times a visitor landed on checkout page.',
	CHECKOUT_STEP2_IND BOOLEAN COMMENT 'The Checkout: Step 2’ metric shows the number of times a visitor landed on checkout order info page.',
	CHECKOUT_STEP3_IND BOOLEAN COMMENT 'The Checkout: Step 3’ metric shows the number of times a visitor landed on checkout payment info page.',
	CHECKOUT_STEP4_IND BOOLEAN COMMENT 'The Checkout: Step 4’ metric shows the number of times a visitor landed on checkout promo code page.',
	CHECKOUT_STEP5_IND BOOLEAN COMMENT 'Web Only: The Checkout: Step 5’ metric shows the number of times a visitor landed on checkout credit on account page.',
	CHECKOUT_STEP6_IND BOOLEAN COMMENT 'The Checkout: Step 6’ metric shows the number of times a visitor landed on checkout cart page.',
	CLUBCARD_SAVINGS_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG - Available when you login',
	COUPON_ID_CLIPPED_NUMBER_IND BOOLEAN COMMENT 'The Number of CouponIDs Clipped’ metric shows the number of times a visitor clipped coupon offer.',
	CONTACT_SAVE_IND BOOLEAN COMMENT 'The App Checkout: Contact Save’ metric shows the number of times a visitor successfully save contact information on checkout',
	COUPON_CLIP_IND BOOLEAN COMMENT 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.',
	COUPON_AVAILABLE_NUMBER_IND BOOLEAN COMMENT 'Future - Not possible from APIs right now on WEB. Number of coupons available to user for the specific item. This is all total coupons tied to user/product for the store that the user is currently shopping in.',
	COUPON_CLIPPED_NUMBER_IND BOOLEAN COMMENT 'Cart Only: The Coupon Clipped’ metric shows the number coupons tied to an item - set in the product stirng. Not the same as e306',
	COUPON_APPLIED_NUMBER_IND BOOLEAN COMMENT 'Cart Only: The Coupon Applied’ metric shows the number of the number valid coupons applied to an item (where there is an actual discount) - set in the product stirng',
	CUSTOMER_WEB_REGISTION_IND BOOLEAN COMMENT 'The Create Account’ metric shows the number of successful registration on the site.',
	CLEAR_RECENT_SEARCH_TERM_IND BOOLEAN COMMENT 'The Search: Clear Recent Search Term’ metric shows the number of times a visitor clicked to clear a recent search term from the search dropdown',
	DETAILS_IND BOOLEAN COMMENT 'This will capture Evar161 data.',
	EMPLOYEE_SAVINGS_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG - Employee discount',
	EDIT_TIMESTAMP_IND BOOLEAN COMMENT 'The Edit Date/Time’ metric shows the number of times a visitor clicked on edit date/time after purchase.',
	EDIT_ORDER_CART_IND BOOLEAN COMMENT 'The Edit Order Cart’ metric shows the number of times a visitor clicked on edit order after purchase.',
	ERROR_IND BOOLEAN COMMENT 'The Error’ metric shows the number of times a visitor came across with error during the attempt of login.',
	EXCLUDE_ROW_IND NUMBER(38,0) COMMENT 'Indicator that the record should be excluded from all analytics.  This is out-of-the-box adobe logic  that we consolidated into a simple flag. CASE WHEN(exclude_hit= 0 and hit_source not in (5 7 8 9)) THEN 0 ELSE 1 END as Exclude_Row_ind   (0=Include(FALSE) 1=Exclude(TRUE))',
	FACILITY_INTEGRATION_ID NUMBER(38,0) COMMENT 'Unique Integration Id generated for each Store # in the Facility Table',
	FLASH_LIGHT_IND BOOLEAN COMMENT 'The Barcode Scan: Flashlight’ metric shows the number of times a visitor successfully opened barcode scanner those had Flashlight turned on.',
	GROSS_ORDER_AMOUNT_IND BOOLEAN COMMENT 'The Gross Orders’ metric shows the number of times a visitor submitted an order. (There are numerous nuances to this metric not the least of which is that order cancellations are tracked as a separate event)',
	GROSS_REVENUE_AMT NUMBER(16,4) COMMENT 'The Gross Revenue’ metric shows the revenue associated with orders submitted by visitors. (There are numerous nuances to this metric not the least of which is that substitutions are handled at the time of fulfillment and not tracked within the Adobe clickstream data.)',
	GROSS_UNITS_NBR NUMBER(16,4) COMMENT 'The Gross Units’ metric shows the number of items in the basket at the time of orders submission by visitors. (There are numerous nuances to this metric not the least of which is that substitutions are handled at the time of fulfillment and not tracked within the Adobe clickstream data.)',
	ITEM_PRICE_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG = Base Price - All item Savings (Excludes order level promo code)',
	J4U_SAVINGS_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG - Available when you clip a J4U coupon',
	LIST_PRICE_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG = Base price - Club Card Savings',
	MINI_CART_OPEN_IND BOOLEAN COMMENT 'Web Only: The Mini Cart Open’ metric shows the number of times a visitor opened mini cart on the site.',
	MODAL_CLICK_IND BOOLEAN COMMENT 'The Modal click’ metric shows the number of times a visitor clicked on modal view.',
	MODAL_VIEW_IND BOOLEAN COMMENT 'The Modal View’ metric shows the number of times a visitor viewed modal / drawer offer',
	NULL_SEARCH_NUMBER_IND BOOLEAN COMMENT 'The Null Searches’ metric shows the number of times a visitor has made a search that returned with zero results',
	OFF_BANNER_DELIVERY_TRUE_IND BOOLEAN COMMENT 'The Off Banner Delivery = True’ metric shows the number of times a visitor viewed Fulfillment Preference Screen and delivery option is an Off Banner Store.',
	OFF_BANNER_DELIVERY_FALSE_IND BOOLEAN COMMENT 'The Off Banner Delivery = False’ metric shows the number of times a visitor viewed Fulfillment Preference Screen and delivery option is on banner.',
	OPEN_IND BOOLEAN COMMENT 'The Barcode Scan: Open’ metric shows the number of times a visitor successfully opened barcode scanner.',
	ORDER_ID VARCHAR(16777216) COMMENT 'ID# of the Order',
	ORDER_UPDATE_TXT_IND BOOLEAN COMMENT 'The App Checkout: Text Order Updates’ metric shows the number of times a visitor successfully submits a phone number to receive order updates by text',
	ORDER_INFO_SAVE_IND BOOLEAN COMMENT 'The App Checkout: Order Info Save’ metric shows the number of times a visitor successfully save order information on checkout',
	ORDER_REVENUE_IND BOOLEAN COMMENT 'Future - Total Price Shipping Taxes etc...',
	ORDER_AHEAD_PRODUCT_VIEW_IND BOOLEAN COMMENT 'Web Only: Event fires when a customer clicks to view the item and the PDP page loads',
	ORDER_AHEAD_CART_ADD_IND BOOLEAN COMMENT 'Web Only: Event fires when customer adds the item to the Cart',
	ORDER_AHEAD_CONFIRMATION_IND BOOLEAN COMMENT 'Web Only: Event fires when customer completes setting day and time to pickup item(s) and click send order.',
	ORDER_AHEAD_TOTAL_OVER_DUE_IND BOOLEAN COMMENT 'Web Only: Event fires along with event73 when the customer clicks send order.',
	OUT_OF_STOCK_IND BOOLEAN COMMENT 'The Out of Stock’ metric shows the number of times a visitor viewed out of stock error on cart or mini-cart page',
	PAYMENT_SAVE_IND BOOLEAN COMMENT 'The App Checkout: Payment Save’ metric shows the number of times a visitor successfully save payment on checkout',
	PLACE_ORDER_COUNT_IND BOOLEAN COMMENT 'The Place Order Count’ metric shows the number of times a visitor clicked on Place Order button in checkout.',
	PRODUCT_DECREASE_QUANTITY_IND BOOLEAN COMMENT 'The Product: Decrease Quantity’ metric shows the number of times a visitor reduced their quantity after adding the product to cart.',
	PRODUCT_INCREASE_QUANTITY_IND BOOLEAN COMMENT 'The Product: Increase Quantity’ metric shows the number of times a visitor increased their quantity after adding the product to cart.',
	PRODUCT_FOUND_1_IND BOOLEAN COMMENT 'The Barcode Scan: Product Found’ metric shows the number of times a visitor successfully scanned and found matching product',
	PRODUCT_FOUND_2_IND BOOLEAN COMMENT 'The Barcode Scan: No Product Found’ metric shows the number of times a visitor scanned and havent found matching product',
	PROMOTION_CODE_IND BOOLEAN COMMENT 'The App Checkout: Promo Code Entered’ metric shows the number of times a visitor successfully entered promo code on checkout',
	PROMOTION_CODE_SAVINGS_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG - When promo code is entered',
	PURCHASE_CANCELLATION_IND BOOLEAN COMMENT 'The Purchase Cancellation’ metric shows the number of times a visitor sucessfully cancelled the orders.',
	PUSH_NOTIFICATION_CLICK_IND BOOLEAN COMMENT 'App Only: The Push notifications click’ metric shows the number of times a visitor successfully clicks on push notification.',
	REWARD_REDEEMED_IND BOOLEAN COMMENT 'The Reward Redeemed’ metric shows the number of times a visitor successfully redeemed the reward.',
	REWARDS_REDEEMED_COUNT_IND BOOLEAN COMMENT 'The # of Rewards Redeemed’ metric shows the number of times a visitor successfully redeemed the reward.',
	RECIPE_SWAP_MODAL_OPEN_IND BOOLEAN COMMENT 'Not Implemented Yet',
	RECIPE_SWAP_MODAL_PRODUCT_IND BOOLEAN COMMENT 'Not Implemented Yet',
	RESERVE_TIME_UPDATE_IND BOOLEAN COMMENT 'The Reserve Time Update’ metric shows the number of times a visitor updated their pre-booked fulfillment after placing the orders',
	RETAIL_CUSTOMER_UUID VARCHAR(16777216) COMMENT 'UUID# of the Retail_Customer',
	REWARDS_SAVINGS_AMOUNT_IND BOOLEAN COMMENT 'Web Only: WYSWYG - When you redeem a reward',
	SCAN_AUTO_CLIP_IND BOOLEAN COMMENT 'App Only: The Scan auto clip’ metric shows the number of times a visitor successfully scanned and clipped a coupon.',
	SEARCH_RESULT_COUNT_IND BOOLEAN COMMENT 'The # of Search Results’ metric shows the number of search results returned on search result page.',
	SIGN_IN_IND BOOLEAN COMMENT 'The Sign in’ metric shows the number of times a visitor had successful sign in.',
	SHIPPING_FEE_AMOUNT_IND BOOLEAN COMMENT 'The Shipping Fee’ metric shows the amount of shipping fee paid by visitor during the purchase.',
	SUB_TOTAL_AMOUNT_IND BOOLEAN COMMENT 'The SubTotal’ metric shows the amount for products paid by visitor during the purchase excluding shipping fee.',
	TOP_NAV_CLICKS_IND BOOLEAN COMMENT 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.',
	USER_ACTION_IND BOOLEAN COMMENT 'Event is incremented by 1 whenever a user takes an action that isnt loading a new page. i.e. Filtering Clipping coupons modal popups etc...',
	USER_LOGOUT_IND BOOLEAN COMMENT 'Web Only: The User Logout’ metric shows the number of times a visitor had successfully logged out.',
	VERSION_NBR NUMBER(38,0) COMMENT 'Version Number of the Grocery Order placed by the Customer',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	CLIP_ACTIONS_PER_VISIT_NBR NUMBER(38,0) COMMENT 'Number of Clip Actions Per Visit',
	primary key (CLICK_STREAM_INTEGRATION_ID)
);