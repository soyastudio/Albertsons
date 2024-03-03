--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_METRICS runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW Click_Stream_Metrics
(
  Application_Registration_E4 comment 'The App Registrations metric shows the number of visitors successfully register for Loyalty Shop on App'
, Base_Price_E340 comment 'Web Only: WYSWYG - Unit Original Price'
, Box_Tops_Get_Started_E323 comment 'The BoxTops Get Started’ metric shows the number of times a visitor clicked on Get Started button.'
, Checkout_Step1_E308 comment 'The Checkout: Step 1’ metric shows the number of times a visitor landed on checkout page.'
, Checkout_Step2_E309 comment 'The Checkout: Step 2’ metric shows the number of times a visitor landed on checkout order info page.'
, Checkout_Step3_E310 comment 'The Checkout: Step 3’ metric shows the number of times a visitor landed on checkout payment info page.'
, Checkout_Step4_E311 comment 'The Checkout: Step 4’ metric shows the number of times a visitor landed on checkout promo code page.'
, Checkout_Step5_E312 comment 'Web Only: The Checkout: Step 5’ metric shows the number of times a visitor landed on checkout credit on account page.'
, Checkout_Step6_E313 comment 'The Checkout: Step 6’ metric shows the number of times a visitor landed on checkout cart page.'
, Clear_Recent_Search_Term_E177 comment 'The Search: Clear Recent Search Term’ metric shows the number of times a visitor clicked to clear a recent search term from the search dropdown'
, Click_Stream_Integration_Id comment 'Unique Key generated for each record in the Adobe transaction data'
, Clubcard_Savings_Amt_E343 comment 'Web Only: WYSWYG - Available when you login'
, Contact_Save_E191 comment 'The App Checkout: Contact Save’ metric shows the number of times a visitor successfully save contact information on checkout'
, Coupon_Applied_Nbr_E358 comment 'Cart Only: The Coupon Applied’ metric shows the number of the number valid coupons applied to an item (where there is an actual discount) - set in the product stirng'
, Coupon_Available_Nbr_E356 comment 'Future - Not possible from APIs right now on WEB. Number of coupons available to user for the specific item. This is all total coupons tied to user/product for the store that the user is currently shopping in.'
, Coupon_Clip_E306 comment 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.'
, Coupon_Clipped_Nbr_E357 comment 'Cart Only: The Coupon Clipped’ metric shows the number coupons tied to an item - set in the product stirng. Not the same as e306'
, Coupon_Id_Clipped_Nbr_E160 comment 'The Number of CouponIDs Clipped’ metric shows the number of times a visitor clipped coupon offer.'
, Customer_Web_Registion_E334 comment 'The Create Account’ metric shows the number of successful registration on the site.'
, Details_E161 comment 'This will capture Evar161 data.'
, Edit_Order_Cart_E207 comment 'The Edit Order Cart’ metric shows the number of times a visitor clicked on edit order after purchase.'
, Edit_Ts_E205 comment 'The Edit Date/Time’ metric shows the number of times a visitor clicked on edit date/time after purchase.'
, Employee_Savings_Amt_E346 comment 'Web Only: WYSWYG - Employee discount'
, Error_E200 comment 'The Error’ metric shows the number of times a visitor came across with error during the attempt of login.'
, Exclude_Row_Ind comment 'Indicator that the record should be excluded from all analytics.  This is out-of-the-box adobe logic  that we consolidated into a simple flag. CASE WHEN(exclude_hit= 0 and hit_source not in (5 7 8 9)) THEN 0 ELSE 1 END as Exclude_Row_ind   (0=Include(FALSE) 1=Exclude(TRUE))'
, Facility_Integration_ID comment 'Unique Integration Id generated for each Store # in the Facility Table'
, Flash_Light_E176 comment 'The Barcode Scan: Flashlight’ metric shows the number of times a visitor successfully opened barcode scanner those had Flashlight turned on.'
, Gross_Order_E1 comment 'The Gross Orders’ metric shows the number of times a visitor submitted an order. (There are numerous nuances to this metric not the least of which is that order cancellations are tracked as a separate event)'
, Gross_Revenue comment 'The Gross Revenue’ metric shows the revenue associated with orders submitted by visitors. (There are numerous nuances to this metric not the least of which is that substitutions are handled at the time of fulfillment and not tracked within the Adobe clickstream data.)'
, Gross_Units comment 'The Gross Units’ metric shows the number of items in the basket at the time of orders submission by visitors. (There are numerous nuances to this metric not the least of which is that substitutions are handled at the time of fulfillment and not tracked within the Adobe clickstream data.)'
, Hit_Id_High comment 'Used in combination with hitid_low to uniquely identify a hit.'
, Hit_Id_Low comment 'Used in combination with hitid_high to uniquely identify a hit.'
, Item_Price_Amt_E342 comment 'Web Only: WYSWYG = Base Price - All item Savings (Excludes order level promo code)'
, J4u_Savings_Amt_E344 comment 'Web Only: WYSWYG - Available when you clip a J4U coupon'
, List_Price_Amt_E341 comment 'Web Only: WYSWYG = Base price - Club Card Savings'
, Mini_Cart_Open_E111 comment 'Web Only: The Mini Cart Open’ metric shows the number of times a visitor opened mini cart on the site.'
, Modal_Click_E168 comment 'The Modal click’ metric shows the number of times a visitor clicked on modal view.'
, Modal_View_E268 comment 'The Modal View’ metric shows the number of times a visitor viewed modal / drawer offer'
, Null_Search_Nbr_E8 comment 'The Null Searches’ metric shows the number of times a visitor has made a search that returned with zero results'
, OFF_BANNER_DELIVERY_TRUE_IND_E240 comment 'The Off Banner Delivery = True’ metric shows the number of times a visitor viewed Fulfillment Preference Screen and delivery option is an Off Banner Store.'
, OFF_BANNER_DELIVERY_FALSE_IND_E241 comment 'The Off Banner Delivery = False’ metric shows the number of times a visitor viewed Fulfillment Preference Screen and delivery option is on banner.'
, Open_E173 comment 'The Barcode Scan: Open’ metric shows the number of times a visitor successfully opened barcode scanner.'
, Order_Ahead_Cart_Add_E72 comment 'Web Only: Event fires when customer adds the item to the Cart'
, Order_Ahead_Confirmation_E73 comment 'Web Only: Event fires when customer completes setting day and time to pickup item(s) and click send order.'
, Order_Ahead_Product_View_E71 comment 'Web Only: Event fires when a customer clicks to view the item and the PDP page loads'
, Order_Ahead_Total_Over_Due_E75 comment 'Web Only: Event fires along with event73 when the customer clicks send order.'
, Order_Id comment 'ID# of the Order'
, Order_Info_Save_E192 comment 'The App Checkout: Order Info Save’ metric shows the number of times a visitor successfully save order information on checkout'
, Order_Revenue_E355 comment 'Future - Total Price Shipping Taxes etc...'
, Order_Update_Txt_E190 comment 'The App Checkout: Text Order Updates’ metric shows the number of times a visitor successfully submits a phone number to receive order updates by text'
, Out_Of_Stock_E70 comment 'The Out of Stock’ metric shows the number of times a visitor viewed out of stock error on cart or mini-cart page'
, Payment_Save_E193 comment 'The App Checkout: Payment Save’ metric shows the number of times a visitor successfully save payment on checkout'
, Place_Order_Cnt_E307 comment 'The Place Order Count’ metric shows the number of times a visitor clicked on Place Order button in checkout.'
, Product_Decrease_Qty_E150 comment 'The Product: Decrease Quantity’ metric shows the number of times a visitor reduced their quantity after adding the product to cart.'
, Product_Found_Ind_E174 comment 'The Barcode Scan: Product Found’ metric shows the number of times a visitor successfully scanned and found matching product'
, Product_Found_Ind_E175 comment 'The Barcode Scan: No Product Found’ metric shows the number of times a visitor scanned and havent found matching product'
, Product_Increase_Qty_E151 comment 'The Product: Increase Quantity’ metric shows the number of times a visitor increased their quantity after adding the product to cart.'
, Promotion_Cd_E194 comment 'The App Checkout: Promo Code Entered’ metric shows the number of times a visitor successfully entered promo code on checkout'
, Promotion_Code_Savings_Amt_E347 comment 'Web Only: WYSWYG - When promo code is entered'
, Purchase_Cancellation_E208 comment 'The Purchase Cancellation’ metric shows the number of times a visitor sucessfully cancelled the orders.'
, Push_Notification_Click_E76 comment 'App Only: The Push notifications click’ metric shows the number of times a visitor successfully clicks on push notification.'
, Recipe_Swap_Modal_Open_E178 comment 'Not Implemented Yet'
, Recipe_Swap_Modal_Product_E179 comment 'Not Implemented Yet'
, Reserve_Time_Update_E17 comment 'The Reserve Time Update’ metric shows the number of times a visitor updated their pre-booked fulfillment after placing the orders'
, Retail_Customer_UUID comment 'UUID# of the Retail_Customer'
, Reward_Redeemed_E166 comment 'The Reward Redeemed’ metric shows the number of times a visitor successfully redeemed the reward.'
, Rewards_Redeemed_Cnt_E167 comment 'The # of Rewards Redeemed’ metric shows the number of times a visitor successfully redeemed the reward.'
, Rewards_Savings_Amt_E345 comment 'Web Only: WYSWYG - When you redeem a reward'
, Scan_Auto_Clip_E104 comment 'App Only: The Scan auto clip’ metric shows the number of times a visitor successfully scanned and clipped a coupon.'
, Search_Result_Cnt_E156 comment 'The # of Search Results’ metric shows the number of search results returned on search result page.'
, Shipping_Fee_Amt_E201 comment 'The Shipping Fee’ metric shows the amount of shipping fee paid by visitor during the purchase.'
, Sign_In_E10 comment 'The Sign in’ metric shows the number of times a visitor had successful sign in.'
, Sub_Total_Amt_E203 comment 'The SubTotal’ metric shows the amount for products paid by visitor during the purchase excluding shipping fee.'
, Top_Nav_Clicks_E306 comment 'The Coupon Clipped’ metric shows the number of times a visitor clipped coupon on the site.'
, User_Action_E95 comment 'Event is incremented by 1 whenever a user takes an action that isnt loading a new page. i.e. Filtering Clipping coupons modal popups etc...'
, User_Logout_E16 comment 'Web Only: The User Logout’ metric shows the number of times a visitor had successfully logged out.'
, Version_Nbr comment 'Version Number of the Grocery Order placed by the Customer'
, Visit_Nbr comment 'Variable used in the Visit number dimension. Starts at 1 and increments each time a new visit starts per visitor.'
, Visit_Page_Nbr comment 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.'
,Clip_Actions_Per_Visit_Nbr comment 'Number of Clip Actions Per Visit.'
, DW_CREATE_TS comment'When a record is created this would be the current timestamp'
, DW_LAST_UPDATE_TS comment'When a record is updated this would be the current timestamp'
, DW_LOGICAL_DELETE_IND comment'Set to True when we receive a delete record for the primary key, else False'
, DW_SOURCE_CREATE_NM comment'The data source name of this insert'
, DW_SOURCE_UPDATE_NM comment'The data source name of this update or delete'
, DW_CURRENT_VERSION_IND comment'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'
                                   
)
COPY GRANTS
comment = 'VIEW For Click_Stream_Metrics' 
AS
SELECT
 Application_Registration_Ind as Application_Registration_E4
,Base_Price_Ind as Base_Price_E340
,Box_Tops_Get_Started_Ind as Box_Tops_Get_Started_E323
,Checkout_Step1_Ind as Checkout_Step1_E308
,Checkout_Step2_Ind as Checkout_Step2_E309
,Checkout_Step3_Ind as Checkout_Step3_E310
,Checkout_Step4_Ind as Checkout_Step4_E311
,Checkout_Step5_Ind as Checkout_Step5_E312
,Checkout_Step6_Ind as Checkout_Step6_E313
,Clear_Recent_Search_Term_Ind as Clear_Recent_Search_Term_E177
,Click_Stream_Integration_Id as Click_Stream_Integration_Id
,Clubcard_Savings_Amount_Ind as Clubcard_Savings_Amt_E343
,Contact_Save_Ind as Contact_Save_E191
,Coupon_Applied_Number_Ind as Coupon_Applied_Nbr_E358
,Coupon_Available_Number_Ind as Coupon_Available_Nbr_E356
,Coupon_Clip_Ind as Coupon_Clip_E306
,Coupon_Clipped_Number_Ind as Coupon_Clipped_Nbr_E357
,Coupon_Id_Clipped_Number_Ind as Coupon_Id_Clipped_Nbr_E160
,Customer_Web_Registion_Ind as Customer_Web_Registion_E334
,Details_Ind as Details_E161
,Edit_Order_Cart_Ind as Edit_Order_Cart_E207
,Edit_Timestamp_Ind as Edit_Ts_E205
,Employee_Savings_Amount_Ind as Employee_Savings_Amt_E346
,Error_Ind as Error_E200
,Exclude_Row_Ind as Exclude_Row_Ind
,Facility_Integration_ID as Facility_Integration_ID
,Flash_Light_Ind as Flash_Light_E176
,GROSS_ORDER_AMOUNT_IND as Gross_Order_E1
,Gross_Revenue_Amt as Gross_Revenue
,Gross_Units_Nbr as Gross_Units
,Hit_Id_High as Hit_Id_High
,Hit_Id_Low as Hit_Id_Low
,Item_Price_Amount_Ind as Item_Price_Amt_E342
,J4u_Savings_Amount_Ind as J4u_Savings_Amt_E344
,List_Price_Amount_Ind as List_Price_Amt_E341
,Mini_Cart_Open_Ind as Mini_Cart_Open_E111
,Modal_Click_Ind as Modal_Click_E168
,Modal_View_Ind as Modal_View_E268
,Null_Search_Number_Ind as Null_Search_Nbr_E8
,OFF_BANNER_DELIVERY_TRUE_IND as OFF_BANNER_DELIVERY_TRUE_IND_E240
,OFF_BANNER_DELIVERY_FALSE_IND as OFF_BANNER_DELIVERY_FALSE_IND_E241
,Open_Ind as Open_E173
,Order_Ahead_Cart_Add_Ind as Order_Ahead_Cart_Add_E72
,Order_Ahead_Confirmation_Ind as Order_Ahead_Confirmation_E73
,Order_Ahead_Product_View_Ind as Order_Ahead_Product_View_E71
,Order_Ahead_Total_Over_Due_Ind as Order_Ahead_Total_Over_Due_E75
,Order_Id as Order_Id
,Order_Info_Save_Ind as Order_Info_Save_E192
,Order_Revenue_Ind as Order_Revenue_E355
,ORDER_UPDATE_TXT_IND as Order_Update_Txt_E190
,Out_Of_Stock_Ind as Out_Of_Stock_E70
,Payment_Save_Ind as Payment_Save_E193
,Place_Order_Count_Ind as Place_Order_Cnt_E307
,Product_Decrease_Quantity_Ind as Product_Decrease_Qty_E150
,Product_Found_1_Ind as Product_Found_Ind_E174
,Product_Found_2_Ind as Product_Found_Ind_E175
,Product_Increase_Quantity_Ind as Product_Increase_Qty_E151
,Promotion_Code_Ind as Promotion_Cd_E194
,Promotion_Code_Savings_Amount_Ind as Promotion_Code_Savings_Amt_E347
,Purchase_Cancellation_Ind as Purchase_Cancellation_E208
,Push_Notification_Click_Ind as Push_Notification_Click_E76
,Recipe_Swap_Modal_Open_Ind as Recipe_Swap_Modal_Open_E178
,Recipe_Swap_Modal_Product_Ind as Recipe_Swap_Modal_Product_E179
,Reserve_Time_Update_Ind as Reserve_Time_Update_E17
,Retail_Customer_UUID as Retail_Customer_UUID
,Reward_Redeemed_Ind as Reward_Redeemed_E166
,Rewards_Redeemed_Count_Ind as Rewards_Redeemed_Cnt_E167
,Rewards_Savings_Amount_Ind as Rewards_Savings_Amt_E345
,Scan_Auto_Clip_Ind as Scan_Auto_Clip_E104
,Search_Result_Count_Ind as Search_Result_Cnt_E156
,Shipping_Fee_Amount_Ind as Shipping_Fee_Amt_E201
,Sign_In_Ind as Sign_In_E10
,Sub_Total_Amount_Ind as Sub_Total_Amt_E203
,Top_Nav_Clicks_Ind as Top_Nav_Clicks_E306
,User_Action_Ind as User_Action_E95
,User_Logout_Ind as User_Logout_E16
,Version_Nbr as Version_Nbr
,Visit_Nbr as Visit_Nbr
,Visit_Page_Nbr as Visit_Page_Nbr
,Clip_Actions_Per_Visit_Nbr
,DW_CREATE_TS 
,DW_LAST_UPDATE_TS 
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.Click_Stream_Metrics;