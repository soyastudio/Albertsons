--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_SHOP runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW Click_Stream_Shop
( 
 Cart_Addition_Txt comment 'The Cart Additions’ metric shows the number of times a visitor added an item to cart. (There are numerous nuances to this metric not the least of which is that incrementing the number of units for a given product in the cart is tracked as a separate event)'
, Cart_Id_V137 comment 'The Id associated with a users cart - Expires After Visit'
, Cart_Product1_V202 comment 'Expires After Hit'
, Cart_Product2_V203 comment 'Expires After Hit'
, Cart_Product3_V204 comment 'Expires After Hit'
, Cart_Product4_V205 comment 'Expires After Hit'
, Cart_Product5_V206 comment 'Expires After Hit'
, Cart_Product6_V207 comment 'Expires After Hit'
, Cart_Type_V143 comment 'The type of cart that is being viewed. The same cart is viewed but there are different way to open it from the main cart view checkout cart view mini cart view etc... i.e. app_cart (main app cart) app_checkout_cart (cart from checkout screen) mini_cart full_cart - Expires After Visit'
, Checkout_Txt comment 'The Checkout’ metric shows the number of times a visitor went to checkout.'
, Click_Stream_Integration_Id comment 'Unique Key generated for each record in the Adobe transaction data'
, Club_Card_Nbr_V46 comment 'The users JFU Card number fires whenever its available  - Expires After Year'
, Customer_HHID_V47 comment 'Fires when a user is logged in with a value like 845029223012 - Expires After Year'
, Delivery_Time_V162 comment 'Delivery time for dug and delivery i.e. 8 AM 5 PM etc.. - Expires After Purchase'
, Delivery_Window_V173 comment 'Web Only: Delivery Window displays delivery slot selected by visitors on checkout page - Expires After Purchase'
, Ecom_Customer_Id_V98 comment 'The internal customer id of the user i.e. 555-051-1578647650762 - Expires After Visit'
, Exclude_Row_Ind comment 'Indicator that the record should be excluded from all analytics.  This is out-of-the-box adobe logic  that we consolidated into a simple flag. CASE WHEN(exclude_hit= 0 and hit_source not in (5 7 8 9)) THEN 0 ELSE 1 END as Exclude_Row_ind   (0=Include(FALSE) 1=Exclude(TRUE))'
, Facility_Integration_ID comment 'Unique Integration Id generated for each Store # in the Facility Table'
, Fulfillment_Banner_Cd_V170 comment 'Shows what Safeway company analytics is firing on. i.e. albertsons  safeway vons etc.. (duplicate of v4) - Expires After Visit'
, Fulfillment_Type_Cd_V35 comment 'Delivery Preference that is set on every analytics call including order confirmations. Main values are instore delivery and dug (pickup) - Expires After Never'
, Gross_Order_Nbr comment 'The Gross Orders’ metric shows the number of times a visitor submitted an order. (There are numerous nuances to this metric not the least of which is that order cancellations are tracked as a separate event)'
, Hit_Id_High comment 'Used in combination with hitid_low to uniquely identify a hit.'
, Hit_Id_Low comment 'Used in combination with hitid_high to uniquely identify a hit.'
, Item_Refund_V198 comment 'Expires After Hit'
, MFC_V71 comment 'Web Only: Micro Fulfillment Center set to true for MFC Carts - Expires After Visit'
, MTO_Flag_V212 comment 'Set for the prodcut on Cart Add and  PDP True Or False  - Expires After Hit'
, Order_Count_V101 comment 'The amount of orders a user has previously made. This fires on every analytics call (not on loyalty app) - Expires After Visit'
, Order_Id comment 'ID# of the Order'
, Order_ID_V32 comment 'Order ID once transcation is successfully completed - Expires After Visit'
, Order_ID_V73 comment 'Web Only: Order Ahead: Order ID displays a list of storeids along with category codes where visitors viewed it on Order Ahead section of the website - Expires After Hit'
, Order_Issue_Reported_V165 comment 'Web Only: Set to teh order issue reported when requesting refund - Expires After Hit'
, Order_Substitution_Options_V194 comment 'Expires After Purchase'
, Payment_Method_V160 comment 'Payment Methods such as i.e. cc ebt cc-coa cc & coa coa and ebt-coa - Expires After Purchase'
, Pickup_Location_V189 comment 'Location where a user is picking up their purchase. i.e. locker counter kiosk dug delivery - Expires After Visit'
, Price_Information_Txt comment 'Price of the item added to cart'
, Product_Finding_Method_V14 comment 'Displays a list of page type search recommendation through which visitors found the products on the website.  - Expires After Purchase'
, Product_List comment 'Product list as passed in through the products variable. Products are delimited by commas while individual product properties are delimited by semicolons.'
, Product_List_V181 comment 'Web Only: Product List displays list unit product id unit price when added to cart by visitor on website - Expires After Hit'
, Product_Name_V77 comment 'The name of the product being added removed viewed in cart and searched - Expires After Visit'
, Product_Pricing_Type_V115 comment 'Web: Shows the pricing type i.e. club card price App: values do not make sense in APP - Use web description - Expires After Visit'
, Product_Purchase_Options_V199 comment 'Instore Online Only  - Expires After Hit'
, Product_View comment 'The Product views’ metric shows the number of times any product was viewed.'
, Retail_Customer_UUID comment 'UUID# of the Retail_Customer'
, GROSS_REVENUE_AMT comment 'Gross Revenue associated with the individual cart addition'
, Store_Id_V13 comment 'The ID of the Store i.e. 3132 - Shop Store ID displays a list of store id which is selected by Visitors.  - Expires After Visit'
, Store_Zip_Cd_V9 comment 'Displays a list of zip code that visitors preferred while navigating on the website - Expires After Visit'
, Total_Cart_Amt_V126 comment 'When a user adds an item to cart the current cart total amount is reported - Expires After Visit'
, Unique_Sku_In_Cart_Nbr_V125 comment '# of unqiue items in cart. If you have 50 items in cart but 10 quantities of 5 unqiue items then value would be 5 - Expires After Visit'
, GROSS_UNITS_NBR comment '# of units associated with the individual cart addition'
, UUID_V182 comment 'Does not work - will look into it - Expires After Visit'
, Version_Nbr comment 'Variable used in the Visit number dimension. Starts at 1 and increments each time a new visit starts per visitor.'
, Visit_Nbr comment 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.'
, Visit_Page_Nbr comment 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.'
, DW_CREATE_TS  comment'When a record is created this would be the current timestamp'
 ,DW_LAST_UPDATE_TS  comment'When a record is updated this would be the current timestamp'
 ,DW_LOGICAL_DELETE_IND  comment'Set to True when we receive a delete record for the primary key, else False'
 ,DW_SOURCE_CREATE_NM  comment'The data source name of this insert'
 ,DW_SOURCE_UPDATE_NM  comment'The data source name of this update or delete'
 ,DW_CURRENT_VERSION_IND  comment'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'		

)
COPY GRANTS
comment = 'VIEW For Click_Stream_Shop' 
AS
SELECT
Cart_Addition_Ind as Cart_Addition_Txt
,Cart_Id as Cart_Id_V137
,Cart_Product1_Nbr as Cart_Product1_V202
,Cart_Product2_Nbr as Cart_Product2_V203
,Cart_Product3_Nbr as Cart_Product3_V204
,Cart_Product4_Nbr as Cart_Product4_V205
,Cart_Product5_Nbr as Cart_Product5_V206
,Cart_Product6_Nbr as Cart_Product6_V207
,Cart_Type_Cd as Cart_Type_V143
,Checkout_Ind as Checkout_Txt
,Click_Stream_Integration_Id as Click_Stream_Integration_Id
,Club_Card_Nbr as Club_Card_Nbr_V46
,Customer_HHID as Customer_HHID_V47
,Delivery_Tm as Delivery_Time_V162
,Delivery_Window_Txt as Delivery_Window_V173
,Ecom_Customer_Id as Ecom_Customer_Id_V98
,Exclude_Row_Ind as Exclude_Row_Ind
,Facility_Integration_ID as Facility_Integration_ID
,Fulfillment_Banner_Cd as Fulfillment_Banner_Cd_V170
,Fulfillment_Type_Cd as Fulfillment_Type_Cd_V35
,Gross_Order_Ind as Gross_Order_Nbr
,Hit_Id_High as Hit_Id_High
,Hit_Id_Low as Hit_Id_Low
,Item_Refund_Cd as Item_Refund_V198
,MFC_Flg as MFC_V71
,MTO_Flg as MTO_Flag_V212
,Order_Cnt as Order_Count_V101
,Order_Id as Order_Id
,Order_2_ID as Order_ID_V32
,Order_3_ID as Order_ID_V73
,Order_Issue_Reported_Txt as Order_Issue_Reported_V165
,Order_Substitution_Options_Txt as Order_Substitution_Options_V194
,Payment_Method_Cd as Payment_Method_V160
,Pickup_Location_Cd as Pickup_Location_V189
,Price_Information_Txt as Price_Information_Txt
,Product_Finding_Method_Cd as Product_Finding_Method_V14
,Product_Lst as Product_List
,Product2_Lst as Product_List_V181
,Product_Nm as Product_Name_V77
,Product_Pricing_Type_Cd as Product_Pricing_Type_V115
,Product_Purchase_Options_Txt as Product_Purchase_Options_V199
,Product_View_Ind as Product_View
,Retail_Customer_UUID as Retail_Customer_UUID
,GROSS_REVENUE_AMT
,Store_Id as Store_Id_V13
,Store_Zip_Cd as Store_Zip_Cd_V9
,Total_Cart_Amt as Total_Cart_Amt_V126
,Unique_Sku_In_Cart_Nbr as Unique_Sku_In_Cart_Nbr_V125
,GROSS_UNITS_NBR
,Customer_UUID as UUID_V182
,Version_Nbr as Version_Nbr
,Visit_Nbr as Visit_Nbr
,Visit_Page_Nbr as Visit_Page_Nbr
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.Click_Stream_Shop;