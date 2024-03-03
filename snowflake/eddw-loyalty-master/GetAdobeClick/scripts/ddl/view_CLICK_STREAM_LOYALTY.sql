--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_LOYALTY runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_STREAM_LOYALTY 
(
  Click_Stream_Integration_Id comment 'Unique Key generated for each record in the Adobe transaction data'
, Clip_Actions_Per_Visit_Nbr_V156 comment 'Coupon Clips'
, Coupon_Carousel_Section_Nm_V111 comment 'Coupon Carousel section details'
, Coupon_Clip_Method_Cd_V155 comment 'The method how a coupon was clipped i.e. singleclip barcodescan multiclip autoclip etc... - Expires After Hit'
, Coupon_Clipped_V136 comment 'Does not work - will look into it - Expires After Visit'
, Coupon_Id_V102 comment 'The internal ID of the coupon being clipped. i.e. 860808600 - Expires After Visit'
, Coupon_Nm_V103 comment 'The name of the coupon being clipped. i.e. On any purchase in the Produce Department - Expires After Visit'
, Coupon_Product_SKU_V106 comment 'The product sku where the coupon was clipped - Expires After Visit'
, Coupon_Savings_Amt_V104 comment 'Shows the savings amount from the a clipped coupon - Expires After Visit'
, Coupon_Status_Cd_V123 comment 'Historic Data Only - Expires After Visit'
, Coupon_Type_Cd_V105 comment 'The type of coupon that was clipped. i.e. cc  sc pd PersonalizedDeals - Expires After Visit'
, Email_Clipall_Coupon_Txt_V159 comment 'Web Only: Email ClipAll Coupon displays a list of totalCoupons couponsClipped autoclipError clipFailCode clipFailMessage when user interact on website - Expires After Visit'
, Email_Offer_Id_Url_Parameter_V130 comment 'Web Only: Email OfferID URL Parameter displays a list of offerID through which visitors landed on website - Expires After Visit'
, Exclude_Row_Ind comment 'Indicator that the record should be excluded from all analytics.  This is out-of-the-box adobe logic  that we consolidated into a simple flag. CASE WHEN(exclude_hit= 0 and hit_source not in (5 7 8 9)) THEN 0 ELSE 1 END as Exclude_Row_ind   (0=Include(FALSE) 1=Exclude(TRUE))'
, Facility_Integration_ID comment 'Unique Integration Id generated for each Store # in the Facility Table'
, Hit_Id_High comment 'Used in combination with hitid_low to uniquely identify a hit.'
, Hit_Id_Low comment 'Used in combination with hitid_high to uniquely identify a hit.'
, J4U_Coupon_Source_V146 comment 'Web Only: We do not know - Expires After Hit'
, J4U_Coupons_Available_Nbr_V147 comment 'Web Only: We do not know - Expires After Hit'
, J4U_Filter_Type_Dsc_V56 comment 'App Only: The Filter Type a user has set in JFU iFrame. i.e. Filter:Only Personalized Deals - Expires After Visit'
, Loyalty_Application_Column_View_V138 comment 'Does not work - will look into it - Expires After Visit'
, Loyalty_Store_Id_V128 comment 'Historic Data Only - Web Only: We do not know - Data not present from Jul20 - Expires After Visit'
, Loyalty_Zip_Cd_V127 comment 'Historic Data Only - Web Only: We do not know - Data not present from Jul20 - Expires After Visit'
, Offer_ID_V21 comment 'Historic Data Only - Expires After Visit'
, Offer_Type_V20 comment 'Web: Displays a list of offer that visitors interact on the website App: Only Loyalty App when users are clipping coupons on J4U. If clipped via Weekly Ad value is Weekly Ad from Personalized Deals the value is Personalized Deal - Expires After Visit'
, Order_Id comment 'ID# of the Order'
, Product_Coupon_Clipped_V112 comment 'Web Only: Product - Coupon Clipped displays a list of product info where coupons are clipped by visitors - Expires After Visit'
, Product_Coupon_Id_V114 comment 'Web Only: Product - CouponID displays a list of coupon id which was visited by visitors - Expires After Visit'
, Product_Coupons_Available_V113 comment 'Web Only: Product - Coupons Available displays a list of product info which was visited by visitors where coupons are available - Expires After Visit'
, Promotion_Cd_V97 comment 'The promo code entered on checkout  i.e. SAVE20 TAKE20 etc.. - Expires After Purchase'
, Redeemed_Rewards_Nbr_V167 comment 'Web Only: # of Rewards Redeemed displays a total number of rewards redeemed by visitor on the website  (until its implemented on App) - Expires After Hit'
, Retail_Customer_UUID comment 'UUID# of the Retail_Customer'
, Reward_Id_V166 comment 'Shows the ID of the reward clipped. i.e. 1533393857 - Expires After Hit'
, Total_Coupon_Clipped_Nbr_V135 comment 'Does not work - will look into it - Expires After Visit'
, Version_Nbr comment 'Version Number of the Grocery Order placed by the Customer'
, Visit_Nbr comment 'Variable used in the Visit number dimension. Starts at 1 and increments each time a new visit starts per visitor.'
, Visit_Page_Nbr comment 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.'
, RETAIL_CUSTOMER_GUID comment 'GUID# of the Retail_Customer'
,DW_CREATE_TS  comment'When a record is created this would be the current timestamp'
,DW_LAST_UPDATE_TS  comment'When a record is updated this would be the current timestamp'
,DW_LOGICAL_DELETE_IND  comment'Set to True when we receive a delete record for the primary key, else False'
,DW_SOURCE_CREATE_NM  comment'The data source name of this insert'
,DW_SOURCE_UPDATE_NM  comment'The data source name of this update or delete'
,DW_CURRENT_VERSION_IND  comment'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'		

 )
COPY GRANTS comment = 'VIEW FOR CLICK_STREAM_LOYALTY' 
AS
SELECT
 Click_Stream_Integration_Id as Click_Stream_Integration_Id
,Clip_Actions_Per_Visit_Nbr as Clip_Actions_Per_Visit_Nbr_V156
,Coupon_Carousel_Section_Nm as Coupon_Carousel_Section_Nm_V111
,Coupon_Clip_Method_Cd as Coupon_Clip_Method_Cd_V155
,Coupon_Clipped_Txt as Coupon_Clipped_V136
,Coupon_Id as Coupon_Id_V102
,Coupon_Nm as Coupon_Nm_V103
,Coupon_Product_SKU as Coupon_Product_SKU_V106
,Coupon_Savings_Amt as Coupon_Savings_Amt_V104
,Coupon_Status_Cd as Coupon_Status_Cd_V123
,Coupon_Type_Cd as Coupon_Type_Cd_V105
,Email_Clipall_Coupon_Txt as Email_Clipall_Coupon_Txt_V159
,Email_Offer_Id_Url_Parameter_Txt as Email_Offer_Id_Url_Parameter_V130
,Exclude_Row_Ind as Exclude_Row_Ind
,Facility_Integration_ID as Facility_Integration_ID
,Hit_Id_High as Hit_Id_High
,Hit_Id_Low as Hit_Id_Low
,J4U_Coupon_Source_Cd as J4U_Coupon_Source_V146
,J4U_Coupons_Available_Nbr as J4U_Coupons_Available_Nbr_V147
,J4U_Filter_Type_Dsc as J4U_Filter_Type_Dsc_V56
,Loyalty_Application_Column_Vw as Loyalty_Application_Column_View_V138
,Loyalty_Store_Id as Loyalty_Store_Id_V128
,Loyalty_Zip_Cd as Loyalty_Zip_Cd_V127
,Offer_ID as Offer_ID_V21
,Offer_Type_Cd as Offer_Type_V20
,Order_Id as Order_Id
,Product_Coupon_Clipped_Txt as Product_Coupon_Clipped_V112
,Product_Coupon_Id as Product_Coupon_Id_V114
,Product_Coupons_Available_Nbr as Product_Coupons_Available_V113
,Promotion_Cd as Promotion_Cd_V97
,Redeemed_Rewards_Nbr as Redeemed_Rewards_Nbr_V167
,Retail_Customer_UUID as Retail_Customer_UUID
,Reward_Id as Reward_Id_V166
,Total_Coupon_Clipped_Nbr as Total_Coupon_Clipped_Nbr_V135
,Version_Nbr as Version_Nbr
,Visit_Nbr as Visit_Nbr
,Visit_Page_Nbr as Visit_Page_Nbr
,RETAIL_CUSTOMER_GUID as RETAIL_CUSTOMER_GUID
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
FROM EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_STREAM_LOYALTY;