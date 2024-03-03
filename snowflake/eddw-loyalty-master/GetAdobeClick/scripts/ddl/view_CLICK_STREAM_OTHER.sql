--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_OTHER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CLICK_STREAM_OTHER
(
 Accepted_Language_Cd comment 'Lists all accepted languages as indicated in the Accept-Language HTTP header in an image request.'
, Accordion_Edit_Nbr_V158 comment 'Identifies how many Accordions that need to be editied in Checkout i.e. 0 1 2 3 etc.. - Expires After Visit'
, Accordion_Edit_Txt_V157 comment 'Aggeration of Accordions that user MUST edit were editied i.e. Payment Order InfoPayment ContactOrder InfoPayment etc... - Expires After Visit'
, Activity_Id_V131 comment 'Web Only: Target: Activity ID displays a list of activity id for the list of campaigns displayed to visitors through Adobe Target - Expires After Visit'
, Activity_Nm_V132 comment 'Web Only: Target: Activity Name displays a list of activity name for the list of campaigns displayed to visitors through Adobe Target - Expires After Visit'
, ADA_Flag_V178 comment 'Americans with Disabilities - Flag that shows if the user has enabled the ADA Experinces - Expires After Visit'
, Adobe_TNT_Id comment 'Used in Adobe Target integrations.'
, App_Banner_Cd comment 'App Only: Banner associated with the Mobile Application (separate from the Brand associated with the Store ID; you can access an Albertsons store from a Safeway app if only to be transferred into an Albertsons App)'
, App_Build_Nbr comment 'App Only: Build number of the Mobile Application: each release is assigned a unique Version Number with each build of the Version identifying more precisely which code build was installed on the users device.'
, Application_Availability_Dt comment 'Flag for whether the product is available or not.'
, Application_Detail_Txt_V116 comment 'Application Level Detail '
, Application_Order_Status_Msg_V119 comment 'Historic Data Only - Expires After Visit'
, Application_Os_Txt comment 'App Only:Operating System the mobile application'
, Application_Os_Type_Cd comment 'App Only:Operating System and App Type of the mobile application'
, Application_Separate_Lists_V149 comment 'We do not know - Values are shopping_list_enable and shopping_list_disable - Expires After Never'
, APPLICATION_SIGN_IN_IND comment 'The Sign in’ metric shows the number of times a visitor had successful sign in. (recorded Once Per Visit)'
, Application_Type_Cd comment 'App Only:Description of the Mobile Application Type: UMA Shop App Legacy Loyalty App etc.'
, Application_Type_Cd_V23 comment 'Web: Reports Content type i.e. safeway albertsons J4U etc... App: Reports App Type iOS or Android - Expires After Visit'
, Application_User_Start_By_V25 comment 'App Only: J4U only variable. Shows how the App was launched. - Expires After Visit'
, Application_Version_Build_Nbr_V116 comment 'App Only: Version and Build number of the Mobile Application: each release is assigned a unique Version Number with each build of the Version identifying more precisely which code build was installed on the users device.'
, Application_Version_Cd comment 'App Only:Version of the Mobile Application: each release is assigned a unique Version Number with each build of the Version tracked in a separate field'
, Availabile_Cd_V70 comment 'Tied to the product string on cart views. Reports if a product is available or not-available - Expires After Visit'
, Banner_Cd_V4 comment 'Shows what Safeway company analytics is firing on. i.e. albertsons  safeway vons etc.. - Expires After Visit'
, Bot_Tracking_V195 comment 'Historcal Only - Captures bot info from Imperva'
, Box_Tops_Auth_State_V129 comment 'Web Only: We do not know - Data not present from Jul20 - Expires After Visit'
, Browser_GEO_V174 comment 'Web Only: Browser GEO displays visitors location through which accessing the website - Expires After Never'
, Browser_Height comment 'Height in pixels of the browser window.'
, Browser_Nm comment 'Name/Version of the browser'
, Browser_Width comment 'Width in pixels of the browser window.'
, Camera_Allowed_Cd_V193 comment 'App Only: Shows what level of camera permissions a user has permitted on their App - Expires After Hit'
, Campaign_Affiliate_Txt_V93 comment 'Web Only: Campaign - Affiliate displays a list of campaign id related to Affiliate through visitors landed on website - Expires After Day'
, Campaign_Stacking_Txt_V18 comment 'Web Only: Displays a list of program along with visitor id through which visitor came to website - Expires After Month'
, Campaign_Txt comment 'Variable used in the Tracking Code dimension.'
, Campaign_Txt_V175 comment 'Does not work - will look into it - Expires After Visit'
, Card_Less_Registration_Cd_V19 comment 'Historic Data Only - Displays YES when visitors does short registrations - Expires After Year'
, Carousel_Size_Txt_V185 comment 'Web Only: Carousel Size displays list of size when visitor add/remove product/quantity from cart - Expires After Hit'
, Carrier_Nm comment 'Adobe Advertising Cloud integration variable. Specifies the mobile carrier. References the carrier lookup table.'
, CDA_Marketing_Channel_V196 comment 'Expires After Visit'
, Channel_Manager_Channel_V36 comment 'Web Only: Displays a list of program through which visitor came to the website - Channel Manager Channel displays a list of program through which visitor came to the website - Expires After Visit'
, Channel_Stacking_Txt_V37 comment 'Web Only: Displays a list of consecutive program through which visitor came to the website - Channel Stacking displays a list of consecutive program through which visitor came to the website - Expires After Visit'
, Click_Stream_Integration_Id comment 'Unique Key generated for each record in the Adobe transaction data'
, Color_Cd comment 'Color depth ID based on the value of the c_color column. References the color_depth.tsv lookup table.'
, Connection_Type_Nm comment 'Connection type The Connection type’ dimension shows how the visitor connected to the internet. This dimension is useful to determine how visitors connect to the internet to browse your site. You can use it to optimize site content based on the connection speed of visitors. Populate this dimension with data This dimension uses a combination of the ct query string and Adobe server-side logic. Adobe uses the following rules in order to determine its value: If the ct query string equals modem set the dimension item to Modem. AppMeasurement only collects this data on unsupported Internet Explorer browsers making this dimension item uncommon. Check the IP address of the hit and reference it to a lookup table internal to Adobe. If the IP address is from a mobile carrier set the dimension item to Mobile Carrier.If the ct query string equals lan set the dimension item to LAN/Wifi. If the hit originates from a Data source or is otherwise considered a special type of hit set the dimension item to Not specified.If none of the above rules are met default to the value of LAN/Wifi. Dimension items Dimension items include LAN/Wifi Mobile Carrier Modem and Not Specified. LAN/Wifi: The visitor connected to the internet through a landline or wifi hotspot.Mobile Carrier: The visitor connected to the internet through a mobile carrier. Modem: The visitor connected to the internet through a modem on an unsupported Internet Explorer browser. Not Specified: The hit did not have a connection type.'
, Country_Nm comment 'Countries: The Countries’ dimension reports the country where the hit originated from. This dimension is useful to help determine what the most popular countries visitors originate from when visiting your site. You could use this data to focus on marketing efforts in these countries or make sure your site experience is optimal in countries that have different primary languages. Populate this dimension with data This dimension references lookup rules internal to Adobe. The lookup value is based on the IP address sent with the hit. Adobe partners with Digital Element to maintain lookups between IP address and country. This dimension works out of the box for all implementations. Dimension items Dimension items include countries all over the world. Example values include United States United Kingdom or India. Differences between reported and actual locationSince this dimension is based on IP address some scenarios can show a difference between reported location and actual location: IP addresses that represent corporate proxies: These visitors can appear as traffic coming through the user’s corporate network which can be a different location if the user is working remotely.Mobile IP addresses: Mobile IP targeting works at varying levels depending on the location and the network. A number of carriers backhaul IP traffic through centralized or regional points of presence.Satellite ISP users: Identifying the specific location of these users is difficult as they typically appear to originate from the uplink location.Military and government IPs: Represents personnel traveling around the globe and entering through their home location rather than the base or office where they are currently stationed.'
, Create_Dt comment 'transform DATE_TIME to date (just date)'
, Create_Ts comment 'transform DATE_TIME to date (just date)'
, Custom_Nav_Link_Tracking_Txt_V74 comment 'App: Only on J4U iFrame when delivery app is clicked Web: Custom Nav/Link Tracking displays just for you app through which visitor selected the deliver apps - Expires After Visit'
, Customer_Status_Cd_V100 comment 'If a user is a first or returning user. A user is considered returning only after they have had 1 or more orders. If a user has never made an order they will always be shown as first - Expires After Visit'
, Delivery_Attended_Unattended_Flag_V161 comment 'Whether a delivery is attended or unattended - Expires After Purchase'
, Detail_View_Txt_V24 comment 'App Only?: Displays a list of popup options on App - Very little hits. Possibly broken. - Expires After Visit'
, Ecom_Login_Id_V7 comment 'Historic Data Only - Expires After Visit'
, Ecom_Nav_Link_Tracking_Cd_V75 comment 'Web Only: We do not know - Expires After Visit'
, Elevaate_Flag_V144 comment 'Historic Data Only - Web Only: We do not know - Expires After Visit'
, Elevaate_Poistion_Nbr_V145 comment 'Historic Data Only - Web Only: We do not know - Expires After Visit'
, Email_HHID_Url_Parameter_V118 comment 'Values appended from email parameters - Expires After Visit'
, Email_Theme_URL_Parameter_V117 comment 'Values appended from email parameters - Expires After Visit'
, Environment_Cd_V99 comment 'Web Only: Environment displays a list of website environment that visitors are interacting - Expires After Visit'
, Error_Feature_Cd_V141 comment 'The component / feature that had an error - Expires After Hit'
, Error_Id_V140 comment 'The Error ID generated from the system - Expires After Hit'
, Error_Message_V142 comment 'The message from an error - Expires After Hit'
, Error_Page_Dsc_V33 comment 'Historic Data Only - Expires After Visit'
, Event_Id_URL_Parameter_V139 comment 'Web Only: EventID URL Parameter displays a list of event id of J4U that visitors clicked on the website - Expires After Hit'
, Event_Nm comment 'Name of the event triggered on the hit. Includes both default events and custom events 1-1000. Uses event.tsv lookup.'
, Exclude_Hit_Flg comment 'Flag indicating that the hit is excluded from reporting. The visit_num column is not incremented for excluded hits.|1: Not used. Part of a scrapped feature.|2: Not used. Part of a scrapped feature.|3: No longer used. User agent exclusion|4: Exclusion based on IP address|5: Vital hit info missing such as page_url pagename page_event or event_list|6: JavaScript did not correctly process hit|7: Account-specific exclusion such as in a VISTA rules|8: Not used. Alternate account-specific exclusion.|9: Not used. Part of a scrapped feature.|10: Invalid currency code|11: Hit missing a timestamp on a timestamp-only report suite or a hit contained a timestamp on a non-timestamp report suite|12: Not used. Part of a scrapped feature.|13: Not used. Part of a scrapped feature.|14: Target hit that did not match up with an Analytics hit|15: Not currently used.|16: Advertising Cloud hit that did not match up to an Analytics hit'
, Exclude_Row_Ind comment 'Indicator that the record should be excluded from all analytics.  This is out-of-the-box adobe logic  that we consolidated into a simple flag. CASE WHEN(exclude_hit= 0 and hit_source not in (5 7 8 9)) THEN 0 ELSE 1 END as Exclude_Row_ind   (0=Include(FALSE) 1=Exclude(TRUE))'
, Experience_Nm_V133 comment 'Web Only: Target: Experience Name displays a list of experience name for the list of campaigns displayed to visitors through Adobe Target - Expires After Visit'
, Face_Book_Account_Nm_V54 comment 'Historic Data Only - Expires After Visit'
, Face_Book_Banner_V55 comment 'Historic Data Only - Expires After Visit'
, Face_Book_Campaign_V64 comment 'Historic Data Only - Expires After Visit'
, Facility_Integration_ID comment 'Unique Integration Id generated for each Store # in the Facility Table'
, Filter_Section_V108 comment 'Shows the filter brand selected for the category as [FILTER TYPE] : [FILTER SELECTION. i.e. brand : signature select - Expires After Visit'
, Filter_Type_Cd_V107 comment 'Shows the filter category selected. i.e. deals aisles. If multiple filter categories are selected they are seperated by commas i.e.  dealsaisles - Expires After Visit'
, First_Hit_Referrer_Type_Cd comment 'Numeric ID representing the referrer type of the very first referrer of the visitor. Uses referrer_type.tsv lookup.'
, GA_Utm_Campaign_Medium_V86 comment 'Web Only: GA utm_ medium displays a list of advertising or marketing through which visitors landed on the website. - Expires After Custom(180 Days)'
, GA_Utm_Campaign_Nm_V84 comment 'Web Only: GA utm_ campaign displays a list of campaign name through which visitors landed on the website. - Expires After Custom(180 Days)'
, GA_Utm_Source_V85 comment 'Web Only: GA utm_source displays a list of advertisers site publication through which visitors landed on the website. - Expires After Custom(180 Days)'
, Global_No_Substitution_V38 comment 'Web: Displays while placing order whether visitor Checked or Unchecked Global Substitution....and more App: Displays while placing order whether visitor Checked or Unchecked Global Substitution - Expires After Purchase'
, Hidden_Categories_Txt_V22 comment 'App Only: Displays a list of hidden categories options at the time of launching the App - Expires After Visit'
, Hit_Id_High comment 'Used in combination with hitid_low to uniquely identify a hit.'
, Hit_Id_Low comment 'Used in combination with hitid_high to uniquely identify a hit.'
, Hit_Source_Cd comment 'Indicates what source the hit came from. Hit sources 1 2 and 6 are billed.|1: Standard image request without timestamp|2: Standard image request with timestamp|3: Live data source upload with timestamps|4: Not used|5: Generic data source upload|6: Full processing data source upload|7: TransactionID data source upload|8: No longer used; Previous versions of Adobe Advertising Cloud data sources|9: No longer used; Adobe Social summary metrics|10: Audience Manager server-side forwarding used'
, Home_Page_Carousel_V26 comment 'Web Only: displays a list of categories where visitors selected the products/coupons on shop page. - Expires After Hit'
, Impressions_Component_V180 comment 'App: Show if users have ZTP enabled or not Web: Impressions Component displays list of components viewed by the visitor on website - Expires After Hit'
, Internal_Campaign_Tracking_Id_V2 comment 'Displays a list of campaigns that visitors have clicked internally on the website - Expires After Purchase'
, Internal_Search_Results_Nbr_V122 comment '# of search results on a search results page. If no search results then the value is 0 - Expires After Visit'
, Internal_Search_Terms_V1 comment 'Displays a list of search terms that were used to return a search results page. If a search term was a typeahead value that value will be shown here the actual user input is in v91 - Expires After Visit'
, Internal_Search_Txt comment 'The Internal Searches’ metric shows the number of times a visitor searched on the site.'
, Internal_Search_Type_V120 comment 'Records the type of search once search results screen is loaded. i.e. internalsearch:typeahead:suggestion internalsearch:typeahead:recent internalsearch:standard internalsearch:standard = you typed in all the search term internalsearch:typeahead:suggestion = you clicked a prefilled option internalsearch:typeahead:popular = you clicked a trending item internalsearch:typeahead:recent = you clicked a Recently Search Item - Expires After Visit'
, IP_Address_V88 comment 'The users IP Address i.e. 65.208.210.98 - Expires After Visit'
, Java_Script_Version_Nbr comment 'Lookup ID of JavaScript version based on j_jscript. Uses lookup table.'
, KMSI_V81 comment 'Web Only: ..... - Expires After Visit'
, Language_Nm comment 'Languaged used'
, Last_Activity_V17 comment 'Historic Data Only - Expires After Visit'
, Launch_Rule_V184 comment 'Web Only: Launch Rule displays list of rules executed when visitor interacted on website - Expires After Hit'
, Link_Detail_V76 comment 'Expires After Visit'
, List_Interaction_Type_V63 comment 'App Only: Displays if visitors clicked on check uncheck add on loyalty app. - Expires After Visit'
, Location_Sharing_Enabled_V201 comment 'App Only: Shows what level of location sharing a user has permitted on their App - Expires After Hit'
, Login_KMSI_V16 comment 'Web Only: Login Keep Me Signed In displays the login status of visitors - Expires After Visit'
, Map_Clicks_V150 comment 'Web Only: Activity Map Clicks displays a list links clicked by visitors on website - Expires After Visit'
, Map_Link comment 'Activity Map link'
, Map_Link_By_Region comment 'Activity Map link by region'
, Map_Page comment 'Activity Map page'
, Map_Region comment 'Activity Map region'
, Marketing_Channel_Cd comment 'Numeric ID that identifies the Last touch channel dimension. Lookup for this ID can be found in the Marketing Channel Manager.'
, Marketing_Channel_Dtl comment 'Variable used in the Last touch detail dimension.'
, Media_Placement_V80 comment 'Web Only: displays a product grid or banner through which products where selected by visitors. - Expires After Hit'
, Media_Type_V148 comment 'Historic Data Only - We do not know - The media type as to how a product was added to cart? i.e. carousels product-grid product-carousel - Expires After Visit'
, Message_Txt_V134 comment 'Does not work - will look into it - Expires After Visit'
, Mobile_Application_First_Launch_Dt_V6 comment 'App Only: First launch date of a mobile app - Expires After Visit'
, Mobile_Device_Id_V57 comment 'App Only: The AAID / IDFA of a mobile app user - Expires After Visit'
, Mobile_Device_Model_Nm_V58 comment 'Historic Data Only - Expires After Visit'
, Mobile_Device_OS_Version_V59 comment 'Historic Data Only - Expires After Visit'
, Mobile_J4U_Application_Version_V53 comment 'App Only: The version of JFU App (iFrame) - Expires After Visit'
, Mobile_Latitude_Longitude_V60 comment 'Historic Data Only - Expires After Visit'
, Mobile_VS_Non_Mobile_V51 comment 'Identifies if a user is on Mobile or Non-mobile device - Expires After Visit'
, Modal_Name_Link_V168 comment 'Shows the modals views / clicks inside those modals. On App - Drawers are also considered Modals - Expires After Hit'
, Navigation_source_V61 comment 'App Only: How users a navigating on J4U iFrame Content (old data) - Expires After Visit'
, Network_Txt_V176 comment 'Does not work - will look into it - Expires After Visit'
, New_Repeat_Visitors_V8 comment 'Historic Data Only - Expires After Visit'
, Notification_Allowed_V192 comment 'App Only: Shows what level of notification permissions a user has permitted on their App - Expires After Hit'
, Operating_System_Cd comment 'Numeric ID representing the operating system of the visitor. Based on the user_agent column. Uses os lookup.'
, Operating_System_Nm comment 'Operating System Name and Version'
, Order_Id comment 'ID# of the Order'
, Page_Nm comment 'Used to populate the Page dimension. If the pagename variable is empty Analytics uses page_url instead.'
, Page_Nm_V5 comment 'Shows the pagename - hierarchy of page levels a user is on. Concatenation of v4:[CHANNEL]:v151:v152:v153:v154 - Expires After Visit'
, Page_URL_V11 comment 'Web Only: Displays a list of current page url that visitor visited on the website - Expires After Visit'
, Past_Purchase_Items_V179 comment 'Web Only: Past Purchase Items displays list of past purchased when visitors done the search and landed on search results page - Expires After Visit'
, PFM_Detail_V14 comment 'Displays a list of page type search recommendation through which visitors found the products on the website.  - Expires After Purchase'
, PFM_Source_V12 comment 'Web Only: Tied to the product string. When an item is added to cart that is out of stock then you have the ability to add similar items to cart. Similar items can be added from PDP or Full Cart page - Expires After Purchase'
, PFM_Subsection_1_V3 comment 'PFM SubSection1 displays a list of page type which was the starting point of product selected by visitors on the website - Expires After Purchase'
, Placement_Type_V78 comment 'Historic Data Only - Expires After Visit'
, Platform_Cd comment 'Breakdown(s) of Web/App app_type_os/web desktop/web mobile etc.'
, Platform_V90 comment 'Breakdown(s) of Web/App app_type_os/web desktop/web mobile etc.'
, Plugin_Nm comment 'No longer used. List of numeric ID’s that correspond to plugins available within the browser. Uses plugins.tsv lookup.'
, Premium_Slots_V169 comment 'Web Only: Premium Slots displays premius slots set to visitors on the checkout or not - Expires After Visit'
, Previous_Page_Nm_V10 comment 'Displays a list of page prior to the current page that visitor visited on the website - Expires After Visit'
, Provider_Txt_V177 comment 'Does not work - will look into it - Expires After Visit'
, Purchase_Id comment 'Unique identifier for a purchase as set using the purchaseID variable. Used by the duplicate_purchase column.'
, Push_Notifications_Message_Id_V62 comment 'App Only: The Push ID clicked by a user. The Push ID is generated from the Push Notification Service - Expires After Visit'
, Recipe_Nm_V190 comment 'Web Only: Shows Recipe name - Expires After Visit'
, Recipe_Source_V191 comment 'Web Only: The category under which the recipes is listed is called the recipe source. This is a numeric value grabbed from the recipe url path. - Expires After Visit'
, Referrer_type_id comment 'Single-string identifier of the type of referral for the hit. Used in the Referrer type dimension. 1: Inside your site,2: Other web sites,3: Search engines,4: Hard drive,5: USENET,6: Typed/Bookmarked (no referrer),7: Email,8: No JavaScript,9: Social Networks'
, Referring_Application_V87 comment 'The utm_source utm_medium and timestamp of referring source - Expires After Visit'
, Resolution_Nm comment 'Numeric ID representing the resolution of the monitor. Used in the Monitor resolution dimension. Uses resolution.tsv lookup table.'
, Retail_Customer_UUID comment 'UUID# of the Retail_Customer'
, SDK_Verison_V183 comment 'App only: The Adobe Experince Platform SDK Version that is install in the App - Expires After Visit'
, Search_Engine_Nm comment 'The Search Engine that referred the visitor to your site. Uses search_engines.tsv lookup.'
, Social_Authors_V65 comment 'Web Only: We do not know - Expires After Visit'
, Social_Media_Channel_V67 comment 'Historic Data Only - Expires After Visit'
, Social_Media_Content_Title_V68 comment 'Historic Data Only - Expires After Visit'
, Social_Platforms_V40 comment 'Historic Data Only - Expires After Visit'
, Sort_Selection_V109 comment 'Shows the sorting method of the filer. i.e. price low to high - Expires After Visit'
, Source_Site_Type_V52 comment 'Web: Source / Site Type displays a list of banner visited by visitors through App or website. App: Shows source of analytics call. i.e. values report as Mobile Application|[BANNER] or Mobile Application|J4U - Expires After Visit'
, Sub_Section1_V151 comment 'Level 1 of pagename - Expires After Visit'
, Sub_Section2_V152 comment 'Level 2 of pagename - Expires After Visit'
, Sub_Section3_V153 comment 'Level 3 of pagename - Expires After Visit'
, Sub_Section4_V154 comment 'Level 4 of pagename - Expires After Visit'
, Subscription_Dt_V164 comment 'Web Only: Subscription Date displays a visitor subscribed date to savings club - Expires After Never'
, Subscription_Funnel_V188 comment 'Web Only: Subscription Funnel displays enrollment to monthly/annually from checkout to confirmation page  (for now until its released for App) - Expires After Hit'
, Subscription_Status_V163 comment 'App: Whether a user is a subscriber or non-subscriber to savings club Web: Subscription Status displays a visitor is a subscriber or non-subscriber to savings club - Expires After Never'
, Syndigo_Content_V197 comment 'Expires After Hit'
, Time_Parting_V15 comment 'Takes the timestamp of collected hits and breaks it into more meaningful dimensions such as “Hour of Day” or “Day of Week”. This eVar is a classification.  - Expires After Visit'
, Timestamp_Marketplace_V172 comment 'Web Only: Scenarios needed to update the description - Data not present from May20 - Expires After Hit'
, Top_Nav_Usage_V171 comment 'Web Only: Top Nav usage displays menu navigation selected from top by visitor on website - Expires After Visit'
, Typed_Search_Cnt_V121 comment 'The amount of characters a user has typed into the input field. Fires on the search results page. - Expires After Visit'
, Typed_Search_Term_V91 comment 'Shows the search term a user typed. If typeahead was prepopulated this value will still show what was typed by the user. - Expires After Visit'
, UMA_Application_V200 comment 'App Only: If value is true then data is only from UMA App - Expires After Hit'
, User_Action_Type_V6 comment 'Historic Data Only - Expires After Visit'
, User_Action_V95 comment 'Interaction events - User initiated actions that happen on pages or values used to define success events - Expires After Visit'
, User_Agent_V94 comment 'The device / browser a user is using - Expires After Visit'
, User_Message_Status_V124 comment 'Historic Data Only - Expires After Visit'
, User_Messages_V96 comment 'Web Only: User Messages displays a list of user interaction error or modal displayed title on the website - Expires After Hit'
, User_Type_V92 comment 'Web Only: User Type displays a list of user type when visitor does the login - Expires After Visit'
, Version_Nbr comment 'Version Number of the Grocery Order placed by the Customer'
, Video_Ad_Load_Txt comment 'Video Ad Load '
, Visit_Id comment 'Unique identifier for a visit as identified by Adobe'
, Visit_Nbr comment 'Variable used in the Visit number dimension. Starts at 1 and increments each time a new visit starts per visitor.'
, Visit_Page_Nbr comment 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.'
, Visitor_Id comment 'Unique identifier for a visitor as identified by Adobe'
, Visitor_Id_V49 comment 'Always fires. Unique value that represents a customer in both the online and offline systems. - Expires After Never'
, Visitor_Interacted_HH_MM comment 'Displays Hours and Minutes where visitors interacted on website. - Expires After Visit'
, Wearable_Device_V89 comment 'The wearable device a user is using i.e. AppleWatch - Expires After Visit'
, ZTP_Default_Method_V186 comment 'The default ZTP (Zero Touch Payment) Method i.e. Apple Pay Empty Wallet ACH Store Value Card PREPAID - Expires After Visit'
, ZTP_Method_V187 comment 'How users are paying with ZTP (Zero Touch Payment) i.e. ACH Store Value Card - Expires After Visit'
,Top_Nav_Clicks_Txt comment 'Number of Clip Actions Per Visit'
,DW_CREATE_TS  comment'When a record is created this would be the current timestamp'
,DW_LAST_UPDATE_TS  comment'When a record is updated this would be the current timestamp'
,DW_LOGICAL_DELETE_IND  comment'Set to True when we receive a delete record for the primary key, else False'
,DW_SOURCE_CREATE_NM  comment'The data source name of this insert'
,DW_SOURCE_UPDATE_NM  comment'The data source name of this update or delete'
,DW_CURRENT_VERSION_IND  comment'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'		

)
COPY GRANTS comment = 'VIEW FOR CLICK_STREAM_OTHER' 
AS 
SELECT
Accepted_Language_Cd as Accepted_Language_Cd
,Accordion_Edit_Nbr as Accordion_Edit_Nbr_V158
,Accordion_Edit_Txt as Accordion_Edit_Txt_V157
,Activity_Id as Activity_Id_V131
,Activity_Nm as Activity_Nm_V132
,ADA_Flg as ADA_Flag_V178
,Adobe_TNT_Id as Adobe_TNT_Id
,App_Banner_Cd as App_Banner_Cd
,App_Build_Nbr as App_Build_Nbr
,Application_Availability_Dt as Application_Availability_Dt
,Application_Detail_Txt as Application_Detail_Txt_V116
,Application_Order_Status_Msg as Application_Order_Status_Msg_V119
,Application_Os_Txt as Application_Os_Txt
,Application_Os_Type_Cd as Application_Os_Type_Cd
,Application_Separate_Lst as Application_Separate_Lists_V149
,APPLICATION_SIGN_IN_IND as APPLICATION_SIGN_IN_IND
,Application_Type_Cd as Application_Type_Cd
,Application_Type2_Cd as Application_Type_Cd_V23
,Application_User_Start_By_Nm as Application_User_Start_By_V25
,Application_Version_Build_Nbr as Application_Version_Build_Nbr_V116
,Application_Version_Cd as Application_Version_Cd
,Availabile_Cd as Availabile_Cd_V70
,Banner_Cd as Banner_Cd_V4
,Bot_Tracking_Txt as Bot_Tracking_V195
,Box_Tops_Auth_State_Cd as Box_Tops_Auth_State_V129
,Browser_GEO_Cd as Browser_GEO_V174
,Browser_Height_Txt as Browser_Height
,Browser_Nm as Browser_Nm
,Browser_Width_Txt as Browser_Width
,Camera_Allowed_Cd as Camera_Allowed_Cd_V193
,Campaign_Affiliate_Txt as Campaign_Affiliate_Txt_V93
,Campaign_Stacking_Txt as Campaign_Stacking_Txt_V18
,Campaign_Txt as Campaign_Txt
,Campaign_2_Txt as Campaign_Txt_V175
,Card_Less_Registration_Cd as Card_Less_Registration_Cd_V19
,Carousel_Size_Txt as Carousel_Size_Txt_V185
,Carrier_Nm as Carrier_Nm
,CDA_Marketing_Channel_Cd as CDA_Marketing_Channel_V196
,Channel_Manager_Channel_Cd as Channel_Manager_Channel_V36
,Channel_Stacking_Txt as Channel_Stacking_Txt_V37
,Click_Stream_Integration_Id as Click_Stream_Integration_Id
,Color_Cd as Color_Cd
,Connection_Type_Nm as Connection_Type_Nm
,Country_Nm as Country_Nm
,Create_Dt as Create_Dt
,Create_Ts as Create_Ts
,Custom_Nav_Link_Tracking_Txt as Custom_Nav_Link_Tracking_Txt_V74
,Customer_Status_Cd as Customer_Status_Cd_V100
,Delivery_Attended_Unattended_Flg as Delivery_Attended_Unattended_Flag_V161
,Detail_View_Txt as Detail_View_Txt_V24
,Ecom_Login_Id as Ecom_Login_Id_V7
,Ecom_Nav_Link_Tracking_Cd as Ecom_Nav_Link_Tracking_Cd_V75
,Elevaate_Flg as Elevaate_Flag_V144
,Elevaate_Poistion_Nbr as Elevaate_Poistion_Nbr_V145
,Email_HHID_URL_Parameter_Txt as Email_HHID_Url_Parameter_V118
,Email_Theme_URL_Parameter_Txt as Email_Theme_URL_Parameter_V117
,Environment_Cd as Environment_Cd_V99
,Error_Feature_Cd as Error_Feature_Cd_V141
,Error_Id as Error_Id_V140
,Error_Message_Dsc as Error_Message_V142
,Error_Page_Dsc as Error_Page_Dsc_V33
,Event_Id_URL_Parameter_Txt as Event_Id_URL_Parameter_V139
,Event_Nm as Event_Nm
,Exclude_Hit_Flg as Exclude_Hit_Flg
,Exclude_Row_Ind as Exclude_Row_Ind
,Experience_Nm as Experience_Nm_V133
,Face_Book_Account_Nm as Face_Book_Account_Nm_V54
,Face_Book_Banner_Cd as Face_Book_Banner_V55
,Face_Book_Campaign_Cd as Face_Book_Campaign_V64
,Facility_Integration_ID as Facility_Integration_ID
,Filter_Section_Txt as Filter_Section_V108
,Filter_Type_Cd as Filter_Type_Cd_V107
,First_Hit_Referrer_Type_Cd as First_Hit_Referrer_Type_Cd
,GA_Utm_Campaign_Medium_Txt as GA_Utm_Campaign_Medium_V86
,GA_Utm_Campaign_Nm as GA_Utm_Campaign_Nm_V84
,GA_Utm_Source_Cd as GA_Utm_Source_V85
,Global_No_Substitution_Cd as Global_No_Substitution_V38
,Hidden_Categories_Txt as Hidden_Categories_Txt_V22
,Hit_Id_High as Hit_Id_High
,Hit_Id_Low as Hit_Id_Low
,Hit_Source_Cd as Hit_Source_Cd
,Home_Page_Carousel_Txt as Home_Page_Carousel_V26
,Impressions_Component_Txt as Impressions_Component_V180
,Internal_Campaign_Tracking_Id as Internal_Campaign_Tracking_Id_V2
,Internal_Search_Results_Nbr as Internal_Search_Results_Nbr_V122
,Internal_Search_Terms_Txt as Internal_Search_Terms_V1
,Internal_Search_Txt as Internal_Search_Txt
,Internal_Search_Type_Cd as Internal_Search_Type_V120
,IP_Address_Nbr as IP_Address_V88
,Java_Script_Version_Nbr as Java_Script_Version_Nbr
,KMSI_Txt as KMSI_V81
,Language_Nm as Language_Nm
,Last_Activity_Flg as Last_Activity_V17
,Launch_Rule_Txt as Launch_Rule_V184
,Link_Detail_Txt as Link_Detail_V76
,List_Interaction_Type_Cd as List_Interaction_Type_V63
,Location_Sharing_Enabled_Flg as Location_Sharing_Enabled_V201
,Login_KMSI_Txt as Login_KMSI_V16
,Map_Clicks_Txt as Map_Clicks_V150
,Map_Link_Dsc as Map_Link
,Map_Link_By_Region_Nm as Map_Link_By_Region
,Map_Page_Cd as Map_Page
,Map_Region_Cd as Map_Region
,Marketing_Channel_Cd as Marketing_Channel_Cd
,Marketing_Channel_Dtl as Marketing_Channel_Dtl
,Media_Placement_Cd as Media_Placement_V80
,Media_Type_Cd as Media_Type_V148
,Message_Txt as Message_Txt_V134
,Mobile_Application_First_Launch_Dt as Mobile_Application_First_Launch_Dt_V6
,Mobile_Device_Id as Mobile_Device_Id_V57
,Mobile_Device_Model_Nm as Mobile_Device_Model_Nm_V58
,Mobile_Device_OS_Version_Cd as Mobile_Device_OS_Version_V59
,Mobile_J4U_Application_Version_Cd as Mobile_J4U_Application_Version_V53
,Mobile_Latitude_Longitude_Dgr as Mobile_Latitude_Longitude_V60
,Mobile_VS_Non_Mobile_Flg as Mobile_VS_Non_Mobile_V51
,Modal_Name_Link_Nm as Modal_Name_Link_V168
,Navigation_Source_Txt as Navigation_source_V61
,Network_Txt as Network_Txt_V176
,New_Repeat_Visitors_Txt as New_Repeat_Visitors_V8
,Notification_Allowed_Txt as Notification_Allowed_V192
,Operating_System_Cd as Operating_System_Cd
,Operating_System_Nm as Operating_System_Nm
,Order_Id as Order_Id
,Page_1_Nm as Page_Nm
,Page_2_Nm as Page_Nm_V5
,Page_URL_Txt as Page_URL_V11
,Past_Purchase_Items_Txt as Past_Purchase_Items_V179
,PFM_Detail_Txt as PFM_Detail_V14
,PFM_Source_Cd as PFM_Source_V12
,PFM_Subsection_1_Cd as PFM_Subsection_1_V3
,Placement_Type_Cd as Placement_Type_V78
,Platform_Cd as Platform_Cd
,Platform_Dsc as Platform_V90
,Plugin_Nm as Plugin_Nm
,Premium_Slots_Txt as Premium_Slots_V169
,Previous_Page_Nm as Previous_Page_Nm_V10
,Provider_Txt as Provider_Txt_V177
,Purchase_Id as Purchase_Id
,Push_Notifications_Message_Id as Push_Notifications_Message_Id_V62
,Recipe_Nm as Recipe_Nm_V190
,Recipe_Source_Cd as Recipe_Source_V191
,Referrer_type_id as Referrer_type_id
,Referring_Application_Cd as Referring_Application_V87
,Resolution_Nm as Resolution_Nm
,Retail_Customer_UUID as Retail_Customer_UUID
,SDK_Verison_Nbr as SDK_Verison_V183
,Search_Engine_Nm as Search_Engine_Nm
,Social_Authors_Txt as Social_Authors_V65
,Social_Media_Channel_Cd as Social_Media_Channel_V67
,Social_Media_Content_Title_Txt as Social_Media_Content_Title_V68
,Social_Platforms_Txt as Social_Platforms_V40
,Sort_Selection_Txt as Sort_Selection_V109
,Source_Site_Type_Cd as Source_Site_Type_V52
,Sub_Section1_Txt as Sub_Section1_V151
,Sub_Section2_Txt as Sub_Section2_V152
,Sub_Section3_Txt as Sub_Section3_V153
,Sub_Section4_Txt as Sub_Section4_V154
,Subscription_Dt as Subscription_Dt_V164
,Subscription_Funnel_Cd as Subscription_Funnel_V188
,Subscription_Status_Cd as Subscription_Status_V163
,Syndigo_Content_Txt as Syndigo_Content_V197
,Time_Parting_Txt as Time_Parting_V15
,Timestamp_Marketplace_Txt as Timestamp_Marketplace_V172
,Top_Nav_Usage_Cd as Top_Nav_Usage_V171
,Typed_Search_Cnt as Typed_Search_Cnt_V121
,Typed_Search_Term_Cd as Typed_Search_Term_V91
,UMA_Application_Nm as UMA_Application_V200
,User_Action_Type_Cd as User_Action_Type_V6
,User_Action_Cd as User_Action_V95
,User_Agent_Nm as User_Agent_V94
,User_Message_Status_Cd as User_Message_Status_V124
,User_Messages_Txt as User_Messages_V96
,User_Type_Cd as User_Type_V92
,Version_Nbr as Version_Nbr
,Video_Ad_Load_Txt as Video_Ad_Load_Txt
,Visit_Id as Visit_Id
,Visit_Nbr as Visit_Nbr
,Visit_Page_Nbr as Visit_Page_Nbr
,Visitor_Id as Visitor_Id
,Visitor2_Id as Visitor_Id_V49
,Visitor_Interacted_HH_MM_Tm as Visitor_Interacted_HH_MM
,Wearable_Device_Cd as Wearable_Device_V89
,ZTP_Default_Method_Cd as ZTP_Default_Method_V186
,ZTP_Method_Cd as ZTP_Method_V187
,Top_Nav_Clicks_Txt
,DW_CREATE_TS
,DW_LAST_UPDATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_SOURCE_UPDATE_NM
,DW_CURRENT_VERSION_IND
FROM EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.CLICK_STREAM_OTHER;