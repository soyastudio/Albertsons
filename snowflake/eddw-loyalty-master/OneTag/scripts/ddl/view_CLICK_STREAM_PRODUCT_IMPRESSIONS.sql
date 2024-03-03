--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_PRODUCT_IMPRESSIONS_OT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_DB_NAME_VIEWS>>;
use schema <<EDM_SCHEMA_VIEWS>>;

create view CLICK_STREAM_PRODUCT_IMPRESSIONS(
    EVENT_ID,
	SESSION_ID,
	VISIT_ID COMMENT 'Unique Identifier assign for each visit/session by a customer',
	BASE_PRODUCT_NBR COMMENT 'Base product number seen by customer',
	SESSION_SEQUENCE_NBR,
	VISIT_PAGE_NBR COMMENT 'Unique numberassigned by adobe sytem for every click',
	DW_FIRST_EFFECTIVE_TS COMMENT 'Dataware House Auidt column to capture the effective start timestamp of record',
	DW_LAST_EFFECTIVE_TS COMMENT 'Dataware House Auidt column to capture the effective end timestamp of record',
	RETAIL_CUSTOMER_UUID COMMENT 'unique ID that Albertsons assigns to each registered customer',
	CLUB_CARD_NBR COMMENT 'Club Card Number derived for unsigned customer using Adove Visitor ID, Mobile Device, IP address and Adobe ID',
	HOUSEHOLD_ID COMMENT 'Household ID derived for unsigned customer using Adobe Visitor ID, Mobile Device, IP address and Adobe ID',
	PRODUCT_SEEN_TS COMMENT 'Date and Time customer seen the products',
	CAROUSEL_NM COMMENT 'Name of the carousel customer seen the products',
	PRODUCT_SEEN_IND COMMENT 'Indicator if customer seen the products atleast 75 percent of the tile',
	PRODUCT_FINDING_METHOD_DSC COMMENT 'Product finding method of the product',
	ROW_LOCATION_CD COMMENT 'Exact row location of the product',
	SLOT_LOCATION_CD COMMENT 'Exact slot location of the product',
	PRODUCTS_SEEN_IN_CAROUSEL_IND COMMENT 'Indicator if customer seen the products in carousel or not',
	HIT_ID_LOW COMMENT 'Used in combination with hitid_low to uniquely identify a hit.',
	HIT_ID_HIGH COMMENT 'Used in combination with hitid_high to uniquely identify a hit.',
	PAGE_NAME COMMENT 'Used to populate the Page dimension. If the pagename variable is empty Analytics uses page_url instead.',
	APPLICATION_DETAIL_TXT COMMENT 'Used to populate the application_detail_txt columns.',
	MODEL_ID_TXT COMMENT 'Used to populate the model_id_txt columns.',
	COOKIE_PREFERENCE_CD,
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated then this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'Name of source system or user created the record',
	DW_SOURCE_UPDATE_NM COMMENT 'Name of source system or user updated the record',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DATA_LOAD_TS COMMENT 'Data Load'
) COMMENT='VIEW for customer click_stream_product_impressions'
 as
 select distinct                EVENT_ID,
								SESSION_ID,
                                null as visit_id,
                                BASE_PRODUCT_NBR,
                                SESSION_SEQUENCE_NBR,
                                null as visit_page_nbr,
								CURRENT_TIMESTAMP() as dw_first_effective_ts,
								CURRENT_TIMESTAMP() as dw_last_effective_ts,
                                RETAIL_CUSTOMER_UUID,
                                try_to_number(CLUB_CARD_NBR) as CLUB_CARD_NBR,
								HOUSEHOLD_ID,
                                LEFT(DATEADD('HOUR', -1, DATE_TRUNC('SECOND', TO_TIMESTAMP(B.EVENT_TS))),19) as PRODUCT_SEEN_TS,
                                CAROUSEL_NM,
                                PRODUCT_SEEN_IND,
                                lower(PRODUCT_FINDING_METHOD_DSC) as PRODUCT_FINDING_METHOD_DSC,
								lower(ROW_LOCATION_CD) as ROW_LOCATION_CD,
								lower(SLOT_LOCATION_CD) as SLOT_LOCATION_CD,
                                PRODUCTS_SEEN_IN_CAROUSEL_IND,
								'NA' as hit_id_low,
                                'NA' as hit_id_high,
                                page_nm as page_name,
								APPLICATION_DETAIL_TXT,
							    MODEL_ID_TXT,
                                COOKIE_PREFERENCE_CD,
								DW_CREATE_TS,
								DW_LAST_UPDATE_TS,
                                DW_LOGICAL_DELETE_IND,
                                DW_CURRENT_VERSION_IND,
                                'OneTag' as DW_SOURCE_CREATE_NM,
                                'OneTag' as DW_SOURCE_UPDATE_NM,
                                CURRENT_TIMESTAMP() as data_load_ts
					FROM EDM_ANALYTICS_<<ENV>>.DW_DIGITAL_EXP.ONETAG_PRODUCT_IMPRESSION
                            where  1=1
                            and date(PRODUCT_SEEN_TS)>='2024-01-02'
                     union all
 SELECT DISTINCT null as EVENT_ID
      , null as SESSION_ID
      ,visit_id 						
      , base_product_nbr
      , null as SESSION_SEQUENCE_NBR
      , visit_page_nbr 				
      , dw_first_effective_ts 		
      , dw_last_effective_ts 			
      , retail_customer_uuid 			
      , club_card_nbr 				
      , household_id 					
      , product_seen_ts 				
      , carousel_nm 					
      , product_seen_ind 				
      , product_finding_method_dsc 		
      , row_location_cd 				
      , slot_location_cd 				
      , products_seen_in_carousel_ind	
      , hit_id_low
     , hit_id_high 
     , page_name 
	 ,APPLICATION_DETAIL_TXT
     ,model_id_txt
     ,null  as COOKIE_PREFERENCE_CD
     , dw_create_ts 					
      , dw_last_update_ts 			
     , dw_logical_delete_ind
     , dw_current_version_ind
     , dw_source_create_nm 			
    , dw_source_update_nm 			
     , data_load_ts	
FROM EDM_ANALYTICS_<<ENV>>.DW_DIGITAL_EXP.CLICK_STREAM_PRODUCT_IMPRESSIONS
where 1=1
and product_seen_ts<='2024-01-01' ;