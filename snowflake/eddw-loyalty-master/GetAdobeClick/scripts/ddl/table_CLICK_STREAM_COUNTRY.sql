--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_COUNTRY runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_COUNTRY (
	COUNTRY_NM VARCHAR(250) NOT NULL COMMENT 'Countries: The Countries’ dimension reports the country where the hit originated from. This dimension is useful to help determine what the most popular countries visitors originate from when visiting your site. You could use this data to focus on marketing efforts in these countries or make sure your site experience is optimal in countries that have different primary languages.  Populate this dimension with data This dimension references lookup rules internal to Adobe. The lookup value is based on the IP address sent with the hit. Adobe partners with Digital Element to maintain lookups between IP address and country. This dimension works out of the box for all implementations. Dimension items: Dimension items include countries all over the world. Example values include United States United Kingdom or India. Differences between reported and actual location Since this dimension is based on IP address some scenarios can show a difference between reported location and actual location: IP addresses that represent corporate proxies: These visitors can appear as traffic coming through the user’s corporate network which can be a different location if the user is working remotely.Mobile IP addresses: Mobile IP targeting works at varying levels depending on the location and the network. A number of carriers backhaul IP traffic through centralized or regional points of presence.Satellite ISP users: Identifying the specific location of these users is difficult as they typically appear to originate from the uplink location. Military and government IPs: Represents personnel traveling around the globe and entering through their home location rather than the base or office where they are currently stationed.',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The timestamp the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is 12/31/9999 24.00.00.0000. For updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 micro second',
	COUNTRY_ID VARCHAR(50) COMMENT 'Numeric ID representing the country the hit came from. Used in the Countries dimension. Uses country.tsv lookup.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	primary key (COUNTRY_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);