--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CONNECTION_TYPE runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_CONNECTION_TYPE (
	CONNECTION_TYPE_NM VARCHAR(250) NOT NULL COMMENT 'Connection type. The Connection type dimension shows how the visitor connected to the internet. This dimension is useful to determine how visitors connect to the internet to browse your site. You can use it to optimize site content based on the connection speed of visitors. Populate this dimension with data This dimension uses a combination of the ct query string and Adobe server-side logic. Adobe uses the following rules in order to determine its value: If the ct query string equals modem set the dimension item to Modem. AppMeasurement only collects this data on unsupported Internet Explorer browsers making this dimension item uncommon. Check the IP address of the hit and reference it to a lookup table internal to Adobe. If the IP address is from a mobile carrier set the dimension item to Mobile Carrier. If the ct query string equals lan set the dimension item to LAN/Wifi. If the hit originates from a Data source or is otherwise considered a special type of hit set the dimension item to Not specified. If none of the above rules are met default to the value of LAN/Wifi. Dimension itemsDimension items include LAN/Wifi Mobile Carrier Modem and Not Specified. LAN/Wifi: The visitor connected to the internet through a landline or wifi hotspot. Mobile Carrier: The visitor connected to the internet through a mobile carrier. Modem: The visitor connected to the internet through a modem on an unsupported Internet Explorer browser.Not Specified: The hit did not have a connection type.',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The timestamp the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is 12/31/9999 24.00.00.0000. For updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 micro second',
	CONNECTION_TYPE_CD VARCHAR(50) COMMENT 'Numeric ID representing the connection type. Variable used in the Connection type dimension. References the connection_type.tsv lookup table.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is created this would be the current timestamp',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The data source name of this insert',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The data source name of this update or delete',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day',
	primary key (CONNECTION_TYPE_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);