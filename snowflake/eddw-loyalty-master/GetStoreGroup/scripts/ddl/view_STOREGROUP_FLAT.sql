USE DATABASE <<EDM_VIEW_NAME>>;
USE SCHEMA DW_VIEWS;

create or replace view STOREGROUP_FLAT(
	FILENAME,
	PAYLOADPART,
	PAGENUM,
	TOTALPAGES,
	ENTITYID,
	PAYLOADTYPE,
	ENTITYTYPE,
	SOURCEACTION,
	PAYLOAD_ID,
	PAYLOAD_NAME,
	PAYLOAD_DESCRIPTION,
	PAYLOAD_CREATETS,
	PAYLOAD_UPDATETS,
	PAYLOAD_CREATEDUSER_USERID,
	PAYLOAD_CREATEDUSER_FIRSTNAME,
	PAYLOAD_CREATEDUSER_LASTNAME,
	PAYLOAD_UPDATEDUSER_USERID,
	PAYLOAD_UPDATEDUSER_FIRSTNAME,
	PAYLOAD_UPDATEDUSER_LASTNAME,
	PAYLOAD_STORES,
	LASTUPDATETS,
	DW_CREATE_TS
) as 

SELECT 
filename 
,PayloadPart 
,PageNum 
,TotalPages 
,EntityId 
,PayLoadType 
,EntityType 
,SourceAction 
,payload_id 
,payload_name 
,payload_description 
,payload_CreateTs 
,payload_updateTs 
,payload_CreatedUser_userid 
,payload_CreatedUser_firstname 
,payload_CreatedUser_lastname 
,payload_updatedUser_userid 
,payload_updatedUser_firstName 
,payload_updatedUser_lastname 
,payload_stores 
,lastUpdateTs 
,DW_CREATE_TS 
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.Storegroup_Flat;
