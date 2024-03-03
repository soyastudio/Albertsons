--liquibase formatted sql
--changeset SYSTEM:CLIP_HEADER runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE View CLIP_HEADER 
Copy Grants
comment = 'View For CLIP_HEADER' 
( 	    Clip_Sequence_Id comment 'System generated key to uniquely identify Clip details'
		,DW_First_Effective_Dt comment 'The timestamp the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key'
		,DW_Last_Effective_Dt comment 'for the current record this is 12/31/9999 24.00.00.0000.'
		,Customer_GUID comment 'When Customer Registered, GUID will be created'
		,Club_Card_Nbr comment 'Club Card Nbr'
		,Facility_Integration_ID comment 'Facility Integration ID'
		,Retail_Store_Id comment 'Selected store Id where the clipping is done'
		,Household_Id comment 'When a customer registered with Safeway HHID will be created'
		,Retail_Customer_UUID comment 'Retail Customer UUID'
		,Banner_Nm comment 'Banner code used by offer providers, required field for Manufacture Offer(MF)'
		,Postal_Cd comment 'Postal code used by offer providers'
		,DW_CREATE_TS comment 'When a record is created  this would be the current timestamp'
		,DW_LAST_UPDATE_TS comment 'When a record is created  this would be the current timestamp'
		,DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False'
		,DW_SOURCE_CREATE_NM comment 'The data source name of this insert'
		,DW_SOURCE_UPDATE_NM comment 'The data source name of this update or delete'
		,DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'
)	AS Select 
		Clip_Sequence_Id
		 ,DW_First_Effective_Dt
		 ,DW_Last_Effective_Dt 
		 ,Customer_GUID
		 ,Club_Card_Nbr
		 ,Facility_Integration_ID
		 ,Retail_Store_Id
		 ,Household_Id
		 ,Retail_Customer_UUID
		 ,Banner_Nm
		 ,Postal_Cd
		 ,DW_CREATE_TS
		 ,DW_LAST_UPDATE_TS
		 ,DW_LOGICAL_DELETE_IND
		 ,DW_SOURCE_CREATE_NM
		 ,DW_SOURCE_UPDATE_NM
		 ,DW_CURRENT_VERSION_IND
from 
EDM_CONFIRMED_PRD.DW_C_LOYALTY.CLIP_HEADER
;