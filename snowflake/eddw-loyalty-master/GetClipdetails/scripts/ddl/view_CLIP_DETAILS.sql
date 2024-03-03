--liquibase formatted sql
--changeset SYSTEM:CLIP_DETAILS runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE View CLIP_DETAILS
Copy Grants
comment = 'View For CLIP_DETAILS' 
(
		Clip_Sequence_Id comment 'System generated key to uniquely identify Clip details'
		,Clip_Id comment 'Unique generated string for every payload'
		,Clip_Ts comment 'Captures the timestamp when an offer has been clipped or unclipped'
		,DW_First_Effective_Dt comment 'The timestamp the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key'
		,DW_Last_Effective_Dt comment 'for the current record this is 12/31/9999 24.00.00.0000.'
		,Offer_Id comment 'clip type Card(C) & List(L) has same J4U offer ID(When we do clip)'
		,Clip_Source_Application_Id comment 'It stores Clip Source Application value'
		,Clip_Type_Cd comment 'Clip Type code (C),(L) etc'
		,Clip_Dt comment 'Date when clipping is done'
		,Clip_Tm comment 'Time when clipping is done'
		,Clip_Source_Cd comment 'Clip source code'
		,Vendor_Banner_Cd comment 'Vendor Banner Code'
		,DW_CREATE_TS comment 'When a record is created  this would be the current timestamp'
		,DW_LAST_UPDATE_TS comment 'When a record is created  this would be the current timestamp'
		,DW_LOGICAL_DELETE_IND comment 'Set to True when we receive a delete record for the primary key, else False'
		,DW_SOURCE_CREATE_NM comment 'The data source name of this insert'
		,DW_SOURCE_UPDATE_NM comment 'The data source name of this update or delete'
		,DW_CURRENT_VERSION_IND comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'
)	
AS Select 
			Clip_Sequence_Id
			 ,Clip_Id 
			 ,Event_Ts as Clip_Ts 
			 ,DW_First_Effective_Dt
			 ,DW_Last_Effective_Dt
			 ,Offer_Id 
			 ,Clip_Source_Application_Id
			 ,Clip_Type_Cd
			 ,Clip_Dt 
			 ,Clip_Tm 
			 ,Clip_Source_Cd 
			 ,Vendor_Banner_Cd 
			 ,DW_CREATE_TS  
			 ,DW_LAST_UPDATE_TS 
			 ,DW_LOGICAL_DELETE_IND 
			 ,DW_SOURCE_CREATE_NM
			 ,DW_SOURCE_UPDATE_NM 
			 ,DW_CURRENT_VERSION_IND 
from 
EDM_CONFIRMED_PRD.DW_C_LOYALTY.CLIP_DETAILS;