--liquibase formatted sql
--changeset SYSTEM:Survey_Response runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW  Survey_Response
(
    Survey_Id    				comment 'Medallia unique identifier',
    Survey_Question_Sequence_Nbr 	  	comment 'Unique Identifier of a record. Generated value',
    Dw_First_Effective_Dt 			comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
    Dw_Last_Effective_Dt 			comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
    Survey_Response_Score_Nbr 			comment 'Survey Response Score Number',
    Survey_Response_Txt 			comment 'Survey Response Text Message',
    Dw_Create_Ts 				comment 'The timestamp the record was inserted.',
    Dw_Last_Update_Ts  				comment 'When a record is updated  this would be the current timestamp',
    Dw_Logical_Delete_Ind  			comment 'Set to True when we receive a delete record for the primary key, else False',
    Dw_Source_Create_Nm 			comment 'The Bod (data source) name of this insert.',
    Dw_Source_Update_Nm 			comment  'The Bod (data source) name of this update or delete.',
    Dw_Current_Version_Ind 			comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
 )
COPY GRANTS
comment = 'VIEW for Survey_Response' 
AS
SELECT
    Survey_Id      					,
    Survey_Question_Sequence_Nbr	,
    Dw_First_Effective_Dt  			,
    Dw_Last_Effective_Dt  			,
    Survey_Response_Score_Nbr 		,
    Survey_Response_Txt				,
    Dw_Create_Ts       				,
    Dw_Last_Update_Ts 				,
    Dw_Logical_Delete_Ind    		,
    Dw_Source_Create_Nm   			,
    Dw_Source_Update_Nm  			,
    Dw_Current_Version_Ind 
FROM  EDM_CONFIRMED_PRD.DW_C_Loyalty.Survey_Response;