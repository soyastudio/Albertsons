--liquibase formatted sql
--changeset SYSTEM:Survey_Question runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW  Survey_Question (
 Survey_Question_Sequence_Nbr 	comment 'Unique Identifier of a record. Generated value',
 Dw_First_Effective_Dt 			comment 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
 Dw_Last_Effective_Dt 			comment 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
 Survey_Type_Nm					comment 'Question Channel Name.',
 Page_Nbr       				comment 'Page Number of the Question',
 Question_Field_Nm  			comment 'Field Name given to question. This value can be used for response lookup' ,
 Survey_Channel_Type_Nm 	 	comment 'Email/UMA',
 Question_Txt       			comment 'Question Text',
 Question_Short_Nm  			comment 'Short Name or Reporting Name of Question',
 Scale_Type_Dsc      			comment 'Type of Response for a question. Likely Scale, Agreement Scale, Open text are few values' ,
 Scale_Options_Txt   			comment 'Options provided for the Scale',
 Question_Category_Txt 			comment 'Category of question',
 Dw_Create_Ts          			comment 'The timestamp the record was inserted.',
 Dw_Last_Update_Ts     			comment 'When a record is updated  this would be the current timestamp',
 Dw_Logical_Delete_Ind 			comment 'Set to True when we receive a delete record for the primary key, else False',
 Dw_Source_Create_Nm   			comment 'The Bod (data source) name of this insert.',
 Dw_Source_Update_Nm 			comment 'The Bod (data source) name of this update or delete.',
 Dw_Current_Version_Ind 		comment 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
 
 )
COPY GRANTS
comment = 'VIEW for Survey_Question' 
AS
SELECT
 Survey_Question_Sequence_Nbr 	,
 Dw_First_Effective_Dt			,
 Dw_Last_Effective_Dt 		 	,
 Survey_Type_Nm					,
 Page_Nbr              			,
 Question_Field_Nm    			,
 Survey_Channel_Type_Nm 		,
 Question_Txt          			,
 Question_Short_Nm    			,
 Scale_Type_Dsc        			,
 Scale_Options_Txt     			,
 Question_Category_Txt  		,
 Dw_Create_Ts          			,
 Dw_Last_Update_Ts      		,
 Dw_Logical_Delete_Ind 			,
 Dw_Source_Create_Nm   			,
 Dw_Source_Update_Nm 			,
 Dw_Current_Version_Ind  
FROM  EDM_CONFIRMED_PRD.DW_C_Loyalty.Survey_Question;