--liquibase formatted sql
--changeset SYSTEM:SP_GETINSTANTALLOCATION_To_BIM_load_Household_Offer_Allocation runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETINSTANTALLOCATION_TO_BIM_LOAD_HOUSEHOLD_OFFER_ALLOCATION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


var cnf_db = CNF_DB ;
var cnf_schema = C_LOYAL;
var wrk_schema = C_STAGE;
var src_tbl = SRC_WRK_TBL
var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Household_Offer_Allocation_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Household_Offer_Allocation`;
// ************** Load for Household_Offer_Allocation_wrk table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        
        }
    catch (err)  {
        return "Truncation of Household_Offer_Allocation_wrk table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
WITH src_wrk_tbl_recs
AS (
	SELECT DISTINCT
		OFFERID AS Oms_Offer_Id
		,HOUSEHOLDID AS Household_Id
		,REGIONID AS Region_Id
		,ALLOCATIONSTARTDATE AS Allocation_Start_Ts
		,ALLOCATIONENDDATE AS Allocation_End_Ts
		,ALLOCATIONQTY AS Allocation_Cnt
		,EVENTNAME AS Event_Nm
		,EVENTTS AS Event_Ts
		,EVENTSOURCE AS Event_Source_Nm
		,false AS DW_Logical_delete_ind
		,DW_CREATE_TS AS CREATIONDT
        ,filename
        ,ROW_NUMBER() OVER (
			PARTITION BY Oms_Offer_Id
			,Household_Id
			,Region_Id
			,Allocation_Start_Ts
			,Allocation_End_Ts ORDER BY EVENT_TS DESC
			) AS rn
	FROM ${SRC_WRK_TBL}
	WHERE  Oms_Offer_Id IS NOT NULL
		AND Household_Id IS NOT NULL
		AND Region_Id IS NOT NULL
		AND Allocation_Start_Ts IS NOT NULL
		AND Allocation_End_Ts IS NOT NULL
  QUALIFY RN=1
	)
SELECT DISTINCT src.Oms_Offer_Id
	,src.Household_Id
	,src.Region_Id
	,src.Allocation_Start_Ts
	,src.Allocation_End_Ts
	,src.Allocation_Cnt
	,src.Event_Nm
	,src.Event_Ts
	,src.Event_Source_Nm
	,src.DW_Logical_delete_ind
	,src.CREATIONDT
	,src.filename
	,CASE 
		WHEN (
				tgt.Oms_Offer_Id IS NULL
				AND tgt.Household_Id IS NULL
				AND tgt.Region_Id IS NULL
				AND tgt.Allocation_Start_Ts IS NULL
				AND tgt.Allocation_End_Ts IS NULL
				)
			THEN 'I'
		ELSE 'U'
		END AS DML_TYPE
	,CASE 
		WHEN tgt.DW_First_Effective_Dt = CURRENT_DATE
			THEN 1
		ELSE 0
		END AS SAMEDAY_CHG_IND
FROM src_wrk_tbl_recs src
LEFT JOIN (
	SELECT DISTINCT tgt.Oms_Offer_Id
		,tgt.Household_Id
		,tgt.Region_Id
		,tgt.Allocation_Start_Ts
		,tgt.Allocation_End_Ts
		,tgt.Allocation_Cnt
		,tgt.Event_Nm
		,tgt.Event_Ts
		,tgt.Event_Source_Nm
		,tgt.DW_Logical_delete_ind
		,tgt.DW_First_Effective_Dt
	FROM ${tgt_tbl} as tgt
	WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
	) tgt ON tgt.Oms_Offer_Id = src.Oms_Offer_Id
	AND tgt.Household_Id = src.Household_Id
	AND tgt.Region_Id = src.Region_Id
	AND tgt.Allocation_Start_Ts = src.Allocation_Start_Ts
	AND tgt.Allocation_End_Ts = src.Allocation_End_Ts
WHERE (
		tgt.Oms_Offer_Id IS NULL
		AND tgt.Household_Id IS NULL
		AND tgt.Region_Id IS NULL
		AND tgt.Allocation_Start_Ts IS NULL
		AND tgt.Allocation_End_Ts IS NULL
		)
	OR (
		NVL(SRC.Allocation_Cnt, '-1') <> NVL(TGT.Allocation_Cnt, '-1')
		OR NVL(SRC.Event_Nm, '-1') <> NVL(TGT.Event_Nm, '-1')
		OR NVL(SRC.Event_Ts, '-1') <> NVL(TGT.Event_Ts, '-1')
		OR NVL(SRC.Event_Source_Nm, '-1') <> NVL(TGT.Event_Source_Nm, '-1')
		OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
		)
        `;

try {
        
        snowflake.execute ({sqlText: sql_command});
        }
    catch (err)  {
        return "Creation of Household_Offer_Allocation work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


var sql_begin = "BEGIN"

// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${tgt_tbl} AS TGT
SET DW_Last_Effective_dt = CURRENT_DATE - 1
	,DW_CURRENT_VERSION_IND = FALSE
	,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
	,DW_SOURCE_UPDATE_NM = filename
FROM (
	SELECT Oms_Offer_Id
		,Household_Id
		,Region_Id
		,Allocation_Start_Ts
		,Allocation_End_Ts
		,filename
	FROM ${tgt_wrk_tbl}
	WHERE DML_TYPE = 'U'
		AND Sameday_chg_ind = 0
		AND Oms_Offer_Id IS NOT NULL
		AND Household_Id IS NOT NULL
		AND Region_Id IS NOT NULL
		AND Allocation_Start_Ts IS NOT NULL
		AND Allocation_End_Ts IS NOT NULL
	) SRC
WHERE TGT.Oms_Offer_Id = SRC.Oms_Offer_Id
	AND TGT.Household_Id = SRC.Household_Id
	AND TGT.Region_Id = SRC.Region_Id
	AND TGT.Allocation_Start_Ts = SRC.Allocation_Start_Ts
	AND TGT.Allocation_End_Ts = SRC.Allocation_End_Ts
	AND TGT.DW_CURRENT_VERSION_IND = TRUE
	AND TGT.DW_LOGICAL_DELETE_IND = FALSE;`;
							 
// Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} AS tgt
SET tgt.Allocation_Cnt = src.Allocation_Cnt
	,tgt.Event_Nm = src.Event_Nm
	,tgt.Event_Ts = src.Event_Ts
	,tgt.Event_Source_Nm = src.Event_Source_Nm
	,DW_Logical_delete_ind = src.DW_Logical_delete_ind
	,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
	,DW_SOURCE_UPDATE_NM = FileName
FROM (
	SELECT Oms_Offer_Id
		,Household_Id
		,Region_Id
		,Allocation_Start_Ts
		,Allocation_End_Ts
	--	,Dw_First_Effective_Dt
	--	,Dw_Last_Effective_Dt
		,Allocation_Cnt
		,Event_Nm
		,Event_Ts
		,Event_Source_Nm
		,DW_Logical_delete_ind
		,creationDt
		,FILENAME
	FROM ${tgt_wrk_tbl}
	WHERE DML_Type = 'U'
		AND Sameday_chg_ind = '1'
		AND Oms_Offer_Id IS NOT NULL
		AND Household_Id IS NOT NULL
		AND Region_Id IS NOT NULL
		AND Allocation_Start_Ts IS NOT NULL
		AND Allocation_End_Ts IS NOT NULL
	) SRC
WHERE TGT.Oms_Offer_Id = SRC.Oms_Offer_Id
	AND TGT.Household_Id = SRC.Household_Id
	AND TGT.Region_Id = SRC.Region_Id
	AND TGT.Allocation_Start_Ts = SRC.Allocation_Start_Ts
	AND TGT.Allocation_End_Ts = SRC.Allocation_End_Ts
	AND tgt.DW_CURRENT_VERSION_IND = TRUE;`;
							
// Processing Inserts
var sql_inserts = `INSERT INTO  ${tgt_tbl}  
	(
	Oms_Offer_Id
	,Household_Id
	,Region_Id
	,Allocation_Start_Ts
	,Allocation_End_Ts
	,Allocation_Cnt
	,Event_Nm
	,Event_Ts
	,Event_Source_Nm
	,Dw_First_Effective_Dt
	,Dw_Last_Effective_Dt
	,Dw_Create_Ts
	--,Dw_Last_Update_Ts
	,Dw_Logical_Delete_Ind
	,Dw_Source_Create_Nm
	--,Dw_Source_Update_Nm
	,Dw_Current_Version_Ind
	)
SELECT DISTINCT Oms_Offer_Id
	,Household_Id
	,Region_Id
	,Allocation_Start_Ts
	,Allocation_End_Ts
	,Allocation_Cnt
	,Event_Nm
	,Event_Ts
	,Event_Source_Nm
	,CURRENT_DATE AS DW_First_Effective_Dt
	,'31-DEC-9999'
	,Current_timestamp AS Dw_Create_Ts
	--,Dw_Last_Update_Ts
	,Dw_Logical_Delete_Ind
	,FILENAME
--	,Dw_Source_Update_Nm
	,TRUE
FROM ${tgt_wrk_tbl}
WHERE Oms_Offer_Id IS NOT NULL
	AND Household_Id IS NOT NULL
	AND Region_Id IS NOT NULL
	AND Allocation_Start_Ts IS NOT NULL
	AND Allocation_End_Ts IS NOT NULL
	AND Sameday_chg_Ind = 0
`;
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
	    snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit}); 
		
	}
	
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback}
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for Household_Offer_Allocation table ENDs *****************


$$;
