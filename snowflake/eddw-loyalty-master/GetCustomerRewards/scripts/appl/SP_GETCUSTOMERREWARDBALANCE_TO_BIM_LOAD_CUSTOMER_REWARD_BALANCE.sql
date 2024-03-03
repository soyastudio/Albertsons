--liquibase formatted sql
--changeset SYSTEM:SP_GETCUSTOMERREWARDBALANCE_TO_BIM_LOAD_CUSTOMER_REWARD_BALANCE runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETCUSTOMERREWARDBALANCE_TO_BIM_LOAD_CUSTOMER_REWARD_BALANCE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

  
   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_schema = C_LOYAL;
   var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Customer_Reward_Balance_WRK`;
   var tgt_tbl = `${CNF_DB}.${cnf_schema}.Customer_Reward_Balance`;
                       
    // **************        Load for Customer_Reward_Balance table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
        var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
				SELECT
Household_Id
,Balance_Update_Ts
,Reward_Origin_Cd
,Reward_Origin_Dsc
,CASE
    WHEN Reward_Dollar_Point_Qty_src = 0 and DML_Type = 'U'
    THEN Reward_Dollar_Point_Qty_tgt
    ELSE Reward_Dollar_Point_Qty_src
END as Reward_Dollar_Point_Qty
,Reward_Token_Point_Qty
,Reward_Token_Point_Expiry_Qty
,CASE
    WHEN Reward_Dollar_Start_Ts_src is NULL and DML_Type = 'U'
    THEN Reward_Dollar_Start_Ts_tgt
    ELSE Reward_Dollar_Start_Ts_src
END as Reward_Dollar_Start_Ts
,CASE
    WHEN Reward_Dollar_End_Ts_src is NULL and DML_Type = 'U'
    THEN Reward_Dollar_End_Ts_tgt
    ELSE Reward_Dollar_End_Ts_src
END as Reward_Dollar_End_Ts
,Reward_Token_End_Ts
,Reward_Token_Will_Expiry_End_Ts 
,filename
,DW_Logical_delete_ind
,creationdt
,actiontypecd
,DML_Type
,Sameday_chg_ind
FROM (
    SELECT src.Household_Id
    ,src.Balance_Update_Ts
    ,src.Reward_Origin_Cd
    ,src.Reward_Origin_Dsc
    ,src.Reward_Dollar_Point_Qty as Reward_Dollar_Point_Qty_src
    ,tgt.Reward_Dollar_Point_Qty as Reward_Dollar_Point_Qty_tgt
    ,src.Reward_Token_Point_Qty
    ,src.Reward_Token_Point_Expiry_Qty
    ,src.Reward_Dollar_Start_Ts as Reward_Dollar_Start_Ts_src
    ,tgt.Reward_Dollar_Start_Ts as Reward_Dollar_Start_Ts_tgt
    ,src.Reward_Dollar_End_Ts as Reward_Dollar_End_Ts_src
    ,tgt.Reward_Dollar_End_Ts as Reward_Dollar_End_Ts_tgt
    ,src.Reward_Token_End_Ts
    ,src.Reward_Token_Will_Expiry_End_Ts 
    ,src.filename
    ,src.DW_Logical_delete_ind
    ,src.creationdt
    ,src.actiontypecd
    ,CASE 
        WHEN 
            tgt.Household_Id is NULL  OR 
            tgt.Balance_Update_Ts is NULL  
        THEN 'I' 
        ELSE 'U' 
    END as DML_Type
    ,CASE 
        WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
        THEN 1 
        Else 0 
    END as Sameday_chg_ind 
    FROM (     
        SELECT 
        Household_Id
        ,Balance_Update_Ts
        ,Reward_Origin_Cd
        ,Reward_Origin_Dsc
        ,Reward_Dollar_Point_Qty
        ,Reward_Token_Point_Qty
        ,Reward_Token_Point_Expiry_Qty
        ,Reward_Dollar_Start_Ts
        ,Reward_Dollar_End_Ts
        ,Reward_Token_End_Ts
        ,Reward_Token_Will_Expiry_End_Ts 
        ,filename
        ,FALSE as DW_Logical_delete_ind
        ,creationdt
        ,actiontypecd
        FROM (
            SELECT 
            Household_Id
            ,Balance_Update_Ts
            ,Reward_Origin_Cd
            ,Reward_Origin_Dsc
            ,Reward_Dollar_Point_Qty
            ,Reward_Token_Point_Qty
            ,Reward_Token_Point_Expiry_Qty
            ,Reward_Dollar_Start_Ts
            ,Reward_Dollar_End_Ts
            ,Reward_Token_End_Ts
            ,Reward_Token_Will_Expiry_End_Ts 
            ,filename
            ,FALSE as DW_Logical_delete_ind
            ,creationdt
            ,actiontypecd
            ,row_number() over ( PARTITION BY Household_Id, TO_TIMESTAMP_LTZ(Balance_Update_TS)
            ORDER BY to_timestamp_ntz(creationdt) desc) as rn
            FROM (
                SELECT 	
                HouseholdId	AS Household_Id
                ,BalanceUpdateTs AS Balance_Update_Ts
                ,RewardOriginCd AS Reward_Origin_Cd
                ,RewardOriginDsc AS Reward_Origin_Dsc
                ,RewardDollarPointsQty AS Reward_Dollar_Point_Qty
                ,RewardTokenPointsQty AS Reward_Token_Point_Qty
                ,RewardTokenPointsExpireQty AS Reward_Token_Point_Expiry_Qty
                ,RewardDollarStartTs AS Reward_Dollar_Start_Ts
                , RewardDollarEndTs AS Reward_Dollar_End_Ts
                ,RewardTokenEndTs AS Reward_Token_End_Ts
                ,RewardTokenWilExpEndTs AS Reward_Token_Will_Expiry_End_Ts 
                ,filename
                ,Actiontypecd
                ,creationdt
                FROM ${src_wrk_tbl}
                WHERE 
                    Household_Id is not NULL  AND
                    Balance_Update_Ts is not NULL 
            ) 
        ) Where rn = 1 
    ) src  
    LEFT JOIN (
        SELECT  
        Household_Id
        ,Balance_Update_Ts
        ,Reward_Origin_Cd
        ,Reward_Origin_Dsc
        ,Reward_Dollar_Point_Qty
        ,Reward_Token_Point_Qty
        ,Reward_Token_Point_Expiry_Qty
        ,Reward_Dollar_Start_Ts
        ,Reward_Dollar_End_Ts
        ,Reward_Token_End_Ts
        ,Reward_Token_Will_Expiry_End_Ts
        ,tgt.DW_Logical_delete_ind
        ,tgt.DW_First_Effective_dt
        FROM ${tgt_tbl} tgt
        WHERE tgt.DW_CURRENT_VERSION_IND = TRUE 
    ) as tgt on 	
    src.Household_Id = tgt.Household_Id AND
    src.Balance_Update_Ts = tgt.Balance_Update_Ts
    where 
    tgt.Household_Id is NULL AND
    tgt.Balance_Update_Ts is NULL 
    OR
    (NVL(tgt.Reward_Origin_Cd,'-1') <> NVL(src.Reward_Origin_Cd,'-1') OR
    NVL(tgt.Reward_Origin_Dsc,'-1') <> NVL(src.Reward_Origin_Dsc,'-1') OR
    NVL(tgt.Reward_Dollar_Point_Qty,'-1') <> NVL(src.Reward_Dollar_Point_Qty,'-1') OR
    NVL(tgt.Reward_Token_Point_Qty,'-1') <> NVL(src.Reward_Token_Point_Qty,'-1') OR
    NVL(tgt.Reward_Token_Point_Expiry_Qty,'-1') <> NVL(src.Reward_Token_Point_Expiry_Qty,'-1') OR
    NVL(tgt.Reward_Dollar_Start_Ts,'9999-12-31 00:00:00.000') <> NVL(src.Reward_Dollar_Start_Ts,'9999-12-31 00:00:00.000') OR
    NVL(tgt.Reward_Dollar_End_Ts,'9999-12-31 00:00:00.000') <> NVL(src.Reward_Dollar_End_Ts,'9999-12-31 00:00:00.000') OR
    NVL(tgt.Reward_Token_End_Ts,'9999-12-31 00:00:00.000') <> NVL(src.Reward_Token_End_Ts,'9999-12-31 00:00:00.000') OR
    NVL(tgt.Reward_Token_Will_Expiry_End_Ts,'9999-12-31 00:00:00.000') <> NVL(src.Reward_Token_Will_Expiry_End_Ts,'9999-12-31 00:00:00.000')
    OR  tgt.dw_logical_delete_ind  <>  src.dw_logical_delete_ind 
    )
)
				`;  	
			try 
			{
				snowflake.execute (
				{sqlText: sql_command  }
				);
			}
            catch (err)  
			{
             	return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
            }
            
               
    //SCD Type2 transaction begins
                var sql_begin = "BEGIN"
                var sql_updates = `// Processing Updates of Type 2 SCD
                UPDATE ${tgt_tbl} as tgt
                SET  DW_Last_Effective_dt = CURRENT_DATE - 1
               ,DW_CURRENT_VERSION_IND = FALSE
               ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
               ,DW_SOURCE_UPDATE_NM = filename
                FROM ( SELECT Household_Id
		,Balance_Update_Ts
                		,filename
                        FROM ${tgt_wrk_tbl}
                        WHERE DML_Type = 'U'
                        AND Sameday_chg_ind = 0
                        AND Household_Id is not null 
                        AND Balance_Update_Ts is not  null
					) src
				WHERE 
				tgt.Household_Id = src.Household_Id AND
		tgt.Balance_Update_Ts = src.Balance_Update_Ts
                AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
                             
	var sql_sameday = `// Processing Sameday updates
			UPDATE ${tgt_tbl} as tgt
			SET       
		    // npks
			Reward_Origin_Cd = src.Reward_Origin_Cd,
		Reward_Origin_Dsc = src.Reward_Origin_Dsc,
		Reward_Dollar_Point_Qty = src.Reward_Dollar_Point_Qty,
		Reward_Token_Point_Qty = src.Reward_Token_Point_Qty,
		Reward_Token_Point_Expiry_Qty = src.Reward_Token_Point_Expiry_Qty,
		Reward_Dollar_Start_Ts = src.Reward_Dollar_Start_Ts,
		Reward_Dollar_End_Ts = src.Reward_Dollar_End_Ts,
		Reward_Token_End_Ts = src.Reward_Token_End_Ts,
		Reward_Token_Will_Expiry_End_Ts = src.Reward_Token_Will_Expiry_End_Ts
			,DW_Logical_delete_ind = src.DW_Logical_delete_ind
			,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
			,DW_SOURCE_UPDATE_NM = filename
			FROM  
			( 
				SELECT 
				Household_Id
				,Balance_Update_Ts
				,Reward_Origin_Cd
				,Reward_Origin_Dsc
				,Reward_Dollar_Point_Qty
				,Reward_Token_Point_Qty
				,Reward_Token_Point_Expiry_Qty
				,Reward_Dollar_Start_Ts
				,Reward_Dollar_End_Ts
				,Reward_Token_End_Ts
				,Reward_Token_Will_Expiry_End_Ts
				,DW_Logical_delete_ind
				,filename
				FROM ${tgt_wrk_tbl}
				WHERE DML_Type = 'U'
				AND Sameday_chg_ind = 1
                AND Household_Id is not null 
                        AND Balance_Update_Ts is not  null
			) src
			WHERE 
			tgt.Household_Id = src.Household_Id AND
		tgt.Balance_Update_Ts = src.Balance_Update_Ts
			AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
                                
                            
	// Processing Inserts
		  var sql_inserts = `INSERT INTO ${tgt_tbl}
		   ( 
			Household_Id
		,Balance_Update_Ts
		,Reward_Origin_Cd
		,Reward_Origin_Dsc
		,Reward_Dollar_Point_Qty  
		,Reward_Token_Point_Qty
		,Reward_Token_Point_Expiry_Qty
		,Reward_Dollar_Start_Ts  
		,Reward_Dollar_End_Ts  
		,Reward_Token_End_Ts
		,Reward_Token_Will_Expiry_End_Ts
			,DW_First_Effective_Dt 
			,DW_Last_Effective_Dt 
			,DW_CREATE_TS          
			,DW_LOGICAL_DELETE_IND  
			,DW_SOURCE_CREATE_NM   
			,DW_CURRENT_VERSION_IND  
			)
		SELECT 
			Household_Id
		,Balance_Update_Ts
		,Reward_Origin_Cd
		,Reward_Origin_Dsc
		,Reward_Dollar_Point_Qty
		,Reward_Token_Point_Qty
		,Reward_Token_Point_Expiry_Qty
		,Reward_Dollar_Start_Ts
		,Reward_Dollar_End_Ts
		,Reward_Token_End_Ts
		,Reward_Token_Will_Expiry_End_Ts
			,CURRENT_DATE
			,'31-DEC-9999'
			,CURRENT_TIMESTAMP
			,DW_Logical_delete_ind
			,filename
			,TRUE
		   FROM ${tgt_wrk_tbl}
		   WHERE Sameday_chg_ind = 0
           AND Household_Id is not null 
                        AND Balance_Update_Ts is not  null
			`;
    var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
            
        snowflake.execute (
           {sqlText: sql_updates  }
            );
           
        snowflake.execute (
            {sqlText: sql_sameday  }
            );
        snowflake.execute (
            {sqlText: sql_inserts  }
            );
        snowflake.execute (
            {sqlText: sql_commit  }
            );
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
	
                // **************        Load for Customer_Reward_Balance table ENDs *****************
    
    
$$;
