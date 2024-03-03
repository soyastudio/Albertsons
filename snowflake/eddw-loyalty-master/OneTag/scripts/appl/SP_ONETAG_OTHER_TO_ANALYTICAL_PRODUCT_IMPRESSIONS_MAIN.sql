--liquibase formatted sql
--changeset SYSTEM:SP_CLICK_STREAM_PRODUCT_IMPRESSIONS_MAIN runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_ANALYTIC_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_ONETAG_OTHER_TO_ANALYTICAL_PRODUCT_IMPRESSIONS_MAIN"("START_DT" VARCHAR(16777216),"HIST_FLAG" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    
    //environment
  	var cur_db = snowflake.execute( {sqlText: `Select current_database()`} );
	cur_db.next();
	var env = cur_db.getColumnValue(1);
	env = env.split(''_'');
	env = env[env.length - 1];
	var history_load_flag=''N''
    
    // Global Variable Declaration
    var edm_db= `EDM_VIEWS_${env}`;
	var edm_views=`DW_VIEWS`;
	if (HIST_FLAG=='''')
	{
		history_load_flag=''N''
	}
	else 
	{
		history_load_flag=HIST_FLAG
	}

	var analytical_db = "EDM_ANALYTICS_${env}";
	var params="''"+START_DT+"''";
	var stg_param="''"+START_DT+"''"+",''"+history_load_flag+"''"
    
    if(START_DT=='''') 
    {
    params= "CURRENT_DATE";
	stg_param="CURRENT_DATE"+",''"+history_load_flag+"''"
    }
       
    // function to facilitate child stored procedure executions   
    function execSubProc(sub_proc_nm, params)
    {
        try {
             ret_obj = snowflake.execute (
                        {sqlText: `call ${sub_proc_nm}(${params})`  }
                        );
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);
            
            }
        catch (err)  {
            return `Error executing stored procedure ${sub_proc_nm}(${params}) ${err}`;   // Return a error message.
            }
        return ret_msg;
    }
	
	//child procedure names
  var sub_proc_nms = [
						''SP_ONETAG_PRODUCT_IMPRESSIONS_TO_ANALYTICAL_LOAD''
						]

    //execution of all child procedures 
    for (index = 0; index < sub_proc_nms.length; index++) 
	{
        sub_proc_nm = sub_proc_nms[index];
	
		return_msg = execSubProc(sub_proc_nm, stg_param);
		
        if (return_msg)
			{   throw return_msg;
			}
    }   
    ';
    
