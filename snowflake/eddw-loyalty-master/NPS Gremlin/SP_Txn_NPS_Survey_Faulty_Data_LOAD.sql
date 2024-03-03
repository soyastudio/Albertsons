Use database EDM_CONFIRMED_PRD;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_Txn_NPS_Survey_Faulty_Data_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS 
	$$ 
		 
		// ************** Load for Txn_NPS_Survey_Faulty_Data table BEGIN *****************		
		
		var cnf_db = "EDM_CONFIRMED_PRD";		
		var cnf_schema = "DW_C_TRANSACTION";
		
		var tgt_tbl = cnf_db + "." + cnf_schema + ".Txn_NPS_Survey_Faulty_Data";
		var TXN_NPS_SURVEY = "EDM_VIEWS_PRD.DW_EDW_VIEWS.TXN_NPS_SURVEY";  
		var TXN_HDR = "EDM_VIEWS_PRD.DW_EDW_VIEWS.TXN_HDR";  		
		
		
		// Inserting into Target table
		var sql_inserts = `Insert into ` + tgt_tbl + `
				with cte as
				(select HDR.Txn_ID, HDR.Store_id,HDR.register_nbr,hdr.txn_dte,hdr.txn_tm,SR.survey_answer_txt,SR.SCREEN_ID						
											from ` + TXN_NPS_SURVEY + ` SR
											join ` + TXN_HDR + ` HDR
											on SR.Txn_id=HDR.Txn_id
											where SR.txn_dte >=current_Date-1 and SR.txn_dte <=current_Date
											and (nvl(survey_answer_txt,0) not like '%-%' or  ( SCREEN_ID <> 1))                            
											order by Store_id,register_nbr,hdr.txn_dte 
				) 

				,Check_all_count as
				(
					select STORE_ID,REGISTER_NBR,TXN_DTE,count(*) as count_total
					from cte 
					group by STORE_ID,REGISTER_NBR,TXN_DTE
				)

				,Check_zero as (
				select STORE_ID,REGISTER_NBR,TXN_DTE,count(SURVEY_ANSWER_TXT) as count_zero
				from cte where SURVEY_ANSWER_TXT = '0'
				group by STORE_ID,REGISTER_NBR,TXN_DTE
				)
  
				,Check_one as (select STORE_ID,REGISTER_NBR,TXN_DTE,count(SCREEN_ID) as count_one
						from cte where screen_id = 1
						group by STORE_ID,REGISTER_NBR,TXN_DTE
						)
						
				,Check_data AS
				(
					select distinct a.STORE_ID,a.REGISTER_NBR,a.TXN_DTE
					from Check_all_count a
					join Check_zero b on a.STORE_ID = b.STORE_ID and a.REGISTER_NBR = b.REGISTER_NBR and a.TXN_DTE = b.TXN_DTE 
					and a.count_total = b.count_zero
					join Check_one c on b.STORE_ID = c.STORE_ID and b.REGISTER_NBR = c.REGISTER_NBR and b.TXN_DTE = c.TXN_DTE 
					and c.count_one = a.count_total
				) 
				
				,Check_ten as
                (
				select STORE_ID, REGISTER_NBR, TXN_DTE,count(*) from cte where (STORE_ID,REGISTER_NBR,TXN_DTE)
				in (select STORE_ID,REGISTER_NBR,TXN_DTE from Check_data)
                group by STORE_ID, REGISTER_NBR, TXN_DTE
                having count(*)>=10
                )
                              
                select TXN_ID, STORE_ID, REGISTER_NBR, TXN_DTE, TXN_TM from cte where (STORE_ID,REGISTER_NBR,TXN_DTE)
				in (select STORE_ID,REGISTER_NBR,TXN_DTE from Check_ten)`;
		
		try {
			snowflake.execute (
				{sqlText: sql_inserts  }
			)
		}
		catch (err)  {
			throw "Insertion of data into target table Failed with error: " + err;   // Return a error message.
		}		
				
				// **************        Load for Txn_NPS_Survey_Faulty_Data ENDs *****************
		
	$$;
