create or replace view TRIAGE_REPORT_DETAIL(
	CHANGE_BY_TYPE_USER_ID COMMENT 'Change created by the user(Id)',
	CHANGE_BY_TYPE_TS COMMENT 'Date/Time of change creation',
	REASON_TYPE_CD COMMENT 'Code for reason type. eg: Funding, supply, others etc',
	REASON_TYPE_DSC COMMENT 'Description for reason type. eg: Request by division etc',
	CHANGE_TYPE_CD COMMENT 'Codes like alloation, supply, pricing etc in change detail',
	CHANGE_CATEGORY_CD COMMENT 'Category codes like strategy, fix, improvement etc',
	PROMOTION_PROGRAM_TYPE_CD COMMENT 'Offer program codes like SPD, GR, BPD, SC',
	YEAR COMMENT 'Offer created year',
	PROMOTION_PERIOD_NM COMMENT 'Period name includes year and week range',
	PROMOTION_WEEK_ID COMMENT 'It includes year and week number',
	OFFER_REQUEST_ID COMMENT 'Unique id for each offer request created in source system',
	OFFER_EXTERNAL_ID COMMENT 'External Offer Id. Ex: Offers from TOGM, EMOM',
	OFFER_REQUEST_DSC COMMENT 'Description of the offer request',
	OFFER_START_DT COMMENT 'Start date of the offer',
	OFFER_END_DT COMMENT 'End date of the offer'
) comment = 'VIEW for Triage_Report_Detail' 
 as
with cte as
(select DISTINCT
Change_By_Type_User_Id   
,Change_By_Type_Ts        
,Reason_Type_Cd           
,Reason_Type_Dsc          
,Change_Type_Cd           
,CASE WHEN UPPER(CHANGE_TYPE_CD) = 'PRICING' AND UPPER(ATTACHED_OFFER_STATUS_DSC) = 'PUBLISHED' THEN 'IMPROVEMENT' 
      WHEN UPPER(CHANGE_TYPE_CD) = 'POD_REMOVAL' AND UPPER(ATTACHED_OFFER_STATUS_DSC) <> 'PUBLISHED' THEN 'STRATEGY' 
	  WHEN UPPER(REASON_TYPE_CD) IN ('ZERO_ALLOCATION','0_ALLOCATION','DISCO','FUNDING',
	  'LT_5_STORE','MERGED_OFFERS','NO_UPCS') THEN 'STRATEGY' ELSE Change_Category_Cd END AS Change_Category_Cd   
,Promotion_Program_Type_Cd
,case when date(Change_By_Type_Ts) >='2021-12-08' and date(Change_By_Type_Ts) <= '2022-01-04' then 2021
				             when date(Change_By_Type_Ts) >='2022-12-07' and date(Change_By_Type_Ts) <= '2023-01-03' then 2022 
							 when date(Change_By_Type_Ts) >='2023-12-06' and date(Change_By_Type_Ts) <= '2024-01-02' then 2023							 
							 else year(Change_By_Type_Ts) end::number as year            
,case when (date(Change_By_Type_Ts) >='2021-01-06' and date(Change_By_Type_Ts) <= '2021-02-02') OR 
                 (date(Change_By_Type_Ts) >='2022-01-05' and date(Change_By_Type_Ts) <= '2022-02-01') OR 
				 (date(Change_By_Type_Ts) >='2023-01-04' and date(Change_By_Type_Ts) <= '2023-01-31') then concat(year(Change_By_Type_Ts),' Wk 01-04')
				 
	        when (date(Change_By_Type_Ts) >='2021-02-03' and date(Change_By_Type_Ts) <= '2021-03-02') OR 
                 (date(Change_By_Type_Ts) >='2022-02-02' and date(Change_By_Type_Ts) <= '2022-03-01') OR 
				 (date(Change_By_Type_Ts) >='2023-02-01' and date(Change_By_Type_Ts) <= '2023-02-28') then concat(year(Change_By_Type_Ts),' Wk 05-08')
				 
			when (date(Change_By_Type_Ts) >='2021-03-03' and date(Change_By_Type_Ts) <= '2021-03-30') OR
                 (date(Change_By_Type_Ts) >='2022-03-02' and date(Change_By_Type_Ts) <= '2022-03-29') OR 
				 (date(Change_By_Type_Ts) >='2023-03-01' and date(Change_By_Type_Ts) <= '2023-03-28') then concat(year(Change_By_Type_Ts),' Wk 09-12')
				 
			when (date(Change_By_Type_Ts) >='2021-03-31' and date(Change_By_Type_Ts) <= '2021-04-27') OR
                 (date(Change_By_Type_Ts) >='2022-03-30' and date(Change_By_Type_Ts) <= '2022-04-26') OR 
				 (date(Change_By_Type_Ts) >='2023-03-29' and date(Change_By_Type_Ts) <= '2023-04-25') then concat(year(Change_By_Type_Ts),' Wk 13-16')
				 
			when (date(Change_By_Type_Ts) >='2021-04-28' and date(Change_By_Type_Ts) <= '2021-05-25') OR
                 (date(Change_By_Type_Ts) >='2022-04-27' and date(Change_By_Type_Ts) <= '2022-05-24') OR 
				 (date(Change_By_Type_Ts) >='2023-04-26' and date(Change_By_Type_Ts) <= '2023-05-23') then concat(year(Change_By_Type_Ts),' Wk 17-20')
				 
			when (date(Change_By_Type_Ts) >='2021-05-26' and date(Change_By_Type_Ts) <= '2021-06-22') OR
                 (date(Change_By_Type_Ts) >='2022-05-25' and date(Change_By_Type_Ts) <= '2022-06-21') OR 
				 (date(Change_By_Type_Ts) >='2023-05-24' and date(Change_By_Type_Ts) <= '2023-06-20') then concat(year(Change_By_Type_Ts),' Wk 21-24')
				 
			when (date(Change_By_Type_Ts) >='2021-06-23' and date(Change_By_Type_Ts) <= '2021-07-20') OR
			     (date(Change_By_Type_Ts) >='2022-06-22' and date(Change_By_Type_Ts) <= '2022-07-19') OR 
				 (date(Change_By_Type_Ts) >='2023-06-21' and date(Change_By_Type_Ts) <= '2023-07-18') then concat(year(Change_By_Type_Ts),' Wk 25-28')
				 
			when (date(Change_By_Type_Ts) >='2021-07-21' and date(Change_By_Type_Ts) <= '2021-08-17') OR 
                 (date(Change_By_Type_Ts) >='2022-07-20' and date(Change_By_Type_Ts) <= '2022-08-16') OR 
				 (date(Change_By_Type_Ts) >='2023-07-19' and date(Change_By_Type_Ts) <= '2023-08-15') then concat(year(Change_By_Type_Ts),' Wk 29-32')
				 
			when (date(Change_By_Type_Ts) >='2021-08-18' and date(Change_By_Type_Ts) <= '2021-09-14') OR
			     (date(Change_By_Type_Ts) >='2022-08-17' and date(Change_By_Type_Ts) <= '2022-09-13') OR 
				 (date(Change_By_Type_Ts) >='2023-08-16' and date(Change_By_Type_Ts) <= '2023-09-12') then concat(year(Change_By_Type_Ts),' Wk 33-36')
				 
			when (date(Change_By_Type_Ts) >='2021-09-15' and date(Change_By_Type_Ts) <= '2021-10-12') OR
                 (date(Change_By_Type_Ts) >='2022-09-14' and date(Change_By_Type_Ts) <= '2022-10-11') OR 
				 (date(Change_By_Type_Ts) >='2023-09-13' and date(Change_By_Type_Ts) <= '2023-10-10') then concat(year(Change_By_Type_Ts),' Wk 37-40')
				 
			when (date(Change_By_Type_Ts) >='2021-10-13' and date(Change_By_Type_Ts) <= '2021-11-09') OR 
			     (date(Change_By_Type_Ts) >='2022-10-12' and date(Change_By_Type_Ts) <= '2022-11-08') OR 
				 (date(Change_By_Type_Ts) >='2023-10-11' and date(Change_By_Type_Ts) <= '2023-11-07') then concat(year(Change_By_Type_Ts),' Wk 41-44')
				 
			when (date(Change_By_Type_Ts) >='2021-11-10' and date(Change_By_Type_Ts) <= '2021-12-07') OR
			     (date(Change_By_Type_Ts) >='2022-11-09' and date(Change_By_Type_Ts) <= '2022-12-06') OR 
				 (date(Change_By_Type_Ts) >='2023-11-08' and date(Change_By_Type_Ts) <= '2023-12-05') then concat(year(Change_By_Type_Ts),' Wk 45-48')
				 
			when (date(Change_By_Type_Ts) >='2021-12-08' and date(Change_By_Type_Ts) <= '2022-01-04') OR
			     (date(Change_By_Type_Ts) >='2022-12-07' and date(Change_By_Type_Ts) <= '2023-01-03') OR 
				 (date(Change_By_Type_Ts) >='2023-12-06' and date(Change_By_Type_Ts) <= '2024-01-02') then 
				 concat(case when date(Change_By_Type_Ts) >='2021-12-08' and date(Change_By_Type_Ts) <= '2022-01-04' then 2021
				             when date(Change_By_Type_Ts) >='2022-12-07' and date(Change_By_Type_Ts) <= '2023-01-03' then 2022 
							 when date(Change_By_Type_Ts) >='2023-12-06' and date(Change_By_Type_Ts) <= '2024-01-02' then 2023 end,' Wk 49-52') 
				 end as Promotion_Period_Nm      
				 
,concat(case when date(Change_By_Type_Ts) >='2021-12-08' and date(Change_By_Type_Ts) <= '2022-01-04' then 2021
				             when date(Change_By_Type_Ts) >='2022-12-07' and date(Change_By_Type_Ts) <= '2023-01-03' then 2022
							 when date(Change_By_Type_Ts) >='2023-12-06' and date(Change_By_Type_Ts) <= '2024-01-02' then 2023							 
							 else year(Change_By_Type_Ts) end		 
							 
,case when (date(Change_By_Type_Ts) >='2021-01-06' and date(Change_By_Type_Ts) <= '2021-01-12') OR (date(Change_By_Type_Ts) >='2022-01-05' and date(Change_By_Type_Ts) <= '2022-01-11') OR (date(Change_By_Type_Ts) >='2023-01-04' and date(Change_By_Type_Ts) <= '2023-01-10') THEN '01'
	 when (date(Change_By_Type_Ts) >='2021-01-13' and date(Change_By_Type_Ts) <= '2021-01-19') OR (date(Change_By_Type_Ts) >='2022-01-12' and date(Change_By_Type_Ts) <= '2022-01-18') OR (date(Change_By_Type_Ts) >='2023-01-11' and date(Change_By_Type_Ts) <= '2023-01-17')THEN '02'
	 when (date(Change_By_Type_Ts) >='2021-01-20' and date(Change_By_Type_Ts) <= '2021-01-26') OR (date(Change_By_Type_Ts) >='2022-01-19' and date(Change_By_Type_Ts) <= '2022-01-25') OR (date(Change_By_Type_Ts) >='2023-01-18' and date(Change_By_Type_Ts) <= '2023-01-24')THEN '03'
	 when (date(Change_By_Type_Ts) >='2021-01-27' and date(Change_By_Type_Ts) <= '2021-02-02') OR (date(Change_By_Type_Ts) >='2022-01-26' and date(Change_By_Type_Ts) <= '2022-02-01') OR (date(Change_By_Type_Ts) >='2023-01-25' and date(Change_By_Type_Ts) <= '2023-01-31')THEN '04'
	 when (date(Change_By_Type_Ts) >='2021-02-03' and date(Change_By_Type_Ts) <= '2021-02-09') OR (date(Change_By_Type_Ts) >='2022-02-02' and date(Change_By_Type_Ts) <= '2022-02-08') OR (date(Change_By_Type_Ts) >='2023-02-01' and date(Change_By_Type_Ts) <= '2023-02-07')THEN '05'
	 when (date(Change_By_Type_Ts) >='2021-02-10' and date(Change_By_Type_Ts) <= '2021-02-16') OR (date(Change_By_Type_Ts) >='2022-02-09' and date(Change_By_Type_Ts) <= '2022-02-15') OR (date(Change_By_Type_Ts) >='2023-02-08' and date(Change_By_Type_Ts) <= '2023-02-14')THEN '06'
	 when (date(Change_By_Type_Ts) >='2021-02-17' and date(Change_By_Type_Ts) <= '2021-02-23') OR (date(Change_By_Type_Ts) >='2022-02-16' and date(Change_By_Type_Ts) <= '2022-02-22') OR (date(Change_By_Type_Ts) >='2023-02-15' and date(Change_By_Type_Ts) <= '2023-02-21')THEN '07'
	 when (date(Change_By_Type_Ts) >='2021-02-24' and date(Change_By_Type_Ts) <= '2021-03-02') OR (date(Change_By_Type_Ts) >='2022-02-23' and date(Change_By_Type_Ts) <= '2022-03-01') OR (date(Change_By_Type_Ts) >='2023-02-22' and date(Change_By_Type_Ts) <= '2023-02-28')THEN '08'
	 when (date(Change_By_Type_Ts) >='2021-03-03' and date(Change_By_Type_Ts) <= '2021-03-09') OR (date(Change_By_Type_Ts) >='2022-03-02' and date(Change_By_Type_Ts) <= '2022-03-08') OR (date(Change_By_Type_Ts) >='2023-03-01' and date(Change_By_Type_Ts) <= '2023-03-07')THEN '09'
	 when (date(Change_By_Type_Ts) >='2021-03-10' and date(Change_By_Type_Ts) <= '2021-03-16') OR (date(Change_By_Type_Ts) >='2022-03-09' and date(Change_By_Type_Ts) <= '2022-03-15') OR (date(Change_By_Type_Ts) >='2023-03-08' and date(Change_By_Type_Ts) <= '2023-03-14')THEN '10'
	 when (date(Change_By_Type_Ts) >='2021-03-17' and date(Change_By_Type_Ts) <= '2021-03-23') OR (date(Change_By_Type_Ts) >='2022-03-16' and date(Change_By_Type_Ts) <= '2022-03-22') OR (date(Change_By_Type_Ts) >='2023-03-15' and date(Change_By_Type_Ts) <= '2023-03-21')THEN '11'
	 when (date(Change_By_Type_Ts) >='2021-03-24' and date(Change_By_Type_Ts) <= '2021-03-30') OR (date(Change_By_Type_Ts) >='2022-03-23' and date(Change_By_Type_Ts) <= '2022-03-29') OR (date(Change_By_Type_Ts) >='2023-03-22' and date(Change_By_Type_Ts) <= '2023-03-28')THEN '12'
	 when (date(Change_By_Type_Ts) >='2021-03-31' and date(Change_By_Type_Ts) <= '2021-04-06') OR (date(Change_By_Type_Ts) >='2022-03-30' and date(Change_By_Type_Ts) <= '2022-04-05') OR (date(Change_By_Type_Ts) >='2023-03-29' and date(Change_By_Type_Ts) <= '2023-04-04')THEN '13'
	 when (date(Change_By_Type_Ts) >='2021-04-07' and date(Change_By_Type_Ts) <= '2021-04-13') OR (date(Change_By_Type_Ts) >='2022-04-06' and date(Change_By_Type_Ts) <= '2022-04-12') OR (date(Change_By_Type_Ts) >='2023-04-05' and date(Change_By_Type_Ts) <= '2023-04-11')THEN '14'
	 when (date(Change_By_Type_Ts) >='2021-04-14' and date(Change_By_Type_Ts) <= '2021-04-20') OR (date(Change_By_Type_Ts) >='2022-04-13' and date(Change_By_Type_Ts) <= '2022-04-19') OR (date(Change_By_Type_Ts) >='2023-04-12' and date(Change_By_Type_Ts) <= '2023-04-18')THEN '15'
	 when (date(Change_By_Type_Ts) >='2021-04-21' and date(Change_By_Type_Ts) <= '2021-04-27') OR (date(Change_By_Type_Ts) >='2022-04-20' and date(Change_By_Type_Ts) <= '2022-04-26') OR (date(Change_By_Type_Ts) >='2023-04-19' and date(Change_By_Type_Ts) <= '2023-04-25')THEN '16'
	 when (date(Change_By_Type_Ts) >='2021-04-28' and date(Change_By_Type_Ts) <= '2021-05-04') OR (date(Change_By_Type_Ts) >='2022-04-27' and date(Change_By_Type_Ts) <= '2022-05-03') OR (date(Change_By_Type_Ts) >='2023-04-26' and date(Change_By_Type_Ts) <= '2023-05-02')THEN '17'
	 when (date(Change_By_Type_Ts) >='2021-05-05' and date(Change_By_Type_Ts) <= '2021-05-11') OR (date(Change_By_Type_Ts) >='2022-05-04' and date(Change_By_Type_Ts) <= '2022-05-10') OR (date(Change_By_Type_Ts) >='2023-05-03' and date(Change_By_Type_Ts) <= '2023-05-09')THEN '18'
	 when (date(Change_By_Type_Ts) >='2021-05-12' and date(Change_By_Type_Ts) <= '2021-05-18') OR (date(Change_By_Type_Ts) >='2022-05-11' and date(Change_By_Type_Ts) <= '2022-05-17') OR (date(Change_By_Type_Ts) >='2023-05-10' and date(Change_By_Type_Ts) <= '2023-05-16')THEN '19'
	 when (date(Change_By_Type_Ts) >='2021-05-19' and date(Change_By_Type_Ts) <= '2021-05-25') OR (date(Change_By_Type_Ts) >='2022-05-18' and date(Change_By_Type_Ts) <= '2022-05-24') OR (date(Change_By_Type_Ts) >='2023-05-17' and date(Change_By_Type_Ts) <= '2023-05-23')THEN '20'
	 when (date(Change_By_Type_Ts) >='2021-05-26' and date(Change_By_Type_Ts) <= '2021-06-01') OR (date(Change_By_Type_Ts) >='2022-05-25' and date(Change_By_Type_Ts) <= '2022-05-31') OR (date(Change_By_Type_Ts) >='2023-05-24' and date(Change_By_Type_Ts) <= '2023-05-30')THEN '21'
	 when (date(Change_By_Type_Ts) >='2021-06-02' and date(Change_By_Type_Ts) <= '2021-06-08') OR (date(Change_By_Type_Ts) >='2022-06-01' and date(Change_By_Type_Ts) <= '2022-06-07') OR (date(Change_By_Type_Ts) >='2023-05-31' and date(Change_By_Type_Ts) <= '2023-06-06')THEN '22'
	 when (date(Change_By_Type_Ts) >='2021-06-09' and date(Change_By_Type_Ts) <= '2021-06-15') OR (date(Change_By_Type_Ts) >='2022-06-08' and date(Change_By_Type_Ts) <= '2022-06-14') OR (date(Change_By_Type_Ts) >='2023-06-07' and date(Change_By_Type_Ts) <= '2023-06-13')THEN '23'
	 when (date(Change_By_Type_Ts) >='2021-06-16' and date(Change_By_Type_Ts) <= '2021-06-22') OR (date(Change_By_Type_Ts) >='2022-06-15' and date(Change_By_Type_Ts) <= '2022-06-21') OR (date(Change_By_Type_Ts) >='2023-06-14' and date(Change_By_Type_Ts) <= '2023-06-20')THEN '24'
	 when (date(Change_By_Type_Ts) >='2021-06-23' and date(Change_By_Type_Ts) <= '2021-06-29') OR (date(Change_By_Type_Ts) >='2022-06-22' and date(Change_By_Type_Ts) <= '2022-06-28') OR (date(Change_By_Type_Ts) >='2023-06-21' and date(Change_By_Type_Ts) <= '2023-06-27')THEN '25'
	 when (date(Change_By_Type_Ts) >='2021-06-30' and date(Change_By_Type_Ts) <= '2021-07-06') OR (date(Change_By_Type_Ts) >='2022-06-29' and date(Change_By_Type_Ts) <= '2022-07-05') OR (date(Change_By_Type_Ts) >='2023-06-28' and date(Change_By_Type_Ts) <= '2023-07-04')THEN '26'
	 when (date(Change_By_Type_Ts) >='2021-07-07' and date(Change_By_Type_Ts) <= '2021-07-13') OR (date(Change_By_Type_Ts) >='2022-07-06' and date(Change_By_Type_Ts) <= '2022-07-12') OR (date(Change_By_Type_Ts) >='2023-07-05' and date(Change_By_Type_Ts) <= '2023-07-11')THEN '27'
	 when (date(Change_By_Type_Ts) >='2021-07-14' and date(Change_By_Type_Ts) <= '2021-07-20') OR (date(Change_By_Type_Ts) >='2022-07-13' and date(Change_By_Type_Ts) <= '2022-07-19') OR (date(Change_By_Type_Ts) >='2023-07-12' and date(Change_By_Type_Ts) <= '2023-07-18')THEN '28'
	 when (date(Change_By_Type_Ts) >='2021-07-21' and date(Change_By_Type_Ts) <= '2021-07-27') OR (date(Change_By_Type_Ts) >='2022-07-20' and date(Change_By_Type_Ts) <= '2022-07-26') OR (date(Change_By_Type_Ts) >='2023-07-19' and date(Change_By_Type_Ts) <= '2023-07-25')THEN '29'
	 when (date(Change_By_Type_Ts) >='2021-07-28' and date(Change_By_Type_Ts) <= '2021-08-03') OR (date(Change_By_Type_Ts) >='2022-07-27' and date(Change_By_Type_Ts) <= '2022-08-02') OR (date(Change_By_Type_Ts) >='2023-07-26' and date(Change_By_Type_Ts) <= '2023-08-01')THEN '30'
	 when (date(Change_By_Type_Ts) >='2021-08-04' and date(Change_By_Type_Ts) <= '2021-08-10') OR (date(Change_By_Type_Ts) >='2022-08-03' and date(Change_By_Type_Ts) <= '2022-08-09') OR (date(Change_By_Type_Ts) >='2023-08-02' and date(Change_By_Type_Ts) <= '2023-08-08')THEN '31'
	 when (date(Change_By_Type_Ts) >='2021-08-11' and date(Change_By_Type_Ts) <= '2021-08-17') OR (date(Change_By_Type_Ts) >='2022-08-10' and date(Change_By_Type_Ts) <= '2022-08-16') OR (date(Change_By_Type_Ts) >='2023-08-09' and date(Change_By_Type_Ts) <= '2023-08-15')THEN '32'
	 when (date(Change_By_Type_Ts) >='2021-08-18' and date(Change_By_Type_Ts) <= '2021-08-24') OR (date(Change_By_Type_Ts) >='2022-08-17' and date(Change_By_Type_Ts) <= '2022-08-23') OR (date(Change_By_Type_Ts) >='2023-08-16' and date(Change_By_Type_Ts) <= '2023-08-22')THEN '33'
	 when (date(Change_By_Type_Ts) >='2021-08-25' and date(Change_By_Type_Ts) <= '2021-08-31') OR (date(Change_By_Type_Ts) >='2022-08-24' and date(Change_By_Type_Ts) <= '2022-08-30') OR (date(Change_By_Type_Ts) >='2023-08-23' and date(Change_By_Type_Ts) <= '2023-08-29')THEN '34'
	 when (date(Change_By_Type_Ts) >='2021-09-01' and date(Change_By_Type_Ts) <= '2021-09-07') OR (date(Change_By_Type_Ts) >='2022-08-31' and date(Change_By_Type_Ts) <= '2022-09-06') OR (date(Change_By_Type_Ts) >='2023-08-30' and date(Change_By_Type_Ts) <= '2023-09-05')THEN '35'
	 when (date(Change_By_Type_Ts) >='2021-09-08' and date(Change_By_Type_Ts) <= '2021-09-14') OR (date(Change_By_Type_Ts) >='2022-09-07' and date(Change_By_Type_Ts) <= '2022-09-13') OR (date(Change_By_Type_Ts) >='2023-09-06' and date(Change_By_Type_Ts) <= '2023-09-12')THEN '36'
	 when (date(Change_By_Type_Ts) >='2021-09-15' and date(Change_By_Type_Ts) <= '2021-09-21') OR (date(Change_By_Type_Ts) >='2022-09-14' and date(Change_By_Type_Ts) <= '2022-09-20') OR (date(Change_By_Type_Ts) >='2023-09-13' and date(Change_By_Type_Ts) <= '2023-09-19')THEN '37'
	 when (date(Change_By_Type_Ts) >='2021-09-22' and date(Change_By_Type_Ts) <= '2021-09-28') OR (date(Change_By_Type_Ts) >='2022-09-21' and date(Change_By_Type_Ts) <= '2022-09-27') OR (date(Change_By_Type_Ts) >='2023-09-20' and date(Change_By_Type_Ts) <= '2023-09-26')THEN '38'
	 when (date(Change_By_Type_Ts) >='2021-09-29' and date(Change_By_Type_Ts) <= '2021-10-05') OR (date(Change_By_Type_Ts) >='2022-09-28' and date(Change_By_Type_Ts) <= '2022-10-04') OR (date(Change_By_Type_Ts) >='2023-09-27' and date(Change_By_Type_Ts) <= '2023-10-03')THEN '39'
	 when (date(Change_By_Type_Ts) >='2021-10-06' and date(Change_By_Type_Ts) <= '2021-10-12') OR (date(Change_By_Type_Ts) >='2022-10-05' and date(Change_By_Type_Ts) <= '2022-10-11') OR (date(Change_By_Type_Ts) >='2023-10-04' and date(Change_By_Type_Ts) <= '2023-10-10')THEN '40'
	 when (date(Change_By_Type_Ts) >='2021-10-13' and date(Change_By_Type_Ts) <= '2021-10-19') OR (date(Change_By_Type_Ts) >='2022-10-12' and date(Change_By_Type_Ts) <= '2022-10-18') OR (date(Change_By_Type_Ts) >='2023-10-11' and date(Change_By_Type_Ts) <= '2023-10-17')THEN '41'
	 when (date(Change_By_Type_Ts) >='2021-10-20' and date(Change_By_Type_Ts) <= '2021-10-26') OR (date(Change_By_Type_Ts) >='2022-10-19' and date(Change_By_Type_Ts) <= '2022-10-25') OR (date(Change_By_Type_Ts) >='2023-10-18' and date(Change_By_Type_Ts) <= '2023-10-24')THEN '42'
	 when (date(Change_By_Type_Ts) >='2021-10-27' and date(Change_By_Type_Ts) <= '2021-11-02') OR (date(Change_By_Type_Ts) >='2022-10-26' and date(Change_By_Type_Ts) <= '2022-11-01') OR (date(Change_By_Type_Ts) >='2023-10-25' and date(Change_By_Type_Ts) <= '2023-10-31')THEN '43'
	 when (date(Change_By_Type_Ts) >='2021-11-03' and date(Change_By_Type_Ts) <= '2021-11-09') OR (date(Change_By_Type_Ts) >='2022-11-02' and date(Change_By_Type_Ts) <= '2022-11-08') OR (date(Change_By_Type_Ts) >='2023-11-01' and date(Change_By_Type_Ts) <= '2023-11-07')THEN '44'
	 when (date(Change_By_Type_Ts) >='2021-11-10' and date(Change_By_Type_Ts) <= '2021-11-16') OR (date(Change_By_Type_Ts) >='2022-11-09' and date(Change_By_Type_Ts) <= '2022-11-15') OR (date(Change_By_Type_Ts) >='2023-11-08' and date(Change_By_Type_Ts) <= '2023-11-14')THEN '45'
	 when (date(Change_By_Type_Ts) >='2021-11-17' and date(Change_By_Type_Ts) <= '2021-11-23') OR (date(Change_By_Type_Ts) >='2022-11-16' and date(Change_By_Type_Ts) <= '2022-11-22') OR (date(Change_By_Type_Ts) >='2023-11-15' and date(Change_By_Type_Ts) <= '2023-11-21')THEN '46'
	 when (date(Change_By_Type_Ts) >='2021-11-24' and date(Change_By_Type_Ts) <= '2021-11-30') OR (date(Change_By_Type_Ts) >='2022-11-23' and date(Change_By_Type_Ts) <= '2022-11-29') OR (date(Change_By_Type_Ts) >='2023-11-22' and date(Change_By_Type_Ts) <= '2023-11-28')THEN '47'
	 when (date(Change_By_Type_Ts) >='2021-12-01' and date(Change_By_Type_Ts) <= '2021-12-07') OR (date(Change_By_Type_Ts) >='2022-11-30' and date(Change_By_Type_Ts) <= '2022-12-06') OR (date(Change_By_Type_Ts) >='2023-11-29' and date(Change_By_Type_Ts) <= '2023-12-05')THEN '48'
	 when (date(Change_By_Type_Ts) >='2021-12-08' and date(Change_By_Type_Ts) <= '2021-12-14') OR (date(Change_By_Type_Ts) >='2022-12-07' and date(Change_By_Type_Ts) <= '2022-12-13') OR (date(Change_By_Type_Ts) >='2023-12-06' and date(Change_By_Type_Ts) <= '2023-12-12')THEN '49'
	 when (date(Change_By_Type_Ts) >='2021-12-15' and date(Change_By_Type_Ts) <= '2021-12-21') OR (date(Change_By_Type_Ts) >='2022-12-14' and date(Change_By_Type_Ts) <= '2022-12-20') OR (date(Change_By_Type_Ts) >='2023-12-13' and date(Change_By_Type_Ts) <= '2023-12-19')THEN '50'
	 when (date(Change_By_Type_Ts) >='2021-12-22' and date(Change_By_Type_Ts) <= '2021-12-28') OR (date(Change_By_Type_Ts) >='2022-12-21' and date(Change_By_Type_Ts) <= '2022-12-27') OR (date(Change_By_Type_Ts) >='2023-12-20' and date(Change_By_Type_Ts) <= '2023-12-26')THEN '51'
	 when (date(Change_By_Type_Ts) >='2021-12-29' and date(Change_By_Type_Ts) <= '2022-01-04') OR (date(Change_By_Type_Ts) >='2022-12-28' and date(Change_By_Type_Ts) <= '2023-01-03') OR (date(Change_By_Type_Ts) >='2023-12-27' and date(Change_By_Type_Ts) <= '2024-01-02')THEN '52'
	END)::number as Promotion_Week_Id        
,offer_Request_id         
,Offer_External_Id        
,Offer_Request_Dsc        
,Offer_Start_Dt           
,Offer_End_Dt 
,ATTACHED_OFFER_STATUS_DSC
from (
select distinct dtl.Change_By_Type_User_Id         
               ,case when UPPER(dtl.CHANGE_TYPE_CD) = 'POD_REMOVAL' then a.updatets::timestamp_ntz else 
										convert_timezone('UTC',dtl.Change_By_Type_Ts)::timestamp_ntz end AS Change_By_Type_Ts          
			   ,dtl.Reason_Type_Cd                 
			   ,dtl.Reason_Type_Dsc                
			   ,dtl.Change_Type_Cd                 
			   ,dtl.Change_Category_Cd             
			   ,offr.Promotion_Program_Type_Cd 			   			   
			   ,offr.offer_Request_id              
			   ,req.Offer_External_Id              
			   ,offr.Offer_Request_Dsc             
			   ,offr.Offer_Start_Dt                
			   ,offr.Offer_End_Dt                  
               ,req.ATTACHED_OFFER_STATUS_DSC
from EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request offr
left join 
(	select offerrequestid, max(updatets) as updatets
	from EDM_CONFIRMED_PRD.DW_C_PRODUCT.getofferrequest_flat 
	group by offerrequestid
	) a on a.offerrequestid = offr.offer_request_id
left join EDM_CONFIRMED_PRD.DW_C_PURCHASING.OFFER_REQUEST_PROMOTION_PERIOD_TYPE typ on typ.OFFER_REQUEST_ID = offr.OFFER_REQUEST_ID and typ.DW_CURRENT_VERSION_IND = TRUE AND typ.DW_LOGICAL_DELETE_IND = FALSE
left join EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Change_Detail dtl on dtl.OFFER_REQUEST_ID = offr.OFFER_REQUEST_ID
			and dtl.DW_CURRENT_VERSION_IND = TRUE AND dtl.DW_LOGICAL_DELETE_IND = FALSE
left join EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Offer req on req.OFFER_REQUEST_ID = offr.OFFER_REQUEST_ID and req.DW_CURRENT_VERSION_IND = TRUE AND req.DW_LOGICAL_DELETE_IND = FALSE
where UPPER(offr.Promotion_Program_Type_Cd) <> 'SC' AND offr.DW_CURRENT_VERSION_IND = TRUE AND offr.DW_LOGICAL_DELETE_IND = FALSE
and upper(dtl.Change_Type_Cd) not in ('OFFER_CREATION')
)
)

select distinct CHANGE_BY_TYPE_USER_ID, CHANGE_BY_TYPE_TS, REASON_TYPE_CD, REASON_TYPE_DSC,CHANGE_TYPE_CD,CHANGE_CATEGORY_CD,PROMOTION_PROGRAM_TYPE_CD,
YEAR,PROMOTION_PERIOD_NM,PROMOTION_WEEK_ID,OFFER_REQUEST_ID,OFFER_EXTERNAL_ID,OFFER_REQUEST_DSC,OFFER_START_DT,OFFER_END_DT
from cte where UPPER(CHANGE_CATEGORY_CD) = 'FIX' and UPPER(ATTACHED_OFFER_STATUS_DSC) = 'PUBLISHED'
union all
select distinct CHANGE_BY_TYPE_USER_ID, CHANGE_BY_TYPE_TS, REASON_TYPE_CD, REASON_TYPE_DSC,CHANGE_TYPE_CD,CHANGE_CATEGORY_CD,PROMOTION_PROGRAM_TYPE_CD,
YEAR,PROMOTION_PERIOD_NM,PROMOTION_WEEK_ID,OFFER_REQUEST_ID,OFFER_EXTERNAL_ID,OFFER_REQUEST_DSC,OFFER_START_DT,OFFER_END_DT
from cte where UPPER(CHANGE_CATEGORY_CD) <> 'FIX'
order by offer_request_id
;
