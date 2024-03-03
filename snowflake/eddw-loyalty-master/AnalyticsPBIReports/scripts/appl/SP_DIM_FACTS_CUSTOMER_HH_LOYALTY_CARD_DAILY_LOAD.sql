CREATE OR REPLACE PROCEDURE SP_DIM_FACTS_CUSTOMER_HH_LOYALTY_CARD_DAILY_LOAD()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$

// stage tables
var stg_retail_customer_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_retail_customer_tmp'
var stg_lu_customer_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_lu_customer_tmp'
var stg_lu_household_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_lu_household_tmp'
var stg_lu_card_account_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_lu_card_account_tmp'
var stg_employee_detail_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_employee_detail_tmp'
var stg_employee_termination_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_employee_termination_tmp'
var stg_cte_airmiles_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_cte_airmiles_tmp'
var stg_cte_general_mills_tmp  = 'EDM_ANALYTICS_PRD.dw_stage.stg_cte_general_mills_tmp'
var stg_cte_customer_address_tmp  = 'EDM_ANALYTICS_PRD.dw_stage.stg_cte_customer_address_tmp'


// target dimension tables
var tgt_d1_retail_customer = 'EDM_ANALYTICS_PRD.dw_reference.D1_Retail_Customer'
var tgt_d1_retail_customer_household = 'EDM_ANALYTICS_PRD.dw_reference.D1_Retail_Customer_household'
var tgt_d1_loyalty_program = 'EDM_ANALYTICS_PRD.dw_reference.D1_Loyalty_Program'

// load ready tables for dimensions
var lr_d1_retail_customer_updt = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Retail_Customer_updt'
var lr_d1_retail_customer_ins = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Retail_Customer_ins'
var lr_d1_retail_customer_household_updt = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Retail_Customer_household_updt'
var lr_d1_retail_customer_household_ins = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Retail_Customer_household_ins'
var lr_d1_loyalty_program_updt = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Loyalty_Program_updt'
var lr_d1_loyalty_program_ins = 'EDM_ANALYTICS_PRD.dw_stage.lr_D1_Loyalty_Program_ins'

// stage/load ready tables for facts
var stg_f_retail_customer = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_retail_customer'
var stg_f_loyalty_program = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_loyalty_program'
var stg_f_household = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_household'

// stage tables for weekly facts load
var stg_f_customer_txn_tmp = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_customer_txn_tmp'
var stg_f_club_card_txn_metrics = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_club_card_txn_metrics'
var stg_f_household_txn_metrics = 'EDM_ANALYTICS_PRD.dw_stage.stg_f_household_txn_metrics'

// target fact tables
var tgt_f_retail_customer = 'EDM_ANALYTICS_PRD.dw_reference.F_Retail_Customer_Household_Loyalty_Program'
var tgt_f_household = 'EDM_ANALYTICS_PRD.dw_reference.F_Household_Retail_Customer_Loyalty_Program'
var tgt_f_loyalty_program = 'EDM_ANALYTICS_PRD.dw_reference.F_Loyalty_Program_Retail_Customer_Household'

                                               
//Peoplesoft data
var sql_stg_employee_detail_tmp = `create or replace transient table ${stg_employee_detail_tmp} 
                                   as 
                                   select emplid::NUMBER as employee_id
                                         ,termination_dt::DATE as employee_end_dt
                                     from EDM_CONFIRMED_PRD.DW_C_LABOR.ps_s_employees;`
                                                            

var sql_stg_retail_customer_tmp = `create or replace transient table ${stg_retail_customer_tmp} 
                                   as 
                                   select distinct rc.retail_customer_uuid  // distinct is needed due to multiple possible active records in customer_account_status
                                         ,rc.gender_cd
                                         ,rc.gender_dsc
                                         ,rc.gender_short_dsc
                                         ,rc.preferred_salutation_cd
                                         ,rc.title_cd
                                         ,rc.formatted_nm
                                         ,rc.nick_nm
                                         ,rc.first_nm
                                         ,rc.middle_nm
                                         ,rc.last_nm
                                         ,rc.maiden_nm
                                         ,rc.generation_affix_cd
                                         ,rc.qualification_affix_cd
                                         ,rc.birth_dt
                                         // 2022-08-17:  added employee termination date from peoplesoft in setting employee_ind
                                         ,case when rc.employee_id is null then false // non-employee
                                               when coalesce(emp.employee_end_dt, '9999-12-31') <= current_date 
                                               then false // non-employee, terminated in peoplesoft
                                               else true 
                                           end::boolean as employee_ind
                                         ,clp.loyalty_program_card_nbr
                                         ,clp.source_nm
                                         ,rch.household_id
                                         ,COALESCE(rch.head_household_ind, FALSE) as head_household_ind
                                         ,cas.status_value_txt
                                         ,cas.status_cd
                                         ,cas.status_dsc
                                         ,cas.status_value_txt as customer_status_value_txt
                                         ,rch.status_value_txt as household_status_value_txt
                                         ,clps.loyalty_program_status_value_txt as card_status_value_txt
                                         // 2022-08-04:  added employee_id, set to null if terminated
                                         ,case when employee_ind = 
                                               true then rc.employee_id 
                                               else null 
                                           end as employee_id
                                     from EDM_VIEWS_PRD.dw_views.retail_customer rc
                                   
                                     join EDM_VIEWS_PRD.dw_views.customer_account_status cas
                                       on cas.retail_customer_uuid = rc.retail_customer_uuid
                                      and cas.dw_current_version_ind = true
                                   
                                     join EDM_VIEWS_PRD.dw_views.retail_customer_household rch
                                       on rch.retail_customer_uuid = rc.retail_customer_uuid
                                      and rch.household_id <> 0
                                      and rch.dw_current_version_ind = true
                                   
                                     join EDM_VIEWS_PRD.dw_views.customer_loyalty_program clp
                                       on clp.retail_customer_uuid = rc.retail_customer_uuid
                                      and clp.loyalty_program_nm = '1' // 1-clubcard, 2-alaska airmiles, 3- general mills (box top)
                                      and clp.loyalty_program_card_nbr <> ''
                                      and clp.dw_current_version_ind = true
                                   
                                     join EDM_VIEWS_PRD.dw_views.customer_loyalty_program_status clps
                                       on clps.retail_customer_uuid = clp.retail_customer_uuid
                                      and clps.loyalty_program_card_nbr = clp.loyalty_program_card_nbr
                                      and clps.loyalty_program_nm = clp.loyalty_program_nm
                                      and clps.dw_current_version_ind = true
                                      // 2022-08-17:  added this table as temporary fix to determine if employee has been terminated in uca (different from peoplesoft)
                                   
                                     left 
                                    outer 
                                     join ${stg_employee_termination_tmp} emp_uca 
                                       on emp_uca.retail_customer_uuid = rc.retail_customer_uuid
                                      and emp_uca.employee_id = rc.employee_id
                                   
                                     // 2022-08-04:  added this table as temporary fix to determinine if employee has been terminated in peoplesoft
                                     // this needs to be replaced with employee table once fixed in edm
                                     left 
                                    outer 
                                     join ${stg_employee_detail_tmp} emp
                                       on emp.employee_id = rc.employee_id
                                   
                                    where rc.dw_current_version_ind = true
                                  qualify rank() over (partition by rc.retail_customer_uuid 
                                                           order by cas.status_value_txt asc
                                                                   ,rch.status_value_txt asc
                                                                   ,clps.loyalty_program_status_value_txt asc
                                                                   ,rc.source_sequence_nbr desc
                                                                   ,rc.source_last_update_ts desc
                                                                   ,rc.dw_create_ts desc
                                                      ) = 1;`

var sql_stg_cte_airmiles = `create or replace transient table ${stg_cte_airmiles_tmp} 
                            as 
                            select rc.retail_customer_uuid
                                  ,clp.loyalty_program_card_nbr
                                  ,clp.create_ts
                              from ${stg_retail_customer_tmp} rc
                            
                              join EDM_VIEWS_PRD.dw_views.customer_loyalty_program clp
                                on clp.retail_customer_uuid = rc.retail_customer_uuid
                               and clp.loyalty_program_nm = '2' // 1-clubcard, 2-alaska airmiles, 3- general mills (box top)
                               and clp.dw_current_version_ind = true
                              
                              join EDM_VIEWS_PRD.dw_views.customer_loyalty_program_status clps
                                on clps.retail_customer_uuid = clp.retail_customer_uuid
                               and clps.loyalty_program_card_nbr = clp.loyalty_program_card_nbr
                               and clps.loyalty_program_nm = clp.loyalty_program_nm
                               and clps.loyalty_program_status_value_txt = '100'  // active clubcards only
                               and clps.dw_current_version_ind = true;`

var sql_stg_cte_general_mills = `create or replace transient table ${stg_cte_general_mills_tmp} 
                                 as 
                                 select rc.retail_customer_uuid
                                       ,clp.loyalty_program_card_nbr
                                       ,clp.create_ts
                                   from ${stg_retail_customer_tmp} rc
                     
                                   join EDM_VIEWS_PRD.dw_views.customer_loyalty_program clp
                                     on clp.retail_customer_uuid = rc.retail_customer_uuid
                                    and clp.loyalty_program_nm = '3' // 1-clubcard, 2-alaska airmiles, 3- general mills (box top)
                                    and clp.dw_current_version_ind = true
                     
                                   join EDM_VIEWS_PRD.dw_views.customer_loyalty_program_status clps
                                     on clps.retail_customer_uuid = clp.retail_customer_uuid
                                    and clps.loyalty_program_card_nbr = clp.loyalty_program_card_nbr
                                    and clps.loyalty_program_nm = clp.loyalty_program_nm
                                    and clps.loyalty_program_status_value_txt = '100'  // active clubcards only
                                    and clps.dw_current_version_ind = true;`

var sql_stg_cte_customer_address = `create or replace transient table ${stg_cte_customer_address_tmp} 
                                    as 
                                    select ca.retail_customer_uuid
                                          ,ca.city_nm
                                          ,ca.address_line1_txt
                                          ,ca.address_line2_txt
                                          ,ca.postal_zone_cd
                                          ,ca.state_cd
                                          ,ca.country_cd
                                          ,cads.first_effective_ts
                                          ,cads.last_effective_ts
                                          ,case when cads.retail_customer_uuid is not null 
                                                then 1 
                                                else 0 
                                            end as in_cads_ind
                                      from ${stg_retail_customer_tmp} rc
                                
                                      join EDM_VIEWS_PRD.dw_views.customer_address ca
                                        on ca.retail_customer_uuid = rc.retail_customer_uuid
                                       and ca.dw_current_version_ind = true
                                
                                      join EDM_VIEWS_PRD.dw_views.customer_address_purpose cap
                                        on cap.retail_customer_uuid = ca.retail_customer_uuid
                                       and cap.customer_address_id = ca.customer_address_id
                                       and cap.customer_address_purpose_cd = '2' // 2 - mailing addresss (CHAMP), 1 - DELIVERY
                                       and cap.dw_current_version_ind = true
                                
                                      join EDM_VIEWS_PRD.dw_views.customer_address_status cads  // all addresses have status so this can be an inner join
                                        on cads.retail_customer_uuid = cap.retail_customer_uuid
                                       and cads.customer_address_id = cap.customer_address_id
                                       and cads.dw_current_version_ind = true
                                   qualify row_number() over (partition by ca.retail_customer_uuid 
                                                                  order by last_effective_ts desc
                                                             ) = 1;`
                                                                 
var sql_stg_lu_customer_tmp = `create or replace transient table ${stg_lu_customer_tmp} 
                               as 
                               select rc.retail_customer_uuid
                                     ,rc.household_id
                                     ,rc.loyalty_program_card_nbr
                                     ,rc.head_household_ind
                                     ,rc.status_value_txt as customer_status_id //take the MIN
                                     ,case when rc.status_value_txt = '100' 
                                           then 'Active'
                                           when rc.status_value_txt = '200' 
                                           then 'Inactive'
                                           else 'N/A'
                                       end::varchar as status_value_dsc
                                     ,coalesce(rc.gender_cd, 'N/A') as gender_cd
                                     ,coalesce(rc.gender_dsc, 'N/A') as gender_dsc
                                     ,coalesce(rc.gender_short_dsc, 'N/A') as gender_short_dsc
                                     ,coalesce(rc.birth_dt, 'i/ss') as birth_dt /* source no longer collects full birth_dt  */
                                     ,coalesce(rc.preferred_salutation_cd, 'N/A') as preferred_salutation_cd  /* cust_title_txt */
                                     ,coalesce(rc.title_cd, 'N/A') as title_cd
                                     ,coalesce(rc.formatted_nm, 'i/ss') as formatted_nm
                                     ,coalesce(rc.nick_nm, 'i/ss') as nick_nm
                                     ,coalesce(rc.first_nm, 'i/ss') as first_nm
                                     ,coalesce(rc.middle_nm, 'i/ss') as middle_nm
                                     ,coalesce(rc.last_nm, 'i/ss') as last_nm
                                     ,coalesce(rc.maiden_nm, 'i/ss') as maiden_nm
                                     ,coalesce(rc.generation_affix_cd, 'N/A') as generation_affix_cd /* name_suffix_1_txt */
                                     ,coalesce(rc.qualification_affix_cd, 'N/A')  as qualification_affix_cd /* name_suffix_2_txt */
                                     ,coalesce(rc.status_cd, 'N/A') as customer_status_cd
                                     ,coalesce(rc.status_dsc, 'N/A') as customer_status_dsc
                                     ,coalesce(ca.city_nm, 'N/A') as city_nm
                                     ,coalesce(ca.address_line1_txt, 'O/DQ') as address_line1_txt
                                     ,coalesce(ca.address_line2_txt, 'O/DQ') as address_line2_txt
                                     ,coalesce(ca.postal_zone_cd, 'N/A') as postal_zone_cd
                                     ,coalesce(ca.state_cd, 'N/A') as state_cd
                                     ,coalesce(ca.country_cd, 'N/A') as country_cd
                                     ,case when cpoc.opt_choice_ind = 'OPT_OUT' 
                                           then false
                                           else true
                                       end::boolean as mail_allowed_ind
                                     ,rc.status_value_txt
                                     ,rc.employee_ind
                                     ,coalesce(rc.source_nm, 'N/A') as source_nm
                                     ,coalesce(cpfc.area_cd, -1) as area_cd
                                     ,coalesce(cpfc.phone_nbr, 'O/zG') as primary_phone_nbr
                                     ,coalesce(cdc.digital_address_txt, 'L/km') as primary_digital_address_txt
                                     ,ca.first_effective_ts as start_eff_dt
                                     ,ca.last_effective_ts as end_eff_dt
                                     ,coalesce(clpa.loyalty_program_card_nbr, 'N/A') as air_mile_acct_nbr
                                     ,coalesce(clpgm.loyalty_program_card_nbr, 'N/A') as general_mills_id
                                     ,rc.customer_status_value_txt
                                     ,rc.household_status_value_txt
                                     ,rc.card_status_value_txt
                                     ,coalesce(rc.employee_id, -1) as employee_id
                                     ,coalesce(cdc.email_domain_nm, 'N/A') as email_domain_nm
                                 from ${stg_retail_customer_tmp} rc

                                 left 
                                outer 
                                 join ${stg_cte_airmiles_tmp} clpa
                                   on clpa.retail_customer_uuid = rc.retail_customer_uuid

                                 left 
                                outer 
                                 join ${stg_cte_general_mills_tmp} clpgm
                                   on clpgm.retail_customer_uuid = rc.retail_customer_uuid

                                 left 
                                outer 
                                 join ${stg_cte_customer_address_tmp} ca
                                   on ca.retail_customer_uuid = rc.retail_customer_uuid

                                 left 
                                outer 
                                 join EDM_VIEWS_PRD.dw_views.customer_preference_opt_choice cpoc
                                   on cpoc.retail_customer_uuid = rc.retail_customer_uuid
                                  and cpoc.preference_definition_id = '235b7ab8-16af-4e46-a392-fe0a4dc2d21b'  // GLOBAL preference
                                  and cpoc.channel_type_cd = '1' // 1-email, 2-SMS
                                  and cpoc.dw_current_version_ind = true

                                 left 
                                outer 
                                 join EDM_VIEWS_PRD.dw_views.customer_phone_fax_contact cpfc
                                   on cpfc.retail_customer_uuid = rc.retail_customer_uuid
                                  and cpfc.phone_purpose_dsc = 'PRIMARY' // LIMIT RESULT TO just the PRIMARY
                                  and cpfc.dw_current_version_ind = true

                                 left 
                                outer 
                                 join EDM_VIEWS_PRD.dw_views.customer_digital_contact cdc
                                   on cdc.retail_customer_uuid = rc.retail_customer_uuid
                                  and cdc.email_purpose_dsc = 'PRIMARY' // LIMIT RESULT TO just the primary
                                  and cdc.dw_current_version_ind = true
                              qualify row_number() over (partition by rc.retail_customer_uuid
                                                             order by cpoc.aggregate_revision_nbr desc
                                                                     ,cpoc.dw_create_ts desc /* resolve multiple customer_preference_opt_choice records */

                                                                     /*resolve multiple primary customer_phone_fax_contact records */
                                                                     ,cpfc.source_aggregate_revision_nbr desc
                                                                     ,cpfc.dw_create_ts desc 
                                                                     ,cpfc.first_effective_ts desc 

                                                                     /*resolve multiple primary email addresses, there are still duplicates*/
                                                                     ,cdc.source_aggregate_revision_nbr desc, cdc.dw_create_ts desc
                                                                     ,cdc.digital_address_txt desc 
                                                                     ,rc.household_id desc
                                                                     ,rc.loyalty_program_card_nbr desc
                                                        ) = 1;`
                                                        

var sql_lr_d1_retail_customer_updt = `create or replace transient table ${lr_d1_retail_customer_updt} 
                                      as
                                      select tgt.retail_customer_d1_sk
                                            ,stg.retail_customer_uuid
                                            ,stg.head_household_ind
                                            ,stg.retail_customer_uuid as customer_id
                                            ,stg.gender_cd
                                            ,stg.gender_dsc
                                            ,stg.gender_short_dsc
                                            ,stg.birth_dt
                                            ,stg.formatted_nm
                                            ,stg.preferred_salutation_cd
                                            ,stg.title_cd
                                            ,stg.nick_nm
                                            ,stg.first_nm
                                            ,stg.middle_nm
                                            ,stg.last_nm
                                            ,stg.maiden_nm
                                            ,stg.generation_affix_cd
                                            ,stg.qualification_affix_cd
                                            ,stg.customer_status_cd
                                            ,stg.customer_status_dsc
                                            ,stg.status_value_txt
                                            ,stg.status_value_dsc
                                            ,stg.city_nm
                                            ,stg.source_nm
                                            ,stg.employee_ind
                                            ,stg.address_line1_txt
                                            ,stg.address_line2_txt
                                            ,stg.postal_zone_cd
                                            ,stg.state_cd
                                            ,stg.country_cd
                                            ,stg.primary_phone_nbr
                                            ,stg.area_cd
                                            ,stg.primary_digital_address_txt
                                            ,stg.mail_allowed_ind
                                            ,stg.air_mile_acct_nbr as air_mile_account_nbr
                                            ,stg.general_mills_id
                                            ,stg.employee_id
                                            ,stg.email_domain_nm
                                        from ${stg_lu_customer_tmp} stg

                                        join ${tgt_d1_retail_customer} tgt
                                          on tgt.retail_customer_uuid = stg.retail_customer_uuid

                                       where tgt.head_household_ind          <> stg.head_household_ind          
                                          or tgt.gender_cd                   <> stg.gender_cd                   
                                          or tgt.gender_dsc                  <> stg.gender_dsc                  
                                          or tgt.gender_short_dsc            <> stg.gender_short_dsc            
                                          or tgt.birth_dt                    <> stg.birth_dt                    
                                          or tgt.formatted_nm                <> stg.formatted_nm                
                                          or tgt.preferred_salutation_cd     <> stg.preferred_salutation_cd     
                                          or tgt.title_cd                    <> stg.title_cd                  
                                          or tgt.nick_nm                     <> stg.nick_nm               
                                          or tgt.first_nm                    <> stg.first_nm              
                                          or tgt.middle_nm                   <> stg.middle_nm             
                                          or tgt.last_nm                     <> stg.last_nm               
                                          or tgt.maiden_nm                   <> stg.maiden_nm             
                                          or tgt.generation_affix_cd         <> stg.generation_affix_cd   
                                          or tgt.qualification_affix_cd      <> stg.qualification_affix_cd
                                          or tgt.customer_status_cd          <> stg.customer_status_cd    
                                          or tgt.customer_status_dsc         <> stg.customer_status_dsc   
                                          or tgt.status_value_txt            <> stg.status_value_txt      
                                          or tgt.status_value_dsc            <> stg.status_value_dsc
                                          or tgt.city_nm                     <> stg.city_nm               
                                          or tgt.source_nm                   <> stg.source_nm             
                                          or tgt.employee_ind                <> stg.employee_ind          
                                          or tgt.address_line1_txt           <> stg.address_line1_txt     
                                          or tgt.address_line2_txt           <> stg.address_line2_txt     
                                          or tgt.postal_zone_cd              <> stg.postal_zone_cd        
                                          or tgt.state_cd                    <> stg.state_cd              
                                          or tgt.country_cd                  <> stg.country_cd                
                                          or tgt.primary_phone_nbr           <> stg.primary_phone_nbr
                                          or tgt.area_cd                     <> stg.area_cd               
                                          or tgt.primary_digital_address_txt <> stg.primary_digital_address_txt
                                          or tgt.mail_allowed_ind            <> stg.mail_allowed_ind      
                                          or tgt.air_mile_account_nbr        <> stg.air_mile_acct_nbr
                                          or tgt.general_mills_id            <> stg.general_mills_id
                                          or tgt.employee_id                 <> stg.employee_id
                                          or tgt.email_domain_nm             <> stg.email_domain_nm;`

var sql_lr_d1_retail_customer_ins = `create or replace transient table ${lr_d1_retail_customer_ins} 
                                     as 
                                     select stg.retail_customer_uuid
                                           ,stg.head_household_ind
                                           ,stg.retail_customer_uuid as customer_id
                                           ,stg.gender_cd
                                           ,stg.gender_dsc
                                           ,stg.gender_short_dsc
                                           ,stg.birth_dt
                                           ,stg.formatted_nm
                                           ,stg.preferred_salutation_cd
                                           ,stg.title_cd
                                           ,stg.nick_nm
                                           ,stg.first_nm
                                           ,stg.middle_nm
                                           ,stg.last_nm
                                           ,stg.maiden_nm
                                           ,stg.generation_affix_cd
                                           ,stg.qualification_affix_cd
                                           ,stg.customer_status_cd
                                           ,stg.customer_status_dsc
                                           ,stg.status_value_txt
                                           ,stg.status_value_dsc
                                           ,stg.city_nm
                                           ,stg.source_nm
                                           ,stg.employee_ind
                                           ,stg.address_line1_txt
                                           ,stg.address_line2_txt
                                           ,stg.postal_zone_cd
                                           ,stg.state_cd
                                           ,stg.country_cd
                                           ,stg.primary_phone_nbr
                                           ,stg.area_cd
                                           ,stg.primary_digital_address_txt
                                           ,stg.mail_allowed_ind
                                           ,stg.air_mile_acct_nbr as air_mile_account_nbr
                                           ,stg.general_mills_id
                                           ,stg.employee_id
                                           ,stg.email_domain_nm
                                       from ${stg_lu_customer_tmp} stg
                                     
                                       left 
                                      outer 
                                       join ${tgt_d1_retail_customer} tgt
                                         on      tgt.retail_customer_uuid = stg.retail_customer_uuid
                                     
                                      where tgt.retail_customer_d1_sk is null;`

var sql_stg_lu_household_tmp = `create or replace transient table ${stg_lu_household_tmp} 
                                as
                                select household_id
                                      ,loyalty_program_card_nbr
                                      ,retail_customer_uuid
                                      ,max(employee_ind) over (partition by household_id) as employee_ind
                                      ,coalesce(last_nm, 'i/ss') as last_nm
                                      ,household_status_value_txt as status_value_txt
                                      ,case when household_status_value_txt = '100' then 'Active'
                                            when household_status_value_txt = '200' then 'Inactive'
                                            else 'N/A'
                                        end::varchar as status_value_dsc
                                  from ${stg_retail_customer_tmp}
                               qualify row_number() over (partition by household_id
                                                              order by head_household_ind desc
                                                                      ,last_nm asc
                                                                      ,loyalty_program_card_nbr desc
                                                                      ,retail_customer_uuid desc
                                                         ) = 1;`

var sql_lr_d1_retail_customer_household_updt = `create or replace transient table ${lr_d1_retail_customer_household_updt} 
                                                as
                                                select tgt.retail_customer_household_d1_sk
                                                      ,stg.employee_ind
                                                      ,stg.last_nm
                                                      ,stg.status_value_txt
                                                      ,stg.status_value_dsc
                                                  from ${stg_lu_household_tmp} stg

                                                  join ${tgt_d1_retail_customer_household} tgt
                                                    on tgt.household_id = stg.household_id

                                                 where tgt.employee_ind     <> stg.employee_ind          
                                                    or tgt.last_nm          <> stg.last_nm                   
                                                    or tgt.status_value_txt <> stg.status_value_txt                  
                                                    or tgt.status_value_dsc <> stg.status_value_dsc   
                                                ;`
                                                
var sql_lr_d1_retail_customer_household_ins = `create or replace transient table ${lr_d1_retail_customer_household_ins} 
                                               as 
                                               select stg.household_id
                                                     ,stg.employee_ind
                                                     ,stg.last_nm
                                                     ,stg.status_value_txt
                                                     ,stg.status_value_dsc
                                                 from ${stg_lu_household_tmp} stg

                                                 left 
                                                outer 
                                                 join ${tgt_d1_retail_customer_household} tgt
                                                   on tgt.household_id = stg.household_id

                                                where tgt.retail_customer_household_d1_sk is null
                                               ;`
                                               
var sql_stg_lu_card_account_tmp = `create or replace transient table ${stg_lu_card_account_tmp} 
                                   as
                                   select clp.loyalty_program_card_nbr
                                         ,rc.retail_customer_uuid
                                         ,rch.household_id
                                         ,clps.loyalty_program_status_value_txt
                                         ,clp.loyalty_program_nm
                                         ,case when loyalty_program_status_value_txt = '100' then 'Active'
                                               when loyalty_program_status_value_txt = '200' then 'Inactive'
                                               else 'N/A'
                                           end::varchar as loyalty_program_status_value_dsc
                                     from EDM_VIEWS_PRD.dw_views.retail_customer rc
                                   
                                     join EDM_VIEWS_PRD.dw_views.customer_account_status cas
                                       on cas.retail_customer_uuid = rc.retail_customer_uuid
                                      and cas.dw_current_version_ind = true
                                   
                                     join EDM_VIEWS_PRD.dw_views.retail_customer_household rch
                                       on rch.retail_customer_uuid = rc.retail_customer_uuid and rch.household_id <> 0
                                      and rch.dw_current_version_ind = true
                                   
                                     join EDM_VIEWS_PRD.dw_views.customer_loyalty_program clp
                                       on clp.retail_customer_uuid = rc.retail_customer_uuid
                                      and clp.loyalty_program_nm = '1' // 1-clubcard, 2-alaska airmiles, 3- general mills (box top)
                                      and clp.loyalty_program_card_nbr <> ''
                                      and clp.dw_current_version_ind = true
                                     
                                     join EDM_VIEWS_PRD.dw_views.customer_loyalty_program_status clps
                                       on clps.retail_customer_uuid = clp.retail_customer_uuid
                                      and clps.loyalty_program_card_nbr = clp.loyalty_program_card_nbr
                                      and clps.loyalty_program_nm = clp.loyalty_program_nm
                                      and clps.dw_current_version_ind = true
                                   
                                    where rc.dw_current_version_ind = true
                                  qualify row_number() over (partition by clp.loyalty_program_card_nbr 
                                                                 order by clps.loyalty_program_status_value_txt asc
                                                                         ,cas.status_value_txt asc
                                                                         ,rch.status_value_txt asc
                                                                         ,rch.head_household_ind desc
                                                                         ,rch.household_id desc
                                                                         ,rc.retail_customer_uuid asc
                                                            ) = 1;`
                                   
var sql_lr_d1_loyalty_program_updt = `create or replace transient table ${lr_d1_loyalty_program_updt} 
                                      as 
                                      select tgt.loyalty_program_d1_sk
                                            ,stg.loyalty_program_card_nbr
                                            ,stg.loyalty_program_status_value_txt
                                            ,stg.loyalty_program_nm
                                            ,stg.loyalty_program_status_value_dsc
                                        from ${stg_lu_card_account_tmp} stg

                                        join ${tgt_d1_loyalty_program} tgt
                                          on tgt.loyalty_program_card_nbr = stg.loyalty_program_card_nbr

                                       where tgt.loyalty_program_status_value_txt <> stg.loyalty_program_status_value_txt
                                          or tgt.loyalty_program_nm <> stg.loyalty_program_nm;`


var sql_lr_d1_loyalty_program_ins = `create or replace transient table ${lr_d1_loyalty_program_ins} 
                                     as 
                                     select stg.loyalty_program_card_nbr
                                           ,stg.loyalty_program_status_value_txt
                                           ,stg.loyalty_program_nm
                                           ,stg.loyalty_program_status_value_dsc
                                       from ${stg_lu_card_account_tmp} stg

                                       left 
                                      outer 
                                       join ${tgt_d1_loyalty_program} tgt
                                         on tgt.loyalty_program_card_nbr = stg.loyalty_program_card_nbr

                                      where tgt.loyalty_program_d1_sk is null;`
                                      

//Prepare update statement for ${tgt_d1_retail_customer}
var sql_update_d1_retail_customer = `update ${tgt_d1_retail_customer} tgt
                                       from ${lr_d1_retail_customer_updt} updt
                                        set head_household_ind          = updt.head_household_ind          
                                           ,gender_cd                   = updt.gender_cd                   
                                           ,gender_dsc                  = updt.gender_dsc                  
                                           ,gender_short_dsc            = updt.gender_short_dsc            
                                           ,birth_dt                    = updt.birth_dt                    
                                           ,formatted_nm                = updt.formatted_nm                
                                           ,preferred_salutation_cd     = updt.preferred_salutation_cd     
                                           ,title_cd                    = updt.title_cd                  
                                           ,nick_nm                     = updt.nick_nm               
                                           ,first_nm                    = updt.first_nm              
                                           ,middle_nm                   = updt.middle_nm             
                                           ,last_nm                     = updt.last_nm               
                                           ,maiden_nm                   = updt.maiden_nm             
                                           ,generation_affix_cd         = updt.generation_affix_cd   
                                           ,qualification_affix_cd      = updt.qualification_affix_cd
                                           ,customer_status_cd          = updt.customer_status_cd    
                                           ,customer_status_dsc         = updt.customer_status_dsc    
                                           ,city_nm                     = updt.city_nm               
                                           ,address_line1_txt           = updt.address_line1_txt     
                                           ,address_line2_txt           = updt.address_line2_txt     
                                           ,postal_zone_cd              = updt.postal_zone_cd        
                                           ,state_cd                    = updt.state_cd              
                                           ,country_cd                  = updt.country_cd                
                                           ,mail_allowed_ind            = updt.mail_allowed_ind      
                                           ,status_value_txt            = updt.status_value_txt      
                                           ,employee_ind                = updt.employee_ind          
                                           ,source_nm                   = updt.source_nm             
                                           ,area_cd                     = updt.area_cd               
                                           ,primary_phone_nbr           = updt.primary_phone_nbr
                                           ,primary_digital_address_txt = updt.primary_digital_address_txt
                                           ,status_value_dsc            = updt.status_value_dsc
                                           ,air_mile_account_nbr        = updt.air_mile_account_nbr
                                           ,general_mills_id            = updt.general_mills_id
                                           ,employee_id                 = updt.employee_id
                                           ,dw_last_update_ts           = current_timestamp
                                           ,dw_logical_delete_ind       = false
                                           ,email_domain_nm             = updt.email_domain_nm
                                      where tgt.retail_customer_d1_sk = updt.retail_customer_d1_sk;`

//Prepare insert statement for ${tgt_d1_retail_customer}
var sql_insert_d1_retail_customer = `insert into ${tgt_d1_retail_customer}
                                        (       
                                         retail_customer_d1_sk
                                        ,retail_customer_uuid
                                        ,head_household_ind
                                        ,customer_id
                                        ,gender_cd
                                        ,gender_dsc
                                        ,gender_short_dsc
                                        ,birth_dt
                                        ,formatted_nm
                                        ,preferred_salutation_cd
                                        ,title_cd
                                        ,nick_nm
                                        ,first_nm
                                        ,middle_nm
                                        ,last_nm
                                        ,maiden_nm
                                        ,generation_affix_cd
                                        ,qualification_affix_cd
                                        ,customer_status_cd
                                        ,customer_status_dsc
                                        ,city_nm
                                        ,address_line1_txt
                                        ,address_line2_txt
                                        ,postal_zone_cd
                                        ,state_cd
                                        ,country_cd
                                        ,mail_allowed_ind
                                        ,status_value_txt
                                        ,status_value_dsc
                                        ,employee_ind
                                        ,source_nm
                                        ,area_cd
                                        ,primary_phone_nbr
                                        ,primary_digital_address_txt
                                        ,air_mile_account_nbr
                                        ,general_mills_id
                                        ,employee_id
                                        ,dw_create_ts
                                        ,dw_last_update_ts
                                        ,dw_logical_delete_ind
                                        ,email_domain_nm
                                        )
                                     select EDM_ANALYTICS_PRD.dw_appl.d1_retail_customer_seq.nextval as retail_customer_d1_sk
                                           ,retail_customer_uuid
                                           ,head_household_ind
                                           ,retail_customer_uuid as customer_id
                                           ,gender_cd
                                           ,gender_dsc
                                           ,gender_short_dsc
                                           ,birth_dt
                                           ,formatted_nm
                                           ,preferred_salutation_cd
                                           ,title_cd
                                           ,nick_nm
                                           ,first_nm
                                           ,middle_nm
                                           ,last_nm
                                           ,maiden_nm
                                           ,generation_affix_cd
                                           ,qualification_affix_cd
                                           ,customer_status_cd
                                           ,customer_status_dsc
                                           ,city_nm
                                           ,address_line1_txt
                                           ,address_line2_txt
                                           ,postal_zone_cd
                                           ,state_cd
                                           ,country_cd
                                           ,mail_allowed_ind
                                           ,status_value_txt
                                           ,status_value_dsc
                                           ,employee_ind
                                           ,source_nm
                                           ,area_cd
                                           ,primary_phone_nbr
                                           ,primary_digital_address_txt
                                           ,air_mile_account_nbr
                                           ,general_mills_id
                                           ,employee_id
                                           ,current_timestamp as dw_create_ts
                                           ,current_timestamp as dw_last_udpate_ts
                                           ,false as dw_logical_delete_ind
                                           ,email_domain_nm
                                       from ${lr_d1_retail_customer_ins};`

//Prepare update statement for ${tgt_d1_retail_customer_household}
var sql_update_d1_retail_customer_household = `update ${tgt_d1_retail_customer_household} tgt
                                                 from ${lr_d1_retail_customer_household_updt} updt
                                                  set employee_ind  = updt.employee_ind    
                                                     ,last_nm = updt.last_nm         
                                                     ,status_value_txt = updt.status_value_txt
                                                     ,status_value_dsc = updt.status_value_dsc
                                                     ,dw_last_update_ts = current_timestamp
                                                     ,dw_logical_delete_ind  = false
                                                where tgt.retail_customer_household_d1_sk = updt.retail_customer_household_d1_sk;`

//Prepare insert statement for ${tgt_d1_retail_customer_household}
var sql_insert_d1_retail_customer_household = `insert into ${tgt_d1_retail_customer_household}
                                               (
                                                retail_customer_household_d1_sk
                                               ,household_id
                                               ,employee_ind
                                               ,last_nm
                                               ,status_value_txt
                                               ,status_value_dsc
                                               ,dw_create_ts
                                               ,dw_last_update_ts
                                               ,dw_logical_delete_ind
                                               )
                                               select EDM_ANALYTICS_PRD.dw_appl.d1_household_seq.nextval
                                                     ,household_id
                                                     ,employee_ind
                                                     ,last_nm
                                                     ,status_value_txt
                                                     ,status_value_dsc
                                                     ,current_timestamp as dw_create_ts
                                                     ,current_timestamp as dw_last_udpate_ts
                                                     ,false as dw_logical_delete_ind
                                                 from ${lr_d1_retail_customer_household_ins};`

//Prepare update statement for ${tgt_d1_loyalty_program}
var sql_update_d1_loyalty_program = `update ${tgt_d1_loyalty_program} tgt
                                       from ${lr_d1_loyalty_program_updt} updt
                                        set loyalty_program_status_value_txt = updt.loyalty_program_status_value_txt
                                           ,loyalty_program_nm = updt.loyalty_program_nm
                                           ,loyalty_program_status_value_dsc = updt.loyalty_program_status_value_dsc
                                           ,dw_last_update_ts = current_timestamp
                                           ,dw_logical_delete_ind = false
                                      where tgt.loyalty_program_d1_sk = updt.loyalty_program_d1_sk;`

//Prepare insert statement for ${tgt_d1_loyalty_program}
var sql_insert_d1_loyalty_program = `insert into ${tgt_d1_loyalty_program}
                                     (
                                      loyalty_program_d1_sk
                                     ,loyalty_program_card_nbr
                                     ,loyalty_program_status_value_txt
                                     ,loyalty_program_nm
                                     ,loyalty_program_status_value_dsc
                                     ,dw_create_ts
                                     ,dw_last_update_ts
                                     ,dw_logical_delete_ind
                                     )
                                     select EDM_ANALYTICS_PRD.dw_appl.d1_loyalty_program_seq.nextval
                                           ,loyalty_program_card_nbr
                                           ,loyalty_program_status_value_txt
                                           ,loyalty_program_nm
                                           ,loyalty_program_status_value_dsc
                                           ,current_timestamp as dw_create_ts
                                           ,current_timestamp as dw_last_udpate_ts
                                           ,false as dw_logical_delete_ind
                                       from ${lr_d1_loyalty_program_ins};`

var sql_begin = "begin";
var sql_commit = "commit";
var sql_rollback = "rollback";

try
{
    snowflake.execute ({sqlText: sql_begin});
    snowflake.execute ({sqlText: sql_stg_employee_detail_tmp});
    snowflake.execute ({sqlText: sql_stg_retail_customer_tmp});
    snowflake.execute ({sqlText: sql_stg_cte_airmiles});
    snowflake.execute ({sqlText: sql_stg_cte_general_mills});
    snowflake.execute ({sqlText: sql_stg_cte_customer_address});
    snowflake.execute ({sqlText: sql_stg_lu_customer_tmp});
    snowflake.execute ({sqlText: sql_lr_d1_retail_customer_updt});
    snowflake.execute ({sqlText: sql_lr_d1_retail_customer_ins});

    snowflake.execute ({sqlText: sql_stg_lu_household_tmp});
    snowflake.execute ({sqlText: sql_lr_d1_retail_customer_household_updt});
    snowflake.execute ({sqlText: sql_lr_d1_retail_customer_household_ins});
    snowflake.execute ({sqlText: sql_stg_lu_card_account_tmp});
    snowflake.execute ({sqlText: sql_lr_d1_loyalty_program_updt});
    snowflake.execute ({sqlText: sql_lr_d1_loyalty_program_ins});


    snowflake.execute ({sqlText: sql_update_d1_retail_customer});
    snowflake.execute ({sqlText: sql_insert_d1_retail_customer});
    snowflake.execute ({sqlText: sql_update_d1_retail_customer_household});
    snowflake.execute ({sqlText: sql_insert_d1_retail_customer_household});
    snowflake.execute ({sqlText: sql_update_d1_loyalty_program});
    snowflake.execute ({sqlText: sql_insert_d1_loyalty_program});
	snowflake.execute ({sqlText: sql_commit});
}
catch (err)  
{
    snowflake.execute ({sqlText: sql_rollback});
    throw "**** :Hint: Failure in the D1 Loads ****" + err;
}

//Prepare retail_customer fact data
var sql_stg_f_retail_customer = `create or replace transient table ${stg_f_retail_customer} 
                                 as 
                                 select coalesce(d1rc.retail_customer_d1_sk, -1) as retail_customer_d1_sk
                                       ,coalesce(d1rch.retail_customer_household_d1_sk, -1) as primary_retail_customer_household_d1_sk
                                       ,coalesce(d1lp.loyalty_program_d1_sk, -1) as primary_loyalty_program_d1_sk
                                       ,1 as retail_customer_cnt
                                       ,coalesce(frc.customer_first_used_store_d1_sk, -1) as customer_first_used_store_d1_sk
                                       ,coalesce(frc.customer_last_used_store_d1_sk, -1) as customer_last_used_store_d1_sk
                                       ,coalesce(frc.customer_most_used_store_d1_sk, -1) as customer_most_used_store_d1_sk
                                       ,coalesce(frc.customer_most_spend_store_d1_sk, -1) as customer_most_spend_store_d1_sk
                                       ,coalesce(frc.customer_first_visit_day_id, -1) as customer_first_visit_day_id
                                       ,coalesce(frc.customer_last_visit_day_id, -1) as customer_last_visit_day_id
                                       ,case when frc.retail_customer_d1_sk is null then true else false end::boolean as new_record_ind
                                       ,case when frc.primary_retail_customer_household_d1_sk <> coalesce(d1rch.retail_customer_household_d1_sk, -1)
                                               or frc.primary_loyalty_program_d1_sk <> coalesce(d1lp.loyalty_program_d1_sk, -1) 
                                             then true
                                             else false
                                         end:: boolean as change_record_ind
                                 from ${stg_lu_customer_tmp} t
                                 
                                 left 
                                outer 
                                 join ${tgt_d1_retail_customer} d1rc
                                   on d1rc.retail_customer_uuid = t.retail_customer_uuid
                                 
                                 left 
                                outer 
                                 join ${tgt_d1_retail_customer_household} d1rch
                                   on d1rch.household_id = t.household_id
                                 
                                 left 
                                outer 
                                 join ${tgt_d1_loyalty_program} d1lp
                                   on d1lp.loyalty_program_card_nbr = t.loyalty_program_card_nbr
                                 
                                 left 
                                outer 
                                 join ${tgt_f_retail_customer} frc
                                   on frc.retail_customer_d1_sk = d1rc.retail_customer_d1_sk;`


var sql_update_f_retail_customer = `update ${tgt_f_retail_customer} tgt
                                      from ${stg_f_retail_customer} stg
                                       set primary_retail_customer_household_d1_sk = stg.primary_retail_customer_household_d1_sk
                                          ,primary_loyalty_program_d1_sk = stg.primary_loyalty_program_d1_sk
                                          ,dw_last_update_ts = current_timestamp
                                      where tgt.retail_customer_d1_sk = stg.retail_customer_d1_sk
                                        and stg.new_record_ind = false
                                        and stg.change_record_ind = true;`

var sql_insert_f_retail_customer = `insert into ${tgt_f_retail_customer}
                                    (
                                      retail_customer_d1_sk
                                     ,primary_retail_customer_household_d1_sk
                                     ,primary_loyalty_program_d1_sk
                                     ,retail_customer_cnt
                                     ,customer_first_used_store_d1_sk
                                     ,customer_last_used_store_d1_sk
                                     ,customer_most_used_store_d1_sk
                                     ,customer_most_spend_store_d1_sk
                                     ,customer_first_visit_day_id
                                     ,customer_last_visit_day_id
                                     ,dw_create_ts
                                     ,dw_last_update_ts
                                     ,dw_logical_delete_ind
                                    )
                                    select retail_customer_d1_sk
                                          ,primary_retail_customer_household_d1_sk
                                          ,primary_loyalty_program_d1_sk
                                          ,retail_customer_cnt
                                          ,customer_first_used_store_d1_sk
                                          ,customer_last_used_store_d1_sk
                                          ,customer_most_used_store_d1_sk
                                          ,customer_most_spend_store_d1_sk
                                          ,customer_first_visit_day_id
                                          ,customer_last_visit_day_id
                                          ,current_timestamp as dw_create_ts
                                          ,current_timestamp as dw_last_update_ts
                                          ,false as dw_logical_delete_ind
                                      from ${stg_f_retail_customer}
                                     where new_record_ind = true;`

//Prepare loyalty program fact data
var sql_stg_f_loyalty_program = `create or replace transient table ${stg_f_loyalty_program} 
                                 as 
                                 select coalesce(d1lp.loyalty_program_d1_sk, -1) as loyalty_program_d1_sk
                                      ,coalesce(d1rch.retail_customer_household_d1_sk, -1) as primary_retail_customer_household_d1_sk
                                      ,coalesce(d1rc.retail_customer_d1_sk, -1) as primary_retail_customer_d1_sk
                                      ,1 as loyalty_program_card_cnt
                                      ,coalesce(flp.card_first_used_store_d1_sk, -1) as card_first_used_store_d1_sk
                                      ,coalesce(flp.card_last_used_store_d1_sk, -1) as card_last_used_store_d1_sk
                                      ,coalesce(flp.card_most_used_store_d1_sk, -1) as card_most_used_store_d1_sk
                                      ,coalesce(flp.card_most_spend_store_d1_sk, -1) as card_most_spend_store_d1_sk
                                      ,coalesce(flp.card_first_visit_day_id, -1) as card_first_visit_day_id
                                      ,coalesce(flp.card_last_visit_day_id, -1) as card_last_visit_day_id
                                      ,case when flp.loyalty_program_d1_sk is null then true else false end::boolean as new_record_ind
                                      ,case when flp.primary_retail_customer_household_d1_sk <> coalesce(d1rch.retail_customer_household_d1_sk, -1)
                                              or flp.primary_retail_customer_d1_sk <> coalesce(d1rc.retail_customer_d1_sk, -1) 
                                            then true
                                            else false
                                        end:: boolean as change_record_ind
                                  from ${stg_lu_card_account_tmp} t

                                  left 
                                 outer 
                                  join ${tgt_d1_retail_customer} d1rc
                                    on d1rc.retail_customer_uuid = t.retail_customer_uuid

                                  left 
                                 outer 
                                  join ${tgt_d1_retail_customer_household} d1rch
                                    on d1rch.household_id = t.household_id

                                  left 
                                 outer 
                                  join ${tgt_d1_loyalty_program} d1lp
                                    on d1lp.loyalty_program_card_nbr = t.loyalty_program_card_nbr

                                  left 
                                 outer 
                                  join ${tgt_f_loyalty_program} flp
                                    on flp.loyalty_program_d1_sk = d1lp.loyalty_program_d1_sk;`

var sql_update_f_loyalty_program = `update ${tgt_f_loyalty_program} tgt
                                      from ${stg_f_loyalty_program} stg
                                       set primary_retail_customer_household_d1_sk = stg.primary_retail_customer_household_d1_sk
                                          ,primary_retail_customer_d1_sk = stg.primary_retail_customer_d1_sk
                                          ,dw_last_update_ts = current_timestamp
                                     where tgt.loyalty_program_d1_sk = stg.loyalty_program_d1_sk
                                       and stg.new_record_ind = false
                                       and stg.change_record_ind = true;`

var sql_insert_f_loyalty_program = `insert into ${tgt_f_loyalty_program}
                                    (       
                                      loyalty_program_d1_sk
                                     ,primary_retail_customer_household_d1_sk
                                     ,primary_retail_customer_d1_sk
                                     ,loyalty_program_card_cnt
                                     ,card_first_used_store_d1_sk
                                     ,card_last_used_store_d1_sk
                                     ,card_most_used_store_d1_sk
                                     ,card_most_spend_store_d1_sk
                                     ,card_first_visit_day_id
                                     ,card_last_visit_day_id
                                     ,dw_create_ts
                                     ,dw_last_update_ts
                                     ,dw_logical_delete_ind
                                    )
                                    select loyalty_program_d1_sk
                                          ,primary_retail_customer_household_d1_sk
                                          ,primary_retail_customer_d1_sk
                                          ,loyalty_program_card_cnt
                                          ,card_first_used_store_d1_sk
                                          ,card_last_used_store_d1_sk
                                          ,card_most_used_store_d1_sk
                                          ,card_most_spend_store_d1_sk
                                          ,card_first_visit_day_id
                                          ,card_last_visit_day_id
                                          ,current_timestamp as dw_create_ts
                                          ,current_timestamp as dw_last_update_ts
                                          ,false as dw_logical_delete_ind
                                      from ${stg_f_loyalty_program}
                                     where new_record_ind = true;`

//Prepare household fact data
var sql_stg_f_household = `create or replace transient table ${stg_f_household} 
                           as
                           select coalesce(d1rch.retail_customer_household_d1_sk, -1) as retail_customer_household_d1_sk
                                 ,coalesce(d1rc.retail_customer_d1_sk, -1) as primary_retail_customer_d1_sk
                                 ,coalesce(d1lp.loyalty_program_d1_sk, -1) as primary_loyalty_program_d1_sk
                                 ,coalesce(hhcnt.household_cnt, 1) as household_cnt
                                 ,coalesce(fh.household_first_used_store_d1_sk, -1) as household_first_used_store_d1_sk
                                 ,coalesce(fh.household_last_used_store_d1_sk, -1) as household_last_used_store_d1_sk
                                 ,coalesce(fh.household_most_used_store_d1_sk, -1) as household_most_used_store_d1_sk
                                 ,coalesce(fh.household_most_spend_store_d1_sk, -1) as household_most_spend_store_d1_sk
                                 ,coalesce(fh.household_first_visit_day_id, -1) as household_first_visit_day_id
                                 ,coalesce(fh.household_last_visit_day_id, -1) as household_last_visit_day_id
                                 ,case when fh.retail_customer_household_d1_sk is null then true else false end::boolean as new_record_ind
                                 ,case when fh.primary_loyalty_program_d1_sk <> coalesce(d1lp.loyalty_program_d1_sk, -1)
                                         or fh.primary_retail_customer_d1_sk <> coalesce(d1rc.retail_customer_d1_sk, -1) 
                                         or fh.household_cnt <> coalesce(hhcnt.household_cnt, 1) 
                                       then true
                                       else false
                                   end:: boolean as change_record_ind
                             from ${stg_lu_household_tmp} t
                           
                             left outer join ${tgt_d1_retail_customer} d1rc
                             on d1rc.retail_customer_uuid = t.retail_customer_uuid
                           
                             left outer join ${tgt_d1_retail_customer_household} d1rch
                             on d1rch.household_id = t.household_id
                           
                             left outer join ${tgt_d1_loyalty_program} d1lp
                             on d1lp.loyalty_program_card_nbr = t.loyalty_program_card_nbr
                           
                             left outer join ${tgt_f_household} fh
                             on fh.retail_customer_household_d1_sk = d1rch.retail_customer_household_d1_sk
                           
                             left 
                            outer 
                             join (
                                    select household_id
                                          ,count(distinct retail_customer_uuid) as household_cnt
                                      from ${stg_lu_customer_tmp}
                                     group 
                                        by household_id
                                  ) hhcnt
                               on hhcnt.household_id = t.household_id;`

var sql_update_f_household = `update ${tgt_f_household} tgt
                                from ${stg_f_household} stg
                                 set primary_loyalty_program_d1_sk = stg.primary_loyalty_program_d1_sk
                                    ,primary_retail_customer_d1_sk = stg.primary_retail_customer_d1_sk
                                    ,household_cnt = stg.household_cnt
                                    ,dw_last_update_ts = current_timestamp
                               where tgt.retail_customer_household_d1_sk = stg.retail_customer_household_d1_sk
                                 and stg.new_record_ind = false
                                 and stg.change_record_ind = true;`

var sql_insert_f_household = `insert into ${tgt_f_household}
                              (       
                                 retail_customer_household_d1_sk
                                ,primary_retail_customer_d1_sk
                                ,primary_loyalty_program_d1_sk
                                ,household_cnt
                                ,household_first_used_store_d1_sk
                                ,household_last_used_store_d1_sk
                                ,household_most_used_store_d1_sk
                                ,household_most_spend_store_d1_sk
                                ,household_first_visit_day_id
                                ,household_last_visit_day_id
                                ,dw_create_ts
                                ,dw_last_update_ts
                                ,dw_logical_delete_ind
                              )
                              select retail_customer_household_d1_sk
                                    ,primary_retail_customer_d1_sk
                                    ,primary_loyalty_program_d1_sk
                                    ,household_cnt
                                    ,household_first_used_store_d1_sk
                                    ,household_last_used_store_d1_sk
                                    ,household_most_used_store_d1_sk
                                    ,household_most_spend_store_d1_sk
                                    ,household_first_visit_day_id
                                    ,household_last_visit_day_id
                                    ,current_timestamp as dw_create_ts
                                    ,current_timestamp as dw_last_update_ts
                                    ,false as dw_logical_delete_ind
                                from ${stg_f_household}
                               where new_record_ind = true;`

try
{
    snowflake.execute ({sqlText: sql_begin});
    
    
    snowflake.execute ({sqlText: sql_stg_f_retail_customer});
    snowflake.execute ({sqlText: sql_update_f_retail_customer});
    snowflake.execute ({sqlText: sql_insert_f_retail_customer});

    snowflake.execute ({sqlText: sql_stg_f_loyalty_program});
    snowflake.execute ({sqlText: sql_update_f_loyalty_program});
    snowflake.execute ({sqlText: sql_insert_f_loyalty_program});

    snowflake.execute ({sqlText: sql_stg_f_household});
    snowflake.execute ({sqlText: sql_update_f_household});
    snowflake.execute ({sqlText: sql_insert_f_household});

	snowflake.execute ({sqlText: sql_commit});
}
catch (err)  
{
    snowflake.execute ({sqlText: sql_rollback});
    throw "**** :Hint: Failure in the Fact Loads ****" + err;
}

$$;