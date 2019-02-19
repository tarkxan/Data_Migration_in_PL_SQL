/*$Id: TEST_SCRIPT.sql,v 1.4 2013/07/01 15:49:52 olando Exp $*/

create table export_grd_backup_23062013 as select * from export_grd_backup;
create table export_grd_23062013        as select * from export_grd;

----------------- CREATE export_grd AND FILTER STATUS = 'W' PROJECTS ----------------- 
create table export_grd_source_backup200513 as select * from export_grd;
delete from export_grd t; --

insert into export_grd t select * from export_grd_source -- 4206
;
delete  -- 4206 - 23 = 4183
from   export_grd_26052013 t
where  t.plw_pjt_id  in('BR_001868', 'VE_000578', 'BR_000952', 'BR_010821', 'BR_004729', 'VE_002298', 'RU_009555', 'VE_011070',
                        'UA_002600', 'VN_006987', 'KR_007050', 'TW_007421', 'KZ_000437', 'JP_008396', 'AR_009492', 'JP_007128',
                        'BY_007514', 'IL_007796', 'IL_000597', 'IL_002515', 'IL_001345', 'IL_010874', 'ZA_011313')
;
commit;

create table export_grd_2 as select * from export_grd;
select count(*) from export_grd_2; -- 8494

----------------- DELETE ----------------- 
delete from ps_task;
delete from ps_project;
delete from export_grd_backup;
delete from oleg;
delete from ps_diff_err;
delete from psnext_unique_project;
commit;
-- PRJ_ID/*GPMS_ID*/, GPMS_EXP_ID, ENV_ACTIVITY_ID
-- CHANGE REGIONS: 'EU', 'US', 'CA', 'IL', 'JP'
select t.* from export_grd t        where t.gpms_id = '000433' and t.region in('EU', 'US', 'CA', 'IL', 'JP') for update;
select t.* from export_grd_backup t where t.gpms_id = '000433' for update;
select count(*) from export_grd t;
select count(*) from export_grd_backup t;

update export_grd t set t.project_progress = 'Tentative Approval' -- instead of 'Submitted'
where  t.gpms_id = '000433'
 -- and  t.source_project_id is null
;
delete from export_grd t where t.gpms_id = '304878';
delete from export_grd_backup t where t.gpms_id = '304878';

-- CHANGE REGIONS: 'EMIA', 'ASIA', 'LATAM'
select t.* from export_grd t        where t.gpms_id = '004548' and t.region in('EMIA', 'ASIA', 'LATAM') and  t.activity_type <> 'R' || chr(38) || 'D' for update
;
select t.* from export_grd_backup t where t.gpms_id = '004548' for update;
update export_grd t 
set    t.strength_fill_volume = '60,80' -- instead of '50,70'
where  t.gpms_id = '004548'
  and  t.country = 'KZ'
;
delete from export_grd t      where t.gpms_id = '304125' and t.country = 'RU';
delete from export_grd_backup t where t.gpms_id = '304125';

select * from export_grd t      where t.gpms_id = '004062'
;
select t.gpms_id--, t.country
from   export_grd t
group by t.gpms_id--, t.country
having count(t.plw_pjt_id) > 1
;


-- 1. total in source (Stan) table
select count(*) from export_grd;  -- 12515

-- 5840 + 1550 = 7390
-- 1.a. cursor for REGION PROJECTS(EU, US, CA, IL, JP)
select count(1)
from   (
         select t.*
         from   export_grd t
         where  1 = 1
           and  (-- LIVE(CA), PRAM
                 (upper(t.region) in('EU', 
                                     'CA') and 
                  upper(t.wp_status) <> 'DRAFT')  or 
                  -- US
                  upper(t.region) = 'US'          or
                  -- IL
                  (upper(t.region)    = 'EMIA' and
                   t.country          = 'IL'   and
                   upper(t.wp_status) <> 'DRAFT') or
                  -- JP
                  (upper(t.region)    = 'ASIA' and
                   t.country          = 'JP'   and
                   upper(t.wp_status) <> 'DRAFT'))
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           and  (t.development_site           not in('BD', 'BD-E') or
                 nvl(t.sales_channel, '-999') not like '%OTC%')
       ) q
       ;
-- 1.b. cursor for PROJECTS level for COUNTRY PROJECTS(EMIA, Asia, LATAM) 
select count(1) -- 1550
from   (
         select distinct 
                t.gpms_id,
                null  plw_pjt_id,
                null  plw_int_number,
                t.gain,
                t.global_dosage_form,
                -- prj / tsl level ???
                --t.strength_fill_volume,
                --t.fill_volume_uom,
                --t.strength_uom,
                --
                t.project_rationale,
                --t.npv,
                decode(upper(t.region), 'ASIA', 'INT',
                                        'EMIA', 'INT',
                                        t.region) region,
                t.development_site
         from   export_grd t
         where  upper(t.region)    in('EMIA', 
                                      'ASIA', 
                                      'LATAM')
           and  t.country          not in ('JP', 'IL')
           and  upper(t.wp_status) <> 'DRAFT'
           and  t.activity_type    <> 'R' || chr(38) || 'D'
           and  t.gpms_id          is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           and  (t.development_site           not in('BD', 'BD-E') or
                 nvl(t.sales_channel, '-999') not like '%OTC%')
                    
         -- plw wps for midrated into PLW projects 
         union
         select distinct 
                null gpms_id,
                plw_pjt_id,
                plw_int_number,
                t.gain,
                t.global_dosage_form,
                -- prj / tsl level ???
                --t.strength_fill_volume,
                --t.fill_volume_uom,
                --t.strength_uom,
                --
                t.project_rationale,
                --t.npv,
                decode(upper(t.region), 'ASIA', 'INT',
                                        'EMIA', 'INT',
                                        t.region) region,
                t.development_site
         from   export_grd t
         where  upper(t.region)    in('EMIA', 
                                      'ASIA', 
                                      'LATAM')
           and  t.country          not in ('JP','IL')
           and  upper(t.wp_status) <> 'DRAFT'
           and  t.activity_type    <> 'R' || chr(38) || 'D'
           and  t.gpms_id          is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           and  (t.development_site           not in('BD', 'BD-E') or
                 nvl(t.sales_channel, '-999') not like '%OTC%')
           and  not exists (
                             select 1
                             from   export_grd_backup b
                             where  upper(b.region)    in('EMIA', 
                                                          'ASIA', 
                                                          'LATAM')
                               and  b.country          not in ('JP','IL')
                               and  upper(t.wp_status) <> 'DRAFT'
                               and  b.activity_type    <> 'R' || chr(38) || 'D'
                               and  b.gpms_id          = t.gpms_id
                               and  b.plw_pjt_id       = t.plw_pjt_id
                               and  b.plw_int_number   = t.plw_int_number
                               and  (b.development_site           not in('BD', 'BD-E') or
                                     nvl(b.sales_channel, '-999') not like '%OTC%')
                           )
           and  exists (select 1
                        from   export_grd_backup)
         union
         -- Projects that were created in PLW
         select distinct 
                null gpms_id,
                t.plw_pjt_id,
                t.plw_int_number,
                t.gain,
                t.global_dosage_form,
                -- prj / tsl level ???
                --t.strength_fill_volume,
                --t.fill_volume_uom,
                --t.strength_uom,
                --
                t.project_rationale,
                --t.npv,
                decode(upper(t.region), 'ASIA', 'INT',
                                        'EMIA', 'INT',
                                        t.region) region,
                t.development_site
         from   export_grd t
         where  upper(t.region)    in('EMIA', 
                                      'ASIA', 
                                      'LATAM')
           and  t.country          not in ('JP','IL')
           and  upper(t.wp_status) <> 'DRAFT'
           and  t.activity_type    <> 'R' || chr(38) || 'D'
           and  t.gpms_id          is null
           and  (t.development_site           not in('BD', 'BD-E') or
                 nvl(t.sales_channel, '-999') not like '%OTC%')
       ) q
;

-- 2. destination tables
-- 2.a. ps_project
select t.* from ps_project t where t.gpms_id = '004062' or t.plw_pjt_id = 'MX_008664';
select count(*) from ps_project t;                            -- 1-ST RUN: 2951 * 2 = 5902;   2-ND RUN: 2951 * 3 = 8853
select count(*) from ps_project t where t.action is null;     -- 1-ST RUN: 2951;              2-ND RUN: 2951 * 2 = 5902
select count(*) from ps_project t where t.action is not null; -- 2951
select *        from ps_project t where t.action is null;
select *        from ps_project t where t.action is not null; -- 
select *        from ps_project t where t.action = 'I';       -- 2951
select *        from ps_project t where t.action = 'N';       -- 
select *        from ps_project t where t.action = 'U';       -- 
select *        from ps_project t where t.action = 'E';       -- 

select distinct t.launching_site, t.env_activity_id from ps_project t where t.action is not null
;
select distinct t.launching_site from ps_task t where t.action is not null
;

-- 2.b. ps_task
select t.* from ps_task t where t.gpms_id = '004062' or t.plw_pjt_id = 'MX_008664';
select count(*) from ps_task t;                               -- 1-ST RUN: 1730 * 2 = 3460;   2-ND RUN: 1730 * 3 = 5190
select count(*) from ps_task t where t.action is null;        -- 1-ST RUN: 1730;              2-ND RUN: 1730 * 2 = 3460
select count(*) from ps_task t where t.action is not null;    -- 1730
select *        from ps_task t where t.action is null;
select *        from ps_task t where t.action is not null;    -- 1730
select *        from ps_task t where t.action = 'I';          -- 
select *        from ps_task t where t.action = 'N';          -- 
select *        from ps_task t where t.action = 'U';          -- 
select *        from ps_task t where t.action = 'E';          -- 

-- 3. backup table
select count(*) from export_grd_backup; -- 4183 

-- 4. sql statements
select * from oleg t;
delete from oleg; 

select count(1) 
--into   v_rec_count
from   ps_project_exp_vw t -- to change the view / view_name!!!
where  t.env_id      = 'GPMS' 
  and  t.gpms_exp_id = '135840'
;
  
select count(1)
from   ps_project_exp_vw t -- to change the view / view_name!!!
where  t.env_id      = 'GRD' 
  and  t.gpms_exp_id = '135840'
;
  
select t.* 
--into   v_rec_count
from   ps_project_exp_vw t
;
  
select max(gpms_exp_id)
--into   l_gpms_exp_id
from   ps_gpms_exp
