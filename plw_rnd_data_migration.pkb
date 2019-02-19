create or replace package body plw_rnd_data_migration is
/*
 $Id: plw_rnd_data_migration.pkb,v 1.30 2015/03/05 12:10:21 olando Exp $
 $Log: plw_rnd_data_migration.pkb,v $
 Revision 1.30  2015/03/05 12:10:21  olando
 Ticket #-999: On 26/02/2015 ten (10) fields added by Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGET...PIVOTAL_STUDY )

 Revision 1.29  2015/02/05 10:35:40  olando
 Ticket #-999:
  Fields STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT were added by Netalee on 03/02/2015 in the followinf procedures:
  create_grd_projects_tasks, create_backup_projects_tasks, update_export_grd_backup_tbl

 Revision 1.28  2015/01/22 11:58:55  olando
 Ticket #-999:
 Field field  WP_DUP_DESC was added by Nessia on 18/01/2015 in the followinf procedures:
 create_grd_projects_tasks, create_backup_projects_tasks, update_export_grd_backup_tbl
 Fields STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT were added by Netalee on 03/02/2015 in the followinf procedures:
 create_grd_projects_tasks, create_backup_projects_tasks, update_export_grd_backup_tbl

 Revision 1.27  2014/09/17 21:49:00  olando
 Ticket #-999
 1. procedures: report_updated_projects, report_updated_tasks are added.
 2. the call to report_updated_records is disabled in the procedure FETCH_MIGRATION_LOG_FILE.
 2 new procedures are called instead, to create separate delta logs for projects and tasks.

 Revision 1.26  2014/08/04 19:09:32  olando
 Ticket #-999:
 new fieldas are added in the following procedures:
 create_grd_projects_tasks, create_backup_projects_tasks, update_export_grd_backup_tbl

 Revision 1.25  2014/07/18 08:53:19  olando
 Ticket #-999:
 function create_export_grd_pure_tbl is added.

 Revision 1.24  2014/03/13 10:09:14  olando
 Ticket #-999
 1. The condition for the field allocation_status was removed from all the queries.
 2. The output.put_line error message print was removed in the procedure validate_null_values.

 Revision 1.23  2014/02/17 10:37:45  olando
 Ticket# -999:
 cvs ticket change

 Revision 1.22  2014/02/17 10:36:05  olando
 Ticket# -999:
 The conditions added to procedure update_export_grd_backup_actin cursors:
 1. nvl(ps_project.action, 'N') <> 'N' 2. nvl(ps_task.action, 'N') <> 'N'

 Revision 1.21  2014/02/17 07:36:57  olando
 Ticket# -999:
 Join by prj_id replaced by rowid in ps_project inner and outer query.

 Revision 1.20  2014/02/16 17:13:06  olando
 Ticket# -999:
 Countings for summary log files for all, il and us regions were changed.

 Revision 1.19  2014/02/12 17:38:01  olando
 Ticket #-999:
 Changes in queries of the procedure count_projects_tasks following the changes for Revision 1.17.

 Revision 1.18  2014/02/12 17:13:20  olando
 Ticket #-999:
 Changes in queries of the procedures update_export_grd_backup_actin, validate_null_values
 following the changes for Revision 1.17.

 Revision 1.17  2014/02/10 18:49:27  olando
 Ticket #-999:
 1. Query for region projects is splitted to 2 queries: for migrated projects, for created in plw projects and work_packages (for migrated projects) in procedures create_grd_projects_tasks, create_backup_projects_tasks.
 2. The field project_rationale was removed from 5 mandatory distinct fields for country projects in procedures: create_grd_projects_tasks, create_backup_projects_tasks, validate_null_values, count_projects_tasks.
 3. Error log (for multiple projects, tasks) created in the procedure create_grd_projects_tasks is added to be inserted into the table ps_log in the procedure print_log.

 Revision 1.16  2014/02/04 08:42:12  olando
 Ticket #-999:
 1. Quries of country projects - to create projects and tasks were changed in the procedures:
 create_grd_projects_tasks, update_export_grd_backup_actin (to resolve bug for projects that were migrated nad filtered out before,
 but now due to value changes are transfered. The same for WPs that were created in PLW for migrated projects, but previously
 were filtered out and now are not).
 2. Multiple projects, tasks error messages are added to summary log file.
 3. Started (but did not complete) to imrove countings in summary log in the procedures: count_delta_projects_tasks, print_log.

 Revision 1.15  2014/01/19 10:30:53  olando
 Ticket #-999:
 Some fixes

 Revision 1.14  2014/01/05 15:44:10  olando
 Ticket #-999:
 1. The borders proportion of the log file table was changed in the procedure print_log
 2. Log files older than 31 days are deleted (istead of 2 weeks) in the procedure print_log
 3. export_grd_backup table is saved for the last 31 days (instead of 2 weeks).

 Revision 1.13  2013/12/19 09:13:26  olando
 Ticket #-999:
 Change in print_log for Error and Warning messages: 1. either gpms_id or plw_pjt_id + plw_int_number are printed
 in the project level. 2. Projects/Tasks 30% -- Error Message 70% ratio was changed to 60%-40% ratio in log file printing.

 Revision 1.12  2013/12/03 08:57:55  olando
 Ticket #-999:
 Distinct is added in the procedure print_log in the following cursors: c_us_failed_task_messages, c_il_failed_task_messages.

 Revision 1.11  2013/11/17 13:42:30  olando
 Ticket #-999:
 1. Exception e_export_grd_empty is added in the procedure CREATE_DELTA_PROJECTS_TASKS to catch no_data in the table export_grd.
 2. Delete log files two weeks old - added in the procedure print_log.

 Revision 1.10  2013/11/11 12:59:40  olando
 Ticket #-999:
 Since GRANT CREATE ANY TABLE for the "Interfaces" was provided by DBS for dev db, the creation of
 backup tables for export_grd_backup is added in the procedure update_export_grd_backup_tbl.

 Revision 1.9  2013/11/11 12:03:04  olando
 Ticket #-999:
 Since GRANT CREATE ANY TABLE for the "Interfaces" was provided by DBS for dev db, the creation of
 backup tables for export_grd_backup is added in the procedure update_export_grd_backup_tbl.

 Revision 1.8  2013/11/10 15:16:42  olando
 Ticket #-999:
 Changes in the procedure update_export_grd_backup_tbl.

 Revision 1.7  2013/11/05 15:39:15  olando
 Ticket #-999:
 Insert of the field ps_task.product_status is added in the procedures: update_export_grd_backup_tbl, create_backup_projects_tasks, create_grd_projects_tasks.

 Revision 1.6  2013/11/04 14:50:10  olando
 Ticket #-999:
 solution for execute immediate l_project_report_program (clob type) in 10g db:
 dbms_sql.execute is used instead.

 Revision 1.5  2013/10/23 15:22:28  olando
 Ticket #-999:
 1. The table PSNEXT_UNIQUE_PROJECTS_BACKUP is added to all jointo the table export_grd_backup
     instead of the join to the table PSNEXT_UNIQUE_PROJECTS.
 2. Manipulation on the field action is added in the procedure create_failed_projects_tasks for cusrsors of failed in java
     projects and tasks.

 Revision 1.4  2013/10/17 11:16:06  olando
 Ticket# -999:
 Cleansing of blank rows.

 Revision 1.3  2013/10/16 06:54:02  olando
 Ticket# -999:
 The order of previous field value and current field value was changed in the delta log in the procedure report_updated_records.

 Revision 1.2  2013/10/15 12:31:30  olando
 Ticket# -999:
 1. Procedure report_updated_records is removed from spec.
 2. Out parameter p_log is removed from the procedures FETCH_MIGRATION_LOG_FILE, CREATE_DELTA_PROJECTS_TASKS
     since log files are saved in the table ps_log to be fetched by BPEL.
 3. Appropriate change to save log files and not tp use out and global parameters for log file.

 Revision 1.1  2013/10/10 17:05:21  olando
 Ticket# -999:
 The pck file was splitted into pks (spec) and pkb (body) files.

 Revision 1.42  2013/10/17 11:15:03  olando
 Ticket# -999:
 Cleansing of blank rows.

 Revision 1.41  2013/10/16 06:55:38  olando
 Ticket# -999:
 The order of previous field value and current field value was changed in the delta log in the procedure report_updated_records.

 Revision 1.40  2013/10/15 12:33:19  olando
 Ticket# -999:
 1. Procedure report_updated_records is removed from spec.
 2. Out parameter p_log is removed from the procedures FETCH_MIGRATION_LOG_FILE, CREATE_DELTA_PROJECTS_TASKS
     since log files are saved in the table ps_log to be fetched by BPEL.
 3. Appropriate change to save log files and not tp use out and global parameters for log file.

 Revision 1.39  2013/09/17 16:51:11  olando
 Ticket #-999
 1. The field product_status is added to populate ps_project, export_grd_backup for region projects only.

 Revision 1.38  2013/09/11 16:17:16  olando
 Ticket #-999
 Exceptions for the procedures print_log, report_updated_records are added.

 Revision 1.37  2013/09/11 16:02:32  olando
 Ticket #-999
 Table oleg was replaced by ps_log.
 Changes in the procedures: print_log, report_updated_records, validate_null_values.

 Revision 1.36  2013/09/10 16:54:04  olando
 Ticket #-999
 Cursors and html log for ended with error projects and tasks for IL and US sites are changed.

 Revision 1.35  2013/09/09 21:25:16  olando
 Ticket #-999
 Changes in html log file.

 Revision 1.34  2013/09/08 14:45:17  olando
 Ticket #-999
 The fields: PLFD_LATE, PLMD_MAIN, LMD_MAIN, LMD_EARLY were added.

 Revision 1.33  2013/09/07 15:34:57  olando
 Ticket #-999
 The changes in HTML log file in the procedures print_log, report_updated_records.

 Revision 1.32  2013/09/05 18:07:33  olando
 Ticket #-999
 The run of the procedure report_updated_records was splitted into sites parameters for all, il and us.

 Revision 1.31  2013/09/04 22:32:18  olando
 Ticket #-999
 Procedures report_updated_records, print_log are modified.

 Revision 1.30  2013/09/04 22:15:29  olando
 Ticket #-999
 Procedures report_updated_records, print_log are modified.

 Revision 1.29  2013/09/04 16:24:56  olando
 Ticket #-999
 Procedure report_updated_records is added.

 Revision 1.28  2013/08/20 16:24:36  olando
 Ticket #-999
 Filtering conditions fixes in the procedure count_migrated_projects_tasks following the changes
 in the procedure create_grd_projects_tasks

 Revision 1.27  2013/08/20 11:46:43  olando
 Ticket #-999
 1. Procedure update_export_grd_backup_actin is added to update export_grd_backup in case java code was not run in the previous
 interface run.
 2. Queries in the procedure create_failed_projects_tasks were modified to run over export_grd_backup in case java code was not run in the previous
 interface run in order to update action in the tables ps_project, ps_task.

 Revision 1.26  2013/08/20 09:07:27  olando
 Ticket #-999
 Conditions for creation projects and tasks were changed in the procedures:
 create_grd_projects_tasks, create_backup_projects_tasks, count_projects_tasks, validate_null_values, create_failed_projects_tasks

 Revision 1.25  2013/08/04 16:07:59  olando
 Ticket #-999
 1. p_site values (il, US) are replaced by (GRD-IL, GRD-US) in the procedure FETCH_MIGRATION_LOG_FILE.
 2. Print of projects / tasks ended with warning in Java Migration program is added in the procedures print_log, count_migrated_projects_tasks.

 Revision 1.24  2013/07/23 08:23:37  olando
 Ticket #-999:
 Some change in html of the log file

 Revision 1.23  2013/07/11 11:30:25  olando
 Ticket #-999:
 Small fix in log print.

 Revision 1.22  2013/07/10 10:09:11  olando
 Ticket #-999:
 Small fix in log print.

 Revision 1.21  2013/07/09 14:28:08  olando
 Ticket #-999:
 Print of projects / tasks error_messages into the file transfered by out parameter is added in the procedure print_log.

 Revision 1.20  2013/07/08 16:13:35  olando
 Ticket #-999:
 a. The program was splitted into two internal procedures:
 1. Procedure CREATE_DELTA_PROJECTS_TASKS instead of MAIN - to create delta data
 2. Procedure FETCH_MIGRATION_LOG_FILE to create log file after java migration program was run.
 b. Appropriate changes in the procedures: print_log, count_delta_projects_tasks, count_migrated_projects_tasks.

 Revision 1.19  2013/07/02 16:46:21  olando
 Ticket #-999
 The log is separated for IL and US development sites - changes in the procedures print_log  and count_commited_projects_tasks.

 Revision 1.18  2013/07/01 08:45:08  olando
 Ticket #-999
 The table PSNEXT_UNIQUE_PROJECTS is added in order to filter out blocked projects
 and tasks in the procedures create_grd_projects_tasks, create_backup_projects_tasks,
 validate_null_values, count_projects_tasks.

 Revision 1.17  2013/06/25 12:30:48  olando
 Ticket #-999
 The population of the fields: TAPI_AFW, SUBMISSION_A_P, APPROVAL_A_P, LAUNCH_A_P, VALUE_FOR_TEVA is added
 in the procedures create_grd_projects_tasks, create_backup_projects_tasks.

 Revision 1.16  2013/06/24 11:16:58  olando
 Ticket #-999
 Populating backup table export_grd_backup_backup_backup for the table export_grd_backup_backup is
 added in the procedure update_export_grd_backup_tbl.

 Revision 1.15  2013/06/23 16:09:18  olando
 Ticket #-999
 Adding logic to count failed projects and tasks in the procedure create_grd_projects_tasks.
 Adding exceptions in the procedure create_backup_projects_tasks.

 Revision 1.14  2013/06/20 15:00:38  olando
 Ticket #-999
 The conditions and development_site not in('BD', 'BD-E') or nvl(sales_channel, '-999') not like '%OTC%' are added.

 Revision 1.13  2013/06/13 15:41:02  olando
 Ticket #-999
 The condition wp_status <> 'DRAFT' is added.

 Revision 1.12  2013/06/11 10:12:20  olando
 Ticket #-999
 Initial Revision

 Revision 1.11  2013/05/27 11:29:50  olando
 Ticket #-999
 Projects for SEE countries with length(gpms_id) > 6 are filtered out in the procedures:
 create_grd_projects_tasks, create_backup_projects_tasks.

 Revision 1.10  2013/05/27 09:56:54  olando
 Ticket #-999
 The procedure create_failed_projects_tasks is added to update status from N to I/U/E to
 failed in bpel projects/tasks from the previous run.

 Revision 1.9  2013/05/09 17:46:14  olando
 Ticket #-999
 Initial Revision.

 Revision 1.8  2013/05/09 17:44:02  olando
 Ticket #-999
 Log print was added.

 Revision 1.7  2013/05/02 13:14:25  olando
 Ticket #-999
 Exceptions and error print in the procedure write_diff_err are added.

 Revision 1.6  2013/05/01 17:02:37  olando
 Ticket #-999
 The procedure update_country_projects_status is added.

 Revision 1.5  2013/04/30 16:29:00  olando
 Ticket #-999
 Changes for Tasks Migration

 Revision 1.4  2013/04/14 15:52:41  olando
 Ticket #-999:
 Initial Revision.

 Revision 1.3  2013/03/20 17:12:14  olando
 Ticket# -999:
 Initial Revision

 Revision 1.2  2013/01/24 13:20:14  olando
 Ticket #-999:
 Initial Revision.

 Revision 1.1  2013/01/14 12:30:33  olando
 Ticket #-999:
 Initial Revision

*/

   C_SUCCESS                      constant varchar2(1) := '0';
   C_WARNING                      constant varchar2(1) := '1';
   C_ERROR                        constant varchar2(1) := '2';

   e_wrong_interface_site_code    exception;
   e_delete_ps_tables             exception;
   e_create_grd_projects_tasks    exception;
   e_create_backup_projects_tasks exception;
   e_ps_manipulation              exception;
   e_delete_diff_data             exception;
   e_get_env_activity_id          exception;
   e_count_exp_records            exception;
   e_write_diff_err               exception;
   e_diff_level                   exception;
   e_UpdateLevelStatus            exception;
   e_update_country_projct_status exception;
   e_update_export_grd_backup_tbl exception;
   e_validate_null_values         exception;
   e_count_projects_tasks         exception;
   e_create_failed_projects_tasks exception;
   e_count_migrated_projcts_tasks exception;
   e_count_delta_projects_tasks   exception;
   e_report_updated_records       exception;
   e_report_updated_projects      exception;
   e_report_updated_tasks         exception;
   e_print_log                    exception;
   e_gpms_exp_id_not_found        exception;
   e_gpms_exp_id_multiple_rows    exception;
   e_generate_gpms_exp_id         exception;
   e_gpms_exp_id_not_found2       exception;
   e_gpms_exp_id_multiple_rows2   exception;
   e_fetch_migration_log_file     exception;
   e_export_grd_empty             exception;
   e_create_export_grd_pure_tbl   exception;

   g_errbuf                       clob;
   g_retcode                      clob;
   g_sql_code                     number;
   g_interface_status             varchar2(13) := 'SUCCESS';

   g_failed_projects_list         clob;
   g_failed_tasks_list            clob;

   ------------------------------------------------
   -- count_migrated_projects_tasks
   ------------------------------------------------
   procedure count_migrated_projects_tasks(p_dev_site_region         in  varchar2,
                                           -- projects
                                           x_total_projects_succs    out number,
                                           x_total_error_projects    out number,
                                           x_total_warning_projects  out number,
                                           x_total_projects_n        out number,
                                           x_total_projects_u        out number,
                                           x_total_projects_delta_u  out number,
                                           x_total_projects_i        out number,
                                           x_total_projects_e        out number,
                                           -- tasks
                                           x_total_tasks_succs       out number,
                                           x_total_error_tasks       out number,
                                           x_total_warning_tasks     out number,
                                           x_total_tasks_n           out number,
                                           x_total_tasks_u           out number,
                                           x_total_tasks_delta_u     out number,
                                           x_total_tasks_i           out number,
                                           x_total_tasks_e           out number) is

      -- Total projects
      l_region_il_projects          number;
      l_country_il_projects         number;
      l_total_il_projects_succs     number;

      l_region_us_projects          number;
      l_country_us_projects         number;
      l_total_us_projects_succs     number;

      -- N projects
      l_region_il_projects_n        number;
      l_country_il_projects_n       number;
      l_total_il_projects_n         number;

      l_region_us_projects_n        number;
      l_country_us_projects_n       number;
      l_total_us_projects_n         number;

      -- U projects
      l_region_il_projects_u        number;
      l_country_il_projects_u       number;
      l_total_il_projects_u         number;

      l_region_us_projects_u        number;
      l_country_us_projects_u       number;
      l_total_us_projects_u         number;

      -- Delta U projects
      l_region_il_delta_projects_u  number;
      l_country_il_delta_projects_u number;
      l_total_il_delta_projects_u   number;

      l_region_us_delta_projects_u  number;
      l_country_us_delta_projects_u number;
      l_total_us_delta_projects_u   number;

      -- I projects
      l_region_il_projects_i        number;
      l_country_il_projects_i       number;
      l_total_il_projects_i         number;

      l_region_us_projects_i        number;
      l_country_us_projects_i       number;
      l_total_us_projects_i         number;

      -- E projects
      --l_region_il_projects_e        number;
      --l_country_il_projects_e       number;
      l_total_il_projects_e         number;

      l_region_us_projects_e        number;
      l_country_us_projects_e       number;
      l_total_us_projects_e         number;

      -- Total tasks
      l_total_il_tasks              number;
      l_total_us_tasks              number;

      -- N tasks
      l_total_il_tasks_n            number;
      l_total_us_tasks_n            number;

      -- U tasks
      l_total_il_tasks_u            number;
      l_total_us_tasks_u            number;

      -- Delta U tasks
      l_total_il_delta_tasks_u      number;
      l_total_us_delta_tasks_u      number;

      -- I tasks
      l_total_il_tasks_i            number;
      l_total_us_tasks_i            number;

      -- E tasks
      l_total_il_tasks_e            number;
      l_total_us_tasks_e            number;

   begin

      /********************* PROJECTS *********************/

      ----------------------- TOTAL -----------------------
      -- count region projects (EU, IL, JP) for IL dev_site
      select count(1)
      into   l_region_il_projects
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  length(nvl(rtrim(ltrim(b.gpms_id)),
                        '-99999'))         = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         not in('E', 'W')
        and  (-- LIVE(CA), PRAM
              (upper(b.region)     in('EU',
                                      'CA') /*and
               upper(b.wp_status)  <> 'DRAFT'*/)  or
               -- US
               upper(b.region)     = 'US'       or
               -- IL
               (upper(b.region)    = 'EMIA' and
                b.country          = 'IL'   /*and
                upper(b.wp_status) <> 'DRAFT'*/)  or
               -- JP
               (upper(b.region)    = 'ASIA' and
                b.country          = 'JP'   /*and
                upper(b.wp_status) <> 'DRAFT'*/))
        /*and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*/
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
       ;

      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_projects
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         not in('E', 'W')
                 --
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  upper(b.wp_status)            <> 'DRAFT'
                 and  (b.development_site           not in('BD', 'BD-E') or
                       nvl(b.sales_channel, '-999') not like '%OTC%')*/
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
            )
      ;

      l_total_il_projects_succs := l_region_il_projects + l_country_il_projects;


      -- count region projects (US, CA) for US dev_site
      select count(1)
      into   l_region_us_projects
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         not in('E', 'W')
        and  (-- LIVE(CA), PRAM
              (upper(b.region)     in('EU',
                                      'CA') /*and
               upper(b.wp_status)  <> 'DRAFT'*/)  or
               -- US
               upper(b.region)     = 'US'       or
               -- IL
               (upper(b.region)    = 'EMIA' and
                b.country          = 'IL'   /*and
                upper(b.wp_status) <> 'DRAFT'*/)  or
               -- JP
               (upper(b.region)    = 'ASIA' and
                b.country          = 'JP'   /*and
                upper(b.wp_status) <> 'DRAFT'*/))
        /*and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null*/
        /*and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*/
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA','BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
       ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_projects
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         not in('E', 'W')
                 --
                 /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null*/
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 /*and  upper(b.wp_status)            <> 'DRAFT'
                 and  (b.development_site           not in('BD', 'BD-E') or
                       nvl(b.sales_channel, '-999') not like '%OTC%')*/
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
             )
      ;

      l_total_us_projects_succs := l_region_us_projects + l_country_us_projects;


      ----------------------- FAILED (Error / Warning) -----------------------
      if(p_dev_site_region = 'GRD-IL') then

         select sum(q.il_failed_projects) total_il_failed_projects
         into   x_total_error_projects
         from   (
                  select count(1) il_failed_projects
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1

                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.status                      = 'E'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = IL --
                    and  ((p.region           in('EU', 'IL', 'JP') and
                           p.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           p.development_site in('BD', 'NA'))                      or
                          (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
                  select count(1)
                  from   ( select distinct
                                  p.rowid,   -- AAAg4vAAIAAG2cZAAY
                                  b.action   -- I
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.status                      = 'E'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             --
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null
                             and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*/
                             -- dev_site = IL --
                             and  ((p.region           in('EU', 'IL', 'JP') and
                                    p.development_site in('BD', 'NA'))                      or
                                   -- for country projects (INT, LATAM) region is not transfered
                                   (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                                    p.development_site in('BD', 'NA'))                      or
                                   (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                          'ZR','ZG','BN','KR','UL','GO','ML',
                                                          'MU','TYK','UL-CoDev','RM','NAG')))
                        )
                ) q
         ;

         select sum(q.il_failed_projects) total_il_failed_projects
         into   x_total_warning_projects
         from   (
                  select count(1) il_failed_projects
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.status                      = 'W'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = IL --
                    and  ((p.region           in('EU', 'IL', 'JP') and
                           p.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           p.development_site in('BD', 'NA'))                      or
                          (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
                  select count(1)
                  from   ( select distinct
                                  p.rowid,   -- AAAg4vAAIAAG2cZAAY
                                  b.action   -- I
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.status                      = 'W'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             --
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null
                             and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*/
                             -- dev_site = IL --
                             and  ((p.region           in('EU', 'IL', 'JP') and
                                    p.development_site in('BD', 'NA'))                      or
                                   -- for country projects (INT, LATAM) region is not transfered
                                   (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                                    p.development_site in('BD', 'NA'))                      or
                                   (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                          'ZR','ZG','BN','KR','UL','GO','ML',
                                                          'MU','TYK','UL-CoDev','RM','NAG')))
                        )
                ) q
         ;

      elsif(p_dev_site_region = 'GRD-US') then

         select sum(q.us_failed_projects) total_us_failed_projects
         into   x_total_error_projects
         from   (
                  select count(1) us_failed_projects
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.status                      = 'E'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null*/
                    /*and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = US --
                    and  ((p.region           in('US', 'CA') and
                           p.development_site in('BD', 'NA'))             or
                          (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for US dev_site
                  select count(1)
                  from   ( select distinct
                                  p.rowid,
                                  b.action
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.status                      = 'E'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             --
                             /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null*/
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             /*and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*/
                             -- dev_site = US --
                             and  ((p.region           in('US', 'CA') and
                                    p.development_site in('BD', 'NA'))             or
                                   (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                          'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                          'AR','CL','VE','PE','KW','NVD')))
                         )
                ) q
         ;

         select sum(q.us_failed_projects) total_us_failed_projects
         into   x_total_warning_projects
         from   (
                  select count(1) us_failed_projects
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.status                      = 'W'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null*/
                    /*and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = US --
                    and  ((p.region           in('US', 'CA') and
                           p.development_site in('BD', 'NA'))             or
                          (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for US dev_site
                  select count(1)
                  from   ( select distinct
                                  p.rowid,
                                  b.action
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.status                      = 'W'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             --
                             /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null*/
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             /*and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*/
                             -- dev_site = US --
                             and  ((p.region           in('US', 'CA') and
                                    p.development_site in('BD', 'NA'))             or
                                   (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                          'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                          'AR','CL','VE','PE','KW','NVD')))
                         )
                ) q
         ;

      end if;

      ----------------------- N -----------------------
      -- count region projects (EU, IL, JP) for IL dev_site
      select count(1)
      into   l_region_il_projects_n
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'N'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))

      ;

      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_projects_n
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'N'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
             )
      ;

      l_total_il_projects_n := l_region_il_projects_n + l_country_il_projects_n;


      -- count region projects (US, CA) for US dev_site
      select count(1)
      into   l_region_us_projects_n
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'N'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
       ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_projects_n
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'N'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
             )
        ;

      l_total_us_projects_n := l_region_us_projects_n + l_country_us_projects_n;

      ----------------------- U -----------------------
      -- count region projects (EU, IL, JP) for IL dev_site
      select count(1)
      into   l_region_il_projects_u
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'U'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_projects_u
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'U'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
             )
      ;

      l_total_il_projects_u := l_region_il_projects_u + l_country_il_projects_u;


      -- count region projects (US, CA) for US dev_site
      select count(1)
      into   l_region_us_projects_u
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'U'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_projects_u
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'U'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
             )
      ;

      l_total_us_projects_u := l_region_us_projects_u + l_country_us_projects_u;


      ----------------------- Delta U -----------------------
      -- count region projects (EU, IL, JP) for IL dev_site
      select count(1)
      into   l_region_il_delta_projects_u
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'U'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
        -- filter out projects updated due to tasks and projects from yesterday run
        and  not exists (select 1
                         from   ps_project pp,
                                ps_task    tt
                         where  pp.prj_id          = tt.prj_id
                           and  pp.env_activity_id = tt.env_activity_id
                           and  pp.env_activity_id = 32
                           and  tt.action         in('I', 'U', 'E')
                           --
                           and  pp.rowid           = p.rowid
                        )
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (p.prj_id = bb.gpms_id or
                                   p.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_delta_projects_u
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'U'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
                 -- filter out projects updated due to tasks and projects from yesterday run
                 and  not exists (select 1
                                  from   ps_project pp,
                                         ps_task    tt
                                  where  pp.prj_id          = tt.prj_id
                                    and  pp.env_activity_id = tt.env_activity_id
                                    and  pp.env_activity_id = 32
                                    and  tt.action         in('I', 'U', 'E')
                                    --
                                    and  pp.rowid           = p.rowid
                                 )
                 and  ((not exists (select 1
                                    from   export_grd_backup_backup bb
                                    where  bb.status in('J', 'E')
                                      and  (p.prj_id = bb.gpms_id or
                                            p.prj_id = bb.plw_int_number)
                                   ) and
                            exists (select 1 from export_grd_backup_backup)
                       ) or
                       not exists (select 1 from export_grd_backup_backup)
                      )
             )
      ;

      l_total_il_delta_projects_u  := l_region_il_delta_projects_u + l_country_il_delta_projects_u;


      -- count region projects (US, CA) for US dev_site
      select count(1)
      into   l_region_us_delta_projects_u
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'U'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- filter out projects updated due to tasks and projects from yesterday run
        and  not exists (select 1
                         from   ps_project pp,
                                ps_task    tt
                         where  pp.prj_id          = tt.prj_id
                           and  pp.env_activity_id = tt.env_activity_id
                           and  pp.env_activity_id = 32
                           and  tt.action          in('I', 'U', 'E')
                           --
                           and  pp.rowid           = p.rowid
                        )
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (p.prj_id = bb.gpms_id or
                                   p.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_delta_projects_u
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'U'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
                 -- filter out projects updated due to tasks and projects from yesterday run
                 and  not exists (select 1
                                  from   ps_project pp,
                                         ps_task    tt
                                  where  pp.prj_id          = tt.prj_id
                                    and  pp.env_activity_id = tt.env_activity_id
                                    and  pp.env_activity_id = 32
                                    and  tt.action          in('I', 'U', 'E')
                                    --
                                    and  pp.rowid           = p.rowid
                                 )
                 and  ((not exists (select 1
                                    from   export_grd_backup_backup bb
                                    where  bb.status in('J', 'E')
                                      and  (p.prj_id = bb.gpms_id or
                                            p.prj_id = bb.plw_int_number)
                                   ) and
                            exists (select 1 from export_grd_backup_backup)
                       ) or
                       not exists (select 1 from export_grd_backup_backup)
                      )
             )
      ;

      l_total_us_delta_projects_u := l_region_us_delta_projects_u + l_country_us_delta_projects_u;

      ----------------------- I -----------------------
      -- count region projects (EU, IL, JP) for IL dev_site
      select count(1)
      into   l_region_il_projects_i
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'I'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_projects_i
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'I'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
             )
      ;

      l_total_il_projects_i := l_region_il_projects_i + l_country_il_projects_i;


      -- count region projects (US, CA) for US dev_site
      select count(1)
      into   l_region_us_projects_i
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'I'
        --
        and  (-- EU
              (upper(b.region)      = 'EU'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'       and
                (b.development_site not in('BD', 'BD-E') or
                 b.sales_channel    like '%OTC%')))
               or -- CA
              (upper(b.region)      = 'CA'                  and
               upper(b.wp_status)   <> 'DRAFT'              and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- US
              (upper(b.region)      = 'US'                  and
               (b.development_site  not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                b.development_site  not like '%BioG%'))
               or -- IL
               (upper(b.region)     = 'EMIA'                and
                b.country           = 'IL'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site  not like '%BioG%'))
               or -- JP
               (upper(b.region)     = 'ASIA'                and
                b.country           = 'JP'                  and
                upper(b.wp_status)  <> 'DRAFT'              and
                (b.development_site not in('BDIR',
                                           'BD-L',
                                           'BN',
                                           'RDI')           and
                 b.development_site not like '%BioG%'))
             )
        /*and  (upper(b.allocation_status) = 'APPROVED'  or
              (b.project_status          = 'Draft' and
               nvl(b.global_nte, '0')    = '1'))*/
        and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))

       ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_projects_i
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 and  b.activity_type               <> 'R' || chr(38) || 'D'
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'I'
                 --
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 and  upper(b.wp_status)            <> 'DRAFT'
                 --
                 and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  b.development_site            not like '%BioG%'
                 /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                       (b.project_status            = 'Draft' and
                        nvl(b.global_nte, '0')      = '1'))*/
                 and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
             )
      ;

      l_total_us_projects_i := l_region_us_projects_i + l_country_us_projects_i;

      ----------------------- E -----------------------
      -- Action E means the projects / tasks do not exist in export_grd,
      -- but since export_grd_backup was overwritten by export_grd
      -- there are no records anymore in export_grd_backup to be updated by java interface

      -- count region projects (EU, IL, JP) for IL dev_site
      /*select count(1)
      into   l_region_il_projects_e
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'E'
        -- these conditions may be the reason for not created action = 10 records and for created delta 'E' records
        and  (-- LIVE(CA), PRAM
              (upper(b.region)     in('EU',
                                      'CA') \*and
               upper(b.wp_status)  <> 'DRAFT'*\)  or
               -- US
               upper(b.region)     = 'US'       or
               -- IL
               (upper(b.region)    = 'EMIA' and
                b.country          = 'IL'   \*and
                upper(b.wp_status) <> 'DRAFT'*\)  or
               -- JP
               (upper(b.region)    = 'ASIA' and
                b.country          = 'JP'   \*and
                upper(b.wp_status) <> 'DRAFT'*\))
        \*and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = IL --
        and  ((p.region           in('EU', 'IL', 'JP') and
               p.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               p.development_site in('BD', 'NA'))                      or
              (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;



      -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
      select count(1)
      into   l_country_il_projects_e
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'E'
                 --
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 -- these conditions may be the reason for not created action = 10 records and for created delta 'E' records
                 \*and  b.activity_type               <> 'R' || chr(38) || 'D'
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  upper(b.wp_status)            <> 'DRAFT'
                 and  (b.development_site           not in('BD', 'BD-E') or
                       nvl(b.sales_channel, '-999') not like '%OTC%')*\
                 -- dev_site = IL --
                 and  ((p.region           in('EU', 'IL', 'JP') and
                        p.development_site in('BD', 'NA'))                      or
                       -- for country projects (INT, LATAM) region is not transfered
                       (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                        p.development_site in('BD', 'NA'))                      or
                       (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                              'ZR','ZG','BN','KR','UL','GO','ML',
                                              'MU','TYK','UL-CoDev','RM','NAG')))
             )
      ;

      l_total_il_projects_e := l_region_il_projects_e + l_country_il_projects_e;*/

      select count(*)
      into   l_total_il_projects_e
      from   ps_project t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.gain             is not null
        -- for country projects (INT, LATAM) region / country are not transfered
        --and  t.region           is not null
        --and  t.country          is not null
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;


      -- count region projects (US, CA) for US dev_site
      /*select count(1)
      into   l_region_us_projects_e
      from   ps_project        p,
             export_grd_backup b
      where  1 = 1
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- join
        --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --  java failed records
        and  p.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  p.action                      = 'E'
        -- these conditions may be the reason for not created action = 10 records and for created delta 'E' records
        and  (-- LIVE(CA), PRAM
              (upper(b.region)     in('EU',
                                      'CA') \*and
               upper(b.wp_status)  <> 'DRAFT'*\)  or
               -- US
               upper(b.region)     = 'US'       or
               -- IL
               (upper(b.region)    = 'EMIA' and
                b.country          = 'IL'   \*and
                upper(b.wp_status) <> 'DRAFT'*\)  or
               -- JP
               (upper(b.region)    = 'ASIA' and
                b.country          = 'JP'   \*and
                upper(b.wp_status) <> 'DRAFT'*\))
        \*and  b.gain                is not null
        and  b.development_site    is not null
        and  b.region              is not null
        and  b.country             is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = US --
        and  ((p.region           in('US', 'CA') and
               p.development_site in('BD', 'NA'))             or
              (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
       ;

      -- count country projects (EMIA, ASIA, LATAM) for US dev_site
      select count(1)
      into   l_country_us_projects_e
      from   ( select distinct
                      p.rowid,
                      b.action
               from   ps_project         p,
                      ps_task            t,
                      export_grd_backup  b
               where  1 = 1
                 and  upper(b.region)               in('EMIA',
                                                       'ASIA',
                                                       'LATAM')
                 and  b.country                     not in('JP','IL')
                 --
                 and  p.project_id                  = t.project_id
                 --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                 and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                 and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                 --
                 and  p.env_activity_id             = '32'
                 and  nvl(b.status, '-999')         <> 'E'
                 and  p.action                      = 'E'
                 --
                 and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 -- these conditions may be the reason for not created action = 10 records and for created delta 'E' records
                 \*and  b.activity_type               <> 'R' || chr(38) || 'D'
                 and  b.gain                        is not null
                 and  b.development_site            is not null
                 and  b.region                      is not null
                 and  b.country                     is not null
                 and  upper(b.wp_status)            <> 'DRAFT'
                 and  (b.development_site           not in('BD', 'BD-E') or
                       nvl(b.sales_channel, '-999') not like '%OTC%')*\
                 -- dev_site = US --
                 and  ((p.region           in('US', 'CA') and
                        p.development_site in('BD', 'NA'))             or
                       (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                              'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                              'AR','CL','VE','PE','KW','NVD')))
             )
      ;

      l_total_us_projects_e := l_region_us_projects_e + l_country_us_projects_e;*/

      select count(*)
      into   l_total_us_projects_e
      from   ps_project t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.gain             is not null
        -- for country projects (INT, LATAM) region is not transfered
        --and  t.region           is not null
        --and  t.country          is not null
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP',
                                     'BDIR','PO','AR','CL','VE','PE','KW','NVD')))
      ;

      /********************** TASKS **********************/
      ----------------------- FAILED -----------------------
      if(p_dev_site_region = 'GRD-IL') then

         select count(1)
         into   x_total_error_tasks
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = IL --
           and  ((t.region           in('EU', 'IL', 'JP') and
                  t.development_site in('BD', 'NA'))                      or
                 -- for country projects (INT, LATAM) region is not transfered
                 (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                  t.development_site in('BD', 'NA'))                      or
                 (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                        'ZR','ZG','BN','KR','UL','GO','ML',
                                        'MU','TYK','UL-CoDev','RM','NAG')))
         ;

         select count(1)
         into   x_total_warning_tasks
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.status                      = 'W'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = IL --
           and  ((t.region           in('EU', 'IL', 'JP') and
                  t.development_site in('BD', 'NA'))                      or
                 -- for country projects (INT, LATAM) region is not transfered
                 (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                  t.development_site in('BD', 'NA'))                      or
                 (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                        'ZR','ZG','BN','KR','UL','GO','ML',
                                        'MU','TYK','UL-CoDev','RM','NAG')))
         ;

      elsif(p_dev_site_region = 'GRD-US') then

         select count(1)
         into   x_total_error_tasks
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = US --
           and  ((t.region           in('US', 'CA') and
                  t.development_site in('BD', 'NA'))             or
                 (t.development_site in('SV', 'NV', 'TO', 'IR', 'OP', 'MC', 'MB',
                                        'MI', 'BDPO', 'BDJP', 'BDAM', 'BDCA', 'BP', 'BDIR', 'PO',
                                        'AR', 'CL', 'VE', 'PE', 'KW', 'NVD')))
         ;

         select count(1)
         into   x_total_warning_tasks
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.status                      = 'W'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = US --
           and  ((t.region           in('US', 'CA') and
                  t.development_site in('BD', 'NA'))             or
                 (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                        'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                        'AR','CL','VE','PE','KW','NVD')))
         ;

      end if;

      ----------------------- N -----------------------
      select count(1)
      into   l_total_il_tasks_n
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'N'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(1)
      into   l_total_us_tasks_n
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'N'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      ----------------------- U -----------------------
      select count(1)
      into   l_total_il_tasks_u
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'U'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(1)
      into   l_total_us_tasks_u
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'U'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      ----------------------- Delta U -----------------------
      select count(1)
      into   l_total_il_delta_tasks_u
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'U'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
        -- filter out tasks from yesterday run
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      select count(1)
      into   l_total_us_delta_tasks_u
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'U'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- filter out tasks from yesterday run
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      ----------------------- I -----------------------
      select count(1)
      into   l_total_il_tasks_i
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'I'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(1)
      into   l_total_us_tasks_i
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'I'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        --
        and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
        and  b.development_site            not like '%BioG%'
        /*and  (upper(b.allocation_status)   = 'APPROVED'  or
              (b.project_status            = 'Draft' and
               nvl(b.global_nte, '0')      = '1'))*/
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      ----------------------- E -----------------------
      -- Action E means the projects / tasks do not exist in export_grd,
      -- but since export_grd_backup was overwritten by export_grd
      -- there are no records anymore in export_grd_backup to be updated by java interface
      /*select count(1)
      into   l_total_il_tasks_e
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'E'
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        \*and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;*/

      select count(*)
      into   l_total_il_tasks_e
      from   ps_task t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.region           is not null
        and  t.country          is not null
        --
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      /*select count(1)
      into   l_total_us_tasks_e
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         <> 'E'
        and  t.action                      = 'E'
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        \*and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null*\
        \*and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;*/

      select count(*)
      into   l_total_us_tasks_e
      from   ps_task t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.region           is not null
        and  t.country          is not null
        --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      ----------------------- TOTAL -----------------------
      -- count tasks for IL dev_site
      /*select count(1)
      into   l_total_il_tasks
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         not in('E', 'W')
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
        \*and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = IL --
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;*/

      l_total_il_tasks := l_total_il_tasks_i + l_total_il_tasks_u + l_total_il_tasks_n;

      /*select count(1)
      into   l_total_us_tasks
      from   ps_task           t,
             export_grd_backup b
      where  1 = 1
        and  upper(b.region)               in('EMIA',
                                              'ASIA',
                                              'LATAM')
        and  b.country                     not in ('JP','IL')
        -- join
        --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
        and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
        and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
        --
        and  t.env_activity_id             = '32'
        and  nvl(b.status, '-999')         not in('E', 'W')
        and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
        -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
        \*and  upper(b.wp_status)            <> 'DRAFT'
        and  b.activity_type               <> 'R' || chr(38) || 'D'
        and  b.gain                        is not null
        and  b.development_site            is not null
        and  b.region                      is not null
        and  b.country                     is not null
        and  (b.development_site           not in('BD', 'BD-E') or
              nvl(b.sales_channel, '-999') not like '%OTC%')*\
        -- dev_site = US --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;*/

      l_total_us_tasks := l_total_us_tasks_i + l_total_us_tasks_u + l_total_us_tasks_n;

      if(p_dev_site_region = 'GRD-IL') then

         x_total_projects_succs    := l_total_il_projects_succs + x_total_warning_projects;
         x_total_projects_n        := l_total_il_projects_n;
         x_total_projects_u        := l_total_il_projects_u;
         x_total_projects_delta_u  := l_total_il_delta_projects_u;
         x_total_projects_i        := l_total_il_projects_i;
         x_total_projects_e        := l_total_il_projects_e;

         x_total_tasks_succs       := l_total_il_tasks          + x_total_warning_tasks;
         x_total_tasks_n           := l_total_il_tasks_n;
         x_total_tasks_u           := l_total_il_tasks_u;
         x_total_tasks_delta_u     := l_total_il_delta_tasks_u ;
         x_total_tasks_i           := l_total_il_tasks_i;
         x_total_tasks_e           := l_total_il_tasks_e;

      elsif(p_dev_site_region = 'GRD-US') then

         x_total_projects_succs    := l_total_us_projects_succs + x_total_warning_projects;
         x_total_projects_n        := l_total_us_projects_n;
         x_total_projects_u        := l_total_us_projects_u;
         x_total_projects_delta_u  := l_total_us_delta_projects_u;
         x_total_projects_i        := l_total_us_projects_i;
         x_total_projects_e        := l_total_us_projects_e;

         x_total_tasks_succs       := l_total_us_tasks          + x_total_warning_tasks;
         x_total_tasks_n           := l_total_us_tasks_n;
         x_total_tasks_u           := l_total_us_tasks_u;
         x_total_tasks_delta_u     := l_total_us_delta_tasks_u ;
         x_total_tasks_i           := l_total_us_tasks_i;
         x_total_tasks_e           := l_total_us_tasks_e;

      end if;

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_count_migrated_projcts_tasks;

   end count_migrated_projects_tasks;

   ------------------------------------------------
   -- count_delta_projects_tasks
   ------------------------------------------------
   procedure count_delta_projects_tasks(-- PROJECTS
                                        x_projects_i                  out number,
                                        x_projects_il_i               out number,
                                        x_projects_us_i               out number,
                                        --
                                        x_projects_n                  out number,
                                        x_projects_il_n               out number,
                                        x_projects_us_n               out number,
                                        --
                                        x_projects_u                  out number,
                                        x_projects_il_u               out number,
                                        x_projects_us_u               out number,
                                        --
                                        x_delta_projects_u            out number,
                                        x_delta_projects_il_u         out number,
                                        x_delta_projects_us_u         out number,
                                        --
                                        x_projects_e                  out number,
                                        x_projects_il_e               out number,
                                        x_projects_us_e               out number,
                                        --
                                        x_projects_total              out number,
                                        x_projects_il_total           out number,
                                        x_projects_us_total           out number,
                                        --
                                        x_other_site_projects         out number,
                                        -- TASKS
                                        x_tasks_i                     out number,
                                        x_tasks_il_i                  out number,
                                        x_tasks_us_i                  out number,
                                        --
                                        x_tasks_n                     out number,
                                        x_tasks_il_n                  out number,
                                        x_tasks_us_n                  out number,
                                        --
                                        x_tasks_u                     out number,
                                        x_tasks_il_u                  out number,
                                        x_tasks_us_u                  out number,
                                        --
                                        x_delta_tasks_u               out number,
                                        x_delta_tasks_il_u            out number,
                                        x_delta_tasks_us_u            out number,
                                        --
                                        x_tasks_e                     out number,
                                        x_tasks_il_e                  out number,
                                        x_tasks_us_e                  out number,
                                        --
                                        x_tasks_total                 out number,
                                        x_tasks_il_total              out number,
                                        x_tasks_us_total              out number,
                                        --
                                        x_other_site_tasks            out number) is


   begin

      -- PS_PROJECT --
      -- I
      select count(*)
      into   x_projects_i
      from   ps_project t
      where  t.action = 'I'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_projects_il_i
      from   ps_project t
      where  t.action = 'I'
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_projects_us_i
      from   ps_project t
      where  t.action   = 'I'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      -- N
      select count(*)
      into   x_projects_n
      from   ps_project t
      where  t.action = 'N'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_projects_il_n
      from   ps_project t
      where  t.action = 'N'
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_projects_us_n
      from   ps_project t
      where  t.action   = 'N'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- U
      select count(*)
      into   x_projects_u
      from   ps_project t
      where  t.action = 'U'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_projects_il_u
      from   ps_project t
      where  t.action = 'U'
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_projects_us_u
      from   ps_project t
      where  t.action   = 'U'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- U without projects updated due to tasks
      select count(*)
      into   x_delta_projects_u
      from   ps_project t
      where  t.action = 'U'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- filter out projects updated due to tasks and projects from yesterday run
        and  not exists (select 1
                         from   ps_project pp,
                                ps_task    tt
                         where  pp.prj_id          = tt.prj_id
                           and  pp.env_activity_id = tt.env_activity_id
                           and  pp.env_activity_id = 32
                           and  tt.action          in('I', 'U', 'E')
                           --
                           and  pp.rowid           = t.rowid
                        )
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)

                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      select count(*)
      into   x_delta_projects_il_u
      from   ps_project t
      where  t.action = 'U'
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
        -- filter out projects updated due to tasks and projects from yesterday run
        and  not exists (select 1
                         from   ps_project pp,
                                ps_task    tt
                         where  pp.prj_id          = tt.prj_id
                           and  pp.env_activity_id = tt.env_activity_id
                           and  pp.env_activity_id = 32
                           and  tt.action          in('I', 'U', 'E')
                           --
                           and  pp.rowid           = t.rowid
                        )
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)

                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      -- to change !!!
      select count(*)
      into   x_delta_projects_us_u
      from   ps_project t
      where  t.action   = 'U'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
        -- filter out projects updated due to tasks and projects from yesterday run
        and  not exists (select 1
                         from   ps_project pp,
                                ps_task    tt
                         where  pp.prj_id          = tt.prj_id
                           and  pp.env_activity_id = tt.env_activity_id
                           and  pp.env_activity_id = 32
                           and  tt.action          in('I', 'U', 'E')
                           --
                           and  pp.rowid           = t.rowid
                        )
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      -- E
      select count(*)
      into   x_projects_e
      from   ps_project t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.gain             is not null
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- for country projects (INT, LATAM) region / country are not transfered
        --and  t.region           is not null
        --and  t.country          is not null
      ;

      select count(*)
      into   x_projects_il_e
      from   ps_project t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.gain             is not null
        -- for country projects (INT, LATAM) region / country are not transfered
        --and  t.region           is not null
        --and  t.country          is not null
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_projects_us_e
      from   ps_project t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.gain             is not null
        -- for country projects (INT, LATAM) region is not transfered
        --and  t.region           is not null
        --and  t.country          is not null
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP',
                                     'BDIR','PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- Total
      select count(*)
      into   x_projects_total
      from   ps_project t
      where  t.action is not null
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))            or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- Removed projects are not calculated in total succeded projects, since it do not exist in the table export_grd
        --and  t.action <> 'E'
      ;

      select count(*)
      into   x_projects_il_total
      from   ps_project t
      where  t.action is not null
        -- Removed projects are not calculated in total succeded projects, since it do not exist in the table export_grd
        --and  t.action <> 'E'
        and  ((t.region           in('EU', 'IL', 'JP') and
               t.development_site in('BD', 'NA'))                      or
              -- for country projects (INT, LATAM) region is not transfered
              (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_projects_us_total
      from   ps_project t
      where  t.action is not null
        -- Removed projects are not calculated in total succeded projects, since it do not exist in the table export_grd
        --and  t.action <> 'E'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      select count(1)
      into   x_other_site_projects
      from   ps_project t
      where  t.action is not null
        and  not exists
                       (
                         select 1
                         from   ps_project p
                         where  p.action is not null
                           and  p.rowid  = t.rowid
                           and  (-- IL Sites
                                 (p.region           in('EU', 'IL', 'JP') and
                                  p.development_site in('BD', 'NA'))            or
                                 -- for country projects (INT, LATAM) region is not transfered
                                 (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                                  p.development_site in('BD', 'NA'))            or
                                 (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                        'ZR','ZG','BN','KR','UL','GO','ML',
                                                        'MU','TYK','UL-CoDev','RM','NAG')) or
                                  -- US Sites
                                 (p.region           in('US', 'CA') and
                                  p.development_site in('BD', 'NA'))            or
                                 (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                        'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                        'AR','CL','VE','PE','KW','NVD')))
                       )
      ;

      -- PS_TASK --
      -- I
      select count(*)
      into   x_tasks_i
      from   ps_task t
      where  t.action = 'I'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_tasks_il_i
      from   ps_task t
      where  t.action = 'I'
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_tasks_us_i
      from   ps_task t
      where  t.action = 'I'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- N
      select count(*)
      into   x_tasks_n
      from   ps_task t
      where  t.action = 'N'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_tasks_il_n
      from   ps_task t
      where  t.action = 'N'
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_tasks_us_n
      from   ps_task t
      where  t.action = 'N'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- U
      select count(*)
      into   x_tasks_u
      from   ps_task t
      where  t.action = 'U'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_tasks_il_u
      from   ps_task t
      where  t.action = 'U'
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_tasks_us_u
      from   ps_task t
      where  t.action = 'U'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- delta u
      select count(*)
      into   x_delta_tasks_u
      from   ps_task t
      where  t.action = 'U'
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- filter out tasks from yesterday run
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      select count(*)
      into   x_delta_tasks_il_u
      from   ps_task t
      where  t.action = 'U'
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
        -- filter out tasks from yesterday run
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      select count(*)
      into   x_delta_tasks_us_u
      from   ps_task t
      where  t.action = 'U'
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
        -- filter out tasks from yesterday run
        and  ((not exists (select 1
                           from   export_grd_backup_backup bb
                           where  bb.status in('J', 'E')
                             and  (t.prj_id = bb.gpms_id or
                                   t.prj_id = bb.plw_int_number)
                          ) and
                   exists (select 1 from export_grd_backup_backup)
              ) or
              not exists (select 1 from export_grd_backup_backup)
             )
      ;

      -- E
      select count(*)
      into   x_tasks_e
      from   ps_task t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.region           is not null
        and  t.country          is not null
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_tasks_il_e
      from   ps_task t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.region           is not null
        and  t.country          is not null
        --
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_tasks_us_e
      from   ps_task t
      where  t.action = 'E'
        and  t.development_site is not null
        and  t.region           is not null
        and  t.country          is not null
        --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      -- Total
      select count(*)
      into   x_tasks_total
      from   ps_task t
      where  t.action is not null
        and  (-- IL Sites
              (t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))  or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')) or
               -- US Sites
              (t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))            or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                     'AR','CL','VE','PE','KW','NVD')))
        -- Removed tasks are not calculated in total succeded tasks, since it do not exist in the table export_grd
        --and  t.action <> 'E'
      ;

      select count(*)
      into   x_tasks_il_total
      from   ps_task t
      where  t.action is not null
        -- Removed tasks are not calculated in total succeded tasks, since it do not exist in the table export_grd
        --and  t.action <> 'E'
        --
        and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
               t.development_site in('BD', 'NA'))                      or
              (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                     'ZR','ZG','BN','KR','UL','GO','ML',
                                     'MU','TYK','UL-CoDev','RM','NAG')))
      ;

      select count(*)
      into   x_tasks_us_total
      from   ps_task t
      where  t.action is not null
        -- Removed tasks are not calculated in total succeded tasks, since it do not exist in the table export_grd
        --and  t.action <> 'E'
        --
        and  ((t.region           in('US', 'CA') and
               t.development_site in('BD', 'NA'))             or
              (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                     'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                     'PO','AR','CL','VE','PE','KW','NVD')))
      ;

      select count(*)
      into   x_other_site_tasks
      from   ps_task t
      where  t.action is not null
        and  not exists
                       (
                         select 1
                         from   ps_task p
                         where  p.action is not null
                           and  p.rowid  = t.rowid
                           and  (-- IL Sites
                                 (p.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
                                  p.development_site in('BD', 'NA'))  or
                                 (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                        'ZR','ZG','BN','KR','UL','GO','ML',
                                                        'MU','TYK','UL-CoDev','RM','NAG')) or
                                  -- US Sites
                                 (p.region           in('US', 'CA') and
                                  p.development_site in('BD', 'NA'))            or
                                 (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                        'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                        'AR','CL','VE','PE','KW','NVD')))
                       )
      ;

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_count_delta_projects_tasks;

   end count_delta_projects_tasks;

   ------------------------------------------------
   -- print_log
   ------------------------------------------------
   procedure print_log(p_start_time            varchar2,
                       p_plw_projects          number,
                       p_plw_tasks             number,
                       p_failed_projects       number,
                       p_failed_tasks          number,
                       p_null_projects_message varchar2,
                       p_from_env_id           varchar2,
                       p_site                  varchar2,
                       p_gpms_exp_id           number) is


      l_interface_name               varchar2(50) := 'PLW -> GRD Interface Program Finished';
      l_end_time                     varchar2(20);

      l_error_projects_list          clob;
      l_warning_projects_list        clob;
      l_error_tasks_list             clob;
      l_warning_tasks_list           clob;
      l_error_projects               clob;
      l_warning_projects             clob;
      l_error_tasks                  clob;
      l_warning_tasks                clob;

      -- delta projects
      l_commited_project_records_n   number := 0;
      l_comited_project_records_il_n number := 0;
      l_comited_project_records_us_n number := 0;
      --
      l_commited_project_records_u   number := 0;
      l_comited_project_records_il_u number := 0;
      l_comited_project_records_us_u number := 0;
      --
      l_comtd_dlta_project_records_u number := 0;
      l_comtd_dlta_projct_rcrds_il_u number := 0;
      l_comtd_dlta_projct_rcrds_us_u number := 0;
      --
      l_commited_project_records_i   number := 0;
      l_comited_project_records_il_i number := 0;
      l_comited_project_records_us_i number := 0;
      --
      l_commited_project_records_e   number := 0;
      l_comited_project_records_il_e number := 0;
      l_comited_project_records_us_e number := 0;
      --
      l_total_commited_prj_records   number := 0;
      l_total_comited_prj_records_il number := 0;
      l_total_comited_prj_records_us number := 0;
      --
      l_other_site_projects          number := 0;

      -- delta tasks
      l_commited_task_records_n      number := 0;
      l_commited_task_records_il_n   number := 0;
      l_commited_task_records_us_n   number := 0;
      --
      l_commited_task_records_u      number := 0;
      l_commited_task_records_il_u   number := 0;
      l_commited_task_records_us_u   number := 0;
      --
      l_comited_delta_task_records_u number := 0;
      l_comitd_delta_task_rcrds_il_u number := 0;
      l_comitd_delta_task_rcrds_us_u number := 0;
      --
      l_commited_task_records_i      number := 0;
      l_commited_task_records_il_i   number := 0;
      l_commited_task_records_us_i   number := 0;
      --
      l_commited_task_records_e      number := 0;
      l_commited_task_records_il_e   number := 0;
      l_commited_task_records_us_e   number := 0;
      --
      l_total_commited_task_records  number := 0;
      l_total_commit_task_records_il number := 0;
      l_total_commit_task_records_us number := 0;
      --
      l_other_site_tasks             number := 0;

      -- migrated projects
      l_total_succ_migrated_projects number := 0;
      l_total_error_projects         number := 0;
      l_total_warning_projects       number := 0;
      l_migrated_project_records_n   number := 0;
      l_migrated_project_records_u   number := 0;
      l_migrated_project_records_i   number := 0;
      l_migrated_project_records_e   number := 0;

      -- migrated tasks
      l_total_succ_migrated_tasks    number := 0;
      l_total_error_tasks            number := 0;
      l_total_warning_tasks          number := 0;
      l_migrated_task_records_n      number := 0;
      l_migrated_task_records_u      number := 0;
      l_migrated_task_records_i      number := 0;
      l_migrated_task_records_e      number := 0;

      -- delta records
      l_migrated_prj_records_delta_u number := 0;
      l_migrated_task_rcords_delta_u number := 0;

      l_main_log_file                clob;
      l_log_type                     varchar2(7) := 'Summary';

      l_error_log_file               clob;
      l_error_log_type               varchar2(9) := 'Error';

      /*cursor c_il_failed_projects(c_status  varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                listagg(q.projects, '<br>' || chr(10)) within group (order by q.projects) as projects
         from   (
                  select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,
                         --
                         b.action,
                         b.error_message
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  b.error_message               is not null
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') \*and
                           upper(b.wp_status)  <> 'DRAFT'*\)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   \*and
                            upper(b.wp_status) <> 'DRAFT'*\)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   \*and
                            upper(b.wp_status) <> 'DRAFT'*\))
                    \*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*\
                    -- dev_site = IL --
                    and  ((p.region           in('EU', 'IL', 'JP') and
                           p.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           p.development_site in('BD', 'NA'))                      or
                          (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))
                    -- parameter --
                    and  b.status                      = c_status --'E'

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
                  select *
                  from   ( select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                                  rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                                  rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,
                                  --
                                  b.action,
                                  b.error_message
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             and  b.error_message               is not null
                             --
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             \*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null
                             and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*\
                             -- dev_site = IL --
                             and  ((p.region           in('EU', 'IL', 'JP') and
                                    p.development_site in('BD', 'NA'))                      or
                                   -- for country projects (INT, LATAM) region is not transfered
                                   (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                                    p.development_site in('BD', 'NA'))                      or
                                   (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                          'ZR','ZG','BN','KR','UL','GO','ML',
                                                          'MU','TYK','UL-CoDev','RM','NAG')))
                             -- parameter --
                             and  b.status                      = c_status --'E'
                        )
                ) q
         group by q.error_message
      ;

      cursor c_us_failed_projects(c_status  varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                listagg(q.projects, '<br>' || chr(10)) within group (order by q.projects) as projects
         from   (
                  select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,
                         --
                         b.action,
                         b.error_message
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') \*and
                           upper(b.wp_status)  <> 'DRAFT'*\)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   \*and
                            upper(b.wp_status) <> 'DRAFT'*\)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   \*and
                            upper(b.wp_status) <> 'DRAFT'*\))
                    \*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null*\
                    \*and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*\
                    -- dev_site = US --
                    and  ((p.region           in('US', 'CA') and
                           p.development_site in('BD', 'NA'))             or
                          (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))
                    -- parameter --
                    and  b.status                      = c_status --'E'

                  union all

                  -- count country projects (EMIA, ASIA, LATAM) for US dev_site
                  select *
                  from   ( select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                                  rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                                  rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,
                                  --
                                  b.action,
                                  b.error_message
                           from   ps_project         p,
                                  ps_task            t,
                                  export_grd_backup  b
                           where  1 = 1
                             and  upper(b.region)               in('EMIA',
                                                                   'ASIA',
                                                                   'LATAM')
                             and  b.country                     not in('JP','IL')
                             --
                             and  p.project_id                  = t.project_id
                             and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                             and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                             and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                             --
                             and  p.env_activity_id             = '32'
                             and  b.action                      in('N', 'U', 'I', 'E')
                             --
                             \*and  b.activity_type               <> 'R' || chr(38) || 'D'
                             and  b.gain                        is not null
                             and  b.development_site            is not null
                             and  b.region                      is not null
                             and  b.country                     is not null*\
                             and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                             \*and  upper(b.wp_status)            <> 'DRAFT'
                             and  (b.development_site           not in('BD', 'BD-E') or
                                   nvl(b.sales_channel, '-999') not like '%OTC%')*\
                             -- dev_site = US --
                             and  ((p.region           in('US', 'CA') and
                                    p.development_site in('BD', 'NA'))             or
                                   (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                          'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                          'AR','CL','VE','PE','KW','NVD')))
                             -- parameter --
                             and  b.status                      = c_status --'E'
                         )
                ) q
         group by q.error_message
      ;

      cursor c_il_failed_tasks(c_status varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                listagg(q.tasks, '<br>' || chr(10)) within group (order by q.tasks) as projects
         from   (
                  select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  tasks,
                         --
                         b.action,
                         b.error_message
                  from   ps_task           t,
                         export_grd_backup b
                  where  1 = 1
                    and  upper(b.region)               in('EMIA',
                                                          'ASIA',
                                                          'LATAM')
                    and  b.country                     not in ('JP','IL')
                    -- join
                    and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --
                    and  t.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
                    \*and  upper(b.wp_status)            <> 'DRAFT'
                    and  b.activity_type               <> 'R' || chr(38) || 'D'
                    and  b.gain                        is not null
                    and  b.development_site            is not null
                    and  b.region                      is not null
                    and  b.country                     is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*\
                    -- dev_site = IL --
                    and  ((t.region           in('EU', 'IL', 'JP') and
                           t.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           t.development_site in('BD', 'NA'))                      or
                          (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))
                    -- parameter --
                    and  b.status                      = c_status --'E'
                ) q
         group by q.error_message
      ;

      cursor c_us_failed_tasks(c_status varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                listagg(q.tasks, '<br>' || chr(10)) within group (order by q.tasks) as projects
         from   (
                  select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  tasks,
                         --
                         b.action,
                         b.error_message
                  from   ps_task           t,
                         export_grd_backup b
                  where  1 = 1
                    and  upper(b.region)               in('EMIA',
                                                          'ASIA',
                                                          'LATAM')
                    and  b.country                     not in ('JP','IL')
                    -- join
                    and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --
                    and  t.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
                    \*and  upper(b.wp_status)            <> 'DRAFT'
                    and  b.activity_type               <> 'R' || chr(38) || 'D'
                    and  b.gain                        is not null
                    and  b.development_site            is not null
                    and  b.region                      is not null
                    and  b.country                     is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*\
                    -- dev_site = US --
                    and  ((t.region           in('US', 'CA') and
                           t.development_site in('BD', 'NA'))             or
                          (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))
                    -- parameter --
                    and  b.status                      = c_status --'E'
                ) q
         group by q.error_message
      ;*/

      cursor c_il_failed_project_messages(c_status  varchar2) is
         select distinct
                q.error_message
         from   (
                 select b.error_message
                 from   ps_project        p,
                        export_grd_backup b
                 where  1 = 1

                   and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                   -- join
                   --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                   and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                   and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                   --  java failed records
                   and  p.env_activity_id             = '32'
                   and  b.action                      in('N', 'U', 'I', 'E')
                   and  b.error_message               is not null
                   and  (-- LIVE(CA), PRAM
                         (upper(b.region)     in('EU',
                                                 'CA') /*and
                          upper(b.wp_status)  <> 'DRAFT'*/)  or
                          -- US
                          upper(b.region)     = 'US'       or
                          -- IL
                          (upper(b.region)    = 'EMIA' and
                           b.country          = 'IL'   /*and
                           upper(b.wp_status) <> 'DRAFT'*/)  or
                          -- JP
                          (upper(b.region)    = 'ASIA' and
                           b.country          = 'JP'   /*and
                           upper(b.wp_status) <> 'DRAFT'*/))
                   /*and  b.gain                is not null
                   and  b.development_site    is not null
                   and  b.region              is not null
                   and  b.country             is not null
                   and  (b.development_site           not in('BD', 'BD-E') or
                         nvl(b.sales_channel, '-999') not like '%OTC%')*/
                   -- dev_site = IL --
                   and  ((p.region           in('EU', 'IL', 'JP') and
                          p.development_site in('BD', 'NA'))                      or
                         -- for country projects (INT, LATAM) region is not transfered
                         (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                          p.development_site in('BD', 'NA'))                      or
                         (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                'ZR','ZG','BN','KR','UL','GO','ML',
                                                'MU','TYK','UL-CoDev','RM','NAG')))
                   -- parameter --
                   and  b.status = c_status --'E' -- 'W'

                 union

                 -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
                 select b.error_message
                 from   ps_project         p,
                        ps_task            t,
                        export_grd_backup  b
                 where  1 = 1
                   and  upper(b.region)               in('EMIA',
                                                         'ASIA',
                                                         'LATAM')
                   and  b.country                     not in('JP','IL')
                   --
                   and  p.project_id                  = t.project_id
                   --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                   and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                   and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                   --
                   and  p.env_activity_id             = '32'
                   and  b.action                      in('N', 'U', 'I', 'E')
                   and  b.error_message               is not null
                   --
                   and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                   /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                   and  b.gain                        is not null
                   and  b.development_site            is not null
                   and  b.region                      is not null
                   and  b.country                     is not null
                   and  upper(b.wp_status)            <> 'DRAFT'
                   and  (b.development_site           not in('BD', 'BD-E') or
                         nvl(b.sales_channel, '-999') not like '%OTC%')*/
                   -- dev_site = IL --
                   and  ((p.region           in('EU', 'IL', 'JP') and
                          p.development_site in('BD', 'NA'))                      or
                         -- for country projects (INT, LATAM) region is not transfered
                         (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                          p.development_site in('BD', 'NA'))                      or
                         (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                'ZR','ZG','BN','KR','UL','GO','ML',
                                                'MU','TYK','UL-CoDev','RM','NAG')))
                   -- parameter --
                   and  b.status = c_status --'E' -- 'W'
                ) q
         ;

      cursor c_il_failure_projects(c_error_message varchar2,
                                   c_status        varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                --listagg(q.projects, '<br>' || chr(10)) within group (order by q.projects) as projects
                q.projects
         from   (
                  select /*rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,*/
                         decode(p.gpms_id, null, rpad('PLW_PJT_ID: '       || p.plw_pjt_id,     21, ' ') ||
                                                 rpad(', PLW_INT_NUMBER: ' || p.plw_int_number, 28, ' '),
                                                 rpad('GPMS_ID: '          || p.gpms_id,        15, ' ')) projects,
                         b.error_message
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1

                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  b.error_message               is not null
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = IL --
                    and  ((p.region           in('EU', 'IL', 'JP') and
                           p.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           p.development_site in('BD', 'NA'))                      or
                          (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))

                    -- parameter --
                    and  b.error_message = c_error_message
                    and  b.status        = c_status --'E' -- 'W'

                  union

                  -- count country projects (EMIA, ASIA, LATAM) for IL dev_site
                  select /*rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,*/
                         decode(p.gpms_id, null, rpad('PLW_PJT_ID: '       || p.plw_pjt_id,     21, ' ') ||
                                                 rpad(', PLW_INT_NUMBER: ' || p.plw_int_number, 28, ' '),
                                                 rpad('GPMS_ID: '          || p.gpms_id,        15, ' ')) projects,
                         b.error_message
                  from   ps_project         p,
                         ps_task            t,
                         export_grd_backup  b
                  where  1 = 1
                    and  upper(b.region)               in('EMIA',
                                                          'ASIA',
                                                          'LATAM')
                    and  b.country                     not in('JP','IL')
                    --
                    and  p.project_id                  = t.project_id
                    --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  b.error_message               is not null
                    --
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                    and  b.gain                        is not null
                    and  b.development_site            is not null
                    and  b.region                      is not null
                    and  b.country                     is not null
                    and  upper(b.wp_status)            <> 'DRAFT'
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = IL --
                    and  ((p.region           in('EU', 'IL', 'JP') and
                           p.development_site in('BD', 'NA'))                      or
                          -- for country projects (INT, LATAM) region is not transfered
                          (nvl(p.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                           p.development_site in('BD', 'NA'))                      or
                          (p.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                                 'ZR','ZG','BN','KR','UL','GO','ML',
                                                 'MU','TYK','UL-CoDev','RM','NAG')))
                    -- parameter --
                    and  b.error_message = c_error_message
                    and  b.status        = c_status
                ) q
         ;

      cursor c_us_failed_project_messages(c_status  varchar2) is
         select distinct
                q.error_message
         from   (
                 select b.error_message
                 from   ps_project        p,
                        export_grd_backup b
                 where  1 = 1
                   and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                   -- join
                   --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                   and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                   and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                   --  java failed records
                   and  p.env_activity_id             = '32'
                   and  b.action                      in('N', 'U', 'I', 'E')
                   and  (-- LIVE(CA), PRAM
                         (upper(b.region)     in('EU',
                                                 'CA') /*and
                          upper(b.wp_status)  <> 'DRAFT'*/)  or
                          -- US
                          upper(b.region)     = 'US'       or
                          -- IL
                          (upper(b.region)    = 'EMIA' and
                           b.country          = 'IL'   /*and
                           upper(b.wp_status) <> 'DRAFT'*/)  or
                          -- JP
                          (upper(b.region)    = 'ASIA' and
                           b.country          = 'JP'   /*and
                           upper(b.wp_status) <> 'DRAFT'*/))
                   /*and  b.gain                is not null
                   and  b.development_site    is not null
                   and  b.region              is not null
                   and  b.country             is not null*/
                   /*and  (b.development_site           not in('BD', 'BD-E') or
                         nvl(b.sales_channel, '-999') not like '%OTC%')*/
                   -- dev_site = US --
                   and  ((p.region           in('US', 'CA') and
                          p.development_site in('BD', 'NA'))             or
                         (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                'AR','CL','VE','PE','KW','NVD')))
                   -- parameter --
                   and  b.status = c_status

                 union

                 -- count country projects (EMIA, ASIA, LATAM) for US dev_site
                 select b.error_message
                 from   ps_project         p,
                        ps_task            t,
                        export_grd_backup  b
                 where  1 = 1
                   and  upper(b.region)               in('EMIA',
                                                         'ASIA',
                                                         'LATAM')
                   and  b.country                     not in('JP','IL')
                   --
                   and  p.project_id                  = t.project_id
                   --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                   and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                   and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                   --
                   and  p.env_activity_id             = '32'
                   and  b.action                      in('N', 'U', 'I', 'E')
                   --
                   /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                   and  b.gain                        is not null
                   and  b.development_site            is not null
                   and  b.region                      is not null
                   and  b.country                     is not null*/
                   and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                   /*and  upper(b.wp_status)            <> 'DRAFT'
                   and  (b.development_site           not in('BD', 'BD-E') or
                         nvl(b.sales_channel, '-999') not like '%OTC%')*/
                   -- dev_site = US --
                   and  ((p.region           in('US', 'CA') and
                          p.development_site in('BD', 'NA'))             or
                         (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                'AR','CL','VE','PE','KW','NVD')))
                   -- parameter --
                   and  b.status = c_status
                ) q
         ;

      cursor c_us_failure_projects(c_error_message varchar2,
                                   c_status        varchar2) is
         select q.error_message,
                -- split projects into different rows in the field for the same error_message
                --listagg(q.projects, '<br>' || chr(10)) within group (order by q.projects) as projects
                q.projects
         from   (
                  select /*rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,*/
                         decode(p.gpms_id, null, rpad('PLW_PJT_ID: '       || p.plw_pjt_id,     21, ' ') ||
                                                 rpad(', PLW_INT_NUMBER: ' || p.plw_int_number, 28, ' '),
                                                 rpad('GPMS_ID: '          || p.gpms_id,        15, ' ')) projects,
                         --
                         --b.action,
                         b.error_message
                  from   ps_project        p,
                         export_grd_backup b
                  where  1 = 1
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    -- join
                    --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --  java failed records
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    and  (-- LIVE(CA), PRAM
                          (upper(b.region)     in('EU',
                                                  'CA') /*and
                           upper(b.wp_status)  <> 'DRAFT'*/)  or
                           -- US
                           upper(b.region)     = 'US'       or
                           -- IL
                           (upper(b.region)    = 'EMIA' and
                            b.country          = 'IL'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/)  or
                           -- JP
                           (upper(b.region)    = 'ASIA' and
                            b.country          = 'JP'   /*and
                            upper(b.wp_status) <> 'DRAFT'*/))
                    /*and  b.gain                is not null
                    and  b.development_site    is not null
                    and  b.region              is not null
                    and  b.country             is not null*/
                    /*and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = US --
                    and  ((p.region           in('US', 'CA') and
                           p.development_site in('BD', 'NA'))             or
                          (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))
                    -- parameter --
                    and  b.error_message = c_error_message
                    and  b.status        = c_status

                  union

                  -- count country projects (EMIA, ASIA, LATAM) for US dev_site
                  select /*rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                         rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                         rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  projects,*/
                         decode(p.gpms_id, null, rpad('PLW_PJT_ID: '       || p.plw_pjt_id,     21, ' ') ||
                                                 rpad(', PLW_INT_NUMBER: ' || p.plw_int_number, 28, ' '),
                                                 rpad('GPMS_ID: '          || p.gpms_id,        15, ' ')) projects,
                         --
                         --b.action,
                         b.error_message
                  from   ps_project         p,
                         ps_task            t,
                         export_grd_backup  b
                  where  1 = 1
                    and  upper(b.region)               in('EMIA',
                                                          'ASIA',
                                                          'LATAM')
                    and  b.country                     not in('JP','IL')
                    --
                    and  p.project_id                  = t.project_id
                    --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
                    and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
                    and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
                    --
                    and  p.env_activity_id             = '32'
                    and  b.action                      in('N', 'U', 'I', 'E')
                    --
                    /*and  b.activity_type               <> 'R' || chr(38) || 'D'
                    and  b.gain                        is not null
                    and  b.development_site            is not null
                    and  b.region                      is not null
                    and  b.country                     is not null*/
                    and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    /*and  upper(b.wp_status)            <> 'DRAFT'
                    and  (b.development_site           not in('BD', 'BD-E') or
                          nvl(b.sales_channel, '-999') not like '%OTC%')*/
                    -- dev_site = US --
                    and  ((p.region           in('US', 'CA') and
                           p.development_site in('BD', 'NA'))             or
                          (p.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                                 'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                                 'AR','CL','VE','PE','KW','NVD')))
                    -- parameter --
                    and  b.error_message = c_error_message
                    and  b.status        = c_status
                ) q
         ;

      cursor c_il_failed_task_messages(c_status varchar2) is
         select distinct
                b.error_message
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = IL --
           and  ((t.region           in('EU', 'IL', 'JP') and
                  t.development_site in('BD', 'NA'))                      or
                 -- for country projects (INT, LATAM) region is not transfered
                 (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                  t.development_site in('BD', 'NA'))                      or
                 (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                        'ZR','ZG','BN','KR','UL','GO','ML',
                                        'MU','TYK','UL-CoDev','RM','NAG')))
           -- parameter --
           and  b.status  = c_status
         ;

      cursor c_il_failure_tasks(c_error_message varchar2,
                                c_status        varchar2) is
            select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                   rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                   rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  tasks,
                   --
                   b.action,
                   b.error_message
            from   ps_task           t,
                   export_grd_backup b
            where  1 = 1
              and  upper(b.region)               in('EMIA',
                                                    'ASIA',
                                                    'LATAM')
              and  b.country                     not in ('JP','IL')
              -- join
              --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
              and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
              and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
              --
              and  t.env_activity_id             = '32'
              and  b.action                      in('N', 'U', 'I', 'E')
              and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
              -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
              /*and  upper(b.wp_status)            <> 'DRAFT'
              and  b.activity_type               <> 'R' || chr(38) || 'D'
              and  b.gain                        is not null
              and  b.development_site            is not null
              and  b.region                      is not null
              and  b.country                     is not null
              and  (b.development_site           not in('BD', 'BD-E') or
                    nvl(b.sales_channel, '-999') not like '%OTC%')*/
              -- dev_site = IL --
              and  ((t.region           in('EU', 'IL', 'JP') and
                     t.development_site in('BD', 'NA'))                      or
                    -- for country projects (INT, LATAM) region is not transfered
                    (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                     t.development_site in('BD', 'NA'))                      or
                    (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                           'ZR','ZG','BN','KR','UL','GO','ML',
                                           'MU','TYK','UL-CoDev','RM','NAG')))
              -- parameter --
              and  b.error_message   = c_error_message
              and  b.status          = c_status
           ;

      cursor c_us_failed_task_messages(c_status varchar2) is
         select distinct
                b.error_message
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA',
                                                 'ASIA',
                                                 'LATAM')
           and  b.country                     not in ('JP','IL')
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --
           and  t.env_activity_id             = '32'
           and  b.action                      in('N', 'U', 'I', 'E')
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
           /*and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  (b.development_site           not in('BD', 'BD-E') or
                 nvl(b.sales_channel, '-999') not like '%OTC%')*/
           -- dev_site = US --
           and  ((t.region           in('US', 'CA') and
                  t.development_site in('BD', 'NA'))             or
                 (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                        'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                        'AR','CL','VE','PE','KW','NVD')))
           -- parameter --
           and  b.status  = c_status
         ;

      cursor c_us_failure_tasks(c_error_message varchar2,
                                c_status        varchar2) is
            select rpad('GPMS_ID: '          || b.gpms_id,        15, ' ') ||
                   rpad(', PLW_PJT_ID: '     || b.plw_pjt_id,     23, ' ') ||
                   rpad(', PLW_INT_NUMBER: ' || b.plw_int_number, 28, ' ')  tasks,
                   --
                   b.action,
                   b.error_message
            from   ps_task           t,
                   export_grd_backup b
            where  1 = 1
              and  upper(b.region)               in('EMIA',
                                                    'ASIA',
                                                    'LATAM')
              and  b.country                     not in ('JP','IL')
              -- join
              --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
              and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
              and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
              --
              and  t.env_activity_id             = '32'
              and  b.action                      in('N', 'U', 'I', 'E')
              and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
              -- these conditions may be the reason for not created action = 10 records and for delta 'E' records
              /*and  upper(b.wp_status)            <> 'DRAFT'
              and  b.activity_type               <> 'R' || chr(38) || 'D'
              and  b.gain                        is not null
              and  b.development_site            is not null
              and  b.region                      is not null
              and  b.country                     is not null
              and  (b.development_site           not in('BD', 'BD-E') or
                    nvl(b.sales_channel, '-999') not like '%OTC%')*/
              -- dev_site = US --
              and  ((t.region           in('US', 'CA') and
                     t.development_site in('BD', 'NA'))             or
                    (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                           'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR','PO',
                                           'AR','CL','VE','PE','KW','NVD')))
              -- parameter --
              and  b.error_message   = c_error_message
              and  b.status          = c_status
           ;

   begin

      -- Print Log to DBMS_OUTPUT and Create Log File before java program was run
      if(p_site = 'ALL') then

         count_delta_projects_tasks(-- projects
                                    l_commited_project_records_i,
                                    l_comited_project_records_il_i,
                                    l_comited_project_records_us_i,
                                    --
                                    l_commited_project_records_n,
                                    l_comited_project_records_il_n,
                                    l_comited_project_records_us_n,
                                    --
                                    l_commited_project_records_u,
                                    l_comited_project_records_il_u,
                                    l_comited_project_records_us_u,
                                    -- delta u
                                    l_comtd_dlta_project_records_u,
                                    l_comtd_dlta_projct_rcrds_il_u,
                                    l_comtd_dlta_projct_rcrds_us_u,
                                    --
                                    l_commited_project_records_e,
                                    l_comited_project_records_il_e,
                                    l_comited_project_records_us_e,
                                    --
                                    l_total_commited_prj_records,
                                    l_total_comited_prj_records_il,
                                    l_total_comited_prj_records_us,
                                    --
                                    l_other_site_projects,
                                    -- tasks
                                    l_commited_task_records_i,
                                    l_commited_task_records_il_i,
                                    l_commited_task_records_us_i,
                                    --
                                    l_commited_task_records_n,
                                    l_commited_task_records_il_n,
                                    l_commited_task_records_us_n,
                                    --
                                    l_commited_task_records_u,
                                    l_commited_task_records_il_u,
                                    l_commited_task_records_us_u,
                                    --  delta u
                                    l_comited_delta_task_records_u,
                                    l_comitd_delta_task_rcrds_il_u,
                                    l_comitd_delta_task_rcrds_us_u,
                                    --
                                    l_commited_task_records_e,
                                    l_commited_task_records_il_e,
                                    l_commited_task_records_us_e,
                                    --
                                    l_total_commited_task_records,
                                    l_total_commit_task_records_il,
                                    l_total_commit_task_records_us,
                                    --
                                    l_other_site_tasks
                                   );

         l_end_time := to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss');

         dbms_output.put_line(chr(10) || '----- Program Run Details -----');
         dbms_output.put_line(rpad('Interface name', 45, ' ')                    || '| ' || l_interface_name);
         dbms_output.put_line(rpad('Start Time', 45, ' ')                        || '| ' || p_start_time);
         dbms_output.put_line(rpad('End Time', 45, ' ')                          || '| ' || l_end_time);

         dbms_output.put_line(rpad('Interface Status', 45, ' ')                  || '| ' || g_interface_status);

         --************ PS_PROJECT Info ************
         dbms_output.put_line(chr(10) || '----- PS_PROJECT Info -----');
         dbms_output.put_line(rpad('Total PLW Projects (except to be deactivated)', 45, ' ') || '| ' || p_plw_projects || chr(10));
         -- N
         dbms_output.put_line(rpad('Total Projects With No Changes', 45, ' ')        || '| ' || l_commited_project_records_n);
         dbms_output.put_line(rpad('Total IL Projects With No Changes', 45, ' ')     || '| ' || l_comited_project_records_il_n);
         dbms_output.put_line(rpad('Total US Projects With No Changes', 45, ' ')     || '| ' || l_comited_project_records_us_n || chr(10));
         -- U
         dbms_output.put_line(rpad('Total Projects To Be Updated', 45, ' ')          || '| ' || l_commited_project_records_u);
         dbms_output.put_line(rpad('Total IL Projects To Be Updated', 45, ' ')       || '| ' || l_comited_project_records_il_u);
         dbms_output.put_line(rpad('Total US Projects To Be Updated', 45, ' ')       || '| ' || l_comited_project_records_us_u || chr(10));
         --
         dbms_output.put_line(rpad('Total Delta Projects To Be Updated', 45, ' ')    || '| ' || l_comtd_dlta_project_records_u);
         dbms_output.put_line(rpad('Total Delta IL Projects To Be Updated', 45, ' ') || '| ' || l_comtd_dlta_projct_rcrds_il_u);
         dbms_output.put_line(rpad('Total Delta US Projects To Be Updated', 45, ' ') || '| ' || l_comtd_dlta_projct_rcrds_us_u || chr(10));
         -- I
         dbms_output.put_line(rpad('Total Projects To Be Inserted', 45, ' ')         || '| ' || l_commited_project_records_i);
         dbms_output.put_line(rpad('Total IL Projects To Be Inserted', 45, ' ')      || '| ' || l_comited_project_records_il_i);
         dbms_output.put_line(rpad('Total US Projects To Be Inserted', 45, ' ')      || '| ' || l_comited_project_records_us_i || chr(10));

         -- E
         dbms_output.put_line(rpad('Total Projects Removed In PLW', 45, ' ')         || '| ' || l_commited_project_records_e);
         dbms_output.put_line(rpad('Total IL Projects Removed In PLW', 45, ' ')      || '| ' || l_comited_project_records_il_e);
         dbms_output.put_line(rpad('Total US Projects Removed In PLW', 45, ' ')      || '| ' || l_comited_project_records_us_e || chr(10));

         -- Total Successfull
         dbms_output.put_line(rpad('Total Successful Projects', 45, ' ')             || '| ' || l_total_commited_prj_records);
         dbms_output.put_line(rpad('Total IL Successful Projects', 45, ' ')          || '| ' || l_total_comited_prj_records_il);
         dbms_output.put_line(rpad('Total US Successful Projects', 45, ' ')          || '| ' || l_total_comited_prj_records_us || chr(10));

         -- Projects from sites other than US and IL (won't be transfered)
         dbms_output.put_line(rpad('Total Projects From Sites Other Than US, IL', 45, ' ') || '| ' || l_other_site_projects || chr(10));

         -- Failed
         dbms_output.put_line(rpad('PLW Failed Projects (were not created)', 45, ' ') || '| ' || p_failed_projects);


         --************ PS_TASK Info ************
         dbms_output.put_line(chr(10) || '----- PS_TASK Info -----');
         dbms_output.put_line(rpad('Total PLW Tasks (except to be deactivated)', 45, ' ') || '| ' || p_plw_tasks || chr(10));
         -- N
         dbms_output.put_line(rpad('Total Tasks With No Changes', 45, ' ')       || '| ' || l_commited_task_records_n);
         dbms_output.put_line(rpad('Total IL Tasks With No Changes', 45, ' ')    || '| ' || l_commited_task_records_il_n);
         dbms_output.put_line(rpad('Total US Tasks With No Changes', 45, ' ')    || '| ' || l_commited_task_records_us_n || chr(10));

         -- U
         dbms_output.put_line(rpad('Total Tasks To Be Updated', 45, ' ')         || '| ' || l_commited_task_records_u);
         dbms_output.put_line(rpad('Total IL Tasks To Be Updated', 45, ' ')      || '| ' || l_commited_task_records_il_u);
         dbms_output.put_line(rpad('Total US Tasks To Be Updated', 45, ' ')      || '| ' || l_commited_task_records_us_u || chr(10));

         -- I
         dbms_output.put_line(rpad('Total Tasks To Be Inserted', 45, ' ')        || '| ' || l_commited_task_records_i);
         dbms_output.put_line(rpad('Total IL Tasks To Be Inserted', 45, ' ')     || '| ' || l_commited_task_records_il_i);
         dbms_output.put_line(rpad('Total US Tasks To Be Inserted', 45, ' ')     || '| ' || l_commited_task_records_us_i || chr(10));

         -- E
         dbms_output.put_line(rpad('Total Tasks Removed In PLW', 45, ' ')        || '| ' || l_commited_task_records_e);
         dbms_output.put_line(rpad('Total IL Tasks Removed In PLW', 45, ' ')     || '| ' || l_commited_task_records_il_e);
         dbms_output.put_line(rpad('Total US Tasks Removed In PLW', 45, ' ')     || '| ' || l_commited_task_records_us_e || chr(10));

         -- Total Successfull
         dbms_output.put_line(rpad('Total Successful Tasks', 45, ' ')            || '| ' || l_total_commited_task_records);
         dbms_output.put_line(rpad('Total IL Successful Tasks', 45, ' ')         || '| ' || l_total_commit_task_records_il);
         dbms_output.put_line(rpad('Total US Successful Tasks', 45, ' ')         || '| ' || l_total_commit_task_records_us || chr(10));

         -- Projects from sites other than US and IL (won't be transfered)
         dbms_output.put_line(rpad('Total Tasks From Sites Other Than US, IL', 45, ' ') || '| ' || l_other_site_tasks || chr(10));

         -- Failed
         dbms_output.put_line(rpad('PLW Failed Tasks (were not created)', 45, ' ') || '| ' || p_failed_tasks);

         -- creating log output file
         l_main_log_file := '<!DOCTYPE html>
                           <html>

                            <head>
                             <title>PLW --> RND Summary Log</title>
                            </head>

                            <body>
                             <div>
                              <h2>Process Report: <br></h2>

                                 <table border="1" width="25%">
                                   <tr bgcolor="Silver">
                                      <th><div align="left"> Interface name </div></th>
                                      <th><div align="left">' || l_interface_name || '</div></th>
                                   </tr> <tr bgcolor="Gainsboro">
                                      <th><div align="left"> Envirnoment </div></th>
                                      <th><div align="left">' || p_from_env_id || '</div></th>
                                   </tr> <tr bgcolor="Gainsboro">
                                      <th><div align="left"> Start Time </div></th>
                                      <th><div align="left">' || p_start_time || '</div></th>
                                   </tr> <tr bgcolor="Gainsboro">
                                      <th><div align="left"> End Time </div></th>
                                      <th><div align="left">' || l_end_time || '</div></th>
                                   </tr> <tr bgcolor="Gainsboro">
                                      <th><div align="left"> Interface Site </div></th>
                                      <th><div align="left">' || p_site || '</div></th>
                                   </tr> <tr bgcolor="Gainsboro">
                                      <th><div align="left"> Interface Status </div></th>
                                      <th><div align="left">' || g_interface_status || '</div></th>
                                   </tr>
                                 </table>
                             </div>' ||
                              --************ PS_PROJECT Info ************
                              '<h2>PS_PROJECT INFO:<br></h2>
                                 <table border="1" width="25%">
                                   <tr bgcolor="Silver">
                                     <th>Projects</th>
                                     <th>Amount</th>
                                   </tr>' ||
                                   -- Total
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total Projects (except to be deactivated) </div></th>
                                      <th>' || p_plw_projects || '</th>
                                   </tr> ' ||
                                   -- N
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Projects With No Changes  </div></th>
                                       <th>' || l_commited_project_records_n || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total IL Projects With No Changes </div></th>
                                       <th>' || l_comited_project_records_il_n || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total US Projects With No Changes </div></th>
                                       <th>' || l_comited_project_records_us_n || '</th>
                                    </tr> ' ||
                                   -- U
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Projects To Be Updated </div></th>
                                       <th>' || l_commited_project_records_u || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Projects To Be Updated </div></th>
                                       <th>' || l_comited_project_records_il_u || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Projects To Be Updated </div></th>
                                       <th>' || l_comited_project_records_us_u || '</th>
                                    </tr> ' ||
                                   -- Delta U
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Delta Projects To Be Updated </div></th>
                                       <th>' || l_comtd_dlta_project_records_u || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total Delta IL Projects To Be Updated </div></th>
                                       <th>' || l_comtd_dlta_projct_rcrds_il_u || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total Delta US Projects To Be Updated </div></th>
                                       <th>' || l_comtd_dlta_projct_rcrds_us_u || '</th>
                                    </tr> ' ||
                                   -- I
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Projects To Be Inserted </div></th>
                                       <th>' || l_commited_project_records_i || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Projects To Be Inserted </div></th>
                                       <th>' || l_comited_project_records_il_i || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Projects To Be Inserted </div></th>
                                       <th>' || l_comited_project_records_us_i || '</th>
                                    </tr> ' ||
                                   -- E
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Projects Removed In PLW (deactivated) </div></th>
                                       <th>' || l_commited_project_records_e || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total IL Projects Removed In PLW (deactivated) </div></th>
                                       <th>' || l_comited_project_records_il_e || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total US Projects Removed In PLW (deactivated) </div></th>
                                       <th>' || l_comited_project_records_us_e || '</th>
                                    </tr> ' ||
                                   -- Total
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Successful Projects </div></th>
                                       <th>' || l_total_commited_prj_records || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Successful Projects </div></th>
                                       <th>' || l_total_comited_prj_records_il || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Successful Projects </div></th>
                                       <th>' || l_total_comited_prj_records_us || '</th>
                                    </tr> ' ||
                                   -- Projects from sites other than US and IL (won't be transfered)
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Projects From Sites Other Than US, IL </div></th>
                                       <th>' || l_other_site_projects || '</th>
                                    </tr> ' ||
                                   -- Failed
                                   '<tr bgcolor="White">
                                       <th><div align="left"> PLW Failed Projects (were not created) </div></th>
                                       <th>' || p_failed_projects || '</th>
                                    </tr> ' ||
                                 '</table> ' ||

                                    p_null_projects_message  || '<br>' ||
                                    --g_failed_projects_list   || '<br>' ||

                               --************ PS_TASK Info ************
                              '<h2>PS_TASK INFO:<br></h2>
                                 <table border="1" width="25%">
                                    <tr bgcolor="Silver">
                                       <th>Projects</th>
                                       <th>Amount</th>
                                    </tr> ' ||
                                    -- Total
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total PLW Tasks (except to be deactivated) </div></th>
                                       <th>' || p_plw_tasks || '</th>
                                    </tr> ' ||
                                    -- N
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Tasks With No Changes </div></th>
                                       <th>' || l_commited_task_records_n || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total IL Tasks With No Changes </div></th>
                                       <th>' || l_commited_task_records_il_n || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total US Tasks With No Changes </div></th>
                                       <th>' || l_commited_task_records_us_n || '</th>
                                    </tr> ' ||
                                    -- U
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Tasks To Be Updated </div></th>
                                       <th>' || l_commited_task_records_u || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Tasks To Be Updated </div></th>
                                       <th>' || l_commited_task_records_il_u || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Tasks To Be Updated </div></th>
                                       <th>' || l_commited_task_records_us_u || '</th>
                                    </tr> ' ||
                                    -- Delta U
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Delta Tasks To Be Updated </div></th>
                                       <th>' || l_comited_delta_task_records_u || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total IL Delta Tasks To Be Updated </div></th>
                                       <th>' || l_comitd_delta_task_rcrds_il_u || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total US Delta Tasks To Be Updated </div></th>
                                       <th>' || l_comitd_delta_task_rcrds_us_u || '</th>
                                    </tr> ' ||
                                    -- I
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Tasks To Be Inserted </div></th>
                                       <th>' || l_commited_task_records_i || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Tasks To Be Inserted </div></th>
                                       <th>' || l_commited_task_records_il_i || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Tasks To Be Inserted </div></th>
                                       <th>' || l_commited_task_records_us_i || '</th>
                                    </tr> ' ||
                                    -- E
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Tasks Removed In PLW (deactivated) </div></th>
                                       <th>' || l_commited_task_records_e || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total IL Tasks Removed In PLW (deactivated) </div></th>
                                       <th>' || l_commited_task_records_il_e || '</th>
                                    </tr>
                                    <tr bgcolor="White">
                                       <th><div align="left"> Total US Tasks Removed In PLW (deactivated) </div></th>
                                       <th>' || l_commited_task_records_us_e || '</th>
                                    </tr> ' ||
                                    -- Total
                                   '<tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total Successful Tasks </div></th>
                                       <th>' || l_total_commited_task_records || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total IL Successful Tasks </div></th>
                                       <th>' || l_total_commit_task_records_il || '</th>
                                    </tr>
                                    <tr bgcolor="Gainsboro">
                                       <th><div align="left"> Total US Successful Tasks </div></th>
                                       <th>' || l_total_commit_task_records_us || '</th>
                                    </tr> ' ||
                                    -- Tasks from sites other than US and IL (won't be transfered)
                                   '<tr bgcolor="White">
                                       <th><div align="left"> Total Tasks From Sites Other Than US, IL </div></th>
                                       <th>' || l_other_site_tasks || '</th>
                                    </tr> ' ||
                                    -- Failed
                                   '<tr bgcolor="White">
                                       <th><div align="left"> PLW Failed Tasks (were not created) </div></th>
                                       <th>' || p_failed_tasks || '</th>
                                    </tr> ' ||
                                 '</table> ' ||
                                 --g_failed_tasks_list   || '<br>' ||
                            '</body>
                           </html>';

         l_error_log_file := '<html>
                                <head>
                                 <title>PLW --> RND Error Log</title>
                                </head>

                                <body>
                                 <div>
                                  <h2>Creating projects / tasks from PLM failed: <br></h2>
                                 </div>
                                  <h2>Failed Projects:</h2> <br>'    ||
                                    g_failed_projects_list || '<br>' ||
                                  '<h2>Failed Tasks:</h2> <br>'      ||
                                    g_failed_tasks_list    || '<br>' ||
                               '</body>
                              </html>';


      -- Create Log File after java program was run
      elsif(p_site = 'GRD-IL') then

         count_migrated_projects_tasks(p_site,
                                       -- projects
                                       l_total_succ_migrated_projects,
                                       l_total_error_projects,
                                       l_total_warning_projects,
                                       l_migrated_project_records_n,
                                       l_migrated_project_records_u,
                                       l_migrated_prj_records_delta_u,
                                       l_migrated_project_records_i,
                                       l_migrated_project_records_e,
                                       -- tasks
                                       l_total_succ_migrated_tasks,
                                       l_total_error_tasks,
                                       l_total_warning_tasks,
                                       l_migrated_task_records_n,
                                       l_migrated_task_records_u,
                                       l_migrated_task_rcords_delta_u,
                                       l_migrated_task_records_i,
                                       l_migrated_task_records_e);


         -- concatenate error_messages for the failed projects
         l_error_projects_list := null;
         if(l_total_error_projects > 0) then

            /*for r_il_failed_projects in c_il_failed_projects('E') loop

               l_error_projects_list := l_error_projects_list                               ||
                                        '<h4>The following projects:</h4>'                  ||
                                        r_il_failed_projects.projects || '<br>'             ||
                                        '<h4>were completed with the following error:</h4>' ||
                                        r_il_failed_projects.error_message                  ||
                                        '<br>---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------'
               ;

            end loop;*/

            for r_il_failed_project_messages in c_il_failed_project_messages('E') loop

               l_error_projects := null;

               for r_il_failure_projects in c_il_failure_projects(r_il_failed_project_messages.error_message,
                                                                  'E') loop
                  -- 1-st iteration or another error message
                  if(l_error_projects is null) then

                     l_error_projects := r_il_failure_projects.projects;

                  -- another project for the same error_message
                  else

                     l_error_projects := l_error_projects || '<br>' || chr(10) || r_il_failure_projects.projects;

                  end if;

               end loop;

               l_error_projects_list := l_error_projects_list                               ||
                                        '<table border="1" width="95%">
                                           <tr bgcolor="Silver">
                                              <th>Projects</th>
                                              <th>Error Message</th>
                                           </tr>' ||
                                          '<tr bgcolor="White">
                                              <td width="25%"><div align="left">' || l_error_projects || '</div></td>
                                              <td width="75%"><div align="left">' || r_il_failed_project_messages.error_message || '</div></td>
                                           </tr> <tr bgcolor="White">
                                         </table>'
                                         ;

            end loop;

            l_error_projects_list:= '<h2>Projects Ended with Error in Java Interface Program:</h2>' || l_error_projects_list   || '<br>'
            ;

         end if;

         l_warning_projects_list := null;
         if(l_total_warning_projects > 0) then

            /*for r_il_failed_projects in c_il_failed_projects('W') loop

               l_warning_projects_list := l_warning_projects_list                             ||
                                        '<h4>The following projects:</h4>'                    ||
                                        r_il_failed_projects.projects || '<br>'               ||
                                        '<h4>were completed with the following warning:</h4>' ||
                                        r_il_failed_projects.error_message                    ||
                                        '<br>---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------'
               ;

            end loop;*/

            for r_il_failed_project_messages in c_il_failed_project_messages('W') loop

               l_warning_projects := null;

               for r_il_failure_projects in c_il_failure_projects(r_il_failed_project_messages.error_message,
                                                                  'W') loop
                  -- 1-st iteration or another error message
                  if(l_warning_projects is null) then

                     l_warning_projects := r_il_failure_projects.projects;

                  -- another project for the same error_message
                  else

                     l_warning_projects := l_warning_projects || '<br>' || chr(10) || r_il_failure_projects.projects;

                  end if;

               end loop;

               l_warning_projects_list := l_warning_projects_list ||
                                          '<table border="1" width="95%">
                                             <tr bgcolor="Silver">
                                                <th>Projects</th>
                                                <th>Error Message</th>
                                             </tr>' ||
                                            '<tr bgcolor="White">
                                                <td width="25%"><div align="left">' || l_warning_projects || '</div></td>
                                                <td width="75%"><div align="left">' || r_il_failed_project_messages.error_message || '</div></td>
                                             </tr> <tr bgcolor="White">
                                           </table>'
                                           ;

            end loop;

            l_warning_projects_list := '<h2>Projects Ended with Warning in Java Interface Program:</h2>' || l_warning_projects_list   || '<br>'
            ;

         end if;

         -- concatenate error_messages for the failed tasks
         l_error_tasks_list := null;
         if(l_total_error_tasks > 0) then

            /*for r_il_failed_tasks in c_il_failed_tasks('E') loop

               l_error_tasks_list := l_error_tasks_list                                  ||
                                     '<h4>The following projects:</h4>'                  ||
                                     r_il_failed_tasks.projects || '<br>'                ||
                                     '<h4>were completed with the following error:</h4>' ||
                                     r_il_failed_tasks.error_message                     ||
                                     '<br>---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------'
               ;

            end loop;*/

            for r_il_failed_task_messages in c_il_failed_task_messages('E') loop

               l_error_tasks := null;

               for r_il_failure_tasks in c_il_failure_tasks(r_il_failed_task_messages.error_message,
                                                            'E') loop
                  -- 1-st iteration or another error message
                  if(l_error_tasks is null) then

                     l_error_tasks := r_il_failure_tasks.tasks;

                  -- another project for the same error_message
                  else

                     l_error_tasks := l_error_tasks || '<br>' || chr(10) || r_il_failure_tasks.tasks;

                  end if;

               end loop;

               l_error_tasks_list := l_error_tasks_list                               ||
                                        '<table border="1" width="95%">
                                           <tr bgcolor="Silver">
                                              <th>Tasks</th>
                                              <th>Error Message</th>
                                           </tr>' ||
                                          '<tr bgcolor="White">
                                              <td width="30%"><div align="left">' || l_error_tasks || '</div></td>
                                              <td width="70%"><div align="left">' || r_il_failed_task_messages.error_message || '</div></td>
                                           </tr> <tr bgcolor="White">
                                         </table>'
                                         ;

            end loop;

            l_error_tasks_list := '<h2>Tasks Ended with Error in Java Interface Program:</h2>' || l_error_tasks_list || '<br>'
            ;

         end if;

         l_warning_tasks_list := null;
         if(l_total_warning_tasks > 0) then

            /*for r_il_failed_tasks in c_il_failed_tasks('W') loop

               l_warning_tasks_list := l_warning_tasks_list                                ||
                                     '<h4>The following projects:</h4>'                    ||
                                     r_il_failed_tasks.projects || '<br>'                  ||
                                     '<h4>were completed with the following warning:</h4>' ||
                                     r_il_failed_tasks.error_message                       ||
                                     '<br>---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------'
               ;

            end loop;*/

            for r_il_failed_task_messages in c_il_failed_task_messages('W') loop

               l_warning_tasks := null;

               for r_il_failure_tasks in c_il_failure_tasks(r_il_failed_task_messages.error_message,
                                                            'W') loop
                  -- 1-st iteration or another error message
                  if(l_warning_tasks is null) then

                     l_warning_tasks := r_il_failure_tasks.tasks;

                  -- another project for the same error_message
                  else

                     l_warning_tasks := l_warning_tasks || '<br>' || chr(10) || r_il_failure_tasks.tasks;

                  end if;

               end loop;

               l_warning_tasks_list := l_warning_tasks_list             ||
                                       '<table border="1" width="95%">
                                          <tr bgcolor="Silver">
                                             <th>Tasks</th>
                                             <th>Error Message</th>
                                          </tr>' ||
                                         '<tr bgcolor="White">
                                             <td width="30%"><div align="left">' || l_warning_tasks || '</div></td>
                                             <td width="70%"><div align="left">' || r_il_failed_task_messages.error_message || '</div></td>
                                          </tr> <tr bgcolor="White">
                                        </table>'
                                        ;

            end loop;

            l_warning_tasks_list := '<h2>Tasks Ended with Warning in Java Interface Program:</h2>' || l_warning_tasks_list || '<br>'
            ;

         end if;

         l_end_time := to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss');

         -- create html log file
         l_main_log_file := '<!DOCTYPE html>
                           <html>

                           <head>
                            <title>PLW --> RND Summary Log</title>
                           </head>

                           <body>
                            <div>
                              <h2>Process Report: <br></h2>
                                <table border="1" width="25%">
                                  <tr bgcolor="Silver">
                                     <th><div align="left"> Interface name </div></th>
                                     <th><div align="left"> ' || l_interface_name || '</div></th>
                                  </tr> ' ||
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Envirnoment </div></th>
                                     <th><div align="left"> ' || p_from_env_id || '</div></th>
                                  </tr> ' ||
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Start Time </div></th>
                                     <th><div align="left"> ' || p_start_time || '</div></th>
                                  </tr> ' ||
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> End Time </div></th>
                                     <th><div align="left"> ' || l_end_time || '</div></th>
                                  </tr> ' ||
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Interface Site </div></th>
                                     <th><div align="left"> ' || p_site || '</div></th>
                                  </tr> ' ||
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Interface Status </div></th>
                                     <th><div align="left"> ' || g_interface_status || '</div></th>
                                  </tr>
                                </table>
                            </div>'   ||

                            --************ PS_PROJECT Info ************
                             '<h2>PS_PROJECT INFO:<br></h2>
                                <table border="1" width="25%">
                                 <tr bgcolor="Silver">
                                   <th>Projects</th>
                                   <th>Amount</th>
                                 </tr> ' ||
                                 -- Total
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total Projects (except to be deactivated) </div></th>
                                    <th>' || p_plw_projects || '</th>
                                 </tr> ' ||
                                -- N
                                '<tr bgcolor="White">
                                    <th><div align="left"> Total IL Projects With No Changes </div></th>
                                    <th>' || l_migrated_project_records_n || '</th>
                                 </tr> ' ||
                                -- U
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total IL Projects To Be Updated </div></th>
                                    <th>' || l_migrated_project_records_u || '</th>
                                 </tr> ' ||
                                -- Delta U
                                '<tr bgcolor="White">
                                       <th><div align="left"> Total IL Delta Projects To Be Updated </div></th>
                                       <th>' || l_migrated_prj_records_delta_u || '</th>
                                    </tr> ' ||
                                -- I
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total IL Projects To Be Inserted </div></th>
                                    <th>' || l_migrated_project_records_i || '</th>
                                 </tr> ' ||
                                -- E
                                -- Action E means the projects / tasks do not exist in export_grd,
                                -- but since export_grd_backup was overwritten by export_grd
                                -- there are no records anymore in export_grd_backup to be updated by java interface
                                '<tr bgcolor="White">
                                    <th><div align="left"> Total IL Projects Removed In PLW (migration status unknown) </div></th>
                                    <th>' || l_migrated_project_records_e || '</th>
                                 </tr> ' ||
                                -- Total Successfull
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total IL Successful Projects </div></th>
                                    <th>' || l_total_succ_migrated_projects || '</th>
                                 </tr> ' ||
                                -- Warning
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total IL Warning Projects (migrated) </div></th>
                                    <th>' || l_total_warning_projects || '</th>
                                 </tr> ' ||
                                -- Error
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Total IL Error Projects (not migrated) </div></th>
                                    <th>' || l_total_error_projects || '</th>
                                 </tr> ' ||
                               '</table> ' ||

                                -- List of Error Messages
                                --'<br>' || l_warning_projects_list || '<br>' ||
                                --'<br>' || l_error_projects_list   || '<br>' ||
                                l_warning_projects_list   ||
                                l_error_projects_list     ||

                              --************ PS_TASK Info ************
                              '<h2>PS_TASK INFO:<br></h2>' ||
                               '<table border="1" width="25%">
                                 <tr bgcolor="Silver">
                                    <th>Projects</th>
                                    <th>Amount</th>
                                 </tr> ' ||

                                 -- Total
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total Tasks (except to be deactivated) </div></th>
                                     <th>' || p_plw_tasks || '</th>
                                  </tr> ' ||
                                 -- N
                                 '<tr bgcolor="White">
                                     <th><div align="left"> Total IL Tasks With No Changes </div></th>
                                     <th>' || l_migrated_task_records_n || '</th>
                                  </tr> ' ||
                                 -- U
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total IL Tasks To Be Updated </div></th>
                                     <th>' || l_migrated_task_records_u || '</th>
                                  </tr> ' ||
                                 -- Delta U
                                 '<tr bgcolor="White">
                                     <th><div align="left"> Total IL Delta Tasks To Be Updated </div></th>
                                     <th>' || l_migrated_task_rcords_delta_u || '</th>
                                  </tr> ' ||
                                 -- I
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total IL Tasks To Be Inserted </div></th>
                                     <th>' || l_migrated_task_records_i || '</th>
                                  </tr> ' ||
                                 -- E
                                 -- Action E means the projects / tasks do not exist in export_grd,
                                 -- but since export_grd_backup was overwritten by export_grd
                                 -- there are no records anymore in export_grd_backup to be updated by java interface
                                 '<tr bgcolor="White">
                                     <th><div align="left"> Total IL Tasks Removed In PLW (migration status unknown) </div></th>
                                     <th>' || l_migrated_task_records_e || '</th>
                                  </tr> ' ||
                                 -- Total Successfull
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total IL Successful Tasks </div></th>
                                     <th>' || l_total_succ_migrated_tasks || '</th>
                                  </tr> ' ||
                                 -- Warning
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total IL Warning Tasks (migrated) </div></th>
                                     <th>' || l_total_warning_tasks || '</th>
                                  </tr> ' ||
                                 -- Error
                                 '<tr bgcolor="Gainsboro">
                                     <th><div align="left"> Total IL Error Tasks (not migrated) </div></th>
                                     <th>' || l_total_error_tasks || '</th>
                                  </tr> ' ||
                              '</table> ' ||

                                -- List of Error Messages
                                --'<br>' || l_warning_tasks_list || '<br>' ||
                                --'<br>' || l_error_tasks_list   || '<br>' ||
                                l_warning_tasks_list ||
                                l_error_tasks_list   ||

                             '<br>
                           </body>
                          </html>';

      elsif(p_site = 'GRD-US') then

         count_migrated_projects_tasks(p_site,
                                       --
                                       l_total_succ_migrated_projects,
                                       l_total_error_projects,
                                       l_total_warning_projects,
                                       l_migrated_project_records_n,
                                       l_migrated_project_records_u,
                                       l_migrated_prj_records_delta_u,
                                       l_migrated_project_records_i,
                                       l_migrated_project_records_e,
                                       --
                                       l_total_succ_migrated_tasks,
                                       l_total_error_tasks,
                                       l_total_warning_tasks,
                                       l_migrated_task_records_n,
                                       l_migrated_task_records_u,
                                       l_migrated_task_rcords_delta_u,
                                       l_migrated_task_records_i,
                                       l_migrated_task_records_e);

         -- concatenate error_messages for the failed projects
         l_error_projects_list := null;
         if(l_total_error_projects > 0) then

            /*for r_us_failed_projects in c_us_failed_projects('E') loop

               l_error_projects_list := l_error_projects_list                               ||
                                        '<h4>The following projects:</h4>'                  ||
                                        r_us_failed_projects.projects || '<br>'             ||
                                        '<h4>were completed with the following error:</h4>' ||
                                        r_us_failed_projects.error_message                  ||
                                        '<br>---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------'
               ;

            end loop;*/

            for r_us_failed_project_messages in c_us_failed_project_messages('E') loop

               l_error_projects := null;

               for r_us_failure_projects in c_us_failure_projects(r_us_failed_project_messages.error_message,
                                                                  'E') loop
                  -- 1-st iteration or another error message
                  if(l_error_projects is null) then

                     l_error_projects := r_us_failure_projects.projects;

                  -- another project for the same error_message
                  else

                     l_error_projects := l_error_projects || '<br>' || chr(10) || r_us_failure_projects.projects;

                  end if;

               end loop;

               l_error_projects_list := l_error_projects_list                               ||
                                        '<table border="1" width="95%">
                                           <tr bgcolor="Silver">
                                              <th>Projects</th>
                                              <th>Error Message</th>
                                           </tr>' ||
                                          '<tr bgcolor="White">
                                              <td width="25%"><div align="left">' || l_error_projects || '</div></td>
                                              <td width="75%"><div align="left">' || r_us_failed_project_messages.error_message || '</div></td>
                                           </tr> <tr bgcolor="White">
                                         </table>'
                                         ;

            end loop;

            l_error_projects_list:= '<h2>Projects Ended with Error in Java Interface Program:</h2>' || l_error_projects_list   || '<br>'
            ;

         end if;

         l_warning_projects_list := null;
         if(l_total_warning_projects > 0) then

            /*for r_us_failed_projects in c_us_failed_projects('W') loop

               l_warning_projects_list := l_warning_projects_list                             ||
                                        '<h4>The following projects:</h4>'                    ||
                                        r_us_failed_projects.projects || '<br>'               ||
                                        '<h4>were completed with the following warning:</h4>' ||
                                        r_us_failed_projects.error_message                    ||
                                        '<br>---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------
                                             ---------------------------------------'
               ;

            end loop;*/

            for r_us_failed_project_messages in c_us_failed_project_messages('W') loop

               l_warning_projects := null;

               for r_us_failure_projects in c_us_failure_projects(r_us_failed_project_messages.error_message,
                                                                  'W') loop
                  -- 1-st iteration or another error message
                  if(l_warning_projects is null) then

                     l_warning_projects := r_us_failure_projects.projects;

                  -- another project for the same error_message
                  else

                     l_warning_projects := l_warning_projects || '<br>' || chr(10) || r_us_failure_projects.projects;

                  end if;

               end loop;

               l_warning_projects_list := l_warning_projects_list ||
                                          '<table border="1" width="95%">
                                             <tr bgcolor="Silver">
                                                <th>Projects</th>
                                                <th>Error Message</th>
                                             </tr>' ||
                                            '<tr bgcolor="White">
                                                <td width="25%"><div align="left">' || l_warning_projects || '</div></td>
                                                <td width="75%"><div align="left">' || r_us_failed_project_messages.error_message || '</div></td>
                                             </tr> <tr bgcolor="White">
                                           </table>'
                                           ;

            end loop;

            l_warning_projects_list := '<h2>Projects Ended with Warning in Java Interface Program:</h2>' || l_warning_projects_list   || '<br>'
            ;

         end if;

         -- concatenate error_messages for the failed tasks
         l_error_tasks_list := null;
         if(l_total_error_tasks > 0) then

            /*for r_us_failed_tasks in c_us_failed_tasks('E') loop

               l_error_tasks_list := l_error_tasks_list                                  ||
                                     '<h4>The following projects:</h4>'                  ||
                                     r_us_failed_tasks.projects || '<br>'                ||
                                     '<h4>were completed with the following error:</h4>' ||
                                     r_us_failed_tasks.error_message                     ||
                                     '<br>---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------'
               ;

            end loop;*/

            for r_us_failed_task_messages in c_us_failed_task_messages('E') loop

               l_error_tasks := null;

               for r_us_failure_tasks in c_us_failure_tasks(r_us_failed_task_messages.error_message,
                                                            'E') loop
                  -- 1-st iteration or another error message
                  if(l_error_tasks is null) then

                     l_error_tasks := r_us_failure_tasks.tasks;

                  -- another project for the same error_message
                  else

                     l_error_tasks := l_error_tasks || '<br>' || chr(10) || r_us_failure_tasks.tasks;

                  end if;

               end loop;

               l_error_tasks_list := l_error_tasks_list                               ||
                                        '<table border="1" width="95%">
                                           <tr bgcolor="Silver">
                                              <th>Tasks</th>
                                              <th>Error Message</th>
                                           </tr>' ||
                                          '<tr bgcolor="White">
                                              <td width="30%"><div align="left">' || l_error_tasks || '</div></td>
                                              <td width="70%"><div align="left">' || r_us_failed_task_messages.error_message || '</div></td>
                                           </tr> <tr bgcolor="White">
                                         </table>'
                                         ;

            end loop;

            l_error_tasks_list := '<h2>Tasks Ended with Error in Java Interface Program:</h2>' || l_error_tasks_list || '<br>'
            ;

         end if;

         l_warning_tasks_list := null;
         if(l_total_warning_tasks > 0) then

            /*for r_us_failed_tasks in c_us_failed_tasks('W') loop

               l_warning_tasks_list := l_warning_tasks_list                                ||
                                     '<h4>The following projects:</h4>'                    ||
                                     r_us_failed_tasks.projects || '<br>'                  ||
                                     '<h4>were completed with the following warning:</h4>' ||
                                     r_us_failed_tasks.error_message                       ||
                                     '<br>---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------
                                          ---------------------------------------'
               ;

            end loop;*/

            for r_us_failed_task_messages in c_us_failed_task_messages('W') loop

               l_warning_tasks := null;

               for r_us_failure_tasks in c_us_failure_tasks(r_us_failed_task_messages.error_message,
                                                            'W') loop
                  -- 1-st iteration or another error message
                  if(l_warning_tasks is null) then

                     l_warning_tasks := r_us_failure_tasks.tasks;

                  -- another project for the same error_message
                  else

                     l_warning_tasks := l_warning_tasks || '<br>' || chr(10) || r_us_failure_tasks.tasks;

                  end if;

               end loop;

               l_warning_tasks_list := l_warning_tasks_list                               ||
                                        '<table border="1" width="95%">
                                           <tr bgcolor="Silver">
                                              <th>Tasks</th>
                                              <th>Error Message</th>
                                           </tr>' ||
                                          '<tr bgcolor="White">
                                              <td width="30%"><div align="left">' || l_warning_tasks || '</div></td>
                                              <td width="70%"><div align="left">' || r_us_failed_task_messages.error_message || '</div></td>
                                           </tr> <tr bgcolor="White">
                                         </table>'
                                         ;

            end loop;

            l_warning_tasks_list := '<h2>Tasks Ended with Warning in Java Interface Program:</h2>' || l_warning_tasks_list || '<br>'
            ;

         end if;

         l_end_time := to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss');

         l_main_log_file := '<!DOCTYPE html>
                           <html>

                           <head>
                            <title>PLW --> RND Summary Log</title>
                           </head>

                           <body>
                            <div>
                             <h2>Process Report: <br></h2>
                               <table border="1" width="25%">
                                 <tr bgcolor="Silver">
                                    <th><div align="left"> Interface name </div></th>
                                    <th><div align="left"> ' || l_interface_name || '</div></th>
                                 </tr> ' ||
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Envirnoment </div></th>
                                    <th><div align="left"> ' || p_from_env_id || '</div></th>
                                 </tr> ' ||
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Start Time </div></th>
                                    <th><div align="left"> ' || p_start_time || '</div></th>
                                 </tr> ' ||
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> End Time </div></th>
                                    <th><div align="left"> ' || l_end_time || '</div></th>
                                 </tr> ' ||
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Interface Site </div></th>
                                    <th><div align="left"> ' || p_site || '</div></th>
                                 </tr> ' ||
                                '<tr bgcolor="Gainsboro">
                                    <th><div align="left"> Interface Status </div></th>
                                    <th><div align="left"> ' || g_interface_status || '</div></th>
                                 </tr>
                               </table>
                             </div>'   ||

                             --************ PS_PROJECT Info ************
                            '<h2>PS_PROJECT INFO:<br></h2>
                                <table border="1" width="25%">
                                   <tr bgcolor="Silver">
                                      <th>Projects</th>
                                      <th>Amount</th>
                                   </tr> ' ||
                                  -- Total
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total Projects (except to be deactivated) </div></th>
                                      <th>' || p_plw_projects || '</th>
                                   </tr> ' ||
                                  -- N
                                  '<tr bgcolor="White">
                                      <th><div align="left"> Total US Projects With No Changes </div></th>
                                      <th>' || l_migrated_project_records_n || '</th>
                                   </tr> ' ||
                                  -- U
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Projects To Be Updated </div></th>
                                      <th>' || l_migrated_project_records_u || '</th>
                                   </tr> ' ||
                                  -- Delta U
                                '<tr bgcolor="White">
                                       <th><div align="left"> Total US Delta Projects To Be Updated </div></th>
                                       <th>' || l_migrated_prj_records_delta_u || '</th>
                                    </tr> ' ||
                                  -- I
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Projects To Be Inserted </div></th>
                                      <th>' || l_migrated_project_records_i || '</th>
                                   </tr> ' ||
                                  -- E
                                  -- Action E means the projects / tasks do not exist in export_grd,
                                  -- but since export_grd_backup was overwritten by export_grd
                                  -- there are no records anymore in export_grd_backup to be updated by java interface
                                  '<tr bgcolor="White">
                                      <th><div align="left"> Total US Projects Removed In PLW (migration status unknown) </div></th>
                                      <th>' || l_migrated_project_records_e || '</th>
                                   </tr> ' ||
                                  '<br>' ||
                                  -- Total Successfull
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Successful Projects </div></th>
                                      <th>' || l_total_succ_migrated_projects || '</th>
                                   </tr> ' ||
                                  -- Warning
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Warning Projects (migrated) </div></th>
                                      <th>' || l_total_warning_projects || '</th>
                                   </tr> ' ||
                                  -- Error
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Error Projects (not migrated) </div></th>
                                      <th>' || l_total_error_projects || '</th>
                                   </tr> ' ||
                               '</table> ' ||

                                -- List of Error Messages
                                --'<br>' || l_warning_projects_list || '<br>' ||
                                --'<br>' || l_error_projects_list   || '<br>' ||
                                l_warning_projects_list ||
                                l_error_projects_list   ||


                                --************ PS_TASK Info ************
                               '<h2>PS_TASK INFO:<br></h2>
                                <table border="1" width="25%">
                                  <tr bgcolor="Silver">
                                     <th>Projects</th>
                                     <th>Amount</th>
                                  </tr> ' ||

                                  -- Total
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total Tasks (except to be deactivated) </div></th>
                                      <th>' || p_plw_tasks || '</th>
                                   </tr> ' ||
                                  -- N
                                  '<tr bgcolor="White">
                                      <th><div align="left"> Total US Tasks With No Changes </div></th>
                                      <th>' || l_migrated_task_records_n || '</th>
                                   </tr> ' ||
                                  -- U
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Tasks To Be Updated </div></th>
                                      <th>' || l_migrated_task_records_u || '</th>
                                   </tr> ' ||
                                  -- Delta U
                                 '<tr bgcolor="White">
                                     <th><div align="left"> Total US Delta Tasks To Be Updated </div></th>
                                     <th>' || l_migrated_task_rcords_delta_u || '</th>
                                  </tr> ' ||
                                  -- I
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Tasks To Be Inserted </div></th>
                                      <th>' || l_migrated_task_records_i || '</th>
                                   </tr> ' ||
                                  -- E
                                  -- Action E means the projects / tasks do not exist in export_grd,
                                  -- but since export_grd_backup was overwritten by export_grd
                                  -- there are no records anymore in export_grd_backup to be updated by java interface
                                  '<tr bgcolor="White">
                                      <th><div align="left"> Total US Tasks Removed In PLW (migration status unknown) </div></th>
                                      <th>' || l_migrated_task_records_e || '</th>
                                   </tr> ' ||
                                  '<br>' ||
                                  -- Total Successfull
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Successful Tasks </div></th>
                                      <th>' || l_total_succ_migrated_tasks || '</th>
                                   </tr> ' ||
                                  -- Warning
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Warning Tasks (migrated) </div></th>
                                      <th>' || l_total_warning_tasks || '</th>
                                   </tr> ' ||
                                  -- Error
                                  '<tr bgcolor="Gainsboro">
                                      <th><div align="left"> Total US Error Tasks (not migrated) </div></th>
                                      <th>' || l_total_error_tasks || '</th>
                                   </tr> ' ||
                               '</table> ' ||

                                -- List of Error Messages
                                l_warning_tasks_list ||
                                l_error_tasks_list   ||
                           '</body>
                          </html>';

      end if;

      -- insert log file into a table for BPEL fetching
      begin

         -- delete log files a month old
         delete from ps_log t
         where  t.date_of_log < trunc(sysdate) - 31
         ;


         -- delete already generated report for the same combination of parameters' values
         delete from ps_log t
         where  t.gpms_exp_id  = p_gpms_exp_id
           and  t.date_of_log  = trunc(sysdate)
           and  t.dev_site     = p_site
           and  t.log_type     = l_log_type
           and  t.report_level = 'ALL'
         ;

         insert into ps_log(gpms_exp_id,
                            date_of_log,
                            dev_site,
                            log_type,
                            report_level,
                            log_file)
         values(p_gpms_exp_id,
                trunc(sysdate),
                p_site,
                l_log_type,
                'ALL',
                l_main_log_file)
         ;

         -- insert error report only for CREATE_DELTA_PROJECTS_TASKS run (all sites)
         if(p_site = 'ALL') then

            -- delete already generated report for the same combination of parameters' values
            delete from ps_log t
            where  t.gpms_exp_id  = p_gpms_exp_id
              and  t.date_of_log  = trunc(sysdate)
              and  t.dev_site     = p_site
              and  t.log_type     = l_error_log_type
              and  t.report_level = 'ALL'
            ;

            insert into ps_log(gpms_exp_id,
                               date_of_log,
                               dev_site,
                               log_type,
                               report_level,
                               log_file)
            values(p_gpms_exp_id,
                   trunc(sysdate),
                   p_site,
                   l_error_log_type,
                   'ALL',
                   l_error_log_file)
            ;

         end if;

         commit;

      exception
         when others then
            rollback;

      end;

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_print_log;

   end print_log;

   ------------------------------------------------
   -- report_updated_records
   ------------------------------------------------
   procedure report_updated_records(p_gpms_exp_id      number,
                                    p_dev_site_region  varchar2,
                                    p_start_time       varchar2) is

      l_project_setup_fields_list_p  varchar2(4000);
      l_project_setup_fields_list_b  varchar2(4000);
      l_project_query                clob;
      l_project_report_program       /*long; --varchar2(32000); --*/clob;
      l_project_if                   clob;
      --
      l_task_setup_fields_list_p     varchar2(4000);
      l_task_setup_fields_list_b     varchar2(4000);
      l_task_query                   clob;
      l_task_report_program          clob;
      l_task_if                      clob;
      --
      l_null_date_p                  varchar2(50) := 'to_date(''01011900'',''ddmmyyyy'')';
      l_null_date_b                  varchar2(50) := 'to_date(''02021900'',''ddmmyyyy'')';
      l_null_char_p                  varchar2(11) := '''teva-9999''';
      l_null_char_b                  varchar2(11) := '''teva-8888''';
      --
      l_interface_name               varchar2(50) := 'PLW -> GRD Interface Program Finished';
      l_end_time                     varchar2(30);

      -- clob issue - varray solution --
      /*l_project_report_program_lngth number;
      l_project_report_program_max_l number := 32000;
      l_varray_amount_of_elements    number;
      l_project_report_program_floor number;
      l_project_report_program_mod   number;
      --
      type att_type                  is varray(32000) of varchar2(32000);
      l_arr_list                     att_type := att_type();
      l_i                            number := 1;*/

      -- definition due to 10g clob execution failure
      l_project_report_program_lngth number;
      c_project_report_program       integer;
      l_project_report_program_array dbms_sql.varchar2s; -- type varchar2s is table of varchar2(256) index by binary_integer;
      l_dbms_execute_result          number;

      cursor c_project_setup_fields is
         select *
         from   ps_env_field_vw t
         where  env_id       =  'GRD'
           and  level_name   =  'PROJECT'
           --and  t.field_name <> 'PRJ_ID'
           and  t.uk_part    = 0
         ;

      cursor c_task_setup_fields is
         select *
         from   ps_env_field_vw t
         where  env_id       =  'GRD'
           and  level_name   =  'TASK'
           --and  t.field_name <> 'PRJ_ID'
           and  t.uk_part    = 0
         ;

   begin

      -- concatenate ps_project fields from the setup table for dynamic select
      for r_project_setup_fields in c_project_setup_fields loop

         -- for projects imported from export_grd with env_activity = 10
         if(l_project_setup_fields_list_p is null) then

            l_project_setup_fields_list_p := 'p.' || r_project_setup_fields.field_name || ' p_' || substr(r_project_setup_fields.field_name, 1, 28);

         else

            l_project_setup_fields_list_p := l_project_setup_fields_list_p || ', ' ||
                                     'p.' || r_project_setup_fields.field_name || ' p_' || substr(r_project_setup_fields.field_name, 1, 28);

         end if;

         -- for projects imported from export_grd_backup with env_activity = 22
         if(l_project_setup_fields_list_b is null) then

            l_project_setup_fields_list_b := 'b.' || r_project_setup_fields.field_name || ' b_' || substr(r_project_setup_fields.field_name, 1, 28);

         else

            l_project_setup_fields_list_b := l_project_setup_fields_list_b || ', ' ||
                                             'b.' || r_project_setup_fields.field_name || ' b_' || substr(r_project_setup_fields.field_name, 1, 28);

         end if;

      end loop;

      -- concatenate ps_task fields from the setup table for dynamic select
      for r_task_setup_fields in c_task_setup_fields loop

         -- for projects imported from export_grd with env_activity = 10
         if(l_task_setup_fields_list_p is null) then

            l_task_setup_fields_list_p := 'p.' || r_task_setup_fields.field_name || ' p_' || substr(r_task_setup_fields.field_name, 1, 28);

         else

            l_task_setup_fields_list_p := l_task_setup_fields_list_p || ', ' ||
                                     'p.' || r_task_setup_fields.field_name || ' p_' || substr(r_task_setup_fields.field_name, 1, 28);

         end if;

         -- for projects imported from export_grd_backup with env_activity = 22
         if(l_task_setup_fields_list_b is null) then

            l_task_setup_fields_list_b := 'b.' || r_task_setup_fields.field_name || ' b_' || substr(r_task_setup_fields.field_name, 1, 28);

         else

            l_task_setup_fields_list_b := l_task_setup_fields_list_b || ', ' ||
                                          'b.' || r_task_setup_fields.field_name || ' b_' || substr(r_task_setup_fields.field_name, 1, 28);

         end if;

      end loop;

      -- build dynamic query
      if(p_dev_site_region = 'ALL') then

         l_project_query := 'select ' || l_project_setup_fields_list_p || ', '           ||
                                         l_project_setup_fields_list_b || ' '            ||
                            'from   ps_project p, '                                      ||
                                   'ps_project b '                                       ||
                            'where  p.prj_id          = b.prj_id '                       ||
                              'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                              'and  p.env_activity_id = 10 '                             ||
                              'and  b.env_activity_id = 22 '                             ||
                              'and  exists(select 1 '                                    ||
                                          'from   ps_project t '                         ||
                                          'where  t.prj_id          = p.prj_id '         ||
                                            'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                            'and  t.action          = ''U'' '            ||
                                          ') '                                           ||
                              'and ('                                                    ||
                                    -- IL Sites
                                    '(p.region   in(''EU'', ''IL'', ''JP'') and '        ||
                                     'p.development_site in(''BD'', ''NA'')) or '        ||
                                     -- for country projects (INT, LATAM) region is not transfered
                                    '(nvl(p.region, ''-999'') not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and ' ||
                                     'p.development_site in(''BD'', ''NA'')) or '        ||
                                    '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'','   ||
                                                           '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'','   ||
                                                           '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')) or '    ||
                                    -- US Sites
                                    '(p.region            in(''US'', ''CA'')  and '      ||
                                     'p.development_site  in(''BD'', ''NA'')) or '       ||
                                    '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                            '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                            '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                            'order by p.gpms_id, '                                       ||
                                     'p.plw_pjt_id, '                                    ||
                                     'p.plw_int_number, '                                ||
                                     'p.env_activity_id'
                            ;

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           'and ('                                             ||
                                 -- IL Sites
                                 '(p.region   in(''EU'', ''IL'', ''JP'', ''INT'', ''LATAM'') and ' ||
                                  'p.development_site in(''BD'', ''NA'')) or ' ||
                                  -- for country projects (INT, LATAM) region is not transfered
                                 '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'','   ||
                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'','   ||
                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')) or '    ||
                                 -- US Sites
                                 '(p.region            in(''US'', ''CA'')  and ' ||
                                  'p.development_site  in(''BD'', ''NA'')) or '  ||
                                 '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                         '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                         '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      elsif(p_dev_site_region = 'GRD-IL') then

         l_project_query := 'select distinct q.* '                                                ||
                            'from   ( '                                                           ||
                                     -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'' '            ||
                                       -- join to export_grd
                                       --'and  nvl(p.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(p.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(p.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- region projects
                                       'and  (' || -- LIVE(CA), PRAM
                                             '(upper(e.region)      in(''EU'', ''CA''))  or '     ||
                                              -- US
                                              'upper(e.region)      = ''US'' or '                 ||
                                              -- IL
                                             '(upper(e.region)      = ''EMIA'' and '              ||
                                              'e.country            =  ''IL'')  or '              ||
                                              -- JP
                                             '(upper(e.region)      = ''ASIA'' and '              ||
                                              'e.country            = ''JP'')) '                  ||
                                       -- dev_site = GRD-IL
                                       'and  ((p.region             in(''EU'', ''IL'', ''JP'') and ' ||
                                              'p.development_site   in(''BD'', ''NA''))        or '  ||
                                       -- for country projects (INT, LATAM) region is not transfered
                                             '(nvl(p.region, ''-999'')  not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and ' ||
                                              'p.development_site     in(''BD'', ''NA''))  or '   ||
                                             '(p.development_site     in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'', ' ||
                                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'', ' ||
                                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')))) '    ||
                                     'union all ' ||
                                     -- country projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e, '                                     ||
                                            'ps_task     t '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- join to export_grd, ps_task
                                       'and  p.project_id                  = t.project_id '       ||
                                       --'and  nvl(t.gpms_id, ''-999'')      = nvl(e.gpms_id, ''-999'') '          ||
                                       'and  nvl(t.plw_pjt_id, ''-999'')   = nvl(e.plw_pjt_id, ''-999'') '       ||
                                       'and  nvl(t.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- country projects
                                       'and  upper(e.region) in(''EMIA'', ''ASIA'', ''LATAM'') '  ||
                                       'and  e.country       not in(''JP'',''IL'') '              ||
                                       -- dev_site = GRD-IL
                                       'and  ((p.region                in(''EU'', ''IL'', ''JP'') and ' ||
                                              'p.development_site      in(''BD'', ''NA'')) or '         ||
                                             -- for country projects (INT, LATAM) region is not transfered
                                             '(nvl(p.region, ''-999'') not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and '   ||
                                              'p.development_site      in(''BD'', ''NA'')) or '   ||
                                             '(p.development_site      in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'', ' ||
                                                                         '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'', ' ||
                                                                         '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG''))) '     ||

                                       ') q '                                                     ||
                            'order by q.p_gpms_id, '                                              ||
                                     'q.p_plw_pjt_id, '                                           ||
                                     'q.p_plw_int_number'
                            ;

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           -- IL Sites
                           'and ((p.region           in(''EU'', ''IL'', ''JP'', ''INT'', ''LATAM'') and ' ||
                                 'p.development_site in(''BD'', ''NA'')) or '  ||
                                  -- for country projects (INT, LATAM) region is not transfered
                                 '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'',' ||
                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'',' ||
                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG''))) '    ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      elsif(p_dev_site_region = 'GRD-US') then

         l_project_query := 'select distinct q.* '                                                ||
                            'from   ( '                                                           ||
                                     -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- join to export_grd
                                       --'and  nvl(p.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(p.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(p.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- region projects
                                       'and  ( ' || -- LIVE(CA), PRAM
                                             '(upper(e.region)     in(''EU'', ''CA'')) or '       ||
                                              -- US
                                              'upper(e.region)     = ''US''    or '               ||
                                              -- IL
                                              '(upper(e.region)    = ''EMIA''  and '              ||
                                               'e.country          = ''IL'')   or '               ||
                                              -- JP
                                              '(upper(e.region)    = ''ASIA''  and '              ||
                                               'e.country          = ''JP'')) '                   ||
                                       -- dev_site = GRD-US
                                       'and  ((p.region            in(''US'', ''CA'')  and '      ||
                                              'p.development_site  in(''BD'', ''NA'')) or '       ||
                                             '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                                     '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                                     '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                                     'union all ' ||

                                    -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e, '                                     ||
                                            'ps_task     t '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- country projects
                                       'and  upper(e.region)   in(''EMIA'', ''ASIA'', ''LATAM'') ' ||
                                       'and  e.country         not in(''JP'',''IL'') '            ||
                                       -- join to ps_task, export_grd
                                       'and  p.project_id                    = t.project_id '                    ||
                                       --'and  nvl(t.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(t.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(t.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- dev_site = GRD-US
                                       'and  ((p.region            in(''US'', ''CA'') and '       ||
                                              'p.development_site  in(''BD'', ''NA'')) or '       ||
                                             '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                                             '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                                             '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) ' ||

                                       ') q '                                                     ||
                            'order by q.p_gpms_id, '                                              ||
                                     'q.p_plw_pjt_id, '                                           ||
                                     'q.p_plw_int_number'
                            ;

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           -- US Sites
                           'and ((p.region            in(''US'', ''CA'')  and ' ||
                                 'p.development_site  in(''BD'', ''NA'')) or '  ||
                                '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                        '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                        '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      end if;

      -- concatenate comparison by all PROJECT fields from the setup table for dynamic anonymous block
      for r_project_setup_fields in c_project_setup_fields loop

         if(r_project_setup_fields.field_type in ('NUMBER', 'FLOAT')) then

            l_project_if := l_project_if || chr(10) ||

                    'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', -9999) <> '    ||
                       'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', -8888) and '   ||
                       -- just one can be null
                       '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in '                              ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_project_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         elsif(r_project_setup_fields.field_type = 'DATE') then

            l_project_if := l_project_if || chr(10) ||

                    'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_date_p || ') <> '  ||
                       'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_date_b || ') and ' ||
                       -- just one can be null
                       '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_project_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         else

            l_project_if := l_project_if || chr(10) ||
                            'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_char_p || ') <> '  ||
                               'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_char_b || ') and ' ||
                               -- just one can be null
                               '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                                'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                               -- do not compare Country Projects by
                               -- GAIN, GLOBAL_DOSAGE_FORM, PROJECT_RATIONALE, REGION, DEVELOPMENT_SITE (all of varchar2 data type)
                               /*'((r_project_updated_records.p_region in (''US'', ''CA'', ''IL'', ''EU'', ''JP'')) or '                     ||
                               ' (nvl(r_project_updated_records.p_region, ''-999'') not in (''US'', ''CA'', ''IL'', ''EU'', ''JP'') and '  ||
                                 chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in '                                     ||
                                       '(''GAIN'', ''GLOBAL_DOSAGE_FORM'', ''PROJECT_RATIONALE'', ''REGION'', ''DEVELOPMENT_SITE'')) and ' ||*/
                               -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                               chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                                    '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                               'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                                       chr(39) ||
                                                       '<tr bgcolor="White">
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                            <td>' || r_project_setup_fields.field_name || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                         </tr> ' ||
                                                       chr(39) || '; '                                                         ||

                            'end if; '
                            ;

         end if;

      end loop;

      -- concatenate comparison by all TASK fields from the setup table for dynamic anonymous block
      for r_task_setup_fields in c_task_setup_fields loop

         if(r_task_setup_fields.field_type in ('NUMBER', 'FLOAT')) then

            l_task_if := l_task_if || chr(10) ||

                    'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', -9999) <> '    ||
                       'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', -8888) and '   ||
                       -- just one can be null
                       '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_task_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         elsif(r_task_setup_fields.field_type = 'DATE') then

            l_task_if := l_task_if || chr(10) ||

                    'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_date_p || ') <> '  ||
                       'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_date_b || ') and ' ||
                       -- just one can be null
                       '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_task_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         else

            l_task_if := l_task_if || chr(10) ||
                            'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_char_p || ') <> '  ||
                               'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_char_b || ') and ' ||
                               -- just one can be null
                               '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                                'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                               -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                               chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                                    '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                               'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                                       chr(39) ||
                                                       '<tr bgcolor="White">
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                            <td>' || r_task_setup_fields.field_name || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                         </tr> ' ||
                                                       chr(39) || '; '                                                         ||

                            'end if; '
                            ;

         end if;

      end loop;

      -- build dynamic program
      l_project_report_program := 'declare '                                                            ||

                                     'l_gpms_exp_id               number := ' || p_gpms_exp_id || '; '  ||
                                     'l_dev_site_region           varchar2(6) := ' || chr(39) || p_dev_site_region || chr(39) || '; ' ||
                                     'l_log_type                  varchar2(7) := ''Delta''; '           ||

                                     'l_log_title                 varchar2(700); '                      ||
                                     'l_log_header                varchar2(500); '                      ||
                                     'l_log_end                   varchar2(100) := '' </body></html>''; '   ||

                                     'l_dynamic_project_h3        varchar2(20) := ''<h2>Projects:</h2>''; ' ||
                                     'l_dynamic_task_h3           varchar2(20) := ''<h2>Tasks:</h2>''; '    ||
                                     'l_no_delta_found_message    varchar2(32) := ''<h2>No Delta Data was Found</h2>''; ' ||

                                     'l_dynamic_table_header      clob; '                               ||
                                     'l_dynamic_table_content     clob; '                               ||
                                     'l_dynamic_table_prj_content clob; '                               ||
                                     'l_dynamic_table_tsk_content clob; '                               ||
                                     'l_dynamic_table_row         clob; '                               ||

                                     'l_dynamic_table_end         varchar2(10) := ''</table>''; '       ||

                                     'l_gpms_id                   varchar2(20); '                       ||
                                     'l_plw_pjt_id                varchar2(20); '                       ||
                                     'l_plw_int_number            varchar2(20); '                       ||

                                     'cursor c_project_updated_records is '                             ||
                                         l_project_query || '; ' || chr(10) || chr(10)                  ||

                                     'cursor c_task_updated_records is '                                ||
                                         l_task_query || '; ' || chr(10) || chr(10)                     ||


                                  'begin '                                                              ||

                                     /*'l_log_title := ' || chr(39)                                      ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                           chr(39) || '; '                              ||*/

                                     'l_log_title := ' || chr(39)                                       ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                                  '<div>'                               ||
                                                                   '<h2>Process Report: <br></h2>'      ||

                                                                      '<table border="1" width="25%">'  ||
                                                                        '<tr bgcolor="White">'          ||
                                                                           '<th><div align="left"> Interface name </div></th>'             ||
                                                                           '<th><div align="left">' || l_interface_name || '</div></th>'   ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Start Time </div></th>'                 ||
                                                                           '<th><div align="left">' || p_start_time  || '</div></th>'      ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> End Time </div></th>'                   ||
                                                                           '<th><div align="left">' || to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss') || '</div></th>'            ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Interface Site </div></th>'             ||
                                                                           '<th><div align="left">' || p_dev_site_region || '</div></th>'  ||
                                                                        '</tr>'                                                            ||
                                                                      '</table>'                                                           ||
                                                                  '</div>' ||
                                                           chr(39) || '; '                                                                 ||

                                     'l_log_header := ' || chr(39)                                      ||
                                                            '<h2>The List of Changed Fields:<br></h2> ' ||
                                                           chr(39) || '; '                              ||

                                     'l_dynamic_table_header := ' || chr(39)                            ||
                                                                    '<table border="1" width="95%">'    ||
                                                                      '<tr bgcolor="Silver">'           ||
                                                                         '<th>GPMS_ID</th>'             ||
                                                                         '<th>PLW_PJT_ID</th>'          ||
                                                                         '<th>PLW_INT_NUMBER</th>'      ||
                                                                         '<th>FIELD NAME</th>'          ||
                                                                         '<th>PREVIOUS FIELD VALUE</th>' ||
                                                                         '<th>CURRENT FIELD VALUE</th>' ||
                                                                       '</tr> '                         ||
                                                                     chr(39) || '; '                    ||

                                     -- ps_project
                                     'for r_project_updated_records in c_project_updated_records loop ' ||

                                        'l_dynamic_table_row := null; '                                 ||

                                        l_project_if                                                    ||

                                        'if(l_dynamic_table_row is not null) then '                     ||

                                           'l_dynamic_table_prj_content := l_dynamic_table_prj_content || ' ||
                                                                           'l_dynamic_table_header || ' ||
                                                                           'l_dynamic_table_row || '    ||
                                                                           'l_dynamic_table_end; '      ||

                                        'end if; '                                                      ||

                                        'l_gpms_id        := r_project_updated_records.p_gpms_id; '     ||
                                        'l_plw_pjt_id     := r_project_updated_records.p_plw_pjt_id; '  ||
                                        'l_plw_int_number := r_project_updated_records.p_plw_int_number; ' ||

                                     'end loop; '                                                       ||

                                     -- ps_task
                                     'for r_task_updated_records in c_task_updated_records loop '       ||

                                        'l_dynamic_table_row := null; '                                 ||

                                        l_task_if                                                       ||

                                        'if(l_dynamic_table_row is not null) then '                     ||

                                           'l_dynamic_table_tsk_content := l_dynamic_table_tsk_content || ' ||
                                                                          'l_dynamic_table_header || '  ||
                                                                          'l_dynamic_table_row || '     ||
                                                                          'l_dynamic_table_end; '       ||

                                        'end if; '                                                      ||

                                        'l_gpms_id        := r_task_updated_records.p_gpms_id; '        ||
                                        'l_plw_pjt_id     := r_task_updated_records.p_plw_pjt_id; '     ||
                                        'l_plw_int_number := r_task_updated_records.p_plw_int_number; ' ||

                                     'end loop; '                                                       ||

                                     'if(l_dynamic_table_prj_content is not null) then '                ||

                                        'l_dynamic_table_content := l_dynamic_project_h3 || '           ||
                                                                   'l_dynamic_table_prj_content; '      ||

                                     'end if; '                                                         ||

                                     'if(l_dynamic_table_tsk_content is not null) then '                ||

                                        'l_dynamic_table_content := l_dynamic_table_content || '        ||
                                                                   'l_dynamic_task_h3 || '              ||
                                                                   'l_dynamic_table_tsk_content; '      ||

                                     'end if; '                                                         ||

                                     'if(l_dynamic_table_content is not null) then '                    ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_log_header                        ||
                                                                    l_dynamic_table_content             ||
                                                                    l_log_end; '                        ||
                                     'else '                                                            ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_no_delta_found_message            ||
                                                                    l_log_end; '                        ||
                                     'end if; '                                                         ||

                                     -- insert log file into a table for BPEL fetching
                                     'begin '                                                           ||

                                        'delete from ps_log t '                                         ||
                                        'where  t.gpms_exp_id  = l_gpms_exp_id '                        ||
                                          'and  t.date_of_log  = trunc(sysdate) '                       ||
                                          'and  t.dev_site     = l_dev_site_region '                    ||
                                          'and  t.log_type     = l_log_type '                           ||
                                          'and  t.report_level = ''ALL''; '                             ||

                                        'insert into ps_log(gpms_exp_id, '                              ||
                                                           'date_of_log, '                              ||
                                                           'dev_site, '                                 ||
                                                           'log_type, '                                 ||
                                                           'report_level, '                             ||
                                                           'log_file) '                                 ||
                                        'values(l_gpms_exp_id, '                                        ||
                                               'trunc(sysdate), '                                       ||
                                               'l_dev_site_region, '                                    ||
                                               'l_log_type, '                                           ||
                                               '''ALL'', '                                              ||
                                               'l_dynamic_table_content); '                             ||

                                        'commit; '                                                      ||

                                     'exception '                                                       ||
                                        'when others then '                                             ||
                                           'rollback; '                                                 ||

                                     'end; ' ||

                                  'end;'
                                  ;

      -------------------------------------------------------------------------------------------------------------
      --                                    for 11g - just execute immediate                                     --
      -------------------------------------------------------------------------------------------------------------
      --execute immediate l_project_report_program;

      /*insert into oleg(sql_stmnt)
      values (l_project_report_program);
      commit;*/

      -------------------------------------------------------------------------------------------------------------
      -- for 10g execute immediate a variable of data type clob is impossible, that's why use the solution below --
      -------------------------------------------------------------------------------------------------------------

      ------------------------ solution 1: splitting clob to varray elements ------------------------
      /*l_project_report_program_lngth := length(l_project_report_program);

      select floor(l_project_report_program_lngth / l_project_report_program_max_l) +
                   decode(mod(l_project_report_program_lngth, l_project_report_program_max_l), 0, 0, 1),
             floor(l_project_report_program_lngth / l_project_report_program_max_l),
             mod(l_project_report_program_lngth, l_project_report_program_max_l)
      into   l_varray_amount_of_elements,
             l_project_report_program_floor,
             l_project_report_program_mod
      from   dual
      ;

      for i in 1..l_varray_amount_of_elements loop

         if(i <= l_project_report_program_floor) then

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_max_l);

         else

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_mod);

         end if;

         l_i := l_i + l_project_report_program_max_l;

         \*insert into OLEG3(ACTION,
                       SQL_STMNT)
         values(i,
                l_arr_list(i)
                )
         ;
         commit;*\

      end loop;

      if(l_varray_amount_of_elements = 1) then

         execute immediate l_arr_list(1);

      elsif(l_varray_amount_of_elements = 2) then

         execute immediate l_arr_list(1) || l_arr_list(2);

      elsif(l_varray_amount_of_elements = 3) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3);

      elsif(l_varray_amount_of_elements = 4) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4);

      elsif(l_varray_amount_of_elements = 5) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5);

      elsif(l_varray_amount_of_elements = 6) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6);

      elsif(l_varray_amount_of_elements = 7) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7);

      elsif(l_varray_amount_of_elements = 8) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8);

      elsif(l_varray_amount_of_elements = 9) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9);

      elsif(l_varray_amount_of_elements = 10) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10);

      elsif(l_varray_amount_of_elements = 11) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11);

      elsif(l_varray_amount_of_elements = 12) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12);

      elsif(l_varray_amount_of_elements = 13) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13);

      elsif(l_varray_amount_of_elements = 14) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14);

      elsif(l_varray_amount_of_elements = 15) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14)|| l_arr_list(15);

      end if;*/

      ------------------------ solution 2: dbms_sql.execute ------------------------

      -- length in batches of 256 chars each
      l_project_report_program_lngth := ceil(dbms_lob.getlength(l_project_report_program) / 256);

      -- fill the array by batches of length 256 each
      for i in 1 .. l_project_report_program_lngth loop

         l_project_report_program_array(i) := dbms_lob.substr(l_project_report_program,
                                                              256,                 -- batch length
                                                              ((i - 1) * 256) + 1) -- 1-st char of each batch
                                                              ;
      end loop;

      c_project_report_program := dbms_sql.open_cursor;

      -- parse array members to prepare the statement for execution
      dbms_sql.parse(c_project_report_program,       -- whether cursor is open
                     l_project_report_program_array, -- entire anonymous block
                     1,                              -- 1-st batch number
                     l_project_report_program_lngth, -- last batch number (length of entire anonymous block in batches of 256 chars each)
                     false,                          -- don't insert a newline after each piece
                     dbms_sql.native);

      -- execute by dbms_sql
      l_dbms_execute_result := dbms_sql.execute(c_project_report_program);

      dbms_sql.close_cursor(c_project_report_program);

      ----------------------------------------------------------------------------------------------
      --                             end of solution for 10g                                      --
      ----------------------------------------------------------------------------------------------

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_report_updated_records;

   end report_updated_records;

   ------------------------------------------------
   -- report_updated_projects
   ------------------------------------------------
   procedure report_updated_projects(p_gpms_exp_id      number,
                                     p_dev_site_region  varchar2,
                                     p_start_time       varchar2) is

      l_project_setup_fields_list_p  varchar2(4000);
      l_project_setup_fields_list_b  varchar2(4000);
      l_project_query                clob;
      l_project_report_program       /*long; --varchar2(32000); --*/clob;
      l_project_if                   clob;
      --
      --l_task_setup_fields_list_p     varchar2(4000);
      --l_task_setup_fields_list_b     varchar2(4000);
      --l_task_query                   clob;
      --l_task_report_program          clob;
      --l_task_if                      clob;
      --
      l_null_date_p                  varchar2(50) := 'to_date(''01011900'',''ddmmyyyy'')';
      l_null_date_b                  varchar2(50) := 'to_date(''02021900'',''ddmmyyyy'')';
      l_null_char_p                  varchar2(11) := '''teva-9999''';
      l_null_char_b                  varchar2(11) := '''teva-8888''';
      --
      l_interface_name               varchar2(50) := 'PLW -> GRD Interface Program Finished';
      l_end_time                     varchar2(30);

      -- clob issue - varray solution --
      /*l_project_report_program_lngth number;
      l_project_report_program_max_l number := 32000;
      l_varray_amount_of_elements    number;
      l_project_report_program_floor number;
      l_project_report_program_mod   number;
      --
      type att_type                  is varray(32000) of varchar2(32000);
      l_arr_list                     att_type := att_type();
      l_i                            number := 1;*/

      -- definition due to 10g clob execution failure
      l_project_report_program_lngth number;
      c_project_report_program       integer;
      l_project_report_program_array dbms_sql.varchar2s; -- type varchar2s is table of varchar2(256) index by binary_integer;
      l_dbms_execute_result          number;

      cursor c_project_setup_fields is
         select *
         from   ps_env_field_vw t
         where  env_id       =  'GRD'
           and  level_name   =  'PROJECT'
           --and  t.field_name <> 'PRJ_ID'
           and  t.uk_part    = 0
         ;

   begin

      -- concatenate ps_project fields from the setup table for dynamic select
      for r_project_setup_fields in c_project_setup_fields loop

         -- for projects imported from export_grd with env_activity = 10
         if(l_project_setup_fields_list_p is null) then

            l_project_setup_fields_list_p := 'p.' || r_project_setup_fields.field_name || ' p_' || substr(r_project_setup_fields.field_name, 1, 28);

         else

            l_project_setup_fields_list_p := l_project_setup_fields_list_p || ', ' ||
                                     'p.' || r_project_setup_fields.field_name || ' p_' || substr(r_project_setup_fields.field_name, 1, 28);

         end if;

         -- for projects imported from export_grd_backup with env_activity = 22
         if(l_project_setup_fields_list_b is null) then

            l_project_setup_fields_list_b := 'b.' || r_project_setup_fields.field_name || ' b_' || substr(r_project_setup_fields.field_name, 1, 28);

         else

            l_project_setup_fields_list_b := l_project_setup_fields_list_b || ', ' ||
                                             'b.' || r_project_setup_fields.field_name || ' b_' || substr(r_project_setup_fields.field_name, 1, 28);

         end if;

      end loop;

      -- build dynamic query
      if(p_dev_site_region = 'ALL') then

         l_project_query := 'select ' || l_project_setup_fields_list_p || ', '           ||
                                         l_project_setup_fields_list_b || ' '            ||
                            'from   ps_project p, '                                      ||
                                   'ps_project b '                                       ||
                            'where  p.prj_id          = b.prj_id '                       ||
                              'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                              'and  p.env_activity_id = 10 '                             ||
                              'and  b.env_activity_id = 22 '                             ||
                              'and  exists(select 1 '                                    ||
                                          'from   ps_project t '                         ||
                                          'where  t.prj_id          = p.prj_id '         ||
                                            'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                            'and  t.action          = ''U'' '            ||
                                          ') '                                           ||
                              'and ('                                                    ||
                                    -- IL Sites
                                    '(p.region   in(''EU'', ''IL'', ''JP'') and '        ||
                                     'p.development_site in(''BD'', ''NA'')) or '        ||
                                     -- for country projects (INT, LATAM) region is not transfered
                                    '(nvl(p.region, ''-999'') not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and ' ||
                                     'p.development_site in(''BD'', ''NA'')) or '        ||
                                    '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'','   ||
                                                           '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'','   ||
                                                           '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')) or '    ||
                                    -- US Sites
                                    '(p.region            in(''US'', ''CA'')  and '      ||
                                     'p.development_site  in(''BD'', ''NA'')) or '       ||
                                    '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                            '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                            '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                            'order by p.gpms_id, '                                       ||
                                     'p.plw_pjt_id, '                                    ||
                                     'p.plw_int_number, '                                ||
                                     'p.env_activity_id'
                            ;

      elsif(p_dev_site_region = 'GRD-IL') then

         l_project_query := 'select distinct q.* '                                                ||
                            'from   ( '                                                           ||
                                     -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'' '            ||
                                       -- join to export_grd
                                       --'and  nvl(p.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(p.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(p.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- region projects
                                       'and  (' || -- LIVE(CA), PRAM
                                             '(upper(e.region)      in(''EU'', ''CA''))  or '     ||
                                              -- US
                                              'upper(e.region)      = ''US'' or '                 ||
                                              -- IL
                                             '(upper(e.region)      = ''EMIA'' and '              ||
                                              'e.country            =  ''IL'')  or '              ||
                                              -- JP
                                             '(upper(e.region)      = ''ASIA'' and '              ||
                                              'e.country            = ''JP'')) '                  ||
                                       -- dev_site = GRD-IL
                                       'and  ((p.region             in(''EU'', ''IL'', ''JP'') and ' ||
                                              'p.development_site   in(''BD'', ''NA''))        or '  ||
                                       -- for country projects (INT, LATAM) region is not transfered
                                             '(nvl(p.region, ''-999'')  not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and ' ||
                                              'p.development_site     in(''BD'', ''NA''))  or '   ||
                                             '(p.development_site     in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'', ' ||
                                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'', ' ||
                                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')))) '    ||
                                     'union all ' ||
                                     -- country projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e, '                                     ||
                                            'ps_task     t '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- join to export_grd, ps_task
                                       'and  p.project_id                  = t.project_id '       ||
                                       --'and  nvl(t.gpms_id, ''-999'')      = nvl(e.gpms_id, ''-999'') '          ||
                                       'and  nvl(t.plw_pjt_id, ''-999'')   = nvl(e.plw_pjt_id, ''-999'') '       ||
                                       'and  nvl(t.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- country projects
                                       'and  upper(e.region) in(''EMIA'', ''ASIA'', ''LATAM'') '  ||
                                       'and  e.country       not in(''JP'',''IL'') '              ||
                                       -- dev_site = GRD-IL
                                       'and  ((p.region                in(''EU'', ''IL'', ''JP'') and ' ||
                                              'p.development_site      in(''BD'', ''NA'')) or '         ||
                                             -- for country projects (INT, LATAM) region is not transfered
                                             '(nvl(p.region, ''-999'') not in(''EU'', ''IL'', ''JP'', ''US'', ''CA'') and '   ||
                                              'p.development_site      in(''BD'', ''NA'')) or '   ||
                                             '(p.development_site      in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'', ' ||
                                                                         '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'', ' ||
                                                                         '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG''))) '     ||

                                       ') q '                                                     ||
                            'order by q.p_gpms_id, '                                              ||
                                     'q.p_plw_pjt_id, '                                           ||
                                     'q.p_plw_int_number'
                            ;

      elsif(p_dev_site_region = 'GRD-US') then

         l_project_query := 'select distinct q.* '                                                ||
                            'from   ( '                                                           ||
                                     -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- join to export_grd
                                       --'and  nvl(p.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(p.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(p.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- region projects
                                       'and  ( ' || -- LIVE(CA), PRAM
                                             '(upper(e.region)     in(''EU'', ''CA'')) or '       ||
                                              -- US
                                              'upper(e.region)     = ''US''    or '               ||
                                              -- IL
                                              '(upper(e.region)    = ''EMIA''  and '              ||
                                               'e.country          = ''IL'')   or '               ||
                                              -- JP
                                              '(upper(e.region)    = ''ASIA''  and '              ||
                                               'e.country          = ''JP'')) '                   ||
                                       -- dev_site = GRD-US
                                       'and  ((p.region            in(''US'', ''CA'')  and '      ||
                                              'p.development_site  in(''BD'', ''NA'')) or '       ||
                                             '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                                     '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                                     '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                                     'union all ' ||

                                    -- region projects
                                     'select ' || l_project_setup_fields_list_p || ', '           ||
                                                  l_project_setup_fields_list_b || ' '            ||
                                     'from   ps_project  p, '                                     ||
                                            'ps_project  b, '                                     ||
                                            'export_grd  e, '                                     ||
                                            'ps_task     t '                                      ||
                                     'where  p.prj_id          = b.prj_id '                       ||
                                       'and  p.gpms_exp_id     = b.gpms_exp_id '                  ||
                                       'and  p.env_activity_id = 10 '                             ||
                                       'and  b.env_activity_id = 22 '                             ||
                                       'and  exists(select 1 '                                    ||
                                                   'from   ps_project t '                         ||
                                                   'where  t.prj_id          = p.prj_id '         ||
                                                     'and  t.gpms_exp_id     = p.gpms_exp_id '    ||
                                                     'and  t.action          = ''U'') '           ||
                                       -- country projects
                                       'and  upper(e.region)   in(''EMIA'', ''ASIA'', ''LATAM'') ' ||
                                       'and  e.country         not in(''JP'',''IL'') '            ||
                                       -- join to ps_task, export_grd
                                       'and  p.project_id                    = t.project_id '                    ||
                                       --'and  nvl(t.gpms_id, ''-999'')        = nvl(e.gpms_id, ''-999'') '        ||
                                       'and  nvl(t.plw_pjt_id, ''-999'')     = nvl(e.plw_pjt_id, ''-999'') '     ||
                                       'and  nvl(t.plw_int_number, ''-999'') = nvl(e.plw_int_number, ''-999'') ' ||
                                       -- dev_site = GRD-US
                                       'and  ((p.region            in(''US'', ''CA'') and '       ||
                                              'p.development_site  in(''BD'', ''NA'')) or '       ||
                                             '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                                             '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                                             '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) ' ||

                                       ') q '                                                     ||
                            'order by q.p_gpms_id, '                                              ||
                                     'q.p_plw_pjt_id, '                                           ||
                                     'q.p_plw_int_number'
                            ;

      end if;

      -- concatenate comparison by all PROJECT fields from the setup table for dynamic anonymous block
      for r_project_setup_fields in c_project_setup_fields loop

         if(r_project_setup_fields.field_type in ('NUMBER', 'FLOAT')) then

            l_project_if := l_project_if || chr(10) ||

                    'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', -9999) <> '    ||
                       'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', -8888) and '   ||
                       -- just one can be null
                       '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in '                              ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_project_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         elsif(r_project_setup_fields.field_type = 'DATE') then

            l_project_if := l_project_if || chr(10) ||

                    'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_date_p || ') <> '  ||
                       'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_date_b || ') and ' ||
                       -- just one can be null
                       '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_project_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         else

            l_project_if := l_project_if || chr(10) ||
                            'if(nvl(r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_char_p || ') <> '  ||
                               'nvl(r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ', ' || l_null_char_b || ') and ' ||
                               -- just one can be null
                               '(r_project_updated_records.p_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                                'r_project_updated_records.b_'    || substr(r_project_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                               -- do not compare Country Projects by
                               -- GAIN, GLOBAL_DOSAGE_FORM, PROJECT_RATIONALE, REGION, DEVELOPMENT_SITE (all of varchar2 data type)
                               /*'((r_project_updated_records.p_region in (''US'', ''CA'', ''IL'', ''EU'', ''JP'')) or '                     ||
                               ' (nvl(r_project_updated_records.p_region, ''-999'') not in (''US'', ''CA'', ''IL'', ''EU'', ''JP'') and '  ||
                                 chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in '                                     ||
                                       '(''GAIN'', ''GLOBAL_DOSAGE_FORM'', ''PROJECT_RATIONALE'', ''REGION'', ''DEVELOPMENT_SITE'')) and ' ||*/
                               -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                               chr(39) || r_project_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                                    '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                               'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                                       chr(39) ||
                                                       '<tr bgcolor="White">
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                            <td>' || r_project_setup_fields.field_name || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.b_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_project_updated_records.p_' || substr(r_project_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                         </tr> ' ||
                                                       chr(39) || '; '                                                         ||

                            'end if; '
                            ;

         end if;

      end loop;

      -- build dynamic program
      l_project_report_program := 'declare '                                                            ||

                                     'l_gpms_exp_id               number := ' || p_gpms_exp_id || '; '  ||
                                     'l_dev_site_region           varchar2(6) := ' || chr(39) || p_dev_site_region || chr(39) || '; ' ||
                                     'l_log_type                  varchar2(7) := ''Delta''; '           ||

                                     'l_log_title                 varchar2(700); '                      ||
                                     'l_log_header                varchar2(500); '                      ||
                                     'l_log_end                   varchar2(100) := '' </body></html>''; '   ||

                                     'l_dynamic_project_h3        varchar2(20) := ''<h2>Projects:</h2>''; ' ||
                                     'l_no_delta_found_message    varchar2(32) := ''<h2>No Delta Data was Found</h2>''; ' ||

                                     'l_dynamic_table_header      clob; '                               ||
                                     'l_dynamic_table_content     clob; '                               ||
                                     'l_dynamic_table_prj_content clob; '                               ||
                                     'l_dynamic_table_row         clob; '                               ||

                                     'l_dynamic_table_end         varchar2(10) := ''</table>''; '       ||

                                     'l_gpms_id                   varchar2(20); '                       ||
                                     'l_plw_pjt_id                varchar2(20); '                       ||
                                     'l_plw_int_number            varchar2(20); '                       ||

                                     'cursor c_project_updated_records is '                             ||
                                         l_project_query || '; ' || chr(10) || chr(10)                  ||

                                  'begin '                                                              ||

                                     /*'l_log_title := ' || chr(39)                                      ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                           chr(39) || '; '                              ||*/

                                     'l_log_title := ' || chr(39)                                       ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                                  '<div>'                               ||
                                                                   '<h2>Process Report: <br></h2>'      ||

                                                                      '<table border="1" width="25%">'  ||
                                                                        '<tr bgcolor="White">'          ||
                                                                           '<th><div align="left"> Interface name </div></th>'             ||
                                                                           '<th><div align="left">' || l_interface_name || '</div></th>'   ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Start Time </div></th>'                 ||
                                                                           '<th><div align="left">' || p_start_time  || '</div></th>'      ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> End Time </div></th>'                   ||
                                                                           '<th><div align="left">' || to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss') || '</div></th>'            ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Interface Site </div></th>'             ||
                                                                           '<th><div align="left">' || p_dev_site_region || '</div></th>'  ||
                                                                        '</tr>'                                                            ||
                                                                      '</table>'                                                           ||
                                                                  '</div>' ||
                                                           chr(39) || '; '                                                                 ||

                                     'l_log_header := ' || chr(39)                                      ||
                                                            '<h2>The List of Changed Fields:<br></h2> ' ||
                                                           chr(39) || '; '                              ||

                                     'l_dynamic_table_header := ' || chr(39)                            ||
                                                                    '<table border="1" width="95%">'    ||
                                                                      '<tr bgcolor="Silver">'           ||
                                                                         '<th>GPMS_ID</th>'             ||
                                                                         '<th>PLW_PJT_ID</th>'          ||
                                                                         '<th>PLW_INT_NUMBER</th>'      ||
                                                                         '<th>FIELD NAME</th>'          ||
                                                                         '<th>PREVIOUS FIELD VALUE</th>' ||
                                                                         '<th>CURRENT FIELD VALUE</th>' ||
                                                                       '</tr> '                         ||
                                                                     chr(39) || '; '                    ||

                                     -- ps_project
                                     'for r_project_updated_records in c_project_updated_records loop ' ||

                                        'l_dynamic_table_row := null; '                                 ||

                                        l_project_if                                                    ||

                                        'if(l_dynamic_table_row is not null) then '                     ||

                                           'l_dynamic_table_prj_content := l_dynamic_table_prj_content || ' ||
                                                                           'l_dynamic_table_header || ' ||
                                                                           'l_dynamic_table_row || '    ||
                                                                           'l_dynamic_table_end; '      ||

                                        'end if; '                                                      ||

                                        'l_gpms_id        := r_project_updated_records.p_gpms_id; '     ||
                                        'l_plw_pjt_id     := r_project_updated_records.p_plw_pjt_id; '  ||
                                        'l_plw_int_number := r_project_updated_records.p_plw_int_number; ' ||

                                     'end loop; '                                                       ||

                                     'if(l_dynamic_table_prj_content is not null) then '                ||

                                        'l_dynamic_table_content := l_dynamic_project_h3 || '           ||
                                                                   'l_dynamic_table_prj_content; '      ||

                                     'end if; '                                                         ||

                                     'if(l_dynamic_table_content is not null) then '                    ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_log_header                        ||
                                                                    l_dynamic_table_content             ||
                                                                    l_log_end; '                        ||
                                     'else '                                                            ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_no_delta_found_message            ||
                                                                    l_log_end; '                        ||
                                     'end if; '                                                         ||

                                     -- insert log file into a table for BPEL fetching
                                     'begin '                                                           ||

                                        'delete from ps_log t '                                         ||
                                        'where  t.gpms_exp_id  = l_gpms_exp_id '                        ||
                                          'and  t.date_of_log  = trunc(sysdate) '                       ||
                                          'and  t.dev_site     = l_dev_site_region '                    ||
                                          'and  t.log_type     = l_log_type '                           ||
                                          'and  t.report_level = ''PROJECTS''; '                        ||

                                        'insert into ps_log(gpms_exp_id, '                              ||
                                                           'date_of_log, '                              ||
                                                           'dev_site, '                                 ||
                                                           'log_type, '                                 ||
                                                           'report_level, '                             ||
                                                           'log_file) '                                 ||
                                        'values(l_gpms_exp_id, '                                        ||
                                               'trunc(sysdate), '                                       ||
                                               'l_dev_site_region, '                                    ||
                                               'l_log_type, '                                           ||
                                               '''PROJECTS'', '                                         ||
                                               'l_dynamic_table_content); '                             ||

                                        'commit; '                                                      ||

                                     'exception '                                                       ||
                                        'when others then '                                             ||
                                           'rollback; '                                                 ||

                                     'end; ' ||

                                  'end;'
                                  ;

      -------------------------------------------------------------------------------------------------------------
      --                                    for 11g - just execute immediate                                     --
      -------------------------------------------------------------------------------------------------------------
      --execute immediate l_project_report_program;

      /*insert into oleg(sql_stmnt)
      values (l_project_report_program);
      commit;*/

      -------------------------------------------------------------------------------------------------------------
      -- for 10g execute immediate a variable of data type clob is impossible, that's why use the solution below --
      -------------------------------------------------------------------------------------------------------------

      ------------------------ solution 1: splitting clob to varray elements ------------------------
      /*l_project_report_program_lngth := length(l_project_report_program);

      select floor(l_project_report_program_lngth / l_project_report_program_max_l) +
                   decode(mod(l_project_report_program_lngth, l_project_report_program_max_l), 0, 0, 1),
             floor(l_project_report_program_lngth / l_project_report_program_max_l),
             mod(l_project_report_program_lngth, l_project_report_program_max_l)
      into   l_varray_amount_of_elements,
             l_project_report_program_floor,
             l_project_report_program_mod
      from   dual
      ;

      for i in 1..l_varray_amount_of_elements loop

         if(i <= l_project_report_program_floor) then

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_max_l);

         else

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_mod);

         end if;

         l_i := l_i + l_project_report_program_max_l;

         \*insert into OLEG3(ACTION,
                       SQL_STMNT)
         values(i,
                l_arr_list(i)
                )
         ;
         commit;*\

      end loop;

      if(l_varray_amount_of_elements = 1) then

         execute immediate l_arr_list(1);

      elsif(l_varray_amount_of_elements = 2) then

         execute immediate l_arr_list(1) || l_arr_list(2);

      elsif(l_varray_amount_of_elements = 3) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3);

      elsif(l_varray_amount_of_elements = 4) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4);

      elsif(l_varray_amount_of_elements = 5) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5);

      elsif(l_varray_amount_of_elements = 6) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6);

      elsif(l_varray_amount_of_elements = 7) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7);

      elsif(l_varray_amount_of_elements = 8) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8);

      elsif(l_varray_amount_of_elements = 9) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9);

      elsif(l_varray_amount_of_elements = 10) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10);

      elsif(l_varray_amount_of_elements = 11) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11);

      elsif(l_varray_amount_of_elements = 12) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12);

      elsif(l_varray_amount_of_elements = 13) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13);

      elsif(l_varray_amount_of_elements = 14) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14);

      elsif(l_varray_amount_of_elements = 15) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14)|| l_arr_list(15);

      end if;*/

      ------------------------ solution 2: dbms_sql.execute ------------------------

      -- length in batches of 256 chars each
      l_project_report_program_lngth := ceil(dbms_lob.getlength(l_project_report_program) / 256);

      -- fill the array by batches of length 256 each
      for i in 1 .. l_project_report_program_lngth loop

         l_project_report_program_array(i) := dbms_lob.substr(l_project_report_program,
                                                              256,                 -- batch length
                                                              ((i - 1) * 256) + 1) -- 1-st char of each batch
                                                              ;
      end loop;

      c_project_report_program := dbms_sql.open_cursor;

      -- parse array members to prepare the statement for execution
      dbms_sql.parse(c_project_report_program,       -- whether cursor is open
                     l_project_report_program_array, -- entire anonymous block
                     1,                              -- 1-st batch number
                     l_project_report_program_lngth, -- last batch number (length of entire anonymous block in batches of 256 chars each)
                     false,                          -- don't insert a newline after each piece
                     dbms_sql.native);

      -- execute by dbms_sql
      l_dbms_execute_result := dbms_sql.execute(c_project_report_program);

      dbms_sql.close_cursor(c_project_report_program);

      ----------------------------------------------------------------------------------------------
      --                             end of solution for 10g                                      --
      ----------------------------------------------------------------------------------------------

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_report_updated_projects;

   end report_updated_projects;

   ------------------------------------------------
   -- report_updated_tasks
   ------------------------------------------------
   procedure report_updated_tasks(p_gpms_exp_id      number,
                                    p_dev_site_region  varchar2,
                                    p_start_time       varchar2) is

      /*l_project_setup_fields_list_p  varchar2(4000);
      l_project_setup_fields_list_b  varchar2(4000);
      l_project_query                clob;*/
      l_project_report_program       /*long; --varchar2(32000); --*/clob;
      --l_project_if                   clob;
      --
      l_task_setup_fields_list_p     varchar2(4000);
      l_task_setup_fields_list_b     varchar2(4000);
      l_task_query                   clob;
      l_task_report_program          clob;
      l_task_if                      clob;
      --
      l_null_date_p                  varchar2(50) := 'to_date(''01011900'',''ddmmyyyy'')';
      l_null_date_b                  varchar2(50) := 'to_date(''02021900'',''ddmmyyyy'')';
      l_null_char_p                  varchar2(11) := '''teva-9999''';
      l_null_char_b                  varchar2(11) := '''teva-8888''';
      --
      l_interface_name               varchar2(50) := 'PLW -> GRD Interface Program Finished';
      l_end_time                     varchar2(30);

      -- clob issue - varray solution --
      /*l_project_report_program_lngth number;
      l_project_report_program_max_l number := 32000;
      l_varray_amount_of_elements    number;
      l_project_report_program_floor number;
      l_project_report_program_mod   number;
      --
      type att_type                  is varray(32000) of varchar2(32000);
      l_arr_list                     att_type := att_type();
      l_i                            number := 1;*/

      -- definition due to 10g clob execution failure
      l_project_report_program_lngth number;
      c_project_report_program       integer;
      l_project_report_program_array dbms_sql.varchar2s; -- type varchar2s is table of varchar2(256) index by binary_integer;
      l_dbms_execute_result          number;

      cursor c_task_setup_fields is
         select *
         from   ps_env_field_vw t
         where  env_id       =  'GRD'
           and  level_name   =  'TASK'
           --and  t.field_name <> 'PRJ_ID'
           and  t.uk_part    = 0
         ;

   begin

      -- concatenate ps_task fields from the setup table for dynamic select
      for r_task_setup_fields in c_task_setup_fields loop

         -- for projects imported from export_grd with env_activity = 10
         if(l_task_setup_fields_list_p is null) then

            l_task_setup_fields_list_p := 'p.' || r_task_setup_fields.field_name || ' p_' || substr(r_task_setup_fields.field_name, 1, 28);

         else

            l_task_setup_fields_list_p := l_task_setup_fields_list_p || ', ' ||
                                     'p.' || r_task_setup_fields.field_name || ' p_' || substr(r_task_setup_fields.field_name, 1, 28);

         end if;

         -- for projects imported from export_grd_backup with env_activity = 22
         if(l_task_setup_fields_list_b is null) then

            l_task_setup_fields_list_b := 'b.' || r_task_setup_fields.field_name || ' b_' || substr(r_task_setup_fields.field_name, 1, 28);

         else

            l_task_setup_fields_list_b := l_task_setup_fields_list_b || ', ' ||
                                          'b.' || r_task_setup_fields.field_name || ' b_' || substr(r_task_setup_fields.field_name, 1, 28);

         end if;

      end loop;

      -- build dynamic query
      if(p_dev_site_region = 'ALL') then

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           'and ('                                             ||
                                 -- IL Sites
                                 '(p.region   in(''EU'', ''IL'', ''JP'', ''INT'', ''LATAM'') and ' ||
                                  'p.development_site in(''BD'', ''NA'')) or ' ||
                                  -- for country projects (INT, LATAM) region is not transfered
                                 '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'','   ||
                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'','   ||
                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG'')) or '    ||
                                 -- US Sites
                                 '(p.region            in(''US'', ''CA'')  and ' ||
                                  'p.development_site  in(''BD'', ''NA'')) or '  ||
                                 '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                         '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                         '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      elsif(p_dev_site_region = 'GRD-IL') then

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           -- IL Sites
                           'and ((p.region           in(''EU'', ''IL'', ''JP'', ''INT'', ''LATAM'') and ' ||
                                 'p.development_site in(''BD'', ''NA'')) or '  ||
                                  -- for country projects (INT, LATAM) region is not transfered
                                 '(p.development_site in(''DB'',''HL'',''GD'',''RN'',''WF'',''IL'',''SA'',' ||
                                                        '''ZR'',''ZG'',''BN'',''KR'',''UL'',''GO'',''ML'',' ||
                                                        '''MU'',''TYK'',''UL-CoDev'',''RM'',''NAG''))) '    ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      elsif(p_dev_site_region = 'GRD-US') then

         l_task_query := 'select ' || l_task_setup_fields_list_p || ', '       ||
                                      l_task_setup_fields_list_b || ' '        ||
                         'from   ps_task p, '                                  ||
                                'ps_task b '                                   ||
                         'where  p.prj_id          = b.prj_id '                ||
                           'and  p.country         = b.country '               ||
                           'and  p.gpms_exp_id     = b.gpms_exp_id '           ||
                           'and  p.env_activity_id = 10 '                      ||
                           'and  b.env_activity_id = 22 '                      ||
                           'and  exists(select 1 '                             ||
                                       'from   ps_task t '                     ||
                                       'where  t.prj_id      = p.prj_id '      ||
                                         'and  t.country     = p.country '     ||
                                         'and  t.gpms_exp_id = p.gpms_exp_id ' ||
                                         'and  t.action      = ''U'' '         ||
                                       ') '                                    ||
                           -- US Sites
                           'and ((p.region            in(''US'', ''CA'')  and ' ||
                                 'p.development_site  in(''BD'', ''NA'')) or '  ||
                                '(p.development_site  in(''SV'',''NV'',''TO'',''IR'',''OP'',''MC'',''MB'', ' ||
                                                        '''MI'', ''BDPO'', ''BDJP'',''BDAM'',''BDCA'', ''BP'', ''BDIR'',''PO'', ' ||
                                                        '''AR'',''CL'',''VE'',''PE'',''KW'',''NVD''))) '     ||
                         'order by p.gpms_id, '                                ||
                                  'p.plw_pjt_id, '                             ||
                                  'p.plw_int_number, '                         ||
                                  'p.env_activity_id'
                         ;

      end if;

      -- concatenate comparison by all TASK fields from the setup table for dynamic anonymous block
      for r_task_setup_fields in c_task_setup_fields loop

         if(r_task_setup_fields.field_type in ('NUMBER', 'FLOAT')) then

            l_task_if := l_task_if || chr(10) ||

                    'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', -9999) <> '    ||
                       'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', -8888) and '   ||
                       -- just one can be null
                       '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_task_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         elsif(r_task_setup_fields.field_type = 'DATE') then

            l_task_if := l_task_if || chr(10) ||

                    'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_date_p || ') <> '  ||
                       'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_date_b || ') and ' ||
                       -- just one can be null
                       '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                        'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                       -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                       chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                            '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                       'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                               chr(39) ||
                                               '<tr bgcolor="White">
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                    <td>' || r_task_setup_fields.field_name || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                    <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                 </tr> ' ||
                                               chr(39) || '; '                                                         ||

                    'end if; '
                    ;

         else

            l_task_if := l_task_if || chr(10) ||
                            'if(nvl(r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_char_p || ') <> '  ||
                               'nvl(r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ', ' || l_null_char_b || ') and ' ||
                               -- just one can be null
                               '(r_task_updated_records.p_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null or '   ||
                                'r_task_updated_records.b_'    || substr(r_task_setup_fields.field_name, 1, 28) || ' is not null) and ' ||
                               -- do not compare by GPMS_ID, PLW_PJT_ID, PLW_INT_NUMBER
                               chr(39) || r_task_setup_fields.field_name || chr(39) || ' not in'                               ||
                                                                    '(''GPMS_ID'', ''PLW_PJT_ID'', ''PLW_INT_NUMBER'')) then '    ||

                               'l_dynamic_table_row := l_dynamic_table_row || '                                                ||
                                                       chr(39) ||
                                                       '<tr bgcolor="White">
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_GPMS_ID ||'        || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_PLW_PJT_ID ||'     || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_PLW_INT_NUMBER ||' || chr(39) || '</td>
                                                            <td>' || r_task_setup_fields.field_name || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.b_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                            <td>' || chr(39) || '|| r_task_updated_records.p_' || substr(r_task_setup_fields.field_name, 1, 28) || ' ||' || chr(39) || '</td>
                                                         </tr> ' ||
                                                       chr(39) || '; '                                                         ||

                            'end if; '
                            ;

         end if;

      end loop;

      -- build dynamic program
      l_project_report_program := 'declare '                                                            ||

                                     'l_gpms_exp_id               number := ' || p_gpms_exp_id || '; '  ||
                                     'l_dev_site_region           varchar2(6) := ' || chr(39) || p_dev_site_region || chr(39) || '; ' ||
                                     'l_log_type                  varchar2(7) := ''Delta''; '           ||

                                     'l_log_title                 varchar2(700); '                      ||
                                     'l_log_header                varchar2(500); '                      ||
                                     'l_log_end                   varchar2(100) := '' </body></html>''; '   ||

                                     'l_dynamic_task_h3           varchar2(20) := ''<h2>Tasks:</h2>''; '    ||
                                     'l_no_delta_found_message    varchar2(32) := ''<h2>No Delta Data was Found</h2>''; ' ||

                                     'l_dynamic_table_header      clob; '                               ||
                                     'l_dynamic_table_content     clob; '                               ||
                                     'l_dynamic_table_tsk_content clob; '                               ||
                                     'l_dynamic_table_row         clob; '                               ||

                                     'l_dynamic_table_end         varchar2(10) := ''</table>''; '       ||

                                     'l_gpms_id                   varchar2(20); '                       ||
                                     'l_plw_pjt_id                varchar2(20); '                       ||
                                     'l_plw_int_number            varchar2(20); '                       ||

                                     'cursor c_task_updated_records is '                                ||
                                         l_task_query || '; ' || chr(10) || chr(10)                     ||


                                  'begin '                                                              ||

                                     /*'l_log_title := ' || chr(39)                                      ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                           chr(39) || '; '                              ||*/

                                     'l_log_title := ' || chr(39)                                       ||
                                                           '<!DOCTYPE html>'                            ||
                                                              '<html>'                                  ||
                                                                '<head>'                                ||
                                                                 '<title>PLW --> RND Delta Fields Log</title>' ||
                                                                '</head>'                               ||
                                                                '<body>'                                ||
                                                                  '<div>'                               ||
                                                                   '<h2>Process Report: <br></h2>'      ||

                                                                      '<table border="1" width="25%">'  ||
                                                                        '<tr bgcolor="White">'          ||
                                                                           '<th><div align="left"> Interface name </div></th>'             ||
                                                                           '<th><div align="left">' || l_interface_name || '</div></th>'   ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Start Time </div></th>'                 ||
                                                                           '<th><div align="left">' || p_start_time  || '</div></th>'      ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> End Time </div></th>'                   ||
                                                                           '<th><div align="left">' || to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss') || '</div></th>'            ||
                                                                        '</tr> <tr bgcolor="White">'                                       ||
                                                                           '<th><div align="left"> Interface Site </div></th>'             ||
                                                                           '<th><div align="left">' || p_dev_site_region || '</div></th>'  ||
                                                                        '</tr>'                                                            ||
                                                                      '</table>'                                                           ||
                                                                  '</div>' ||
                                                           chr(39) || '; '                                                                 ||

                                     'l_log_header := ' || chr(39)                                      ||
                                                            '<h2>The List of Changed Fields:<br></h2> ' ||
                                                           chr(39) || '; '                              ||

                                     'l_dynamic_table_header := ' || chr(39)                            ||
                                                                    '<table border="1" width="95%">'    ||
                                                                      '<tr bgcolor="Silver">'           ||
                                                                         '<th>GPMS_ID</th>'             ||
                                                                         '<th>PLW_PJT_ID</th>'          ||
                                                                         '<th>PLW_INT_NUMBER</th>'      ||
                                                                         '<th>FIELD NAME</th>'          ||
                                                                         '<th>PREVIOUS FIELD VALUE</th>' ||
                                                                         '<th>CURRENT FIELD VALUE</th>' ||
                                                                       '</tr> '                         ||
                                                                     chr(39) || '; '                    ||

                                     -- ps_task
                                     'for r_task_updated_records in c_task_updated_records loop '       ||

                                        'l_dynamic_table_row := null; '                                 ||

                                        l_task_if                                                       ||

                                        'if(l_dynamic_table_row is not null) then '                     ||

                                           'l_dynamic_table_tsk_content := l_dynamic_table_tsk_content || ' ||
                                                                          'l_dynamic_table_header || '  ||
                                                                          'l_dynamic_table_row || '     ||
                                                                          'l_dynamic_table_end; '       ||

                                        'end if; '                                                      ||

                                        'l_gpms_id        := r_task_updated_records.p_gpms_id; '        ||
                                        'l_plw_pjt_id     := r_task_updated_records.p_plw_pjt_id; '     ||
                                        'l_plw_int_number := r_task_updated_records.p_plw_int_number; ' ||

                                     'end loop; '                                                       ||

                                     'if(l_dynamic_table_tsk_content is not null) then '                ||

                                        'l_dynamic_table_content := l_dynamic_table_content || '        ||
                                                                   'l_dynamic_task_h3 || '              ||
                                                                   'l_dynamic_table_tsk_content; '      ||

                                     'end if; '                                                         ||

                                     'if(l_dynamic_table_content is not null) then '                    ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_log_header                        ||
                                                                    l_dynamic_table_content             ||
                                                                    l_log_end; '                        ||
                                     'else '                                                            ||

                                        'l_dynamic_table_content := l_log_title                         ||
                                                                    l_no_delta_found_message            ||
                                                                    l_log_end; '                        ||
                                     'end if; '                                                         ||

                                     -- insert log file into a table for BPEL fetching
                                     'begin '                                                           ||

                                        'delete from ps_log t '                                         ||
                                        'where  t.gpms_exp_id  = l_gpms_exp_id '                        ||
                                          'and  t.date_of_log  = trunc(sysdate) '                       ||
                                          'and  t.dev_site     = l_dev_site_region '                    ||
                                          'and  t.log_type     = l_log_type '                           ||
                                          'and  t.report_level = ''TASKS''; '                           ||

                                        'insert into ps_log(gpms_exp_id, '                              ||
                                                           'date_of_log, '                              ||
                                                           'dev_site, '                                 ||
                                                           'log_type, '                                 ||
                                                           'report_level, '                             ||
                                                           'log_file) '                                 ||
                                        'values(l_gpms_exp_id, '                                        ||
                                               'trunc(sysdate), '                                       ||
                                               'l_dev_site_region, '                                    ||
                                               'l_log_type, '                                           ||
                                               '''TASKS'', '                                            ||
                                               'l_dynamic_table_content); '                             ||

                                        'commit; '                                                      ||

                                     'exception '                                                       ||
                                        'when others then '                                             ||
                                           'rollback; '                                                 ||

                                     'end; ' ||

                                  'end;'
                                  ;

      -------------------------------------------------------------------------------------------------------------
      --                                    for 11g - just execute immediate                                     --
      -------------------------------------------------------------------------------------------------------------
      --execute immediate l_project_report_program;

      /*insert into oleg(sql_stmnt)
      values (l_project_report_program);
      commit;*/

      -------------------------------------------------------------------------------------------------------------
      -- for 10g execute immediate a variable of data type clob is impossible, that's why use the solution below --
      -------------------------------------------------------------------------------------------------------------

      ------------------------ solution 1: splitting clob to varray elements ------------------------
      /*l_project_report_program_lngth := length(l_project_report_program);

      select floor(l_project_report_program_lngth / l_project_report_program_max_l) +
                   decode(mod(l_project_report_program_lngth, l_project_report_program_max_l), 0, 0, 1),
             floor(l_project_report_program_lngth / l_project_report_program_max_l),
             mod(l_project_report_program_lngth, l_project_report_program_max_l)
      into   l_varray_amount_of_elements,
             l_project_report_program_floor,
             l_project_report_program_mod
      from   dual
      ;

      for i in 1..l_varray_amount_of_elements loop

         if(i <= l_project_report_program_floor) then

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_max_l);

         else

            l_arr_list.extend;
            l_arr_list(i) := substr(l_project_report_program, l_i, l_project_report_program_mod);

         end if;

         l_i := l_i + l_project_report_program_max_l;

         \*insert into OLEG3(ACTION,
                       SQL_STMNT)
         values(i,
                l_arr_list(i)
                )
         ;
         commit;*\

      end loop;

      if(l_varray_amount_of_elements = 1) then

         execute immediate l_arr_list(1);

      elsif(l_varray_amount_of_elements = 2) then

         execute immediate l_arr_list(1) || l_arr_list(2);

      elsif(l_varray_amount_of_elements = 3) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3);

      elsif(l_varray_amount_of_elements = 4) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4);

      elsif(l_varray_amount_of_elements = 5) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5);

      elsif(l_varray_amount_of_elements = 6) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6);

      elsif(l_varray_amount_of_elements = 7) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7);

      elsif(l_varray_amount_of_elements = 8) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8);

      elsif(l_varray_amount_of_elements = 9) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9);

      elsif(l_varray_amount_of_elements = 10) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5) ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10);

      elsif(l_varray_amount_of_elements = 11) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11);

      elsif(l_varray_amount_of_elements = 12) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12);

      elsif(l_varray_amount_of_elements = 13) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13);

      elsif(l_varray_amount_of_elements = 14) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14);

      elsif(l_varray_amount_of_elements = 15) then

         execute immediate l_arr_list(1) || l_arr_list(2) || l_arr_list(3) || l_arr_list(4) || l_arr_list(5)  ||
                           l_arr_list(6) || l_arr_list(7) || l_arr_list(8) || l_arr_list(9) || l_arr_list(10) ||
                           l_arr_list(11)|| l_arr_list(12)|| l_arr_list(13)|| l_arr_list(14)|| l_arr_list(15);

      end if;*/

      ------------------------ solution 2: dbms_sql.execute ------------------------

      -- length in batches of 256 chars each
      l_project_report_program_lngth := ceil(dbms_lob.getlength(l_project_report_program) / 256);

      -- fill the array by batches of length 256 each
      for i in 1 .. l_project_report_program_lngth loop

         l_project_report_program_array(i) := dbms_lob.substr(l_project_report_program,
                                                              256,                 -- batch length
                                                              ((i - 1) * 256) + 1) -- 1-st char of each batch
                                                              ;
      end loop;

      c_project_report_program := dbms_sql.open_cursor;

      -- parse array members to prepare the statement for execution
      dbms_sql.parse(c_project_report_program,       -- whether cursor is open
                     l_project_report_program_array, -- entire anonymous block
                     1,                              -- 1-st batch number
                     l_project_report_program_lngth, -- last batch number (length of entire anonymous block in batches of 256 chars each)
                     false,                          -- don't insert a newline after each piece
                     dbms_sql.native);

      -- execute by dbms_sql
      l_dbms_execute_result := dbms_sql.execute(c_project_report_program);

      dbms_sql.close_cursor(c_project_report_program);

      ----------------------------------------------------------------------------------------------
      --                             end of solution for 10g                                      --
      ----------------------------------------------------------------------------------------------

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_report_updated_tasks;

   end report_updated_tasks;

   ------------------------------------------------
   -- update_export_grd_backup_tbl
   ------------------------------------------------
   procedure update_export_grd_backup_tbl is

      l_deleted_records              number;
      l_inserted_records             number;

      l_drop_tbl_stmnt               varchar2(50);
      l_create_tbl_stmnt             varchar2(200);

      cursor c_export_grd_backup is
         select t.object_name
         from   all_objects t
         where  t.object_type                = 'TABLE'
           and  t.object_name                like 'EXPORT_GRD_BACKUP%'
           and  t.object_name                <> 'EXPORT_GRD_BACKUP_BACKUP'
           and  substr(t.object_name, 18,1)  not in('_', '-')
           and  length(t.object_name)        = 23
           -- a month back
           and  to_date(substr(t.object_name, 18, 6), 'ddmmrr') < (trunc(sysdate) - 31)
           ;

   begin

      begin

         -- create daily backup table of export_grd_backup
         l_create_tbl_stmnt := 'create table export_grd_backup' || to_char(trunc(sysdate), 'ddmmrr') ||
                              ' as select * from export_grd_backup';

         begin
            execute immediate l_create_tbl_stmnt;

         exception
            when others then
               null;
         end;

         -- drop all export_grd_backup_backup backup tables older than 2 weeks
         for r_export_grd_backup in c_export_grd_backup loop

            l_drop_tbl_stmnt := 'drop table ' || r_export_grd_backup.object_name;

            begin
               execute immediate l_drop_tbl_stmnt;
            exception
               when others then
                  null;
            end;

         end loop;

         -- copy from export_grd_backup to export_grd_backup_backup
         delete from export_grd_backup_backup;

         insert into export_grd_backup_backup
         select * from export_grd_backup;
         commit;

         -- copy from export_grd to export_grd_backup
         begin

            --delete from export_grd_backup t where nvl(t.status, '-999') <> 'E';
            delete from export_grd_backup t;
            --l_deleted_records := sql%rowcount;
            --commit;

            insert into export_grd_backup
            select t.gpms_id,
                   t.plw_pjt_id,
                   t.plw_int_number,
                   t.gain,
                   t.global_dosage_form,
                   t.development_site,
                   t.dev_type,
                   t.launching_site,
                   t.project_name,
                   t.project_rationale,
                   t.rationale_comments,
                   t.project_technology,
                   t.project_status,
                   t.wp_status,
                   t.project_progress,
                   t.business,
                   t.region,
                   t.country,
                   t.project_priority,
                   t.allocation_status,
                   t.ftf,
                   t.ftf_reason,
                   t.handling_category,
                   t.il_plmd,
                   t.reason_for_allocation,
                   t.innovator_company,
                   t.innovator_shelf_life,
                   t.ip_manager,
                   t.plfd,
                   t.plfd_remark,
                   t.plmd,
                   t.plmd_asap,
                   t.filing_strategy,
                   t.product_type,
                   t.sales_channel,
                   t.generic_segment,
                   t.therapeutic_class,
                   t.source_project_id,
                   t.type_of_project,
                   t.pipeline_manager,
                   t.go_no_go_pivotal,
                   t.go_no_go_biostudy,
                   t.go_no_go_submission,
                   t.go_no_go_launch,
                   t.activity_type,
                   t.name,
                   t.submission_to_authorities,
                   t.ims_sales,
                   t.ims_units_,
                   t.lcm,
                   t.npv,
                   t.tld,
                   t.nce_date,
                   t.preliminary_allocation,
                   t.approval_date,
                   t.allocation_committee_date,
                   t.launch_date,
                   t.mars_date,
                   t.strength_fill_volume,
                   t.ims_percent_growth,
                   t.strength_uom,
                   t.fill_volume_uom,
                   t.init_proj_strat_appro,
                   t.product_rationale,
                   t.ims_sales_percentage,
                   t.ims_units_percentage,
                   t.subm_to_authorities_act_start,
                   t.global_nte,
                   t.afw,
                   t.tapi_afw,
                   t.submission_a_p,
                   t.approval_a_p,
                   t.launch_a_p,
                   t.value_for_teva,
                   t.plfd_late,
                   t.plmd_main,
                   t.lmd_main,
                   t.lmd_early,
                   t.plfd_asap,
                   t.plfd_late_asap,
                   t.plmd_main_asap,
                   t.lmd_main_asap,
                   t.lmd_early_asap,
                   t.product_status,
                   --
                   t.INNOVATOR_BRAND_NAME,
                   t.STABILITY_RISK,
                   t.FORMULATION_COMPLEXITY,
                   t.DEV_WITHOUT_RLD,
                   t.API_COMPLEXITY,
                   t.COMPLEX_TECHNOLOGY,
                   t.STERILE_Y_N,
                   t.AGENT_COMPANY,
                   t.MFG_SITE,
                   t.PRODUCT_TECHNOLOGY,
                   t.PRODUCT_DOSAGE_FORM,
                   t.OVERALL_BIO_RISK,
                   t.FINAL_CATEGORIZATION,
                   t.DO_YOU_AGREE_WITH_RESULT,
                   t.CALCULATED_CATEGORIZATION,
                   t.GLOBAL_DEVELOPMENT_STATUS,
                   --
                   -- Nessia 15.01.2015 - new field
                   t.WP_DUP_DESC,
                   --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                   t.STRATEGY_MEET_D,
                   t.STRATEGY_MEET,
                   t.STRATEGY_STAT,
                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGET...PIVOTAL_STUDY )
                   t.API_BUDGET,
                   t.BIO_STUDY,
                   t.ENPV,
                   t.T_RND_BUDGET,
                   t.RND_MFG_PACK_BUDGET,
                   t.O_METERIALS_BUDGET,
                   t.OUT_CONTRACTORS,
                   t.PILOT_STUDY,
                   t.CLINICAL,
                   t.PIVOTAL_STUDY,
                   null,  -- status
                   null,  -- action
                   null   -- error_message
            from   export_grd t/*
            where  1 = 1
              and  t.gain             is not null
              and  t.development_site is not null
              and  t.region           is not null
              and  t.country          is not null
              and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6*/
            ;


            delete from psnext_unique_projects_backup
            ;

            insert into psnext_unique_projects_backup
            select * from psnext_unique_projects
            ;


            --l_inserted_records := sql%rowcount;
            --commit;

            --dbms_output.put_line(l_deleted_records  || ' records are deleted  from export_grd_backup.');
            --dbms_output.put_line(l_inserted_records || ' records are inserted into export_grd_backup.');

         exception
            when others then
               rollback;
               g_errbuf   := sqlerrm;
               g_sql_code := sqlcode;
               raise e_update_export_grd_backup_tbl;

         end;

      exception
         when others then
            rollback;
            g_errbuf   := sqlerrm;
            g_sql_code := sqlcode;
            raise e_update_export_grd_backup_tbl;

      end;

   end update_export_grd_backup_tbl;

   ------------------------------------------------
   -- update_country_projects_status
   ------------------------------------------------
   procedure update_country_projects_status is

      l_commited_rows number := 0;

      cursor c_projects_to_update is
         select p.rowid    project_rowid,
                t.rowid    task_rowid,
                t.action,
                p.project_id
         from   ps_project p,
                ps_task    t
         where  p.prj_id          = t.prj_id
           and  p.env_activity_id = t.env_activity_id
           and  p.env_activity_id = 32
           and  t.action          in('I', 'U', 'E')
           and  p.action          <> t.action
         ;

      cursor c_link_task_to_projects is
         select p.rowid    project_rowid,
                t.rowid    task_rowid,
                t.action,
                p.project_id
         from   ps_project p,
                ps_task    t
         where  p.prj_id          = t.prj_id
           and  p.env_activity_id = t.env_activity_id
           and  p.env_activity_id = 32
           and  t.action          in('I', 'U', 'E', 'N')
         ;

      cursor c_tasks_to_update is
         select t.rowid,
                p.project_id
         from   ps_project p,
                ps_task    t
         where  p.prj_id          = t.prj_id
           and  p.env_activity_id = t.env_activity_id
           and  p.env_activity_id = 32
           and  p.action          = 'E'
         ;

   begin

      for r_projects_to_update in c_projects_to_update loop

         -- update action of the project, which tasks were updated / deleted
         update ps_project p
         set    p.action = 'U' -- r_projects_to_update.action
         where  1 = 1
           and  p.rowid           = r_projects_to_update.project_rowid
           and  p.env_activity_id = 32
         ;

         -- connect updated / removed tasks with it's project by project's project_id
         /*update ps_task p
         set    p.project_id = r_projects_to_update.project_id
         where  p.rowid      = r_projects_to_update.task_rowid
         ;*/

      end loop;

      l_commited_rows := sql%rowcount;
      commit;

      -- connect updated / removed tasks with it's project by project's project_id
      for r_link_task_to_projects in c_link_task_to_projects loop

         update ps_task t
         set    t.project_id = r_link_task_to_projects.project_id
         where  t.rowid      = r_link_task_to_projects.task_rowid
         ;

      end loop;

      l_commited_rows := sql%rowcount;
      commit;

      --dbms_output.put_line('An action of ' || l_commited_rows || ' projects was updated to U.');

      l_commited_rows := 0;

      -- connect removed projects with it's tasks by project's project_id
      for r_tasks_to_update in c_tasks_to_update loop

         update ps_task t
         set    t.project_id = r_tasks_to_update.project_id
         where  t.rowid      = r_tasks_to_update.rowid
         ;

      end loop;

      l_commited_rows := sql%rowcount;
      commit;

      --dbms_output.put_line(l_commited_rows || ' removed tasks are connected to projects.');

   exception

      when others then

         rollback;
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_update_country_projct_status;

   end update_country_projects_status;

   ------------------------------------------------
   -- create_failed_projects_tasks
   ------------------------------------------------
   procedure create_failed_projects_tasks(p_java_updated varchar2) is

      -- 1. cursor for REGION PROJECTS(EU, US, CA, IL, JP)
      cursor c_region_proj is
         -- java failed records
         select p.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action -- b.action ?
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E' -- b.action ?
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- join
           and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           and  b.gpms_id  is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select p.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action -- b.action ?
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E' -- b.action ?
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- join
           --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           --
           and  (-- projects created in plw
                 (b.gpms_id is null) or
                 (-- WPs created in plw for migrated projects
                  b.gpms_id is not null and
                  not exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = b.gpms_id
                                and  gl.plw_pjt_id     = b.plw_pjt_id
                                and  gl.plw_int_number = b.plw_int_number)
                 )
                )
           -- parameters --
           --and  p_java_updated = 'Y'

         union all

         -- java program does not run records
         select p.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action -- b.action ?
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E' -- b.action ?
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- join
           and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  p.env_activity_id             = '32'
           --and  p.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id  is not null
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'N'
         union all
         select p.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action -- b.action ?
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E' -- b.action ?
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- join
           --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  p.env_activity_id             = '32'
           --and  p.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           --
           and  (-- projects created in plw
                 (b.gpms_id is null) or
                 (-- WPs created in plw for migrated projects
                  b.gpms_id is not null and
                  not exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = b.gpms_id
                                and  gl.plw_pjt_id     = b.plw_pjt_id
                                and  gl.plw_int_number = b.plw_int_number)
                 )
                )
           -- parameters --
           --and  p_java_updated = 'N'
          ;

      -- 2.b. cursor for COUNTY level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_cntr is
         -- java failed records
         select t.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed
           and  t.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  b.gpms_id                     is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select t.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed
           and  t.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   PSNEXT_UNIQUE_PROJECTS_BACKUP s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           and  b.gpms_id                     is not null
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select t.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed
           and  t.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           --
           and  b.gpms_id                     is null
           -- parameters --
           --and  p_java_updated = 'Y'

         union all
         -- java program does not run records
         select t.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  t.env_activity_id             = '32'
           --and  t.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  b.gpms_id                     is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'N'
         union all
         select t.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  t.env_activity_id             = '32'
           --and  t.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           and  b.gpms_id is not null
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'N'
         union all
         select t.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then t.action
                   -- I
                   when b.action = 'I' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        t.action = 'E'
                      then t.action
                   -- U
                   when b.action = 'U' and
                        t.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        t.action = 'E'
                      then t.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_task           t,
                export_grd_backup b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  t.env_activity_id             = '32'
           --and  t.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           and  b.gpms_id                     is null
           -- parameters --
           --and  p_java_updated = 'N'
         ;

      -- 2.a. cursor for PROJECTS level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_prj is
         select distinct
                p.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  p.project_id                  = t.project_id
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is not null
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select distinct
                p.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is not null
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select distinct
                p.rowid,
                --b.action,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           and  b.status                      = 'E'
           and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is null
           -- parameters --
           --and  p_java_updated = 'Y'

         -- java program does not run records
         union all
         select distinct
                p.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  p.project_id                  = t.project_id
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           --and  p.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is not null
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select distinct
                p.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           --and  p.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is not null
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           -- parameters --
           --and  p_java_updated = 'Y'
         union all
         select distinct
                p.rowid,
                case
                   -- N
                   when b.action = 'N'
                      then p.action
                   -- I
                   when b.action = 'I' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'I' and
                        p.action = 'E'
                      then p.action
                   -- U
                   when b.action = 'U' and
                        p.action in('N', 'U')
                      then b.action
                   when b.action = 'U' and
                        p.action = 'E'
                      then p.action
                   -- E
                   when b.action = 'E'
                      then b.action
                end action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  1 = 1
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java failed records
           and  p.env_activity_id             = '32'
           --and  p.action                      in ('I', 'U', 'E')
           and  b.status                      = 'J'
           --and  b.action                      in('N', 'U', 'I', 'E')
           -- filter out blocked projects
           and  b.gpms_id                     is null
           -- parameters --
           --and  p_java_updated = 'Y'
         ;

   begin

      -- update status for the failed region projects(EU, US, CA, IL, JP)
      for r_region_proj in c_region_proj loop

         update ps_project t
         set    t.action = r_region_proj.action
         where  t.rowid  = r_region_proj.rowid
         ;

      end loop;

      -- update status for the failed country tasks(EMIA(not IL), ASIA(not JP), LATAM)
      for r_country_proj_cntr in c_country_proj_cntr loop

         update ps_task t
         set    t.action = r_country_proj_cntr.action
         where  t.rowid  = r_country_proj_cntr.rowid
         ;

      end loop;

      -- update status for the failed country projects(EMIA(not IL), ASIA(not JP), LATAM)
      for r_country_proj_prj in c_country_proj_prj loop

         update ps_project t
         set    t.action = r_country_proj_prj.action
         where  t.rowid  = r_country_proj_prj.rowid
         ;

      end loop;

   exception

      when others then

         rollback;
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;

         raise e_create_failed_projects_tasks;

   end create_failed_projects_tasks;

   ------------------------------------------------
   -- UpdateLevelStatus
   ------------------------------------------------
         -- This Procedure Will Update Level Status
         -- Step: E = T (That means - steps exist in PSNext and not on GPMS, that's why need to Terminate)
         -- Task: For GRD & PRAM it will never be "I"
         -- ********** The order of the code is very important!!!!!***********
   ------------------------------------------------
   procedure UpdateLevelStatus(P_Env_Id  Varchar2) is

      v_return_status    varchar2(1);
      v_proj_id          varchar2(100);
      l_commited_records number;

      cursor cr_proj is
         select *
         from   ps_project pp
         where  pp.env_activity_id in (32, 33, 31);

      /*Cursor Cr_Proj_T Is
         Select *
         From   Ps_Project pp
         Where  pp.env_activity_id = 10
           And  pp.action          = 'T';*/

      /*Cursor Cr_Steps(Cp_Proj_Id varchar2) Is
         Select distinct
                ps.task_id,
                ps.action
         From   Ps_Step ps
         Where  ps.gpms_id = Cp_Proj_Id
           And  ps.action In ('I','T','U','N');*/

      cursor cr_task(cp_proj_id varchar2) is
         select distinct pt.gpms_id
         from   ps_task pt
         where  pt.gpms_id    = cp_proj_id
           and  pt.action     = 'U'
           --And  pt.country_id <> 'GRD'
           and  pt.country    <> 'GRD'
           ;

      cursor cr_task_grd(cp_proj_id varchar2) is
         select distinct pt.gpms_id
         from   ps_task pt
         where  pt.gpms_id    = cp_proj_id
           and  pt.action     = 'U'
           --And  pt.country_id = 'GRD'
           and  pt.country    = 'GRD'
           ;

      /*Cursor Cr_Task_Prisma(Cp_Proj_Id varchar2) Is
         Select distinct pt.gpms_id
         From   Ps_Task pt
         Where  pt.gpms_id = Cp_Proj_Id
           And  pt.action  = 'I'; */

      /*Arik 06/12/2007 - Temp, to change status for existing projects*/
      cursor cr_psnext is
         select ltrim(rtrim(p.project_id)) project_id
         from   psnext_unique_project p;

      /*Arik 17/05/2010  Update status to N when project is PURGE*/
      /*Cursor Cr_prisma_Psnext Is
         select ltrim(rtrim(p.project_id)) project_id
         from   psnext_prisma_unique_project p;*/


   begin

      --update Step If stsus=E (Means that step not exist in GPMS and need to terminate it)
      /*If P_Env_Id <> 'PRISMA' Then

         Update Ps_Step ps
         Set    action = 'T'
         Where  ps.action = 'E'
         And    ps.status Not In('Terminated','Historic');--In PRAM historic strengt no need to br delete

         Commit;

         --In PRAM historic strengt no need to br delete
         Update Ps_Step ps
         Set    action = 'N'
         Where  ps.action = 'E'
           And  ps.status In ('Terminated','Historic');

         Commit;

      End If;

      --For GRD Only (After tue delta if no step delta no need to update task)
      Update Ps_Task pt
      Set    action = 'N'
      Where  pt.task_id Not In (Select ps.task_id
                                From   ps_step ps
                                Where  ps.action In ('I','U','T'))
        --And  pt.country_id = 'GRD'
        And  pt.country = 'GRD'
      ;

      --For GRD Only (After tue delta need to verify that the steps will upload, so task level need to be U)
      If P_Env_Id <> 'PRISMA' Then

         If P_Env_Id = 'PRAM' Then

           Update Ps_Task pt
           Set    action = 'N'
           Where  1 = 1
             --and  pt.country_id = 'HR'
             and  pt.country = 'HR'
             And  pt.project_id In (Select pp.project_id
                                    From   ps_project pp
                                    Where  pp.env_activity_id = '33'
                                      And  pp.action          <> 'I')
             And  action = 'I';

         End If;

         commit;

         Update Ps_Task pt
         Set    action = 'U'
         Where  pt.task_id    In (Select ps.task_id
                                  From   ps_step ps
                                  Where  ps.action In ('I','U','T'))
           And  pt.project_id In (Select pp.project_id
                                  From   ps_project pp
                                  Where  pp.env_activity_id In ('32','33'))
           --And  pt.country_id Not In ('GRD','HR')
           And  pt.country    Not In ('GRD','HR')
           ;

         Commit;

         Update Ps_Task pt
         Set    action = 'U'
         Where  pt.task_id In (Select ps.task_id
                               From   ps_step ps
                               Where  ps.action In ('I','U','T'))
           And  pt.project_id In (Select pp.project_id
                                  From   ps_project pp
                                  Where  pp.env_activity_id = '32'
                                    And  pp.target_market   <> 'LATAM');
         Commit;

      Else

         Update Ps_Task pt
         Set    action = 'U'
         Where  --nvl(pt.action,'C') in ('U','I') and
                pt.task_id In (Select ps.task_id
                                  From   ps_step ps
                                  Where  ps.action In('I','U','T'))
           And  pt.project_id In (Select pp.project_id
                                  From   ps_project pp
                                  Where  pp.env_activity_id In('31'))
           And  action        <> 'I';

      End If;

      --Handle cases with T status
      For t In Cr_Proj_T Loop

         v_proj_id := null;

         --Will not create projects that came with T
         Update Ps_Project pp
         Set    action     = 'N'
         Where  pp.gpms_id = t.gpms_id
           And  pp.action  = 'I';

         If P_Env_Id = 'GRD' Then

            --Will not update/create tasks that came with T (Only MOP)
            Update Ps_Task pt
            Set    action = 'N'
            Where  pt.gpms_id  = t.gpms_id
              And  ((pt.action  in ('I','U','E') And
                     --country_id = 'GRD')  -- 19/07/2011 By Request to update H/T countries for H/T projects
                     country    = 'GRD')
                Or (pt.action  in ('I','E')     And
                    --country_id <> 'GRD'))
                    country    <> 'GRD'))
            ;

            Update Ps_Step ps
            Set    action     = 'N'
            Where  ps.gpms_id = t.gpms_id
              And  ps.action  in ('I','U');

         End If;

         --Verify that Not Active project from gpms will update Active projects in PSNext
         Begin

            select t.gpms_id
            Into   v_proj_id
            From   Ps_Project ps
            Where  ps.gpms_id  = t.gpms_id
              And  substr(ps.env_activity_id,1,1) = 2
              And  ps.status  not in ('Terminated','Historic');

         Exception
            when others Then
               v_proj_id := null;
         End;

         If v_proj_id Is Not Null Then

            Update Ps_Project pp
            Set    action = 'U'
            Where  pp.gpms_id                     = v_proj_id
              And  substr(pp.env_activity_id,1,1) = 3;

         Else--For GRD Only

            Update Ps_Project pp
            Set    action = 'N'
            Where  pp.gpms_id         = t.gpms_id
              And  pp.env_activity_id = 32
              And  pp.action          <> 'I'
              And  pp.status          = 'Terminated';--20/07/10 If the status is Historic -> needs to update into Terminated -> action will stay U

         End If;

      End Loop;

      Commit;*/

      -- Verify that project will be updated if it has task/step that need to be update.
      for t in cr_proj loop

         -- All tasks(all ENV) beside MOP(GRD countries)
         for b in cr_task (ltrim(rtrim(t.gpms_id))) loop

            update ps_project pp
            set    action     = 'U'
            where  pp.gpms_id = b.gpms_id
              and  pp.action  = 'N'
              /*if GRD & status is T => don't update.*/
              /*22/07/10 By Request to update H/T countries for H/T GRD projects as well (LATAM). Condition marked*/
              and  ((pp.env_activity_id = '32' /*and pp.status not in ('Terminated','Historic')*/ And
                     pp.target_market   in ('LATAM','INT'))
               or  (pp.env_activity_id  <> '32'));

         end loop;

         l_commited_records := sql%rowcount;
         commit;

         /*if(l_commited_records > 0) then
            dbms_output.put_line('An action of ' || l_commited_records || ' project (country <> GRD) records was updated from N to U.');
         end if;*/

         --Only for GRD countries
         for b in cr_task_grd (ltrim(rtrim(t.gpms_id))) loop

            update ps_project pp
            set    action     = 'U'
            where  pp.gpms_id         = b.gpms_id
              and  pp.action          = 'N'
              /*if GRD & status is T => don't update.*/
              and  pp.env_activity_id = '32'
              and  pp.status          not in ('Terminated','Historic')
              and  pp.target_market   <> 'LATAM';

         end loop;

         l_commited_records := sql%rowcount;
         commit;

         /*if(l_commited_records > 0) then
            dbms_output.put_line('An action of ' || l_commited_records || ' project (country = GRD) records was updated from N to U.');
         end if;*/


         /*If P_Env_Id = 'PRISMA' Then

            For b In Cr_Task_Prisma (ltrim(rtrim(t.gpms_id))) Loop

               Update Ps_Project pp
               Set    action     = 'U'
               Where  pp.gpms_id = b.gpms_id
                 And  pp.action  = 'N'
               ;

            End Loop;

            commit;

         End If;*/

      End Loop;

      -- No need to update/create projects with no GAIN
      /*update ps_project p
      set    p.action = 'N'
      where  p.gain            is null
        and  p.env_activity_id like '3%';

      l_commited_records := sql%rowcount;
      commit;

      if(l_commited_records > 0) then
         dbms_output.put_line('An action of ' || l_commited_records || ' project (without GAIN) records was updated to N.');
      end if;*/

      --Action T will be as U in JAVA code.
      /*If P_Env_Id = 'PRISMA' Then

         Update ps_project p
         Set p.action = 'U'
         where p.action ='T'
           and p.env_activity_id = '31';
         Commit;

      End If;*/


      if(p_env_id = 'GRD') then

         -- Task is created in PLW - Action must be 'U'
         /*update ps_task p
         set    p.action = 'U'
         where  p.action = 'I';

         l_commited_records := sql%rowcount;
         commit;

         if(l_commited_records > 0) then
            dbms_output.put_line('An action of ' || l_commited_records || ' task records was updated from I to U.');
         end if;*/

         /*for i in cr_psnext loop

             update ps_project pr
             set    action = 'N'
             where  ltrim(rtrim(pr.gpms_id)) = i.project_id
               and  pr.env_activity_id       = 32
               and  pr.action                = 'I';

             l_commited_records := sql%rowcount;
             commit;

             if(l_commited_records > 0) then
                dbms_output.put_line('An action of ' || l_commited_records || ' existing project records was updated from I to N.');
             end if;

          end loop;*/

          null;

      /*elsif P_Env_Id = 'PRISMA' Then

         For i in Cr_prisma_Psnext Loop

            Update PS_PROJECT pr
            Set    action = 'N'
            Where  ltrim(rtrim(pr.gpms_id)) = i.project_id
              And  pr.env_activity_id       = 32
              And  pr.action                = 'I';

            Commit;

         End Loop;*/

      end if;

   end UpdateLevelStatus;

   ------------------------------------------------
   -- handle_log_table
   ------------------------------------------------
   procedure handle_log_table(p_bpel_process_name  varchar2,
                              p_process_step       varchar2,
                              p_bpel_instance_id   number   default null,
                              p_java_step          varchar2 default null,
                              p_gpms_id            varchar2 default null,
                              p_gpms_exp_id        number   default null,
                              p_site               varchar2,
                              p_status             varchar2,
                              p_env                varchar2,
                              p_error              varchar2 default null,
                              p_status_out     out varchar2) is
      v_bpel_id  number(10);

   begin

      select bpel_log_sq.nextval
      into   v_bpel_id
      from   dual;

      --P_Status_Out := 'S';

      begin

         insert into teva_bpel_ps_log
         values(v_bpel_id,
                p_bpel_process_name,
                p_process_step,
                p_bpel_instance_id,
                p_java_step,
                p_gpms_id,
                p_gpms_exp_id,
                p_site,
                p_env,
                p_status,
                p_error,
                sysdate)
         ;

      exception
         when others then
            p_status_out := 'E';
            dbms_output.put_line(substr(sqlerrm, 1, 240));
      end;

   end handle_log_table;


   ------------------------------------------------
   -- get_where_for_u
   /*------------------------------------------------
      NAME:       GET_WHERE_FOR_U
      PURPOSE:

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        04/02/2007          1. Created this function.

      NOTES:

      Automatically available Auto Replace Keywords:
         Object Name:     GET_WHERE_FOR_U
         Sysdate:         04/02/2007
         Date and Time:   04/02/2007, 12:34:49, and 04/02/2007 12:34:49
         Username:         ik
         Table Name:       PS_FIELD

   ------------------------------------------------*/
   function get_where_for_u(p_env_id     ps_env.env_id%type,
                            p_level_name ps_level_name.level_name%type ) return long is


      v_fields      long; --varchar2(5000);
      v_field       varchar2(4000);
      v_null_number number       := 0;
      v_null_date   varchar2(50) := 'to_date(''01011900'',''ddmmyyyy'')';
      v_null_char   varchar2(4)  := 'null';

      cursor c_fields is
         select  *
         from    ps_env_field_vw
         where   env_id     = p_env_id
           and   level_name = p_level_name;

   begin

      for  v_rec in c_fields loop

         --   DBMS_OUTPUT.PUT_LINE ('IN LOOP...V_REC.FIELD_TYPE='||V_REC.FIELD_TYPE); -- FOR DEBUG
         --IF V_REC.FIELD_NAME <> 'GPMS_ID' AND V_REC.FIELD_NAME <> 'COUNTRY_ID'  AND V_REC.FIELD_NAME <> ' STRENGTH_ID'  THEN

         if(v_rec.uk_part = 0)  then

           --  DBMS_OUTPUT.PUT_LINE ('V_REC.UK_PART=' || V_REC.UK_PART); -- FOR DEBUG
           if(v_rec.field_type in('NUMBER', 'FLOAT')) then

              v_field := ' nvl(round(tbl_from.' || v_rec.field_name || ', 6), ' || v_null_number || ') ' ||
                       '<> nvl(round(tbl_to.'   || v_rec.field_name || ', 6), ' || v_null_number || ')';

           elsif(v_rec.field_type = 'DATE') then

              v_field  := ' nvl(tbl_from.' || v_rec.field_name || ',' || v_null_date || ') ' ||
                        '<> nvl(tbl_to.'   || v_rec.field_name || ',' || v_null_date || ')';

           else

              v_field  := ' nvl(tbl_from.' || v_rec.field_name || ',''' || v_null_char || ''') ' ||
                        '<> nvl(tbl_to.'   || v_rec.field_name || ',''' || v_null_char || ''')';

           end if;

           -- DBMS_OUTPUT.PUT_LINE ('V_FIELD=' || V_FIELD); -- FOR DEBUG

           if(v_fields is null) then

              v_fields := '('||v_field;

           else

              v_fields := v_fields || ' or ' || v_field;

           end if;

           --DBMS_OUTPUT.PUT_LINE ('V_FIELDS=' || V_FIELDS); -- FOR DEBUG

         end if;

      end loop;

      if(p_env_id = 'GRD' and
         v_fields is null) then

         /*v_field  := '(nvl(tbl_from.' || 'country_id' || ', ''' || v_null_char || ''') ' ||
                   '<> nvl(tbl_to.'   || 'country_id' || ', ''' || v_null_char || ''')';*/

         v_field  := '(nvl(tbl_from.' || 'country' || ', ''' || v_null_char || ''') ' ||
                   '<> nvl(tbl_to.'   || 'country' || ', ''' || v_null_char || ''')';

         v_fields := v_field || ') and not(tbl_to.terminated = 0 and tbl_from.terminated = 1)';
         --v_fields := v_field || ')';

         --' 1=1 AND NOT(TBL_TO.TERMINATED =0 AND TBL_FROM.TERMINATED =1)';

      else

         v_fields := v_fields || ') and not(tbl_to.terminated = 0 ' ||
                                  ' and tbl_from.terminated   = 1)';

         --v_fields := v_field || ')';

      end if;

      -- V_FIELDS := V_FIELDS||') AND NOT(TBL_TO.TERMINATED =0 AND TBL_FROM.TERMINATED =1)';

      return v_fields;

      exception

        when no_data_found then
           null;

        when others then
           -- consider logging the error and then re-raise
           raise;

   end get_where_for_u;

   ------------------------------------------------
   -- get_where_for_n
   ------------------------------------------------
   /*
     NAME:       GET_ENV_FIELDS_N
     PURPOSE:

     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        04/02/2007          1. Created this function.

     NOTES:

     Automatically available Auto Replace Keywords:
        Object Name:     GET_WHERE_FOR_N
        Sysdate:         04/02/2007
        Date and Time:   04/02/2007, 12:18:44, and 04/02/2007 12:18:44
        Username:         ik
        Table Name:      PS_FIELD
   ------------------------------------------------*/
   function get_where_for_n(p_env_id     ps_env.env_id%type,
                            p_level_name ps_level_name.level_name%type) return long IS


      v_fields      long; --varchar2(5000);
      v_field       varchar2(4000);
      v_null_number number       := 0;
      v_null_date   varchar2(50) := 'to_date(''01011900'',''ddmmyyyy'')';
      v_null_char   varchar2(4)  := 'null';

   cursor c_fields is
      select *
      from   ps_env_field_vw
      where  env_id     = p_env_id
        and  level_name = p_level_name;

   begin

      for  v_rec in c_fields loop

         if(v_rec.field_type in ('NUMBER', 'FLOAT')) then

            v_field := ' nvl(tbl_from.' || v_rec.field_name || ',' || v_null_number || ') = ' ||
                        'nvl(tbl_to.'   || v_rec.field_name || ',' || v_null_number || ')';

         elsif(v_rec.field_type = 'DATE') then

            v_field := ' nvl(tbl_from.' || v_rec.field_name || ',' || v_null_date || ') = ' ||
                        'nvl(tbl_to.'   || v_rec.field_name || ',' || v_null_date  || ')';

         else

            v_field := ' nvl(tbl_from.' || v_rec.field_name || ',''' || v_null_char || ''')= ' ||
                        'nvl(tbl_to.'   || v_rec.field_name || ',''' || v_null_char || ''')';

         end if;

         if(v_fields is null) then

            v_fields := v_field;

         else

            v_fields := v_fields || ' and' || v_field;

         end if;

      end loop;

      return v_fields;

   exception

      when no_data_found then
         null;

      when others then
         -- consider logging the error and then re-raise
         raise;

   end get_where_for_n;

   ------------------------------------------------
   -- get_where_for_pair
   ------------------------------------------------
   /* NAME:       GET_WHERE_FOR_PAIR
     PURPOSE:

     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        08/02/2007          1. Created this function.

     NOTES:

     Automatically available Auto Replace Keywords:
        Object Name:     GET_WHERE_FOR_PAIR
        Sysdate:         08/02/2007
        Date and Time:   08/02/2007, 14:38:33, and 08/02/2007 14:38:33
        Username:         (set in TOAD Options, Procedure Editor)
        Table Name:       (set in the "New PL/SQL Object" dialog)
   ------------------------------------------------*/
   function get_where_for_pair(p_env_id      ps_env.env_id%type,
                               p_level_name  ps_level_name.level_name%type ) return varchar2 is


      v_fields varchar2(4000);
      v_field  varchar2(400);

      cursor c_fields is
         select t.*
         from   ps_env_field_vw t
         where  t.env_id     = p_env_id
           and  t.level_name = p_level_name
         ;

   begin

      for  v_rec in c_fields loop

         if(v_rec.uk_part = 1 and
            v_rec.field_name not in('GPMS_ID',    -- GPMS_ID is not UK for PS_PROJECT any more, due to data created in PLW
                                    'COUNTRY_ID') -- COUNTRY_ID is replaced by COUNTRY for PS_TASK UK
                                    ) then

            v_field  := ' tbl_from.' || v_rec.field_name || ' = tbl_to.' || v_rec.field_name;

            -- DBMS_OUTPUT.PUT_LINE ('V_FIELD=' || V_FIELD);

            if(v_fields is null) then

               v_fields := v_field;

            else

               v_fields := v_fields || ' and ' || v_field;

            end if;

         end if;

      end loop;

      /*If((P_LEVEL_NAME = 'STEP') Or (P_LEVEL_NAME = 'TASK' And P_ENV_ID in ('PRAM','PRISMA'))) Then

       If V_FIELDS Is Null Then

        V_FIELDS := 'TBL_FROM.GPMS_ID = TBL_TO.GPMS_ID ';

       Else

        V_FIELDS := 'TBL_FROM.GPMS_ID = TBL_TO.GPMS_ID AND '||V_FIELDS;

       End If;

      End If;*/

      return v_fields;

    exception

       when no_data_found then
          null;

       when others then
          -- consider logging the error and then re-raise
          raise;

   end get_where_for_pair;

   ------------------------------------------------
   -- get_env_fields
      /******************************************************************************
        NAME:       GET_ENV_FIELDS
        PURPOSE:

        REVISIONS:
        VER        DATE        AUTHOR           DESCRIPTION
        ---------  ----------  ---------------  ------------------------------------
        1.0        04/02/2007          1. CREATED THIS FUNCTION.

        NOTES:

        AUTOMATICALLY AVAILABLE AUTO REPLACE KEYWORDS:
           OBJECT NAME:     GET_PROJECT_FIELDS
           SYSDATE:         04/02/2007
           DATE AND TIME:   04/02/2007, 11:36:43, AND 04/02/2007 11:36:43
           USERNAME:        IK
           TABLE NAME:     PS_FIELD
      ******************************************************************************/
   ------------------------------------------------
   function get_env_fields(p_env_id      ps_env.env_id%type,
                           p_level_name  ps_level_name.level_name%type,
                           p_tbl         varchar) return varchar2 is

      v_fields      varchar2(32000);
      v_field       varchar2(2000);

      cursor c_fields is
         select *
         from   ps_env_field_vw t
         where  env_id       =  p_env_id
           and  level_name   =  p_level_name
           and  t.field_name <> 'PRJ_ID';

   begin
           
      for  v_rec in c_fields loop

         if(p_tbl is null) then

            v_field := v_rec.field_name;

         else

            v_field := p_tbl || '.' || v_rec.field_name;

         end if;
         
          --DBMS_OUTPUT.PUT_LINE ('V_FIELD=' || V_FIELD);

         if(v_fields is null) then

            v_fields :=  v_field;

         else

           v_fields := v_fields || ', ' ||  v_field;

         end if;
         
         if(v_fields is null) then

            v_fields :=  v_field;         

         end if;
      end loop;

      --In case step - GPMS_ID field is not exist in the env' table.
      /*If((P_LEVEL_NAME = 'STEP') Or
         (P_LEVEL_NAME In ('STEP','TASK') And
         P_ENV_ID in ('PRAM','PRISMA'))) Then
         IF P_TBL IS NULL THEN
          V_FIELDS := 'GPMS_ID' || ',' ||V_FIELDS;
        ELSE
          V_FIELDS := P_TBL||'.'||'GPMS_ID' || ',' ||V_FIELDS;
        END IF;
       End If;*/

      return v_fields;

      exception
         when no_data_found then
            null;

         when others then
            -- consider logging the error and then re-raise
            raise;

   end get_env_fields;

   ------------------------------------------------
   -- write_diff_err
   ------------------------------------------------
   procedure write_diff_err(p_gpms_exp_id   ps_diff_err.gpms_exp_id%type,
                            p_env_id        ps_diff_err.env_id%type,
                            p_err_type_id   ps_diff_err.err_type_id%type,
                            p_level_name    ps_diff_err.level_name%type  default null,
                            p_gpms_id       ps_diff_err.gpms_id%type default null,
                            p_country_id    ps_diff_err.country_id%type  default null,
                            p_strength_id   ps_diff_err.strength_id%type  default null,
                            p_ora_err_code  ps_diff_err.ora_err_code%type default null,
                            p_err_text      ps_diff_err.err_text%type  default null,
                            p_sql_text      ps_diff_err.sql_text%type  default null) is

   /******************************************************************************
      NAME:       WRITE_DIFF_ERR
      PURPOSE:    WRITE_DIFF_ERR

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        01/02/2007          1. Created this procedure.

      NOTES:

      Automatically available Auto Replace Keywords:
         Object Name:     WRITE_DIFF_ERR
         Sysdate:         01/02/2007
         Date and Time:   01/02/2007, 15:50:33, and 01/02/2007 15:50:33
         Username:         ik
         Table Name:       PS_DIFF_ERR

   ******************************************************************************/

      l_records number := 0;

   begin

      insert into ps_diff_err(gpms_exp_id,
                              env_id,
                              gpms_id,
                              country_id,
                              strength_id ,
                              err_type_id,
                              err_date,
                              err_text,
                              ora_err_code,
                              sql_text,
                              level_name)
      values(nvl(p_gpms_exp_id, 0),
             nvl(p_env_id, ''),
             nvl(p_gpms_id, '000000'),
             nvl(p_country_id, ''),
             nvl(p_strength_id, '') ,
             nvl(p_err_type_id, 10),
             sysdate,
             p_err_text,
             p_ora_err_code,
             p_sql_text,
             p_level_name)
      ;

      l_records := sql%rowcount;
      commit;

      --dbms_output.put_line(l_records || ' are inserted into ps_diff_err.');

   exception
      when others then
         g_errbuf := sqlerrm;
         raise e_write_diff_err;

   end write_diff_err;

   ------------------------------------------------
   -- get_env_activity_id
   ------------------------------------------------
   /******************************************************************************
      NAME:       GET_ENV_PSNEXT_ID
      PURPOSE:     GET ENV_ACTIVITY_ID FOR INSERT  PROJECT RECORDS

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        07/02/2007          1. Created this function.

      NOTES:

      Automatically available Auto Replace Keywords:
         Object Name:     GET_ENV_ACTIVITY_ID
         Sysdate:         07/02/2007
         Date and Time:   07/02/2007, 14:56:46, and 07/02/2007 14:56:46
         Username:         ik
         Table Name:       PS_ENV_ACTIVITY

   ******************************************************************************/
   function get_env_activity_id(p_env_id       varchar2,
                                p_activity_id  number) return number is

      l_env_activity_id number;

   begin

      select env_activity_id
      into   l_env_activity_id
      from   ps_env_activity
      where  env_id      = p_env_id
        and  activity_id = p_activity_id;

      return l_env_activity_id;

   exception

      when others then
         g_errbuf := sqlerrm;
         raise e_get_env_activity_id;

   end get_env_activity_id;

   ------------------------------------------------
   -- diff_level
   ------------------------------------------------
   /*   Data comparison for one Level (PROJECT or TASK or STEP)
        Procedure compares data exported by GPMS (environment identifies by P_FROM_ENV_ID)
        and data exported by PSNext (environment identifies by P_TO_ENV_ID).
        Comparison result:  value I/U/T/E/N in field ps_project.ACTION in the new lines,
        created by this procedure. Procedure creates one line per each line of GPMS (with the same data of GPMS) for importing by PSNext.

        ACTION VALUES:
        I - The new line for inserting by PSNext
        U - Line exists - for updating by PSNext
        T - GPMS status changed to Brand disc/Historic/Terminated
        E - Error in data
        N - No difference*/
   ------------------------------------------------
   procedure diff_level(p_level          varchar2,
                        p_gpms_exp_id    number,
                        p_from_env_id    varchar2,
                        p_to_env_id      varchar2) is

      v_sql                        /*long;--*/ varchar2(32000);
      v_env_fields                 varchar2 (4000);
      v_env_fields_tbl             long;--   varchar2 (4000);
      v_sql_where                  long;--varchar2 (5000);
      v_sql_pair_cr0               varchar2 (4000);
      v_sql_pair_cr                varchar2 (4000);
      v_action                     ps_project.action%type;

      v_rec_count                  number;
      v_env_activity_id            ps_env_activity.env_activity_id%type;

      v_ora_err_text               varchar(500);

      v_tbl                        varchar(100);
      v_view                       varchar(100);
      v_err_type_id                ps_err_type.err_type_id%type;
      v_return_status              varchar2(5);

      l_primary_key_field          varchar2(100);
      l_unique_key_fields          varchar2(100);
      l_primary_key_field_value    varchar2(200);
      l_unique_key_fields_values   varchar2(200);

      l_commited_records_t         number := 0;
      l_commited_records_n         number := 0;
      l_commited_records_u         number := 0;
      l_commited_records_i         number := 0;
      l_commited_records_e         number := 0;
      l_commited_records_err       number := 0;
      l_total_commited_records     number := 0;

   begin
      -- SET TABLE NAME AND VIEW NAME FOR LEVEL
      v_tbl  := 'PS_' || p_level;
      v_view := 'PS_' || p_level || '_EXP_VW';

      -- get env_activity_id for compare activity
      -- (activity_id = 2 is activity for input) and current environment( p_to_env_id)
      v_env_activity_id := get_env_activity_id(p_to_env_id,
                                               2);  -- 32

      -- set variables to general_pack
      general_pack.set_env_activity_id(v_env_activity_id);
      general_pack.set_gpms_exp_id(p_gpms_exp_id);

      --  validate PLW (GPMS) has data

      if(p_level = 'PROJECT') then

         select count(1)
         into   v_rec_count
         from   ps_project_exp_vw t
         where  t.env_id      = p_from_env_id
           and  t.gpms_exp_id = p_gpms_exp_id
         ;

      elsif(p_level = 'TASK') then

         select count(1)
         into   v_rec_count
         from   ps_task_exp_vw t
         where  t.env_id      = p_from_env_id
           and  t.gpms_exp_id = p_gpms_exp_id
         ;

      end if;


      if(v_rec_count = 0) then

         dbms_output.put_line('No PLW (GPMS) ' || p_level || ' records.');

         v_err_type_id := 1;
         write_diff_err(p_gpms_exp_id,
                        p_to_env_id,
                        v_err_type_id,
                        p_level);

      end if;

      --  validate GRD has data
      if(p_level = 'PROJECT') then


         select count(1)
         into   v_rec_count
         from   ps_project_exp_vw t
         where  t.env_id      = p_to_env_id
           and  t.gpms_exp_id = p_gpms_exp_id
         ;

      elsif(p_level = 'TASK') then

         select count(1)
         into   v_rec_count
         from   ps_task_exp_vw t
         where  t.env_id      = p_to_env_id
           and  t.gpms_exp_id = p_gpms_exp_id
         ;

      end if;

      if(v_rec_count = 0) then

         dbms_output.put_line('No GRD ' || p_level || ' records.');

         v_err_type_id := 2;
         write_diff_err(p_gpms_exp_id,
                        p_to_env_id,
                        v_err_type_id,
                        p_level);

      end if;

      -- get fields' names from setup tables separated by coma (field1, field2)

      v_env_fields := get_env_fields(p_to_env_id,
                                     p_level,
                                     null);


      -- get fields' names from setup tables with alias suffix and separated by coma (tbl_from.field1, tbl_from.filed2)
      v_env_fields_tbl := get_env_fields(p_to_env_id,
                                         p_level,
                                         'TBL_FROM');


      -- build condition for pair matching (tbl_from.field1 = tbl_to.field1 and tbl_from.field2 = tbl_to.field2 ...)
      v_sql_pair_cr0 := get_where_for_pair(p_to_env_id,
                                           p_level);


      -- concatenate parameters' values (gpms_exp_id, env_id)
      v_sql_pair_cr := v_sql_pair_cr0 || ' and tbl_to.gpms_exp_id = '   || p_gpms_exp_id ||
                                         ' and tbl_from.gpms_exp_id = ' || p_gpms_exp_id ||
                                         ' and tbl_to.env_id = '''      || p_to_env_id   ||
                                       ''' and tbl_from.env_id = '''    || p_from_env_id || '''';

      --============== INSERT RECORDS THAT BECAME TERMINATED *** T *** =============
      /*dbms_output.put_line('T');

      v_action := 'T';

      v_sql_where := ' where ' || v_sql_pair_cr || ' '  ||
                        'and tbl_from.terminated <> 0 ' ||
                        'and tbl_to.terminated   = 0 ';

      v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                     -- pk
                                                     l_primary_key_field || ', ' ||
                                                     -- uk
                                                     l_unique_key_fields || ', ' ||
                                                     --
                                                     'ACTION, ' ||
                                                     -- fields from setup table
                                                     v_env_fields || ') ' ||
               'select \*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) *\ ' ||
                       -- pk
                       'ps_project_seq.nextval, ' ||
                       -- uk
                       'tbl_from.prj_id, '        ||
                       'tbl_from.gpms_exp_id, '   ||
                       --
                       '32, '                     ||
                       chr(39) || v_action || chr(39) || ',' ||
                       v_env_fields_tbl || ' ' ||
               'from ' || v_view || ' tbl_from, ' ||
                          v_view || ' tbl_to '    ||
               v_sql_where;

      --execute immediate v_sql;
      l_commited_records_t := sql%rowcount;
      commit;

      dbms_output.put_line('Inserted T: ' || l_commited_records_t);*/


      if(p_level = 'PROJECT') then

         l_primary_key_field         := 'project_id, ';

         l_unique_key_fields         := 'prj_id, '        ||
                                        'gpms_exp_id, '   ||
                                        'env_activity_id, ';

         l_primary_key_field_value   := 'ps_project_seq.nextval, ';

         l_unique_key_fields_values  := 'tbl_from.prj_id, '      ||
                                        'tbl_from.gpms_exp_id, ' ||
                                        '32, ';

      elsif(p_level = 'TASK') then

         l_primary_key_field         := 'task_id, ';

         l_unique_key_fields         := 'prj_id,  '         ||
                                        --'country, '         || -- is fetched in v_env_fields
                                        -- not part of UK
                                        'gpms_exp_id, '     ||
                                        'env_activity_id, ' ||
                                        'project_id, '
                                        ;

         l_primary_key_field_value   := 'ps_task_sq.nextval, ';

         l_unique_key_fields_values  := 'tbl_from.prj_id, '      ||
                                        --'tbl_from.country, '     || -- is fetched in v_env_fields_tbl
                                        -- not part of UK
                                        'tbl_from.gpms_exp_id, ' ||
                                        '32, '                   ||
                                        'tbl_from.project_id, '
                                        ;

      end if;

      --============== INSERT RECORDS THAT WERE NOT CHANGED  *** N *** (Not changed in PLW) =============
      v_action := 'N';

      v_sql_where := get_where_for_n(p_to_env_id,
                                     p_level);

      --SELECT GET_WHERE_FOR_N(P_TO_ENV_ID,P_LEVEL) INTO V_SQL_WHERE from dual; Arik 17/01/08 Can't Select into Long
      v_sql_where := ' where ' || v_sql_pair_cr ||
                       ' and ' || v_sql_where;

      
      v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                     -- pk
                                                     l_primary_key_field ||
                                                     -- uk
                                                     l_unique_key_fields ||
                                                     --
                                                     'ACTION, ' ||
                                                     -- fields from setup table
                                                     v_env_fields || ') ' ||
               'select /*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) */ ' ||
                       -- pk
                       l_primary_key_field_value  ||
                       -- uk
                       l_unique_key_fields_values ||
                       --
                       chr(39) || v_action || chr(39) || ',' ||
                       v_env_fields_tbl || ' ' ||
               'from ' || v_view || ' tbl_from, ' ||
                          v_view || ' tbl_to '    ||
               v_sql_where;
               
      execute immediate v_sql;
      l_commited_records_n := sql%rowcount;
      commit;

      /*insert into oleg(action,
                       sql_stmnt)
      values ('N',
              v_sql)
      ;
      commit;*/

      --dbms_output.put_line('Inserted N: ' || l_commited_records_n);

      --============== INSERT CHANGED RECORDS *** U *** (Updated in PLW) =============
      v_action    := 'U';
      v_sql_where := get_where_for_u(p_to_env_id,
                                     p_level);

      --  SELECT GET_WHERE_FOR_U(P_TO_ENV_ID,P_LEVEL) INTO V_SQL_WHERE from dual;

      v_sql_where := ' where ' || v_sql_pair_cr ||
                       ' and ' || v_sql_where;


      v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                     -- pk
                                                     l_primary_key_field ||
                                                     -- uk
                                                     l_unique_key_fields ||
                                                     --
                                                     'ACTION, ' ||
                                                     -- fields from setup table
                                                     v_env_fields || ') ' ||
               'select /*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) */ ' ||
                       -- pk
                       l_primary_key_field_value  ||
                       -- uk
                       l_unique_key_fields_values ||
                       --
                       chr(39) || v_action || chr(39) || ',' ||
                       v_env_fields_tbl || ' ' ||
               'from ' || v_view || ' tbl_from, ' ||
                          v_view || ' tbl_to '    ||
               v_sql_where;

      execute immediate v_sql;
      l_commited_records_u := sql%rowcount;
      commit;

      /*insert into oleg(action,
                       sql_stmnt)
      values ('U',
              v_sql)
      ;
      commit;*/

      --dbms_output.put_line('Inserted U: ' || l_commited_records_u);

      --============== INSERT ABSENT RECORDS *** I *** (Inserted in PLW) =============
      --dbms_output.put_line('I');

      v_action    := 'I';

      v_sql_where := ' where tbl_from.gpms_exp_id   = '   || p_gpms_exp_id ||
                        ' and ' || ' tbl_from.env_id = ''' || p_from_env_id ||
                     ''' and not exists(select gpms_id ' ||
                                       'from ' || v_view || ' tbl_to ' ||
                                       'where '|| v_sql_pair_cr0 ||
                                        ' and tbl_to.gpms_exp_id = ' || p_gpms_exp_id ||
                                        ' and tbl_to.env_id      ='''|| p_to_env_id   || ''')';

      v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                     -- pk
                                                     l_primary_key_field ||
                                                     -- uk
                                                     l_unique_key_fields ||
                                                     --
                                                     'ACTION, ' ||
                                                     -- fields from setup table
                                                     v_env_fields || ') ' ||
               'select /*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) */ ' ||
                       -- pk
                       l_primary_key_field_value                                     ||
                       -- uk
                       l_unique_key_fields_values                                    ||
                       --
                       chr(39) || v_action || chr(39) || ','                         ||
                       v_env_fields_tbl || ' '                                       ||
               'from ' || v_view || ' tbl_from '                                     ||
               v_sql_where || ' '                                                    ||
                 'and nvl(tbl_from.gpms_id, tbl_from.plw_pjt_id) not in'             ||

                                       '( select nvl(b.gpms_id, b.plw_pjt_id) ' ||
                                         'from   export_grd_backup b '          ||
                                         'where  b.status = ' || chr(39) || 'E' || chr(39) || ')'
               ;

      execute immediate v_sql;
      l_commited_records_i := sql%rowcount;
      commit;

      /*insert into oleg(action,
                       sql_stmnt)
      values ('I',
              v_sql)
      ;
      commit;*/

      --dbms_output.put_line('Inserted I: ' || l_commited_records_i);

      --============== INSERT RECORDS FROM TARGET ENVIRONMENT TO_ENV_ID (PSNEXT) ABSENT
      -- IN SOURCE ENVIRONMENT FROM_ENV_ID (GPMS ) *** E *** =============  ??? to remove or replace
      --dbms_output.put_line('E');

      if(p_level = 'PROJECT') then

         l_unique_key_fields_values  := 'tbl_to.prj_id, '      ||
                                        'tbl_to.gpms_exp_id, ' ||
                                        '32, ';


      elsif(p_level = 'TASK') then

         l_unique_key_fields_values  := 'tbl_to.prj_id, '      ||
                                        --'tbl_to.country, '     || -- is fetched in v_env_fields_tbl
                                        -- not part of UK
                                        'tbl_to.gpms_exp_id, ' ||
                                        '32, '                   ||
                                        'tbl_to.project_id, '
                                        ;

      end if;

      v_action := 'E';

      v_env_fields_tbl := get_env_fields(p_to_env_id,
                                         p_level,
                                         'TBL_TO');

      v_sql_where := 'where tbl_to.gpms_exp_id = ' || p_gpms_exp_id ||
                      ' and ' || 'tbl_to.env_id = ''' || p_to_env_id ||
                    ''' and not exists (select gpms_id ' ||
                                       'from ' || v_view || ' tbl_from ' ||
                                       'where '|| v_sql_pair_cr0 ||
                                        ' and tbl_from.gpms_exp_id = '   || p_gpms_exp_id ||
                                        ' and tbl_from.env_id      = ''' || p_from_env_id || ''')';

      if(p_level = 'PROJECT') then

         v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                        -- pk
                                                        l_primary_key_field ||
                                                        -- uk
                                                        l_unique_key_fields ||
                                                        --
                                                        'ACTION, ' ||
                                                        -- fields from setup table
                                                        v_env_fields || ') ' ||
                  'select /*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) */ ' ||
                          -- pk
                          l_primary_key_field_value  ||
                          -- uk
                          l_unique_key_fields_values ||
                          --
                          chr(39) || v_action || chr(39) || ',' ||
                          v_env_fields_tbl || ' ' ||
                  'from ' || v_view || ' tbl_to ' ||
                  v_sql_where                     ||
                  --
                  ' and  tbl_to.development_site is not null ' ||
                   'and  tbl_to.gain    is not null '          /*||
                   'and  tbl_to.region  is not null '          ||
                   'and  tbl_to.country is not null'*/
                   ;

      elsif(p_level ='TASK') then

         v_sql := 'insert into  ' || v_tbl ||' tbl_diff(' ||
                                                        -- pk
                                                        l_primary_key_field ||
                                                        -- uk
                                                        l_unique_key_fields ||
                                                        --
                                                        'ACTION, ' ||
                                                        -- fields from setup table
                                                        v_env_fields || ') ' ||
                  'select /*+ no_merge(tbl_from) NO_MERGE(TBL_TO) use_hash(tbl_to) */ ' ||
                          -- pk
                          l_primary_key_field_value  ||
                          -- uk
                          l_unique_key_fields_values ||
                          --
                          chr(39) || v_action || chr(39) || ',' ||
                          v_env_fields_tbl || ' ' ||
                  'from ' || v_view || ' tbl_to ' ||
                  v_sql_where                     ||
                  --
                  ' and  tbl_to.development_site is not null ' ||
                   'and  tbl_to.region  is not null '          ||
                   'and  tbl_to.country is not null'
                  ;

      end if;

      begin

         execute immediate v_sql;
         l_commited_records_e := sql%rowcount;
         commit;

         --dbms_output.put_line('Inserted E: ' || l_commited_records_e);

      exception
         when others then
            -- TEVAPSNEXTPHASE2.HANDLELOGTABLE(..)
            handle_log_table(p_bpel_process_name => 'TevaRunPsDelta',
                             p_process_step      => 'DIFF_LEVEL E',
                             p_gpms_id           => null,
                             p_gpms_exp_id       => p_gpms_exp_id,
                             p_site              => null,
                             p_status            => 'E',
                             p_env               => null,
                             p_error             => substr(sqlerrm, 1, 500),
                             p_status_out        => v_return_status);
            commit;
      end;

      --============== INSERT RECORDS TO PS_DIFF_ERR (Deleted in PLW) =============
      --dbms_output.put_line('ERR');

      v_err_type_id := 3;

      if(p_level = 'PROJECT') then

         v_sql := 'insert into ps_diff_err(gpms_exp_id, ' ||
                                          'env_id, '      ||
                                          'err_type_id, ' ||
                                          'err_date, '    ||
                                          'level_name, '  ||
                                          'gpms_id) '     ||
                  'select ' || p_gpms_exp_id || ', '''    ||
                               p_to_env_id   || ''', '    ||
                               v_err_type_id || ', '      ||
                               'sysdate, '''              ||
                               p_level || ''', '          ||
                               'tbl_to.gpms_id '          ||
                  'from ' || v_view || ' tbl_to '         ||
                  v_sql_where                             ||
                  --
                   ' and tbl_to.development_site is not null ' ||
                    'and tbl_to.gain    is not null '     ||
                    'and tbl_to.region  is not null '     /*||
                    'and tbl_to.country is not null'*/
                  ;

      elsif(p_level ='TASK') then

         v_sql := 'insert into ps_diff_err(gpms_exp_id, ' ||
                                          'env_id, '      ||
                                          'err_type_id, ' ||
                                          'err_date, '    ||
                                          'level_name, '  ||
                                          'gpms_id, '     ||
                                          'country_id) '  ||
                  'select ' || p_gpms_exp_id || ', '''    ||
                               p_to_env_id || ''', '      ||
                               v_err_type_id || ', '      ||
                               'sysdate, '''              ||
                               p_level || ''', '          ||
                               'tbl_to.gpms_id, '         ||
                               --'tbl_to.country_id '       ||
                               'tbl_to.country '          ||
                  'from ' || v_view || ' tbl_to '         ||
                  v_sql_where                             ||
                  --
                   ' and tbl_to.development_site is not null ' ||
                    'and tbl_to.region  is not null '     ||
                    'and tbl_to.country is not null'
                  ;

      end if;

      execute immediate v_sql;
      l_commited_records_err := sql%rowcount;
      commit;

      --dbms_output.put_line('Inserted Err into PS_DIFF_ERR: ' || l_commited_records_err);

      l_total_commited_records := l_commited_records_t +
                                  l_commited_records_n +
                                  l_commited_records_u +
                                  l_commited_records_i +
                                  l_commited_records_e /*+
                                  l_commited_records_err*/
                                  ;

      --dbms_output.put_line('Total inserted: ' || l_total_commited_records);

   exception

      when others then
         begin

            rollback;
            g_errbuf       := sqlerrm;
            g_sql_code     := sqlcode;
            --raise e_diff_level;

            v_ora_err_text := substr(sqlerrm, 1, 500);
            v_err_type_id  := 10;

            dbms_output.put_line(g_errbuf);

            write_diff_err(p_gpms_exp_id,
                           p_to_env_id,
                           v_err_type_id,
                           p_level,
                           null,
                           null,
                           null,
                           g_sql_code,
                           v_ora_err_text,
                           substr(v_sql, 1, 3999));

            handle_log_table(p_bpel_process_name => 'TevaRunPsDelta',
                             p_process_step      => 'DIFF_LEVEL',
                             p_gpms_id           => null,
                             p_gpms_exp_id       => p_gpms_exp_id,
                             p_site              => null,
                             p_status            => 'E',
                             p_env               => null,
                             p_error             => substr(sqlerrm, 1, 500),
                             p_status_out        => v_return_status);
            commit;

            raise e_diff_level;

         end;

   end diff_level;

   ------------------------------------------------
   -- delete_diff_data
   ------------------------------------------------
   procedure delete_diff_data(p_gpms_exp_id number) is

      l_deleted_from_ps_task_imp_vw number;
      l_deleted_ps_project_imp_vw   number;
      l_deleted_from_ps_diff_err    number;

   begin

      begin

         delete from ps_task_imp_vw where gpms_exp_id = p_gpms_exp_id;
         l_deleted_from_ps_task_imp_vw := sql%rowcount;
         commit;
         --dbms_output.put_line('Deleted from ps_task_imp_vw: ' || l_deleted_from_ps_task_imp_vw);

      exception

         when others then
            rollback;
            g_errbuf   := '[delete ps_task_imp_vw - ]' || sqlerrm;
            g_sql_code := sqlcode;
            raise e_delete_diff_data;

      end;

      begin

         delete from ps_project_imp_vw where gpms_exp_id = p_gpms_exp_id;
         l_deleted_ps_project_imp_vw := sql%rowcount;
         commit;
         --dbms_output.put_line('Deleted from ps_project_imp_vw: ' || l_deleted_ps_project_imp_vw);

      exception

         when others then
            rollback;
            g_errbuf   := '[delete ps_project_imp_vw - ]' || sqlerrm;
            g_sql_code := sqlcode;
            raise e_delete_diff_data;

      end;

      begin

         delete from ps_diff_err where gpms_exp_id = p_gpms_exp_id;
         l_deleted_from_ps_diff_err := sql%rowcount;
         commit;
         --dbms_output.put_line('Deleted from ps_diff_err: '       || l_deleted_from_ps_diff_err);

      exception

         when others then
            rollback;
            g_errbuf := '[delete ps_diff_err] - ' || sqlerrm;
            g_sql_code := sqlcode;
            raise e_delete_diff_data;

      end;

   end delete_diff_data;

   ------------------------------------------------
   -- ps_manipulation - only GRD part was copied
   ------------------------------------------------
   procedure ps_manipulation is

      l_commited_project_records number;
      l_commited_task_records    number;

   begin

      begin

         update ps_project a
         set    a.status = 'Historic'
         where  a.status = 'Divested'
           and  a.env_activity_id = '10';

         l_commited_project_records := sql%rowcount;
         commit;

         update ps_project pp
         set    pp.development_site = 'BD'/*,
                pp.vap_project      = upper(pp.vap_project)*/
         where  pp.env_activity_id  = '10'
           and  pp.development_site = 'BD-E';


         l_commited_project_records := l_commited_project_records + sql%rowcount;
         commit;

         --dbms_output.put_line('Updated in ps_project: ' || l_commited_project_records);


         --NA = Null. 23/03/09
         /*update ps_project pp
         set    pp.transfer_type   = null
         where  pp.env_activity_id = '10'
           and  pp.transfer_type   = 'NA';

         commit; */

      exception

         when others then
            rollback;
            g_errbuf   := '[update ps_project] - ' || sqlerrm;
            g_sql_code := sqlcode;
            raise e_ps_manipulation;

      end;

      begin

         update ps_task p
         set    p.plmd_asap            = null,
                p.development_site     = null,
                p.mars_date            = null,
                p.region               = null
                --p.innovator_brand_name = null,  --|
                --p.lmd                  = null,  --|
                --p.plmd_date            = null,  --|
                --p.plmd_earlylate       = null,  --|
                --p.status               = null,  --|\__  fields do not exist !!!
                --p.task_priority        = null,  --|/
                --p.strengths            = null,  --|
                --p.country_comments     = null,  --|
                --p.spc                  = null   --|
         where  --p.country_id           = 'GRD'
                p.country              = 'GRD'
         ;

         l_commited_task_records := sql%rowcount;
         commit;

         --dbms_output.put_line('Updated in ps_task: '    || l_commited_task_records);

      exception

         when others then
            rollback;
            g_errbuf   := '[update ps_task] - ' || sqlerrm;
            g_sql_code := sqlcode;
            raise e_ps_manipulation;

      end;

   end ps_manipulation;

   ------------------------------------------------
   -- create_backup_projects_tasks
   ------------------------------------------------
   procedure create_backup_projects_tasks(p_gpms_exp_id number) is

      l_plw_date_format varchar2(10) := ('dd/mm/rrrr');
      l_db_date_format  varchar2(10) := ('mm/dd/rrrr');

      l_project_id      number;
      l_prj_id          varchar2(100);

      -- 1. cursor for REGION PROJECTS(EU, US, CA, IL, JP)
      cursor c_region_proj is

         -- migrated projects
         select t.*,
                t.gpms_id gpmsid
         from   export_grd_backup t
         where  1 = 1
           and  t.gain                 is not null
           and  t.development_site     is not null
           and  t.region               is not null
           and  t.country              is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(t.region)      = 'EU'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'       and
                   (t.development_site not in('BD', 'BD-E') or
                    t.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(t.region)      = 'CA'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- US
                 (upper(t.region)      = 'US'                  and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(t.region)     = 'EMIA'                and
                   t.country           = 'IL'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(t.region)     = 'ASIA'                and
                   t.country           = 'JP'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site not like '%BioG%'))
                )
           --
           /*and  (upper(t.allocation_status) = 'APPROVED'  or
                 (t.project_status          = 'Draft' and
                  nvl(t.global_nte, '0')    = '1'))*/
           --
           and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = t.gpms_id
                              and  s.country = t.country)
           --
           and  t.gpms_id  is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = t.gpms_id
                          and  gl.plw_pjt_id     = t.plw_pjt_id
                          and  gl.plw_int_number = t.plw_int_number)
         -- Projects created in PLW and
         -- WPs created in PLW for migrated projects
         union all
         select t.*,
                null gpmsid
         from   export_grd_backup t
         where  1 = 1
           and  t.gain                 is not null
           and  t.development_site     is not null
           and  t.region               is not null
           and  t.country              is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(t.region)      = 'EU'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'       and
                   (t.development_site not in('BD', 'BD-E') or
                    t.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(t.region)      = 'CA'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- US
                 (upper(t.region)      = 'US'                  and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(t.region)     = 'EMIA'                and
                   t.country           = 'IL'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(t.region)     = 'ASIA'                and
                   t.country           = 'JP'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site not like '%BioG%'))
                )
           --
           /*and  (upper(t.allocation_status) = 'APPROVED'  or
                 (t.project_status          = 'Draft' and
                  nvl(t.global_nte, '0')    = '1'))*/
           --
           and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = t.gpms_id
                              and  s.country = t.country)
           --
           and  ((-- projects created in plw
                  t.gpms_id  is null) or
                 (-- WPs created in plw for migrated projects
                  t.gpms_id  is not null and
                  not exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
                 )
                )
         ;

      -- 2.a. cursor for PROJECTS level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_prj is

         select q.gpms_id,
                q.plw_pjt_id,
                q.plw_int_number,
                nvl(q.gpms_id, q.plw_pjt_id)     plw_prj,
                nvl(q.gpms_id, q.plw_int_number) plw_wp,
                q.gain,
                q.global_dosage_form,
                --q.strength_fill_volume,
                --q.fill_volume_uom,
                --q.strength_uom,
                --q.project_rationale,
                --q.npv,
                q.region,
                q.development_site
         from   (
                  -- Projects that were migrated into PLW
                  select distinct
                         t.gpms_id,
                         null plw_pjt_id,
                         null plw_int_number,
                         t.gain,
                         t.global_dosage_form,
                         -- prj / tsl level ???
                         --t.strength_fill_volume,
                         --t.fill_volume_uom,
                         --t.strength_uom,
                         --
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd_backup t
                  where  t.gpms_id                   is not null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP', 'IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))       = 6 -- filter out SEE projects
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   PSNEXT_UNIQUE_PROJECTS_BACKUP s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                    --
                    --and  nvl(t.status, '-999') <> 'E'
                  -- WPs created in PLW for migrated into PLW projects
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
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd_backup t
                  where  t.gpms_id                   is not null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP', 'IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))       = 6 -- filter out SEE projects
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- to filter out WPs created in PLW for Migrated Projects
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                    --
                    /*and  exists (select 1
                                 from   export_grd_backup)*/
                  -- Projects that were created in PLW
                  union
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
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd_backup t
                  where  t.gpms_id                   is null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP', 'IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                    --
                    --and  nvl(t.status, '-999') <> 'E'
                ) q
         ;

      -- 2.b. cursor for COUNTY level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_cntr(l_gpms_id              varchar2,
                                 l_plw_pjt_id           varchar2,
                                 l_plw_int_number       varchar2,
                                 l_gain                 varchar2,
                                 l_global_dosage_form   varchar2,
                                 --l_strength_fill_volume varchar2,
                                 --l_strength_uom         varchar2,
                                 --l_fill_volume_uom      varchar2,
                                 --l_project_rationale    varchar2,
                                 --l_npv                  varchar2,
                                 l_development_site     varchar2) is

         select q.*
         from   (
                  -- Projects that were migrated into PLW
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd_backup t
                  where  l_gpms_id                         is not null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    and  t.gpms_id                         = l_gpms_id
                    and  t.gain                            = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects_backup s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  -- WPs created in PLW for migrated into PLW projects
                  union
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd_backup t
                  where  l_gpms_id                         is null
                    and  t.gpms_id                         is not null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    --and  t.gpms_id                         = l_gpms_id
                    and  t.plw_pjt_id                      = l_plw_pjt_id
                    and  t.plw_int_number                  = l_plw_int_number
                    and  t.gain                            = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                  = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')
                    --
                    and  nvl(l_gpms_id, '-999')            <> nvl(l_plw_int_number, '-999')
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- filter out migrated tasks
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  -- Tasks that were created in PLW for non migrated projects
                  union
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd_backup t
                  where  l_gpms_id                         is null
                    and  t.gpms_id                         is null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    and  t.plw_pjt_id                      = l_plw_pjt_id
                    and  t.plw_int_number                  = l_plw_int_number
                    and  t.gain                            = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')
                ) q
         order by q.plw_int_number
         ;

   begin

      -- 1. REGION PROJECTS(EU, US, CA, IL, JP)
      for r_region_proj in c_region_proj loop

         --l_prj_id := nvl(r_region_proj.gpms_id, r_region_proj.plw_pjt_id);

         begin

            insert into ps_project(project_id,
                                   prj_id,
                                   gpms_id,
                                   --
                                   gpms_exp_id,     -- to be removed
                                   env_activity_id, -- to be removed
                                   --
                                   plw_pjt_id,
                                   plw_int_number,
                                   gain,
                                   global_dosage_form,
                                   fill_volume_uom,
                                   strength_uom,
                                   strength_fill_volume,
                                   development_site,
                                   country,
                                   target_market,
                                   activity_type,
                                   launching_site,
                                   project_name,
                                   project_rationale,
                                   rationale_comments,
                                   project_technology,
                                   product_rationale,
                                   project_status,
                                   wp_status,
                                   project_progress,
                                   business,
                                   ims_sales,
                                   ims_sales_percentage,
                                   ims_units_,
                                   ims_units_percentage,
                                   first_allocation_approval_date, -- allocation_approval_date
                                   region,
                                   approval_date,
                                   project_priority,
                                   allocation_status,
                                   ftf,
                                   ftf_reason,
                                   handling_category,
                                   il_plmd,
                                   init_proj_strat_appro,
                                   innovator_company,
                                   innovator_shelf_life,
                                   ip_manager,
                                   lcm,
                                   launch_date,
                                   mars_date,
                                   nce_date,
                                   plfd_remark,
                                   filing_strategy,
                                   product_type,
                                   sales_channel,
                                   generic_segment,
                                   submission_to_authorties_fnish,
                                   submission_to_authrt_actl_strt,
                                   therapeutic_class,
                                   source_project_id,
                                   type_of_project,
                                   pipeline_manager,
                                   go_no_go_pivotal,
                                   go_no_go_biostudy,
                                   go_no_go_submission,
                                   go_no_go_launch,
                                   preliminary_allocation,
                                   reason_for_allocation, -- wp only ???
                                   dev_type,
                                   npv,
                                   tld,
                                   afw,
                                   global_nte,
                                   tapi_afw,
                                   submission_a_p,
                                   approval_a_p,
                                   launch_a_p,
                                   value_for_teva,
                                   -- added 08/09/2013
                                   plfd,
                                   plfd_asap,
                                   plfd_late,
                                   plfd_late_asap,
                                   --
                                   plmd,
                                   plmd_asap,
                                   plmd_main,
                                   plmd_main_asap,
                                   --
                                   lmd_main,
                                   lmd_main_asap,
                                   lmd_early,
                                   lmd_early_asap,
                                   product_status,
                                   -- added 04/08/2014
                                   INNOVATOR_BRAND_NAME,
                                   STABILITY_RISK,
                                   FORMULATION_COMPLEXITY,
                                   DEV_WITHOUT_RLD,
                                   API_COMPLEXITY,
                                   COMPLEX_TECHNOLOGY,
                                   STERILE_Y_N,
                                   AGENT_COMPANY,
                                   MFG_SITE,
                                   PRODUCT_TECHNOLOGY,
                                   PRODUCT_DOSAGE_FORM,
                                   OVERALL_BIO_RISK,
                                   FINAL_CATEGORIZATION,
                                   DO_YOU_AGREE_WITH_RESULT,
                                   CALCULATED_CATEGORIZATION,
                                   GLOBAL_DEVELOPMENT_STATUS,
                                    -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                                   WP_DUP_DESC,
                                   --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                                   STRATEGY_MEET_D,
                                   STRATEGY_MEET,
                                   STRATEGY_STAT,
                                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGET...PIVOTAL_STUDY )
                                   API_BUDGET,
                                   BIO_STUDY,
                                   ENPV,
                                   T_RND_BUDGET,
                                   RND_MFG_PACK_BUDGET,
                                   O_METERIALS_BUDGET,
                                   OUT_CONTRACTORS,
                                   PILOT_STUDY,
                                   CLINICAL,
                                   PIVOTAL_STUDY,
                                   -- fields do not exist in FDS, but have comments --
                                   action,
                                   time_sensitive,
                                   late,
                                   market_activated,
                                   is_generic
                                  )
            values(ps_project_seq.nextval,
                   nvl(r_region_proj.gpmsid /*gpms_id*/, r_region_proj.plw_int_number), --nvl(r_region_proj.gpms_id, r_region_proj.plw_pjt_id),
                   r_region_proj.gpmsid, --gpms_id,
                   --
                   p_gpms_exp_id,                    -- to be removed
                   22, -- general_pack.get_env_activity_id, -- to be removed
                   --
                   r_region_proj.plw_pjt_id,
                   r_region_proj.plw_int_number,
                   r_region_proj.gain,
                   r_region_proj.global_dosage_form,
                   r_region_proj.fill_volume_uom,
                   r_region_proj.strength_uom,
                   r_region_proj.strength_fill_volume,
                   r_region_proj.development_site,
                   r_region_proj.country,
                   r_region_proj.country,  -- target_market
                   r_region_proj.activity_type,
                   r_region_proj.launching_site,
                   r_region_proj.project_name,
                   r_region_proj.project_rationale,
                   r_region_proj.rationale_comments,
                   r_region_proj.project_technology,
                   r_region_proj.product_rationale,
                   r_region_proj.project_status,
                   r_region_proj.wp_status,
                   r_region_proj.project_progress,
                   r_region_proj.business,
                   r_region_proj.ims_sales,
                   r_region_proj.ims_sales_percentage,
                   r_region_proj.ims_units_,
                   r_region_proj.ims_units_percentage,
                   r_region_proj.allocation_committee_date, --first_allocation_approval_date,
                   decode(r_region_proj.country, 'IL', 'IL', 'JP', 'JP', r_region_proj.region), -- region
                   r_region_proj.approval_date,
                   r_region_proj.project_priority,
                   r_region_proj.allocation_status,
                   r_region_proj.ftf,
                   r_region_proj.ftf_reason,
                   r_region_proj.handling_category,
                   r_region_proj.il_plmd,
                   r_region_proj.init_proj_strat_appro,
                   r_region_proj.innovator_company,
                   r_region_proj.innovator_shelf_life,
                   r_region_proj.ip_manager,
                   r_region_proj.lcm,
                   r_region_proj.launch_date,
                   r_region_proj.mars_date,
                   r_region_proj.nce_date,
                   r_region_proj.plfd_remark,
                   r_region_proj.filing_strategy,
                   r_region_proj.product_type,
                   r_region_proj.sales_channel,
                   r_region_proj.generic_segment,
                   to_date(to_char(to_date(r_region_proj.submission_to_authorities,
                                           l_plw_date_format),
                                   l_db_date_format),
                           l_db_date_format),
                   to_date(to_char(to_date(r_region_proj.subm_to_authorities_act_start,
                                           l_plw_date_format),
                                   l_db_date_format),
                           l_db_date_format),
                   r_region_proj.therapeutic_class,
                   r_region_proj.source_project_id,
                   r_region_proj.type_of_project,
                   r_region_proj.pipeline_manager,
                   r_region_proj.go_no_go_pivotal,
                   r_region_proj.go_no_go_biostudy,
                   r_region_proj.go_no_go_submission,
                   r_region_proj.go_no_go_launch,
                   r_region_proj.preliminary_allocation,
                   r_region_proj.reason_for_allocation, -- wp only ???
                   r_region_proj.dev_type,
                   r_region_proj.npv,
                   r_region_proj.tld,
                   decode(upper(r_region_proj.region), 'US', r_region_proj.afw, null),
                   r_region_proj.global_nte,
                   r_region_proj.tapi_afw,
                   r_region_proj.submission_a_p,
                   r_region_proj.approval_a_p,
                   r_region_proj.launch_a_p,
                   r_region_proj.value_for_teva,
                   -- added 08/09/2013
                   r_region_proj.plfd,
                   r_region_proj.plfd_asap,
                   r_region_proj.plfd_late,
                   r_region_proj.plfd_late_asap,
                   --
                   r_region_proj.plmd,
                   r_region_proj.plmd_asap,
                   r_region_proj.plmd_main,
                   r_region_proj.plmd_main_asap,
                   --
                   r_region_proj.lmd_main,
                   r_region_proj.lmd_main_asap,
                   r_region_proj.lmd_early,
                   r_region_proj.lmd_early_asap,
                   r_region_proj.product_status,
                   -- added 04/08/2014
                   r_region_proj.INNOVATOR_BRAND_NAME,
                   r_region_proj.STABILITY_RISK,
                   r_region_proj.FORMULATION_COMPLEXITY,
                   r_region_proj.DEV_WITHOUT_RLD,
                   r_region_proj.API_COMPLEXITY,
                   r_region_proj.COMPLEX_TECHNOLOGY,
                   r_region_proj.STERILE_Y_N,
                   r_region_proj.AGENT_COMPANY,
                   r_region_proj.MFG_SITE,
                   r_region_proj.PRODUCT_TECHNOLOGY,
                   r_region_proj.PRODUCT_DOSAGE_FORM,
                   r_region_proj.OVERALL_BIO_RISK,
                   r_region_proj.FINAL_CATEGORIZATION,
                   r_region_proj.DO_YOU_AGREE_WITH_RESULT,
                   r_region_proj.CALCULATED_CATEGORIZATION,
                   r_region_proj.GLOBAL_DEVELOPMENT_STATUS,
                   -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                   r_region_proj.WP_DUP_DESC,
                   --
                   --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                   r_region_proj.STRATEGY_MEET_D,
                   r_region_proj.STRATEGY_MEET,
                   r_region_proj.STRATEGY_STAT,
                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGET...PIVOTAL_STUDY )
                   r_region_proj.API_BUDGET,
                   r_region_proj.BIO_STUDY,
                   r_region_proj.ENPV,
                   r_region_proj.T_RND_BUDGET,
                   r_region_proj.RND_MFG_PACK_BUDGET,
                   r_region_proj.O_METERIALS_BUDGET,
                   r_region_proj.OUT_CONTRACTORS,
                   r_region_proj.PILOT_STUDY,
                   r_region_proj.CLINICAL,
                   r_region_proj.PIVOTAL_STUDY,
                   -----
                   null,
                   null,
                   null,
                   null,
                   null
                  );

         exception
            when others then
               null;
               /*dbms_output.put_line('*** ERROR in create_grd_projects_tasks -'              ||
                                    ' GPMS_ID: '         || r_region_proj.gpmsid \*gpms_id*\||
                                   ', PLW_PJT_ID: '      || r_region_proj.plw_pjt_id        ||
                                   ', PLW_INT_NUMBER: '  || r_region_proj.plw_int_number    ||
                                   ', Region: '          || r_region_proj.region   || ' - ' || sqlerrm);*/

         end;

      end loop;

      -- 2.a. COUNTRY PROJECTS(EMIA, Asia, LATAM)
      for r_country_proj_prj in c_country_proj_prj loop

         select ps_project_seq.nextval
         into   l_project_id
         from   dual;

         l_prj_id := r_country_proj_prj.plw_prj;


         begin

            insert into ps_project(project_id,
                                   prj_id,
                                   gpms_id,
                                   -- to be removed
                                   gpms_exp_id,
                                   env_activity_id,
                                   --
                                   plw_pjt_id,
                                   plw_int_number,
                                   gain,
                                   global_dosage_form,
                                   -- ??? project level or wp level ???
                                   --strength_fill_volume,
                                   --strength_uom,
                                   --fill_volume_uom,
                                   --
                                   --project_rationale,
                                   --dev_type,
                                   --npv,
                                   target_market,
                                   development_site
                                  )
            values(l_project_id,
                   r_country_proj_prj.plw_wp, -- r_country_proj_prj.plw_prj,
                   r_country_proj_prj.gpms_id,
                   --
                   p_gpms_exp_id,                    -- to be removed
                   22, --general_pack.get_env_activity_id, -- to be removed
                   --
                   r_country_proj_prj.plw_prj,
                   r_country_proj_prj.plw_wp,
                   r_country_proj_prj.gain,
                   r_country_proj_prj.global_dosage_form,
                   --
                   --r_country_proj_prj.strength_fill_volume,
                   --r_country_proj_prj.strength_uom,
                   --r_country_proj_prj.fill_volume_uom,
                   --
                   --r_country_proj_prj.project_rationale,
                   --r_country_proj_prj.dev_type,
                   --r_country_proj_prj.npv,
                   r_country_proj_prj.region,
                   r_country_proj_prj.development_site
                  );

            -- 2.b. cursor for COUNTY level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
            for r_country_proj_cntr in c_country_proj_cntr(r_country_proj_prj.gpms_id,
                                                           r_country_proj_prj.plw_pjt_id,
                                                           r_country_proj_prj.plw_int_number,
                                                           r_country_proj_prj.gain,
                                                           r_country_proj_prj.global_dosage_form,
                                                           --r_country_proj_prj.strength_fill_volume,
                                                           --r_country_proj_prj.strength_uom,
                                                           --r_country_proj_prj.fill_volume_uom,
                                                           --r_country_proj_prj.project_rationale,
                                                           --r_country_proj_prj.npv,
                                                           r_country_proj_prj.development_site) loop

               insert into ps_task(project_id,
                                   prj_id,
                                   task_id,
                                   country,         -- country_id,
                                   --
                                   gpms_exp_id,     -- to be removed
                                   env_activity_id, -- to be removed
                                   --
                                   gpms_id,
                                   plw_pjt_id,
                                   plw_int_number,
                                   -- ??? project or task level ???
                                   strength_fill_volume,
                                   strength_uom,
                                   fill_volume_uom,
                                   ---------------------
                                   development_site,
                                   activity_type,
                                   launching_site,
                                   project_name,
                                   project_rationale,
                                   rationale_comments,
                                   project_technology,
                                   product_rationale,
                                   project_status,
                                   wp_status,
                                   project_progress,
                                   business,
                                   ims_sales,
                                   ims_sales_percentage,
                                   ims_units_,
                                   ims_units_percentage,
                                   first_allocation_approval_date, -- allocation_approval_date
                                   region,
                                   approval_date,
                                   project_priority,
                                   allocation_status,
                                   ftf,
                                   ftf_reason,
                                   handling_category,
                                   il_plmd,
                                   init_proj_strat_appro,
                                   innovator_company,
                                   innovator_shelf_life,
                                   ip_manager,
                                   lcm,
                                   launch_date,
                                   mars_date,
                                   nce_date,
                                   plfd_remark,
                                   filing_strategy,
                                   product_type,
                                   sales_channel,
                                   generic_segment,
                                   submission_to_authorties_fnish,
                                   submission_to_authrt_actl_strt,
                                   therapeutic_class,
                                   source_project_id,
                                   type_of_project,
                                   pipeline_manager,
                                   go_no_go_pivotal,
                                   go_no_go_biostudy,
                                   go_no_go_submission,
                                   go_no_go_launch,
                                   preliminary_allocation,
                                   reason_for_allocation,
                                   dev_type,
                                   npv,
                                   global_nte,
                                   submission_a_p,
                                   approval_a_p,
                                   launch_a_p,
                                   value_for_teva,
                                   -- added 08/09/2013
                                   plfd,
                                   plfd_asap,
                                   plfd_late,
                                   plfd_late_asap,
                                   --
                                   plmd,
                                   plmd_asap,
                                   plmd_main,
                                   plmd_main_asap,
                                   --
                                   lmd_main,
                                   lmd_main_asap,
                                   lmd_early,
                                   lmd_early_asap,
                                   product_status,
                                   -- added 04/08/2014
                                   INNOVATOR_BRAND_NAME,
                                   STABILITY_RISK,
                                   FORMULATION_COMPLEXITY,
                                   DEV_WITHOUT_RLD,
                                   API_COMPLEXITY,
                                   COMPLEX_TECHNOLOGY,
                                   STERILE_Y_N,
                                   AGENT_COMPANY,
                                   MFG_SITE,
                                   PRODUCT_TECHNOLOGY,
                                   PRODUCT_DOSAGE_FORM,
                                   OVERALL_BIO_RISK,
                                   FINAL_CATEGORIZATION,
                                   DO_YOU_AGREE_WITH_RESULT,
                                   CALCULATED_CATEGORIZATION,
                                   GLOBAL_DEVELOPMENT_STATUS,
                                   -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                                   WP_DUP_DESC,
                                   --
                                   --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                                   STRATEGY_MEET_D,
                                   STRATEGY_MEET,
                                   STRATEGY_STAT,
                                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGE..PIVOTAL_STUDY )
                                   API_BUDGET,
                                   BIO_STUDY,
                                   ENPV,
                                   T_RND_BUDGET,
                                   RND_MFG_PACK_BUDGET,
                                   O_METERIALS_BUDGET,
                                  OUT_CONTRACTORS,
                                   PILOT_STUDY,
                                   CLINICAL,
                                   PIVOTAL_STUDY,
                                   ------
                                   action
                                  )
               values(l_project_id,
                      r_country_proj_prj.plw_wp, --nvl(r_country_proj_cntr.gpms_id, r_country_proj_cntr.plw_int_number), --nvl(r_country_proj_cntr.gpms_id, r_country_proj_cntr.plw_pjt_id),
                      ps_task_sq.nextval, -- to be removed
                      r_country_proj_cntr.country,
                      --
                      p_gpms_exp_id,        -- to be removed
                      22,                   -- general_pack.get_env_activity_id, -- to be removed
                      --
                      r_country_proj_cntr.gpmsid, --gpms_id,
                      r_country_proj_cntr.plw_pjt_id, --decode(r_country_proj_cntr.gpms_id, null, r_country_proj_cntr.plw_pjt_id,     null),
                      r_country_proj_cntr.plw_int_number, --decode(r_country_proj_cntr.gpms_id, null, r_country_proj_cntr.plw_int_number, null),
                      -- ??? project or task level ???
                      r_country_proj_cntr.strength_fill_volume,
                      r_country_proj_cntr.strength_uom,
                      r_country_proj_cntr.fill_volume_uom,
                      ----------------------------------------
                      r_country_proj_cntr.development_site,
                      r_country_proj_cntr.activity_type,
                      r_country_proj_cntr.launching_site,
                      r_country_proj_cntr.project_name,
                      r_country_proj_cntr.project_rationale,
                      r_country_proj_cntr.rationale_comments,
                      r_country_proj_cntr.project_technology,
                      r_country_proj_cntr.product_rationale,
                      r_country_proj_cntr.project_status,
                      r_country_proj_cntr.wp_status,
                      r_country_proj_cntr.project_progress,
                      r_country_proj_cntr.business,
                      r_country_proj_cntr.ims_sales,
                      r_country_proj_cntr.ims_sales_percentage,
                      r_country_proj_cntr.ims_units_,
                      r_country_proj_cntr.ims_units_percentage,
                      r_country_proj_cntr.allocation_committee_date, --first_allocation_approval_date,
                      --r_country_proj_cntr.region,
                      decode(upper(r_country_proj_cntr.region), 'ASIA', 'INT',
                                                                   'EMIA', 'INT',
                                                                   r_country_proj_cntr.region),
                      /*to_date(to_char(to_date(r_country_proj_cntr.approval_date,
                                              l_plw_date_format),
                                      l_db_date_format),
                              l_db_date_format),*/ -- to be changed once the source data is date
                      r_country_proj_cntr.approval_date,
                      r_country_proj_cntr.project_priority,
                      r_country_proj_cntr.allocation_status,
                      r_country_proj_cntr.ftf,
                      r_country_proj_cntr.ftf_reason,
                      r_country_proj_cntr.handling_category,
                      r_country_proj_cntr.il_plmd,
                      r_country_proj_cntr.init_proj_strat_appro,
                      r_country_proj_cntr.innovator_company,
                      r_country_proj_cntr.innovator_shelf_life,
                      r_country_proj_cntr.ip_manager,
                      r_country_proj_cntr.lcm,
                      r_country_proj_cntr.launch_date,
                      /*to_date(to_char(to_date(r_country_proj_cntr.mars_date,
                                              l_plw_date_format),
                                      l_db_date_format),
                              l_db_date_format),*/ -- to be changed once the source data is date
                      r_country_proj_cntr.mars_date,
                      /*to_date(to_char(to_date(r_country_proj_cntr.nce_date,
                                              l_plw_date_format),
                                      l_db_date_format),
                              l_db_date_format),*/ -- to be changed once the source data is date
                      r_country_proj_cntr.nce_date,
                      r_country_proj_cntr.plfd_remark,
                      r_country_proj_cntr.filing_strategy,
                      r_country_proj_cntr.product_type,
                      r_country_proj_cntr.sales_channel,
                      r_country_proj_cntr.generic_segment,
                      to_date(to_char(to_date(r_country_proj_cntr.submission_to_authorities,
                                              l_plw_date_format),
                                      l_db_date_format),
                              l_db_date_format),
                      to_date(to_char(to_date(r_country_proj_cntr.subm_to_authorities_act_start,
                                              l_plw_date_format),
                                      l_db_date_format),
                              l_db_date_format),
                      r_country_proj_cntr.therapeutic_class,
                      r_country_proj_cntr.source_project_id,
                      r_country_proj_cntr.type_of_project,
                      r_country_proj_cntr.pipeline_manager,
                      r_country_proj_cntr.go_no_go_pivotal,
                      r_country_proj_cntr.go_no_go_biostudy,
                      r_country_proj_cntr.go_no_go_submission,
                      r_country_proj_cntr.go_no_go_launch,
                      r_country_proj_cntr.preliminary_allocation,
                      r_country_proj_cntr.reason_for_allocation,
                      r_country_proj_cntr.dev_type,
                      r_country_proj_cntr.npv,
                      r_country_proj_cntr.global_nte,
                      r_country_proj_cntr.submission_a_p,
                      r_country_proj_cntr.approval_a_p,
                      r_country_proj_cntr.launch_a_p,
                      r_country_proj_cntr.value_for_teva,
                      -- added 08/09/2013
                      r_country_proj_cntr.plfd,
                      r_country_proj_cntr.plfd_asap,
                      r_country_proj_cntr.plfd_late,
                      r_country_proj_cntr.plfd_late_asap,
                      --
                      r_country_proj_cntr.plmd,
                      r_country_proj_cntr.plmd_asap,
                      r_country_proj_cntr.plmd_main,
                      r_country_proj_cntr.plmd_main_asap,
                      --
                      r_country_proj_cntr.lmd_main,
                      r_country_proj_cntr.lmd_main_asap,
                      r_country_proj_cntr.lmd_early,
                      r_country_proj_cntr.lmd_early_asap,
                      r_country_proj_cntr.product_status,
                      -- added 04/08/2014
                      r_country_proj_cntr.INNOVATOR_BRAND_NAME,
                      r_country_proj_cntr.STABILITY_RISK,
                      r_country_proj_cntr.FORMULATION_COMPLEXITY,
                      r_country_proj_cntr.DEV_WITHOUT_RLD,
                      r_country_proj_cntr.API_COMPLEXITY,
                      r_country_proj_cntr.COMPLEX_TECHNOLOGY,
                      r_country_proj_cntr.STERILE_Y_N,
                      r_country_proj_cntr.AGENT_COMPANY,
                      r_country_proj_cntr.MFG_SITE,
                      r_country_proj_cntr.PRODUCT_TECHNOLOGY,
                      r_country_proj_cntr.PRODUCT_DOSAGE_FORM,
                      r_country_proj_cntr.OVERALL_BIO_RISK,
                      r_country_proj_cntr.FINAL_CATEGORIZATION,
                      r_country_proj_cntr.DO_YOU_AGREE_WITH_RESULT,
                      r_country_proj_cntr.CALCULATED_CATEGORIZATION,
                      r_country_proj_cntr.GLOBAL_DEVELOPMENT_STATUS,
                      -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                      r_country_proj_cntr.WP_DUP_DESC,
                      --
                      --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                      r_country_proj_cntr.STRATEGY_MEET_D,
                      r_country_proj_cntr.STRATEGY_MEET,
                      r_country_proj_cntr.STRATEGY_STAT,
                      -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGEr_country_proj_cntr...PIVOTAL_STUDY )
                      r_country_proj_cntr.API_BUDGET,
                      r_country_proj_cntr.BIO_STUDY,
                      r_country_proj_cntr.ENPV,
                      r_country_proj_cntr.T_RND_BUDGET,
                      r_country_proj_cntr.RND_MFG_PACK_BUDGET,
                      r_country_proj_cntr.O_METERIALS_BUDGET,
                      r_country_proj_cntr.OUT_CONTRACTORS,
                      r_country_proj_cntr.PILOT_STUDY,
                      r_country_proj_cntr.CLINICAL,
                      r_country_proj_cntr.PIVOTAL_STUDY,
                      ----
                      null -- action
                     );

            end loop;

         exception
            when others then
               null;
               /*dbms_output.put_line('*** ERROR in create_backup_projects_tasks -' ||
                                     ' GPMS_ID: ' || r_country_proj_prj.gpms_id   ||
                                    ', PLW_PRJ: ' || r_country_proj_prj.plw_prj   ||
                                    ', PLW_WP: '  || r_country_proj_prj.plw_wp    ||
                                    ', Region: '  || r_country_proj_prj.region    || ' - ' || sqlerrm);*/

         end;

      end loop;

   exception

      when others then
         rollback;
         g_errbuf   := '[PRJ_ID: ' || l_prj_id || '] - ' || sqlerrm;
         g_sql_code := sqlcode;

         raise e_create_backup_projects_tasks;

   end create_backup_projects_tasks;

   ------------------------------------------------
   -- create_grd_projects_tasks
   ------------------------------------------------
   procedure create_grd_projects_tasks(p_gpms_exp_id     in  number,
                                       x_failed_projects out number,
                                       x_failed_tasks    out number) is

      l_plw_date_format           varchar2(10) := ('dd/mm/rrrr');
      l_db_date_format            varchar2(10) := ('mm/dd/rrrr');

      l_project_id                number;
      l_prj_id                    varchar2(100);

      l_failed_tasks              number;

      -- 1. cursor for REGION PROJECTS(EU, US, CA, IL, JP)
      cursor c_region_proj is

         -- migrated projects
         select t.*,
                t.gpms_id  gpmsid
         from   export_grd t
         where  1 = 1
           and  t.gain                 is not null
           and  t.development_site     is not null
           and  t.region               is not null
           and  t.country              is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(t.region)      = 'EU'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR', 'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'       and
                   (t.development_site not in('BD', 'BD-E') or
                    t.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(t.region)      = 'CA'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- US
                 (upper(t.region)      = 'US'                  and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(t.region)     = 'EMIA'                and
                   t.country           = 'IL'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(t.region)     = 'ASIA'                and
                   t.country           = 'JP'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site not like '%BioG%'))
                )
           --
           /*and  (upper(t.allocation_status) = 'APPROVED'  or
                 (t.project_status          = 'Draft' and
                  nvl(t.global_nte, '0')    = '1'))*/
           --
           and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects s
                            where  s.gpms_id = t.gpms_id
                              and  s.country = t.country)
           and  t.gpms_id  is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = t.gpms_id
                          and  gl.plw_pjt_id     = t.plw_pjt_id
                          and  gl.plw_int_number = t.plw_int_number)
         -- Projects created in PLW and
         -- WPs created in PLW for migrated projects
         union all
         select t.*,
                null gpmsid
         from   export_grd t
         where  1 = 1
           and  t.gain                 is not null
           and  t.development_site     is not null
           and  t.region               is not null
           and  t.country              is not null
           and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(t.region)      = 'EU'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR', 'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'       and
                   (t.development_site not in('BD', 'BD-E') or
                    t.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(t.region)      = 'CA'                  and
                  upper(t.wp_status)   <> 'DRAFT'              and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- US
                 (upper(t.region)      = 'US'                  and
                  (t.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   t.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(t.region)     = 'EMIA'                and
                   t.country           = 'IL'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(t.region)     = 'ASIA'                and
                   t.country           = 'JP'                  and
                   upper(t.wp_status)  <> 'DRAFT'              and
                   (t.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    t.development_site not like '%BioG%'))
                )
           --
           /*and  (upper(t.allocation_status) = 'APPROVED'  or
                 (t.project_status          = 'Draft' and
                  nvl(t.global_nte, '0')    = '1'))*/
           --
           and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects s
                            where  s.gpms_id = t.gpms_id
                              and  s.country = t.country)
           and  (-- projects created in plw
                 (t.gpms_id is null) or
                 (-- WPs created in plw for migrated projects
                  t.gpms_id is not null and
                  not exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
                 )
                )
         ;

      -- 2.a. cursor for PROJECTS level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_prj is

         select q.gpms_id,
                q.plw_pjt_id,
                q.plw_int_number,
                nvl(q.gpms_id, q.plw_pjt_id)     plw_prj,
                nvl(q.gpms_id, q.plw_int_number) plw_wp,
                q.gain,
                q.global_dosage_form,
                --q.strength_fill_volume,
                --q.fill_volume_uom,
                --q.strength_uom,
                --q.project_rationale,
                --q.npv,
                q.region,
                q.development_site
         from   (
                  -- Projects that were migrated into PLW
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
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd t
                  where  t.gpms_id                   is not null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP', 'IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))       = 6 -- filter out SEE projects
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)

                  -- WPs created in PLW for migrated into PLW projects
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
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd t
                  where  t.gpms_id                   is not null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP', 'IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))       = 6 -- filter out SEE projects
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- to filter out WPs created in PLW for Migrated Projects
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                    --
                    /*and  exists (select 1
                                 from   export_grd_backup)*/
                  -- Projects that were created in PLW
                  union
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
                         --t.project_rationale,
                         --t.npv,
                         decode(upper(t.region), 'ASIA', 'INT',
                                                 'EMIA', 'INT',
                                                 t.region) region,
                         t.development_site
                  from   export_grd t
                  where  t.gpms_id                   is null
                    and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                   not in ('JP','IL')
                    and  upper(t.wp_status)          <> 'DRAFT'
                    and  t.activity_type             <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site          not like '%BioG%'
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    --
                    and  t.gain                      is not null
                    and  t.development_site          is not null
                    and  t.region                    is not null
                    and  t.country                   is not null
                ) q
         ;

      -- 2.b. cursor for COUNTY (TASK) level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_cntr(l_gpms_id              varchar2,
                                 l_plw_pjt_id           varchar2,
                                 l_plw_int_number       varchar2,
                                 l_gain                 varchar2,
                                 l_global_dosage_form   varchar2,
                                 --l_strength_fill_volume varchar2,
                                 --l_strength_uom         varchar2,
                                 --l_fill_volume_uom      varchar2,
                                 --l_project_rationale    varchar2,
                                 --l_npv                  varchar2,
                                 l_development_site     varchar2) is

         select q.*
         from   (
                  -- Tasks that were migrated into PLW
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd t
                  where  l_gpms_id                         is not null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    and  t.gpms_id                         = l_gpms_id
                    and  gain                              = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  -- WPs created in PLW for migrated into PLW projects
                  union
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd t
                  where  l_gpms_id                         is null
                    and  t.gpms_id                         is not null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    --and  t.gpms_id                         = l_gpms_id
                    and  t.plw_pjt_id                      = l_plw_pjt_id
                    and  t.plw_int_number                  = l_plw_int_number
                    and  t.gain                            = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                  = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')
                    --
                    and  nvl(l_gpms_id, '-999')            <> nvl(l_plw_int_number, '-999')
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  -- Tasks that were created in PLW for non migrated projects
                  union
                  select t.*,
                         l_gpms_id gpmsid
                  from   export_grd t
                  where  l_gpms_id                         is null
                    and  t.gpms_id                         is null
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- Parameters --
                    and  t.plw_pjt_id                      = l_plw_pjt_id
                    and  t.plw_int_number                  = l_plw_int_number
                    and  t.gain                            = l_gain
                    and  nvl(t.global_dosage_form, '-999') = nvl(l_global_dosage_form, '-999')
                    --
                    --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                    --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                    --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                    --
                    --and  nvl(t.project_rationale, '-999')  = nvl(l_project_rationale, '-999')
                    --and  nvl(t.npv, '-999')                = nvl(l_npv, '-999')
                    and  nvl(t.development_site, '-999')   = nvl(l_development_site, '-999')

                )q
         order by q.plw_int_number
         ;

   begin

      x_failed_projects  := 0;
      x_failed_tasks     := 0;

      -- 1. REGION PROJECTS(EU, US, CA, IL, JP)
      for r_region_proj in c_region_proj loop

         --l_prj_id := nvl(r_region_proj.gpms_id, r_region_proj.plw_pjt_id);

         begin

            insert into ps_project(project_id,
                                   prj_id,
                                   gpms_id,
                                   --
                                   gpms_exp_id,     -- to be removed
                                   env_activity_id, -- to be removed
                                   --
                                   plw_pjt_id,
                                   plw_int_number,
                                   gain,
                                   global_dosage_form,
                                   fill_volume_uom,
                                   strength_uom,
                                   strength_fill_volume,
                                   development_site,
                                   country,
                                   target_market,
                                   activity_type,
                                   launching_site,
                                   project_name,
                                   project_rationale,
                                   rationale_comments,
                                   project_technology,
                                   product_rationale,
                                   project_status,
                                   wp_status,
                                   project_progress,
                                   business,
                                   ims_sales,
                                   ims_sales_percentage,
                                   ims_units_,
                                   ims_units_percentage,
                                   first_allocation_approval_date, -- allocation_approval_date
                                   region,
                                   approval_date,
                                   project_priority,
                                   allocation_status,
                                   ftf,
                                   ftf_reason,
                                   handling_category,
                                   il_plmd,
                                   init_proj_strat_appro,
                                   innovator_company,
                                   innovator_shelf_life,
                                   ip_manager,
                                   lcm,
                                   launch_date,
                                   mars_date,
                                   nce_date,
                                   --
                                   plfd_remark,
                                   filing_strategy,
                                   product_type,
                                   sales_channel,
                                   generic_segment,
                                   submission_to_authorties_fnish,
                                   submission_to_authrt_actl_strt,
                                   therapeutic_class,
                                   source_project_id,
                                   type_of_project,
                                   pipeline_manager,
                                   go_no_go_pivotal,
                                   go_no_go_biostudy,
                                   go_no_go_submission,
                                   go_no_go_launch,
                                   preliminary_allocation,
                                   reason_for_allocation, -- wp only ???
                                   dev_type,
                                   npv,
                                   tld,
                                   afw,
                                   global_nte,
                                   tapi_afw,
                                   submission_a_p,
                                   approval_a_p,
                                   launch_a_p,
                                   value_for_teva,
                                   -- added 08/09/2013
                                   plfd,
                                   plfd_asap,
                                   plfd_late,
                                   plfd_late_asap,
                                   --
                                   plmd,
                                   plmd_asap,
                                   plmd_main,
                                   plmd_main_asap,
                                   --
                                   lmd_main,
                                   lmd_main_asap,
                                   lmd_early,
                                   lmd_early_asap,
                                   product_status,
                                   -- added 04/08/2014
                                   INNOVATOR_BRAND_NAME,
                                   STABILITY_RISK,
                                   FORMULATION_COMPLEXITY,
                                   DEV_WITHOUT_RLD,
                                   API_COMPLEXITY,
                                   COMPLEX_TECHNOLOGY,
                                   STERILE_Y_N,
                                   AGENT_COMPANY,
                                   MFG_SITE,
                                   PRODUCT_TECHNOLOGY,
                                   PRODUCT_DOSAGE_FORM,
                                   OVERALL_BIO_RISK,
                                   FINAL_CATEGORIZATION,
                                   DO_YOU_AGREE_WITH_RESULT,
                                   CALCULATED_CATEGORIZATION,
                                   GLOBAL_DEVELOPMENT_STATUS,
                                   -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                                   WP_DUP_DESC,
                                   -- 03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                                   STRATEGY_MEET_D,
                                   STRATEGY_MEET,
                                   STRATEGY_STAT,    
                                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGE..PIVOTAL_STUDY )
                                   API_BUDGET,
                                   BIO_STUDY,
                                   ENPV,
                                   T_RND_BUDGET,
                                   RND_MFG_PACK_BUDGET,
                                   O_METERIALS_BUDGET,
                                   OUT_CONTRACTORS,
                                   PILOT_STUDY,
                                   CLINICAL,
                                   PIVOTAL_STUDY,                             
                                   -- fields do not exist in FDS, but have comments --
                                   action,
                                   time_sensitive,
                                   late,
                                   market_activated,
                                   is_generic
                                  )
            values(ps_project_seq.nextval,
                   nvl(r_region_proj.gpmsid /*gpms_id*/, r_region_proj.plw_int_number), --, nvl(r_region_proj.gpms_id, r_region_proj.plw_pjt_id),
                   r_region_proj.gpmsid, --gpms_id,
                   --
                   p_gpms_exp_id,        -- to be removed
                   10,                   -- general_pack.get_env_activity_id, -- to be removed
                   --
                   r_region_proj.plw_pjt_id,
                   r_region_proj.plw_int_number,
                   r_region_proj.gain,
                   r_region_proj.global_dosage_form,
                   r_region_proj.fill_volume_uom,
                   r_region_proj.strength_uom,
                   r_region_proj.strength_fill_volume,
                   r_region_proj.development_site,
                   r_region_proj.country,
                   r_region_proj.country, -- target_market
                   r_region_proj.activity_type,
                   r_region_proj.launching_site,
                   r_region_proj.project_name,
                   r_region_proj.project_rationale,
                   r_region_proj.rationale_comments,
                   r_region_proj.project_technology,
                   r_region_proj.product_rationale,
                   r_region_proj.project_status,
                   r_region_proj.wp_status,
                   r_region_proj.project_progress,
                   r_region_proj.business,
                   r_region_proj.ims_sales,
                   r_region_proj.ims_sales_percentage,
                   r_region_proj.ims_units_,
                   r_region_proj.ims_units_percentage,
                   r_region_proj.allocation_committee_date, --first_allocation_approval_date,
                   decode(r_region_proj.country, 'IL', 'IL', 'JP', 'JP', r_region_proj.region), -- region
                   r_region_proj.approval_date,
                   r_region_proj.project_priority,
                   r_region_proj.allocation_status,
                   r_region_proj.ftf,
                   r_region_proj.ftf_reason,
                   r_region_proj.handling_category,
                   r_region_proj.il_plmd,
                   r_region_proj.init_proj_strat_appro,
                   r_region_proj.innovator_company,
                   r_region_proj.innovator_shelf_life,
                   r_region_proj.ip_manager,
                   r_region_proj.lcm,
                   r_region_proj.launch_date,
                   r_region_proj.mars_date,
                   r_region_proj.nce_date,
                   r_region_proj.plfd_remark,
                   r_region_proj.filing_strategy,
                   r_region_proj.product_type,
                   r_region_proj.sales_channel,
                   r_region_proj.generic_segment,
                   to_date(to_char(to_date(r_region_proj.submission_to_authorities,
                                           l_plw_date_format),
                                   l_db_date_format),
                           l_db_date_format),
                   to_date(to_char(to_date(r_region_proj.subm_to_authorities_act_start,
                                           l_plw_date_format),
                                   l_db_date_format),
                           l_db_date_format),
                   r_region_proj.therapeutic_class,
                   r_region_proj.source_project_id,
                   r_region_proj.type_of_project,
                   r_region_proj.pipeline_manager,
                   r_region_proj.go_no_go_pivotal,
                   r_region_proj.go_no_go_biostudy,
                   r_region_proj.go_no_go_submission,
                   r_region_proj.go_no_go_launch,
                   r_region_proj.preliminary_allocation,
                   r_region_proj.reason_for_allocation,
                   r_region_proj.dev_type,
                   r_region_proj.npv,
                   r_region_proj.tld,
                   decode(upper(r_region_proj.region), 'US', r_region_proj.afw, null),
                   r_region_proj.global_nte,
                   r_region_proj.tapi_afw,
                   r_region_proj.submission_a_p,
                   r_region_proj.approval_a_p,
                   r_region_proj.launch_a_p,
                   r_region_proj.value_for_teva,
                   -- added 08/09/2013
                   r_region_proj.plfd,
                   r_region_proj.plfd_asap,
                   r_region_proj.plfd_late,
                   r_region_proj.plfd_late_asap,
                   --
                   r_region_proj.plmd,
                   r_region_proj.plmd_asap,
                   r_region_proj.plmd_main,
                   r_region_proj.plmd_main_asap,
                   --
                   r_region_proj.lmd_main,
                   r_region_proj.lmd_main_asap,
                   r_region_proj.lmd_early,
                   r_region_proj.lmd_early_asap,
                   r_region_proj.product_status,
                   -- added 04/08/2014
                   r_region_proj.INNOVATOR_BRAND_NAME,
                   r_region_proj.STABILITY_RISK,
                   r_region_proj.FORMULATION_COMPLEXITY,
                   r_region_proj.DEV_WITHOUT_RLD,
                   r_region_proj.API_COMPLEXITY,
                   r_region_proj.COMPLEX_TECHNOLOGY,
                   r_region_proj.STERILE_Y_N,
                   r_region_proj.AGENT_COMPANY,
                   r_region_proj.MFG_SITE,
                   r_region_proj.PRODUCT_TECHNOLOGY,
                   r_region_proj.PRODUCT_DOSAGE_FORM,
                   r_region_proj.OVERALL_BIO_RISK,
                   r_region_proj.FINAL_CATEGORIZATION,
                   r_region_proj.DO_YOU_AGREE_WITH_RESULT,
                   r_region_proj.CALCULATED_CATEGORIZATION,
                   r_region_proj.GLOBAL_DEVELOPMENT_STATUS,
                   --  18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                   r_region_proj.WP_DUP_DESC,
                   --
                   --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                   r_region_proj.STRATEGY_MEET_D,
                   r_region_proj.STRATEGY_MEET,
                   r_region_proj.STRATEGY_STAT,
                   -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGEr_region_proj...PIVOTAL_STUDY )
                   r_region_proj.API_BUDGET,
                   r_region_proj.BIO_STUDY,
                   r_region_proj.ENPV,
                   r_region_proj.T_RND_BUDGET,
                   r_region_proj.RND_MFG_PACK_BUDGET,
                   r_region_proj.O_METERIALS_BUDGET,
                   r_region_proj.OUT_CONTRACTORS,
                   r_region_proj.PILOT_STUDY,
                   r_region_proj.CLINICAL,
                   r_region_proj.PIVOTAL_STUDY,
                   ------
                   null,
                   null,
                   null,
                   null,
                   null
                  );

         exception
            when others then
               x_failed_projects := x_failed_projects + 1;
               /*dbms_output.put_line('*** ERROR in create_grd_projects_tasks -'              ||
                                    ' GPMS_ID: '         || r_region_proj.gpmsid\*gpms_id*\ ||
                                   ', PLW_PJT_ID: '      || r_region_proj.plw_pjt_id        ||
                                   ', PLW_INT_NUMBER: '  || r_region_proj.plw_int_number    ||
                                   ', Region: '          || r_region_proj.region   || ' - ' || sqlerrm);*/

               if(g_failed_projects_list is null) then

                  g_failed_projects_list := ' GPMS_ID: '         || r_region_proj.gpmsid/*gpms_id*/   ||
                                            ', PLW_PJT_ID: '     || r_region_proj.plw_pjt_id          ||
                                            ', PLW_INT_NUMBER: ' || r_region_proj.plw_int_number      ||
                                            ', Region: '         || r_region_proj.region   || ' - '   || sqlerrm;

               else

                  g_failed_projects_list := g_failed_projects_list || '<br>'                          ||
                                              ' GPMS_ID: '         || r_region_proj.gpmsid/*gpms_id*/ ||
                                             ', PLW_PJT_ID: '      || r_region_proj.plw_pjt_id        ||
                                             ', PLW_INT_NUMBER: '  || r_region_proj.plw_int_number    ||
                                             ', Region: '          || r_region_proj.region   || ' - ' || sqlerrm;

               end if;

         end;

      end loop;

      -- 2.a. COUNTRY PROJECTS(EMIA, Asia, LATAM)
      for r_country_proj_prj in c_country_proj_prj loop

         --l_prj_id := r_country_proj_prj.plw_prj;

         select ps_project_seq.nextval
         into   l_project_id
         from   dual;

         begin

            insert into ps_project(project_id,
                                   prj_id,
                                   gpms_id,
                                   -- to be removed
                                   gpms_exp_id,
                                   env_activity_id,
                                   --
                                   plw_pjt_id,
                                   plw_int_number,
                                   gain,
                                   global_dosage_form,
                                   -- ??? project level or wp level ???
                                   --strength_fill_volume,
                                   --strength_uom,
                                   --fill_volume_uom,
                                   --
                                   --project_rationale,
                                   --dev_type,
                                   --npv,
                                   target_market,
                                   development_site
                                  )
            values(l_project_id,
                   r_country_proj_prj.plw_wp, --r_country_proj_prj.plw_prj,
                   r_country_proj_prj.gpms_id,
                   -- to be removed
                   p_gpms_exp_id,
                   10,                   -- general_pack.get_env_activity_id
                   --
                   r_country_proj_prj.plw_prj,
                   r_country_proj_prj.plw_wp,
                   r_country_proj_prj.gain,
                   r_country_proj_prj.global_dosage_form,
                   --
                   --r_country_proj_prj.strength_fill_volume,
                   --r_country_proj_prj.strength_uom,
                   --r_country_proj_prj.fill_volume_uom,
                   --
                   --r_country_proj_prj.project_rationale,
                   --r_country_proj_prj.dev_type,
                   --r_country_proj_prj.npv,
                   r_country_proj_prj.region,
                   r_country_proj_prj.development_site
                  );

            -- 2.b. cursor for COUNTY level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
            for r_country_proj_cntr in c_country_proj_cntr(r_country_proj_prj.gpms_id,
                                                           r_country_proj_prj.plw_pjt_id,
                                                           r_country_proj_prj.plw_int_number,
                                                           r_country_proj_prj.gain,
                                                           r_country_proj_prj.global_dosage_form,
                                                           --r_country_proj_prj.strength_fill_volume,
                                                           --r_country_proj_prj.strength_uom,
                                                           --r_country_proj_prj.fill_volume_uom,
                                                           --r_country_proj_prj.project_rationale,
                                                           --r_country_proj_prj.npv,
                                                           r_country_proj_prj.development_site) loop

               begin

                  insert into ps_task(project_id,
                                      prj_id,
                                      task_id,
                                      country, --country_id,
                                      --
                                      gpms_exp_id,     -- to be removed
                                      env_activity_id, -- to be removed
                                      --
                                      gpms_id,
                                      plw_pjt_id,
                                      plw_int_number,
                                      -- ??? in FDs for Project's level only
                                      strength_uom,
                                      fill_volume_uom,  ----
                                      strength_fill_volume,
                                      -----------------------
                                      development_site,
                                      activity_type,
                                      launching_site,
                                      project_name,
                                      project_rationale,
                                      rationale_comments,
                                      project_technology,
                                      product_rationale,
                                      project_status,
                                      wp_status,
                                      project_progress,
                                      business,
                                      ims_sales,
                                      ims_sales_percentage,
                                      ims_units_,
                                      ims_units_percentage,
                                      first_allocation_approval_date, -- allocation_approval_date
                                      region,
                                      approval_date,
                                      project_priority,
                                      allocation_status,
                                      ftf,
                                      ftf_reason,
                                      handling_category,
                                      il_plmd,
                                      init_proj_strat_appro,
                                      innovator_company,
                                      innovator_shelf_life,
                                      ip_manager,
                                      lcm,
                                      launch_date,
                                      mars_date,
                                      nce_date,
                                      plfd_remark,
                                      filing_strategy,
                                      product_type,
                                      sales_channel,
                                      generic_segment,
                                      submission_to_authorties_fnish,
                                      submission_to_authrt_actl_strt,
                                      therapeutic_class,
                                      source_project_id,
                                      type_of_project,
                                      pipeline_manager,
                                      go_no_go_pivotal,
                                      go_no_go_biostudy,
                                      go_no_go_submission,
                                      go_no_go_launch,
                                      preliminary_allocation,
                                      reason_for_allocation,
                                      dev_type,
                                      npv,
                                      global_nte,
                                      submission_a_p,
                                      approval_a_p,
                                      launch_a_p,
                                      value_for_teva,
                                      -- added 08/09/2013
                                      plfd,
                                      plfd_asap,
                                      plfd_late,
                                      plfd_late_asap,
                                      --
                                      plmd,
                                      plmd_asap,
                                      plmd_main,
                                      plmd_main_asap,
                                      --
                                      lmd_main,
                                      lmd_main_asap,
                                      lmd_early,
                                      lmd_early_asap,
                                      product_status,
                                      -- added 04/08/2014
                                      INNOVATOR_BRAND_NAME,
                                      STABILITY_RISK,
                                      FORMULATION_COMPLEXITY,
                                      DEV_WITHOUT_RLD,
                                      API_COMPLEXITY,
                                      COMPLEX_TECHNOLOGY,
                                      STERILE_Y_N,
                                      AGENT_COMPANY,
                                      MFG_SITE,
                                      PRODUCT_TECHNOLOGY,
                                      PRODUCT_DOSAGE_FORM,
                                      OVERALL_BIO_RISK,
                                      FINAL_CATEGORIZATION,
                                      DO_YOU_AGREE_WITH_RESULT,
                                      CALCULATED_CATEGORIZATION,
                                      GLOBAL_DEVELOPMENT_STATUS,
                                      -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                                      WP_DUP_DESC,
                                      --
                                      --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                                      STRATEGY_MEET_D,
                                      STRATEGY_MEET,
                                      STRATEGY_STAT,
                                      -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGE..PIVOTAL_STUDY )
                                      API_BUDGET,
                                      BIO_STUDY,
                                      ENPV,
                                      T_RND_BUDGET,
                                      RND_MFG_PACK_BUDGET,
                                      O_METERIALS_BUDGET,
                                      OUT_CONTRACTORS,
                                      PILOT_STUDY,
                                      CLINICAL,
                                      PIVOTAL_STUDY,
                                      -----
                                      action
                                     )
                  values(l_project_id,
                         r_country_proj_prj.plw_wp, --r_country_proj_prj.plw_prj,
                         ps_task_sq.nextval, -- to be removed
                         r_country_proj_cntr.country,
                         --
                         p_gpms_exp_id,        -- to be removed
                         10,                   -- general_pack.get_env_activity_id, -- to be removed
                         --
                         r_country_proj_cntr.gpmsid, -- gpms_id,
                         r_country_proj_cntr.plw_pjt_id, --decode(r_country_proj_cntr.gpms_id, null, r_country_proj_cntr.plw_pjt_id,     null),
                         r_country_proj_cntr.plw_int_number, --decode(r_country_proj_cntr.gpms_id, null, r_country_proj_cntr.plw_int_number, null),
                         --
                         r_country_proj_cntr.strength_uom,
                         r_country_proj_cntr.fill_volume_uom,
                         r_country_proj_cntr.strength_fill_volume,
                         --
                         r_country_proj_cntr.development_site,
                         r_country_proj_cntr.activity_type,
                         r_country_proj_cntr.launching_site,
                         r_country_proj_cntr.project_name,
                         r_country_proj_cntr.project_rationale,
                         r_country_proj_cntr.rationale_comments,
                         r_country_proj_cntr.project_technology,
                         r_country_proj_cntr.product_rationale,
                         r_country_proj_cntr.project_status,
                         r_country_proj_cntr.wp_status,
                         r_country_proj_cntr.project_progress,
                         r_country_proj_cntr.business,
                         r_country_proj_cntr.ims_sales,
                         r_country_proj_cntr.ims_sales_percentage,
                         r_country_proj_cntr.ims_units_,
                         r_country_proj_cntr.ims_units_percentage,
                         r_country_proj_cntr.allocation_committee_date, --first_allocation_approval_date,
                         --r_country_proj_cntr.region,
                         decode(upper(r_country_proj_cntr.region), 'ASIA', 'INT',
                                                                   'EMIA', 'INT',
                                                                   r_country_proj_cntr.region),
                         /*to_date(to_char(to_date(r_country_proj_cntr.approval_date,
                                                 l_plw_date_format),
                                         l_db_date_format),
                                 l_db_date_format),*/ -- to be changed once the source data is date
                         r_country_proj_cntr.approval_date,
                         r_country_proj_cntr.project_priority,
                         r_country_proj_cntr.allocation_status,
                         r_country_proj_cntr.ftf,
                         r_country_proj_cntr.ftf_reason,
                         r_country_proj_cntr.handling_category,
                         r_country_proj_cntr.il_plmd,
                         r_country_proj_cntr.init_proj_strat_appro,
                         r_country_proj_cntr.innovator_company,
                         r_country_proj_cntr.innovator_shelf_life,
                         r_country_proj_cntr.ip_manager,
                         r_country_proj_cntr.lcm,
                         r_country_proj_cntr.launch_date,
                         /*to_date(to_char(to_date(r_country_proj_cntr.mars_date,
                                                 l_plw_date_format),
                                         l_db_date_format),
                                 l_db_date_format),*/ -- to be changed once the source data is date
                         r_country_proj_cntr.mars_date,
                         /*to_date(to_char(to_date(r_country_proj_cntr.nce_date,
                                                 l_plw_date_format),
                                         l_db_date_format),
                                 l_db_date_format),*/ -- to be changed once the source data is date
                         r_country_proj_cntr.nce_date,
                         r_country_proj_cntr.plfd_remark,
                         r_country_proj_cntr.filing_strategy,
                         r_country_proj_cntr.product_type,
                         r_country_proj_cntr.sales_channel,
                         r_country_proj_cntr.generic_segment,
                         to_date(to_char(to_date(r_country_proj_cntr.submission_to_authorities,
                                                 l_plw_date_format),
                                         l_db_date_format),
                                 l_db_date_format),
                         to_date(to_char(to_date(r_country_proj_cntr.subm_to_authorities_act_start,
                                                 l_plw_date_format),
                                         l_db_date_format),
                                 l_db_date_format),
                         r_country_proj_cntr.therapeutic_class,
                         r_country_proj_cntr.source_project_id,
                         r_country_proj_cntr.type_of_project,
                         r_country_proj_cntr.pipeline_manager,
                         r_country_proj_cntr.go_no_go_pivotal,
                         r_country_proj_cntr.go_no_go_biostudy,
                         r_country_proj_cntr.go_no_go_submission,
                         r_country_proj_cntr.go_no_go_launch,
                         r_country_proj_cntr.preliminary_allocation,
                         r_country_proj_cntr.reason_for_allocation,
                         r_country_proj_cntr.dev_type,
                         r_country_proj_cntr.npv,
                         r_country_proj_cntr.global_nte,
                         r_country_proj_cntr.submission_a_p,
                         r_country_proj_cntr.approval_a_p,
                         r_country_proj_cntr.launch_a_p,
                         r_country_proj_cntr.value_for_teva,
                         -- added 08/09/2013
                         r_country_proj_cntr.plfd,
                         r_country_proj_cntr.plfd_asap,
                         r_country_proj_cntr.plfd_late,
                         r_country_proj_cntr.plfd_late_asap,
                         --
                         r_country_proj_cntr.plmd,
                         r_country_proj_cntr.plmd_asap,
                         r_country_proj_cntr.plmd_main,
                         r_country_proj_cntr.plmd_main_asap,
                         --
                         r_country_proj_cntr.lmd_main,
                         r_country_proj_cntr.lmd_main_asap,
                         r_country_proj_cntr.lmd_early,
                         r_country_proj_cntr.lmd_early_asap,
                         r_country_proj_cntr.product_status,
                         -- added 04/08/2014
                         r_country_proj_cntr.INNOVATOR_BRAND_NAME,
                         r_country_proj_cntr.STABILITY_RISK,
                         r_country_proj_cntr.FORMULATION_COMPLEXITY,
                         r_country_proj_cntr.DEV_WITHOUT_RLD,
                         r_country_proj_cntr.API_COMPLEXITY,
                         r_country_proj_cntr.COMPLEX_TECHNOLOGY,
                         r_country_proj_cntr.STERILE_Y_N,
                         r_country_proj_cntr.AGENT_COMPANY,
                         r_country_proj_cntr.MFG_SITE,
                         r_country_proj_cntr.PRODUCT_TECHNOLOGY,
                         r_country_proj_cntr.PRODUCT_DOSAGE_FORM,
                         r_country_proj_cntr.OVERALL_BIO_RISK,
                         r_country_proj_cntr.FINAL_CATEGORIZATION,
                         r_country_proj_cntr.DO_YOU_AGREE_WITH_RESULT,
                         r_country_proj_cntr.CALCULATED_CATEGORIZATION,
                         r_country_proj_cntr.GLOBAL_DEVELOPMENT_STATUS,
                         -- 18/01/2015.  Nessia : Add new field: WP_DUP_DESC
                         r_country_proj_cntr.WP_DUP_DESC,
                         --
                         --  03/02/2015.  Netalee : Add new fields: STRATEGY_MEET_D,STRATEGY_MEET,STRATEGY_STAT
                         r_country_proj_cntr.STRATEGY_MEET_D,
                         r_country_proj_cntr.STRATEGY_MEET,
                         r_country_proj_cntr.STRATEGY_STAT,
                         -- 26/02/2015. Nessia : add budget new fields: (API_BUDGET, BIO_STUDY, ENPV, T_RND_BUDGEr_country_proj_cntr...PIVOTAL_STUDY )
                         r_country_proj_cntr.API_BUDGET,
                         r_country_proj_cntr.BIO_STUDY,
                         r_country_proj_cntr.ENPV,
                         r_country_proj_cntr.T_RND_BUDGET,
                         r_country_proj_cntr.RND_MFG_PACK_BUDGET,
                         r_country_proj_cntr.O_METERIALS_BUDGET,
                         r_country_proj_cntr.OUT_CONTRACTORS,
                         r_country_proj_cntr.PILOT_STUDY,
                         r_country_proj_cntr.CLINICAL,
                         r_country_proj_cntr.PIVOTAL_STUDY,
                         ------
                         null -- action
                        );

               exception
                  when others then -- PRJ_ID, COUNTRY, ENV_ACTIVITY_ID
                     x_failed_tasks := x_failed_tasks + 1;
                     /*dbms_output.put_line('*** ERROR in create_grd_projects_tasks - failed tasks *****' || chr(10)  ||
                                          ' GPMS_ID: ' || r_country_proj_prj.gpms_id  ||
                                         ', PLW_PRJ: ' || r_country_proj_prj.plw_prj  ||
                                         ', PLW_WP: '  || r_country_proj_prj.plw_wp   ||
                                         ', Region: '  || r_country_proj_prj.region   || ' - ' || sqlerrm);*/

                     /*dbms_output.put_line('*** ERROR in create_grd_projects_tasks - failed tasks *****' || chr(10)  ||
                                          'PRJ_ID: '           || r_country_proj_prj.plw_wp          ||
                                          ', COUNTRY: '        || r_country_proj_cntr.country        ||
                                          ', GPMS_ID: '        || r_country_proj_cntr.gpmsid         || --r_country_proj_prj.gpms_id  ||
                                          ', PLW_PJT_ID: '     || r_country_proj_cntr.plw_pjt_id     ||
                                          ', PLW_INT_NUMBER: ' || r_country_proj_cntr.plw_int_number || ' - ' || sqlerrm);*/

                     --dbms_output.put_line('x_failed_tasks: ' || x_failed_tasks);

                     if(g_failed_tasks_list is null) then

                        g_failed_tasks_list := ' PRJ_ID: '          || r_country_proj_prj.plw_wp          ||
                                               ', COUNTRY: '        || r_country_proj_cntr.country        ||
                                               ', GPMS_ID: '        || r_country_proj_cntr.gpmsid         || --r_country_proj_prj.gpms_id  ||
                                               ', PLW_PJT_ID: '     || r_country_proj_cntr.plw_pjt_id     ||
                                               ', PLW_INT_NUMBER: ' || r_country_proj_cntr.plw_int_number || ' - ' || sqlerrm;

                     else

                        g_failed_tasks_list := g_failed_tasks_list  || '<br>'                             ||
                                               'PRJ_ID: '           || r_country_proj_prj.plw_wp          ||
                                               ', COUNTRY: '        || r_country_proj_cntr.country        ||
                                               ', GPMS_ID: '        || r_country_proj_cntr.gpmsid         || --r_country_proj_prj.gpms_id  ||
                                               ', PLW_PJT_ID: '     || r_country_proj_cntr.plw_pjt_id     ||
                                               ', PLW_INT_NUMBER: ' || r_country_proj_cntr.plw_int_number || ' - ' || sqlerrm;

                     end if;

               end;

            end loop;

         exception
            when others then
               x_failed_projects := x_failed_projects + 1;

               -- count tasks that were not created for failed projects
               begin

                  select count(*)
                  into   l_failed_tasks
                  from   (
                           -- Projects that were migrated into PLW
                           select t.*
                           from   export_grd t
                           where  r_country_proj_prj.gpms_id        is not null
                             and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                             and  t.country                         not in ('JP','IL')
                             and  upper(t.wp_status)                <> 'DRAFT'
                             and  t.activity_type                   <> 'R' || chr(38) || 'D'
                             --
                             and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                             and  t.development_site                not like '%BioG%'
                             /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                                   (t.project_status                = 'Draft' and
                                    nvl(t.global_nte, '0')          = '1'))*/
                             and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                             -- Parameters --
                             and  t.gpms_id                         = r_country_proj_prj.gpms_id
                             and  t.gain                            = r_country_proj_prj.gain
                             and  nvl(t.global_dosage_form, '-999') = nvl(r_country_proj_prj.global_dosage_form, '-999')
                             --
                             --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                             --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                             --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                             --
                             --and  nvl(t.project_rationale, '-999')    = nvl(r_country_proj_prj.project_rationale, '-999')
                             --and  nvl(t.npv, '-999')                  = nvl(l_npv, '-999')
                             and  nvl(t.development_site, '-999')   = nvl(r_country_proj_prj.development_site, '-999')
                             -- filter out blocked projects
                             and  not exists (select 1
                                              from   psnext_unique_projects s
                                              where  s.gpms_id = t.gpms_id
                                                and  s.country = t.country)
                             -- migrated projects only
                             and  exists (select 1
                                          from   export_grd_go_live gl
                                          where  gl.gpms_id        = t.gpms_id
                                            and  gl.plw_pjt_id     = t.plw_pjt_id
                                            and  gl.plw_int_number = t.plw_int_number)
                           union
                           select t.*
                           from   export_grd t
                           where  r_country_proj_prj.gpms_id          is null
                             and  t.gpms_id                           is not null
                             and  upper(t.region)                     in('EMIA', 'ASIA', 'LATAM')
                             and  t.country                           not in ('JP','IL')
                             and  upper(t.wp_status)                  <> 'DRAFT'
                             and  t.activity_type                     <> 'R' || chr(38) || 'D'
                             --
                             and  t.development_site                  not in('BDIR', 'BD-L', 'BN', 'RDI')
                             and  t.development_site                  not like '%BioG%'
                             /*and  (upper(t.allocation_status)         = 'APPROVED'  or
                                   (t.project_status                  = 'Draft' and
                                    nvl(t.global_nte, '0')            = '1'))*/
                             and  nvl(t.generic_segment, '1')         not like '%Authorized Generic%'
                             -- Parameters --
                             --and  t.gpms_id                           = r_country_proj_prj.gpms_id
                             and  t.plw_pjt_id                        = r_country_proj_prj.plw_pjt_id
                             and  t.plw_int_number                    = r_country_proj_prj.plw_int_number
                             and  t.gain                              = r_country_proj_prj.gain
                             and  nvl(t.global_dosage_form, '-999')   = nvl(r_country_proj_prj.global_dosage_form, '-999')
                             --
                             --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                             --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                             --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                             --
                             --and  nvl(t.project_rationale, '-999')    = nvl(r_country_proj_prj.project_rationale, '-999')
                             --and  nvl(t.npv, '-999')                  = nvl(l_npv, '-999')
                             and  nvl(t.development_site, '-999')     = nvl(r_country_proj_prj.development_site, '-999')
                             --
                             and  nvl(r_country_proj_prj.gpms_id, '-999') <> nvl(r_country_proj_prj.plw_int_number, '-999')
                             -- filter out blocked projects
                             and  not exists (select 1
                                              from   psnext_unique_projects s
                                              where  s.gpms_id = t.gpms_id
                                                and  s.country = t.country)
                             and  not exists (select 1
                                              from   export_grd_go_live gl
                                              where  gl.gpms_id        = t.gpms_id
                                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                                and  gl.plw_int_number = t.plw_int_number)
                           union
                           -- Projects that were created in PLW
                           select t.*
                           from   export_grd t
                           where  r_country_proj_prj.gpms_id          is null
                             and  t.gpms_id                           is null
                             and  upper(t.region)                     in('EMIA', 'ASIA', 'LATAM')
                             and  t.country                           not in ('JP','IL')
                             and  upper(t.wp_status)                  <> 'DRAFT'
                             and  t.activity_type                     <> 'R' || chr(38) || 'D'
                             --
                             and  t.development_site                  not in('BDIR', 'BD-L', 'BN', 'RDI')
                             and  t.development_site                  not like '%BioG%'
                             /*and  (upper(t.allocation_status)         = 'APPROVED'  or
                                   (t.project_status                  = 'Draft' and
                                    nvl(t.global_nte, '0')            = '1'))*/
                             and  nvl(t.generic_segment, '1')         not like '%Authorized Generic%'
                             -- Parameters --
                             and  t.plw_pjt_id                        = r_country_proj_prj.plw_pjt_id
                             and  t.plw_int_number                    = r_country_proj_prj.plw_int_number
                             and  t.gain                              = r_country_proj_prj.gain
                             and  nvl(t.global_dosage_form, '-999')   = nvl(r_country_proj_prj.global_dosage_form, '-999')
                             --
                             --and  nvl(t.strength_fill_volume, '-999') = nvl(l_strength_fill_volume, '-999')
                             --and  nvl(t.strength_uom, '-999')         = nvl(l_strength_uom, '-999')
                             --and  nvl(t.fill_volume_uom, '-999')      = nvl(l_fill_volume_uom, '-999')
                             --
                             --and  nvl(t.project_rationale, '-999')    = nvl(r_country_proj_prj.project_rationale, '-999')
                             --and  nvl(t.npv, '-999')                  = nvl(l_npv, '-999')
                             and  nvl(t.development_site, '-999')     = nvl(r_country_proj_prj.development_site, '-999')
                         )q
                  order by q.plw_int_number
                  ;

               exception
                  when others then
                     dbms_output.put_line('*** ERROR in create_grd_projects_tasks during failed task count.');

               end;

               x_failed_tasks := x_failed_tasks + l_failed_tasks;
               /*dbms_output.put_line('*** ERROR in create_grd_projects_tasks -'  ||
                                    ' GPMS_ID: ' || r_country_proj_prj.gpms_id  ||
                                   ', PLW_PRJ: ' || r_country_proj_prj.plw_prj  ||
                                   ', PLW_WP: '  || r_country_proj_prj.plw_wp   ||
                                   ', Region: '  || r_country_proj_prj.region   || ' - ' || sqlerrm);*/

               if(g_failed_projects_list is null) then

                  g_failed_projects_list := ' GPMS_ID: '         || r_country_proj_prj.gpms_id  ||
                                            ', PLW_PJT_ID: '     || r_country_proj_prj.plw_prj  ||
                                            ', PLW_INT_NUMBER: ' || r_country_proj_prj.plw_wp   ||
                                            ', Region: '         || r_country_proj_prj.region   || ' - ' || sqlerrm;

               else

                  g_failed_projects_list := g_failed_projects_list || '<br>'                      ||
                                              ' GPMS_ID: '         || r_country_proj_prj.gpms_id  ||
                                             ', PLW_PJT_ID: '      || r_country_proj_prj.plw_prj  ||
                                             ', PLW_INT_NUMBER: '  || r_country_proj_prj.plw_wp   ||
                                             ', Region: '          || r_country_proj_prj.region   || ' - ' || sqlerrm;

               end if;

         end;

      end loop;

   exception

      when others then

         rollback;
         g_errbuf   := '[PRJ_ID: ' || l_prj_id || '] - ' || sqlerrm;
         g_sql_code := sqlcode;

         raise e_create_grd_projects_tasks;

   end create_grd_projects_tasks;

   ------------------------------------------------
   -- update_export_grd_backup_actin
   ------------------------------------------------
   procedure update_export_grd_backup_actin(x_java_updated out varchar2) is

      -- 1. cursor for REGION PROJECTS(EU, US, CA, IL, JP)
      cursor c_region_proj is
         -- migrated projects
         select b.rowid,
                p.action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           --
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           --
           and  b.gpms_id  is not null
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           -- join
           and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  p.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(p.action, 'N')            <> 'N'
           --and  p.action                      in ('I', 'U', 'E', 'N')
         -- Projects created in PLW and
         -- WPs created in PLW for migrated projects
         union all
         select b.rowid,
                p.action
         from   ps_project        p,
                export_grd_backup b
         where  1 = 1
           and  b.gain                 is not null
           and  b.development_site     is not null
           and  b.region               is not null
           and  b.country              is not null
           and  length(nvl(rtrim(ltrim(b.gpms_id)), '-99999')) = 6 -- filter out SEE projects
           --
           and  (-- EU
                 (upper(b.region)      = 'EU'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'       and
                   (b.development_site not in('BD', 'BD-E') or
                    b.sales_channel    like '%OTC%')))
                  or -- CA
                 (upper(b.region)      = 'CA'                  and
                  upper(b.wp_status)   <> 'DRAFT'              and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- US
                 (upper(b.region)      = 'US'                  and
                  (b.development_site  not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                   b.development_site  not like '%BioG%'))
                  or -- IL
                  (upper(b.region)     = 'EMIA'                and
                   b.country           = 'IL'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site  not like '%BioG%'))
                  or -- JP
                  (upper(b.region)     = 'ASIA'                and
                   b.country           = 'JP'                  and
                   upper(b.wp_status)  <> 'DRAFT'              and
                   (b.development_site not in('BDIR',
                                              'BD-L',
                                              'BN',
                                              'RDI')           and
                    b.development_site not like '%BioG%'))
                )
           /*and  (upper(b.allocation_status) = 'APPROVED'  or
                 (b.project_status          = 'Draft' and
                  nvl(b.global_nte, '0')    = '1'))*/
           --
           and  nvl(b.generic_segment, '1') not like '%Authorized Generic%'
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           --
           and  (-- projects created in plw
                 (b.gpms_id is null) or
                 (-- WPs created in plw for migrated projects
                  b.gpms_id is not null and
                  not exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = b.gpms_id
                                and  gl.plw_pjt_id     = b.plw_pjt_id
                                and  gl.plw_int_number = b.plw_int_number)
                 )
                )
           -- join
           --and  nvl(p.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(p.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(p.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  p.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(p.action, 'N')            <> 'N'
           --and  p.action                      in ('I', 'U', 'E', 'N')
        ;

      -- 2.b. cursor for TASK level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_cntr is
         -- Tasks that were migrated into PLW
         select b.rowid,
                t.action
         from   ps_task           t,
                export_grd_backup b
         where  b.gpms_id                     is not null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           -- filter out blocked projects
           and  not exists (select 1
                            from   PSNEXT_UNIQUE_PROJECTS_BACKUP s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           --  java does not run records
           and  t.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(t.action, 'N')            <> 'N'
           --and  t.action                      in ('I', 'U', 'E', 'N')
         union all
         -- WPs created in PLW for migrated into PLW projects
         select b.rowid,
                t.action
         from   ps_task           t,
                export_grd_backup b
         where  b.gpms_id                     is not null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           -- filter out blocked projects
           and  not exists (select 1
                            from   PSNEXT_UNIQUE_PROJECTS_BACKUP s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           --  java does not run records
           and  t.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           --and  t.action                      in ('I', 'U', 'E', 'N')
         -- Tasks that were created in PLW for non migrated projects
         union all
         select b.rowid,
                t.action
         from   ps_task           t,
                export_grd_backup b
         where  b.gpms_id                     is null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in ('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java does not run records
           and  t.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(t.action, 'N')            <> 'N'
           --and  t.action                      in ('I', 'U', 'E', 'N')
         ;

      -- 2.a. cursor for PROJECTS level for COUNTRY PROJECTS(EMIA, Asia, LATAM)
      cursor c_country_proj_prj is
         -- Migrated Projects
         select --distinct
                b.rowid,
                p.action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  b.gpms_id                     is not null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  length(nvl(rtrim(ltrim(b.gpms_id)),
                           '-99999')) = 6 -- filter out SEE projects
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  p.project_id                  = t.project_id
           and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  exists (select 1
                        from   export_grd_go_live gl
                        where  gl.gpms_id        = b.gpms_id
                          and  gl.plw_pjt_id     = b.plw_pjt_id
                          and  gl.plw_int_number = b.plw_int_number)
           --  java not run records
           and  p.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(p.action, 'N')            <> 'N'
           --and  p.action                      in ('I', 'U', 'E')
         union all
         -- WPs created in PLW for migrated projects
         select --distinct
                b.rowid,
                p.action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  b.gpms_id                     is not null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  length(nvl(rtrim(ltrim(b.gpms_id)),
                           '-99999')) = 6 -- filter out SEE projects
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           -- filter out blocked projects
           and  not exists (select 1
                            from   psnext_unique_projects_backup s
                            where  s.gpms_id = b.gpms_id
                              and  s.country = b.country)
           -- migrated projects only
           and  not exists (select 1
                            from   export_grd_go_live gl
                            where  gl.gpms_id        = b.gpms_id
                              and  gl.plw_pjt_id     = b.plw_pjt_id
                              and  gl.plw_int_number = b.plw_int_number)
           --  java not run records
           and  p.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(p.action, 'N')            <> 'N'
           --and  p.action                      in ('I', 'U', 'E')
         union all
         -- Projects created in PLW
         select --distinct
                b.rowid,
                p.action
         from   ps_project         p,
                ps_task            t,
                export_grd_backup  b
         where  b.gpms_id                     is null
           and  upper(b.region)               in('EMIA', 'ASIA', 'LATAM')
           and  b.country                     not in('JP','IL')
           and  upper(b.wp_status)            <> 'DRAFT'
           and  b.activity_type               <> 'R' || chr(38) || 'D'
           and  length(nvl(rtrim(ltrim(b.gpms_id)),
                           '-99999')) = 6 -- filter out SEE projects
           --
           and  b.development_site            not in('BDIR', 'BD-L', 'BN', 'RDI')
           and  b.development_site            not like '%BioG%'
           /*and  (upper(b.allocation_status)   = 'APPROVED'  or
                 (b.project_status            = 'Draft' and
                  nvl(b.global_nte, '0')      = '1'))*/
           and  nvl(b.generic_segment, '1')   not like '%Authorized Generic%'
           --
           and  b.gain                        is not null
           and  b.development_site            is not null
           and  b.region                      is not null
           and  b.country                     is not null
           -- join
           and  p.project_id                  = t.project_id
           --and  nvl(t.gpms_id, '-999')        = nvl(b.gpms_id, '-999')
           and  nvl(t.plw_pjt_id, '-999')     = nvl(b.plw_pjt_id, '-999')
           and  nvl(t.plw_int_number, '-999') = nvl(b.plw_int_number, '-999')
           --  java not run records
           and  p.env_activity_id             = '32'
           and  b.status                      is null
           and  b.action                      is null
           and  nvl(p.action, 'N')            <> 'N'
           --and  p.action                      in ('I', 'U', 'E')
        ;

   begin

      select nvl(max('Y'), 'N')
      into   x_java_updated
      from   export_grd_backup b
      where  b.status  is not null
         or  b.action  is not null
      ;

      -- if java code did not run - update action from ps_project / ps_task
      --if(x_java_updated = 'N') then - disabled, because java runs twice (for IL and for US sites)

         -- update status for region projects(EU, US, CA, IL, JP)
         for r_region_proj in c_region_proj loop

            update export_grd_backup b
            set    b.action = r_region_proj.action,
                   b.status = 'J'
            where  b.rowid  = r_region_proj.rowid
            ;

         end loop;

         -- update status for country tasks(EMIA(not IL), ASIA(not JP), LATAM)
         for r_country_proj_cntr in c_country_proj_cntr loop

            update export_grd_backup b
            set    b.action = r_country_proj_cntr.action,
                   b.status = 'J'
            where  b.rowid  = r_country_proj_cntr.rowid
            ;

         end loop;

         -- update status for country projects(EMIA(not IL), ASIA(not JP), LATAM)
         for r_country_proj_prj in c_country_proj_prj loop

            update export_grd_backup b
            set    b.action = r_country_proj_prj.action,
                   b.status = 'J'
            where  b.rowid  = r_country_proj_prj.rowid
            ;

         end loop;

      --end if;

   end update_export_grd_backup_actin;

   ------------------------------------------------
   -- validate_null_values
   ------------------------------------------------
   procedure validate_null_values(x_null_projects         out number,
                                  x_null_tasks            out number,
                                  x_null_projects_message out varchar2) is

      l_region_null_projects  number;
      l_country_null_projects number;

      cursor c_blank_project_values is
         select *
         from   (
                  ---------------- GAIN ----------------
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'GAIN'   field_name
                  from   export_grd t
                  where  t.gain                                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects
                    and  t.gpms_id  is not null
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select null gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'GAIN'   field_name
                  from   export_grd t
                  where  t.gain                                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- non-migrated projects
                    and  (-- projects created in plw
                          (t.gpms_id is null) or
                          (-- WPs created in plw for migrated projects
                           t.gpms_id is not null and
                           not exists (select 1
                                       from   export_grd_go_live gl
                                       where  gl.gpms_id        = t.gpms_id
                                         and  gl.plw_pjt_id     = t.plw_pjt_id
                                         and  gl.plw_int_number = t.plw_int_number)
                          )
                         )
                  ---------------- DEVELOPMENT_SITE ----------------
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'DEVELOPMENT_SITE'   field_name
                  from   export_grd t
                  where  t.development_site                             is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'       and
                            ('development_site' not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects
                    and  t.gpms_id  is not null
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select null gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'DEVELOPMENT_SITE'   field_name
                  from   export_grd t
                  where  t.development_site                             is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'       and
                            ('development_site' not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           ('development_site'  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            'development_site'  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            ('development_site' not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             'development_site' not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- non-migrated projects
                    and  (-- projects created in plw
                          (t.gpms_id is null) or
                          (-- WPs created in plw for migrated projects
                           t.gpms_id is not null and
                           not exists (select 1
                                       from   export_grd_go_live gl
                                       where  gl.gpms_id        = t.gpms_id
                                         and  gl.plw_pjt_id     = t.plw_pjt_id
                                         and  gl.plw_int_number = t.plw_int_number)
                          )
                         )
                  ---------------- REGION ----------------
                  union
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'REGION'   field_name
                  from   export_grd t
                  where  t.region                                       is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (--upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (--upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (--upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (--upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (--upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (/*upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and*/
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects
                    and  t.gpms_id  is not null
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select null gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'REGION'   field_name
                  from   export_grd t
                  where  t.region                                       is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (--upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (--upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (--upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (--upper(t.region)     = 'EMIA'                and
                            t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (--upper(t.region)     = 'ASIA'                and
                            t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (/*upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and*/
                            upper(t.wp_status)  <> 'DRAFT'              and
                            t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- non-migrated projects
                    and  (-- projects created in plw
                          (t.gpms_id is null) or
                          (-- WPs created in plw for migrated projects
                           t.gpms_id is not null and
                           not exists (select 1
                                       from   export_grd_go_live gl
                                       where  gl.gpms_id        = t.gpms_id
                                         and  gl.plw_pjt_id     = t.plw_pjt_id
                                         and  gl.plw_int_number = t.plw_int_number)
                          )
                         )
                  ---------------- COUNTRY ----------------
                  union
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'COUNTRY'   field_name
                  from   export_grd t
                  where  t.country                                      is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            --t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            --t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            --t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       --and  s.country = t.country
                                    )
                    -- migrated projects
                    and  t.gpms_id  is not null
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select null gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'COUNTRY'   field_name
                  from   export_grd t
                  where  t.country                                      is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                    --
                    and  (-- EU
                          (upper(t.region)      = 'EU'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'       and
                            (t.development_site not in('BD', 'BD-E') or
                             t.sales_channel    like '%OTC%')))
                           or -- CA
                          (upper(t.region)      = 'CA'                  and
                           upper(t.wp_status)   <> 'DRAFT'              and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- US
                          (upper(t.region)      = 'US'                  and
                           (t.development_site  not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                            t.development_site  not like '%BioG%'))
                           or -- IL
                           (upper(t.region)     = 'EMIA'                and
                            --t.country           = 'IL'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- JP
                           (upper(t.region)     = 'ASIA'                and
                            --t.country           = 'JP'                  and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                           or -- PRISMA
                           (upper(t.region)     in('EMIA',
                                                   'ASIA',
                                                   'LATAM')             and
                            upper(t.wp_status)  <> 'DRAFT'              and
                            --t.country           not in('JP','IL')       and
                            t.activity_type     <> 'R' || chr(38) || 'D' and
                            (t.development_site not in('BDIR',
                                                       'BD-L',
                                                       'BN',
                                                       'RDI')           and
                             t.development_site not like '%BioG%'))
                         )
                    --
                    /*and  (upper(t.allocation_status) = 'APPROVED'  or
                          (t.project_status          = 'Draft' and
                           nvl(t.global_nte, '0')    = '1'))*/
                    --
                    and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       --and  s.country = t.country
                                    )
                    -- non-migrated projects
                    and  (-- projects created in plw
                          (t.gpms_id is null) or
                          (-- WPs created in plw for migrated projects
                           t.gpms_id is not null and
                           not exists (select 1
                                       from   export_grd_go_live gl
                                       where  gl.gpms_id        = t.gpms_id
                                         and  gl.plw_pjt_id     = t.plw_pjt_id
                                         and  gl.plw_int_number = t.plw_int_number)
                          )
                         )
               ) q
         order by q.field_name
         ;

      cursor c_blank_task_values is
         select *
         from   (
                  ---------------- GAIN ----------------
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'GAIN'   field_name
                  from   export_grd t
                  where  t.gain                            is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'GAIN'   field_name
                  from   export_grd t
                  where  t.gain                            is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'GAIN'   field_name
                  from   export_grd t
                  where  t.gain                            is null
                    and  t.gpms_id                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                  ---------------- DEVELOPMENT_SITE ----------------
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'DEVELOPMENT_SITE'   field_name
                  from   export_grd t
                  where  t.development_site                is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    --and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'DEVELOPMENT_SITE'   field_name
                  from   export_grd t
                  where  t.development_site                is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    --and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'DEVELOPMENT_SITE'   field_name
                  from   export_grd t
                  where  t.development_site                is null
                    and  t.gpms_id                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    --and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                  ---------------- REGION ----------------
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'REGION'   field_name
                  from   export_grd t
                  where  t.region                          is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'REGION'   field_name
                  from   export_grd t
                  where  t.region                          is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       and  s.country = t.country)
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'REGION'   field_name
                  from   export_grd t
                  where  t.region                          is null
                    and  t.gpms_id                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                  ---------------- COUNTRY ----------------
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'COUNTRY'   field_name
                  from   export_grd t
                  where  t.country                         is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    --and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       --and  s.country = t.country
                                    )
                    -- migrated projects only
                    and  exists (select 1
                                 from   export_grd_go_live gl
                                 where  gl.gpms_id        = t.gpms_id
                                   and  gl.plw_pjt_id     = t.plw_pjt_id
                                   and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'COUNTRY'   field_name
                  from   export_grd t
                  where  t.country                         is null
                    and  t.gpms_id                         is not null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    --and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                    -- filter out blocked projects
                    and  not exists (select 1
                                     from   psnext_unique_projects s
                                     where  s.gpms_id = t.gpms_id
                                       --and  s.country = t.country
                                    )
                    and  not exists (select 1
                                     from   export_grd_go_live gl
                                     where  gl.gpms_id        = t.gpms_id
                                       and  gl.plw_pjt_id     = t.plw_pjt_id
                                       and  gl.plw_int_number = t.plw_int_number)
                  union all
                  select t.gpms_id,
                         t.plw_pjt_id,
                         t.plw_int_number,
                         'COUNTRY'   field_name
                  from   export_grd t
                  where  t.country                         is null
                    and  t.gpms_id                         is null
                    and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                    '-99999'))             = 6 -- filter out SEE projects
                    and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                    --and  t.country                         not in ('JP','IL')
                    and  upper(t.wp_status)                <> 'DRAFT'
                    and  t.activity_type                   <> 'R' || chr(38) || 'D'
                    --
                    and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                    and  t.development_site                not like '%BioG%'
                    /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                          (t.project_status                = 'Draft' and
                           nvl(t.global_nte, '0')          = '1'))*/
                    and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
               ) q
         order by q.field_name
         ;

   begin

      /*select count(1)
      into   x_null_projects
      from   export_grd t
      where  t.gain             is null
         or  t.development_site is null
         or  t.region           is null
         or  t.country          is null
      ;*/

      select count(1)
      into   l_region_null_projects
      from   (
               -- migrated
               select t.*
               from   export_grd t
               where  1 = 1
                 -- nulls
                 and  (t.gain             is null
                   or  t.development_site is null
                   or  t.region           is null
                   or  t.country          is null)
                 --
                 and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  (-- EU
                       (--upper(t.region)      = 'EU'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'       and
                         ('development_site' not in('BD', 'BD-E') or
                          t.sales_channel    like '%OTC%')))
                        or -- CA
                       (--upper(t.region)      = 'CA'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'))
                        or -- US
                       (--upper(t.region)      = 'US'                  and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'))
                        or -- IL
                        (--upper(t.region)     = 'EMIA'                and
                         --t.country           = 'IL'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         ('development_site' not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          'development_site' not like '%BioG%'))
                        or -- JP
                        (--upper(t.region)     = 'ASIA'                and
                         --t.country           = 'JP'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         ('development_site' not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          'development_site' not like '%BioG%'))
                      )
                 --
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 --
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                  )
                 -- migrated projects
                 and  t.gpms_id  is not null
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
               -- non-migrated
               union all
               select t.*
               from   export_grd t
               where  1 = 1
                 -- nulls
                 and  (t.gain             is null
                   or  t.development_site is null
                   or  t.region           is null
                   or  t.country          is null)
                 --
                 and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  (-- EU
                       (--upper(t.region)      = 'EU'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'       and
                         ('development_site' not in('BD', 'BD-E') or
                          t.sales_channel    like '%OTC%')))
                        or -- CA
                       (--upper(t.region)      = 'CA'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'))
                        or -- US
                       (--upper(t.region)      = 'US'                  and
                        ('development_site'  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         'development_site'  not like '%BioG%'))
                        or -- IL
                        (--upper(t.region)     = 'EMIA'                and
                         --t.country           = 'IL'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         ('development_site' not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          'development_site' not like '%BioG%'))
                        or -- JP
                        (--upper(t.region)     = 'ASIA'                and
                         --t.country           = 'JP'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         ('development_site' not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          'development_site' not like '%BioG%'))
                      )
                 --
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 --
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                 )
                 -- non-migrated projects
                 and  (-- projects created in plw
                       (t.gpms_id is null) or
                       (-- WPs created in plw for migrated projects
                        t.gpms_id is not null and
                        not exists (select 1
                                    from   export_grd_go_live gl
                                    where  gl.gpms_id        = t.gpms_id
                                      and  gl.plw_pjt_id     = t.plw_pjt_id
                                      and  gl.plw_int_number = t.plw_int_number)
                       )
                      )
             )
      ;


      select count(1)
      into   l_country_null_projects
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is not null
                 --and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                   not in ('JP', 'IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))       = 6 -- filter out SEE projects
                 --
                 --and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- nulls
                 and  (t.gain                      is null or
                       t.development_site          is null or
                       t.region                    is null or
                       t.country                   is null)
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                 )
                 -- migrated projects only
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)

               -- WPs created in PLW for migrated into PLW projects
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is not null
                 --and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                   not in ('JP', 'IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))       = 6 -- filter out SEE projects
                 --
                 --and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- nulls
                 and  (t.gain                     is null or
                       t.development_site         is null or
                       t.region                   is null or
                       t.country                  is null)
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                 )
                 -- to filter out WPs created in PLW for Migrated Projects
                 and  not exists (select 1
                                  from   export_grd_go_live gl
                                  where  gl.gpms_id        = t.gpms_id
                                    and  gl.plw_pjt_id     = t.plw_pjt_id
                                    and  gl.plw_int_number = t.plw_int_number)
                 --
                 /*and  exists (select 1
                              from   export_grd_backup)*/
               -- Projects that were created in PLW
               union
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is null
                 --and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                   not in ('JP','IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 --
                 --and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- nulls
                 and  (t.gain                      is null or
                       t.development_site          is null or
                       t.region                    is null or
                       t.country                   is null)
             ) q
      ;


      x_null_projects := l_region_null_projects + l_country_null_projects;

      select sum(q.a)
      into   x_null_tasks
      from   (
               select count(t.plw_int_number) a
               from   export_grd t
               where  t.gpms_id                    is not null
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))        = 6 -- filter out SEE projects
                 --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)                <> 'DRAFT'
                 and  t.activity_type                   <> 'R' || chr(38) || 'D'
                 --
                 --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)  = 'APPROVED'  or
                       (t.project_status           = 'Draft' and
                        nvl(t.global_nte, '0')     = '1'))*/
                 and  nvl(t.generic_segment, '1')  not like '%Authorized Generic%'
                 -- nulls
                 and  (t.gain                      is null
                  or   t.development_site          is null
                  or   t.region                    is null
                  or   t.country                   is null)
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                 )
                 -- migrated projects only
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
               union all
               select count(t.plw_int_number) a
               from   export_grd t
               where  t.gpms_id                    is not null
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))        = 6 -- filter out SEE projects
                 --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)           <> 'DRAFT'
                 and  t.activity_type              <> 'R' || chr(38) || 'D'
                 --
                 --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)  = 'APPROVED'  or
                       (t.project_status           = 'Draft' and
                        nvl(t.global_nte, '0')     = '1'))*/
                 and  nvl(t.generic_segment, '1')  not like '%Authorized Generic%'
                 -- nulls
                 and  (t.gain                      is null
                  or   t.development_site          is null
                  or   t.region                    is null
                  or   t.country                   is null)
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    --and  s.country = t.country
                                 )
                 and  not exists (select 1
                                  from   export_grd_go_live gl
                                  where  gl.gpms_id        = t.gpms_id
                                    and  gl.plw_pjt_id     = t.plw_pjt_id
                                    and  gl.plw_int_number = t.plw_int_number)
               union all
               select count(t.plw_int_number) a
               from   export_grd t
               where  t.gpms_id                    is null
                 --and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 --and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)           <> 'DRAFT'
                 and  t.activity_type              <> 'R' || chr(38) || 'D'
                 --
                 --and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 --and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)  = 'APPROVED'  or
                       (t.project_status           = 'Draft' and
                        nvl(t.global_nte, '0')     = '1'))*/
                 and  nvl(t.generic_segment, '1')  not like '%Authorized Generic%'
                  -- nulls
                 and  (t.gain                         is null
                  or   t.development_site             is null
                  or   t.region                       is null
                  or   t.country                      is null)
             )q
      ;

      if(x_null_projects > 0) then

         --dbms_output.put_line('********************** WARNING **********************');

         for r_blank_project_values in c_blank_project_values loop

            if(r_blank_project_values.gpms_id is not null) then

               --dbms_output.put_line('GPMS_ID ' || r_blank_project_values.gpms_id ||  ' - the field ' || r_blank_project_values.field_name || ' has null');

               if(x_null_projects_message is null) then

                  x_null_projects_message := '<h2>************** WARNING **************</h2>'  ||
                                             'GPMS_ID ' || r_blank_project_values.gpms_id || ' - the field ' ||
                                             r_blank_project_values.field_name || ' has null<br>';

               else

                  x_null_projects_message := x_null_projects_message ||
                                             'GPMS_ID ' || r_blank_project_values.gpms_id || ' - the field ' ||
                                             r_blank_project_values.field_name || ' has null<br>';

               end if;

            else

               /*dbms_output.put_line('PLW_PJT_ID ' || r_blank_project_values.plw_pjt_id || ', PLW_INT_NUMBER ' || r_blank_project_values.plw_int_number ||
                                    ' - the field ' || r_blank_project_values.field_name || ' has null');*/

               if(x_null_projects_message is null) then

                  x_null_projects_message := '<h2>************** WARNING **************</h2>' ||
                                             'PLW_PJT_ID ' || r_blank_project_values.plw_pjt_id || ', PLW_INT_NUMBER ' || r_blank_project_values.plw_int_number ||
                                             ' - the field ' || r_blank_project_values.field_name || ' has null<br>';

               else

                  x_null_projects_message := x_null_projects_message ||
                                             'PLW_PJT_ID ' || r_blank_project_values.plw_pjt_id || ', PLW_INT_NUMBER ' || r_blank_project_values.plw_int_number ||
                                             ' - the field ' || r_blank_project_values.field_name || ' has null<br>';

               end if;

            end if;

         end loop;

         if(x_null_projects_message is not null) then

            x_null_projects_message := x_null_projects_message || '<h2>**************************************</h2>';

         end if;

         /*for r_blank_task_values in c_blank_task_values loop

            if(r_blank_task_values.gpms_id is not null) then

               dbms_output.put_line('GPMS_ID ' || r_blank_task_values.gpms_id ||  ' - the field ' || r_blank_task_values.field_name || ' has null');

            else

               dbms_output.put_line('PLW_PJT_ID ' || r_blank_task_values.plw_pjt_id || ', PLW_INT_NUMBER ' || r_blank_task_values.plw_int_number ||
                                    ' - the field ' || r_blank_task_values.field_name || ' has null');

            end if;

         end loop;*/

         --dbms_output.put_line('*****************************************************' || chr(10));

      end if;

   end validate_null_values;

   ------------------------------------------------
   -- count_projects_tasks
   ------------------------------------------------
   procedure count_projects_tasks(x_plw_projects out number,
                                  x_plw_tasks    out number) is

      l_region_projects  number;
      l_country_projects number;

   begin

      select count(1)
      into   l_region_projects
      from   ( -- Projects that were migrated into PLW
               select t.*,
                      t.gpms_id  gpmsid
               from   export_grd t
               where  1 = 1
                 and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  (-- EU
                       (upper(t.region)      = 'EU'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'       and
                         (t.development_site not in('BD', 'BD-E') or
                          t.sales_channel    like '%OTC%')))
                        or -- CA
                       (upper(t.region)      = 'CA'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'))
                        or -- US
                       (upper(t.region)      = 'US'                  and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'))
                        or -- IL
                        (upper(t.region)     = 'EMIA'                and
                         t.country           = 'IL'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         (t.development_site not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          t.development_site  not like '%BioG%'))
                        or -- JP
                        (upper(t.region)     = 'ASIA'                and
                         t.country           = 'JP'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         (t.development_site not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          t.development_site not like '%BioG%'))
                      )
                 --
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 --
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 and  t.gpms_id  is not null
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
               -- Projects created in PLW and
               -- WPs created in PLW for migrated projects
               union all
               select t.*,
                      null gpmsid
               from   export_grd t
               where  1 = 1
                 and  length(nvl(rtrim(ltrim(t.gpms_id)), '-99999')) = 6 -- filter out SEE projects
                 --
                 and  (-- EU
                       (upper(t.region)      = 'EU'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'       and
                         (t.development_site not in('BD', 'BD-E') or
                          t.sales_channel    like '%OTC%')))
                        or -- CA
                       (upper(t.region)      = 'CA'                  and
                        upper(t.wp_status)   <> 'DRAFT'              and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'))
                        or -- US
                       (upper(t.region)      = 'US'                  and
                        (t.development_site  not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                         t.development_site  not like '%BioG%'))
                        or -- IL
                        (upper(t.region)     = 'EMIA'                and
                         t.country           = 'IL'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         (t.development_site not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          t.development_site  not like '%BioG%'))
                        or -- JP
                        (upper(t.region)     = 'ASIA'                and
                         t.country           = 'JP'                  and
                         upper(t.wp_status)  <> 'DRAFT'              and
                         (t.development_site not in('BDIR',
                                                    'BD-L',
                                                    'BN',
                                                    'RDI')           and
                          t.development_site not like '%BioG%'))
                      )
                 --
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 --
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 and  (-- projects created in plw
                       (t.gpms_id is null) or
                       (-- WPs created in plw for migrated projects
                        t.gpms_id is not null and
                        not exists (select 1
                                    from   export_grd_go_live gl
                                    where  gl.gpms_id        = t.gpms_id
                                      and  gl.plw_pjt_id     = t.plw_pjt_id
                                      and  gl.plw_int_number = t.plw_int_number)
                       )
                      )
             ) q
      ;


      select count(1)
      into   l_country_projects
      from   (
               -- Projects that were migrated into PLW
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is not null
                 and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                   not in ('JP', 'IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))       = 6 -- filter out SEE projects
                 --
                 and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 -- migrated projects only
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)

               -- WPs created in PLW for migrated into PLW projects
               union all
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is not null
                 and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                   not in ('JP', 'IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                                 '-99999'))       = 6 -- filter out SEE projects
                 --
                 and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 -- to filter out WPs created in PLW for Migrated Projects
                 and  not exists (select 1
                                  from   export_grd_go_live gl
                                  where  gl.gpms_id        = t.gpms_id
                                    and  gl.plw_pjt_id     = t.plw_pjt_id
                                    and  gl.plw_int_number = t.plw_int_number)
                 --
                 /*and  exists (select 1
                              from   export_grd_backup)*/
               -- Projects that were created in PLW
               union all
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
                      --t.project_rationale,
                      --t.npv,
                      decode(upper(t.region), 'ASIA', 'INT',
                                              'EMIA', 'INT',
                                              t.region) region,
                      t.development_site
               from   export_grd t
               where  t.gpms_id                   is null
                 and  upper(t.region)             in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                   not in ('JP','IL')
                 and  upper(t.wp_status)          <> 'DRAFT'
                 and  t.activity_type             <> 'R' || chr(38) || 'D'
                 --
                 and  t.development_site          not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site          not like '%BioG%'
                 /*and  (upper(t.allocation_status) = 'APPROVED'  or
                       (t.project_status          = 'Draft' and
                        nvl(t.global_nte, '0')    = '1'))*/
                 and  nvl(t.generic_segment, '1') not like '%Authorized Generic%'
             ) q
      ;

      x_plw_projects := l_region_projects + l_country_projects;

      select sum(q.a)
      into   x_plw_tasks
      from   (
               select count(t.plw_int_number) a
               from   export_grd t
               where  t.gpms_id                         is not null
                 and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)                <> 'DRAFT'
                 and  t.activity_type                   <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                              '-99999'))                = 6 -- filter out SEE projects
                 --
                 and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                       (t.project_status                = 'Draft' and
                        nvl(t.global_nte, '0')          = '1'))*/
                 and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 -- migrated projects only
                 and  exists (select 1
                              from   export_grd_go_live gl
                              where  gl.gpms_id        = t.gpms_id
                                and  gl.plw_pjt_id     = t.plw_pjt_id
                                and  gl.plw_int_number = t.plw_int_number)
               -- WPs created in PLW for migrated into PLW projects
               union all
               select count(t.plw_int_number)
               from   export_grd t
               where  t.gpms_id                         is not null
                 and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)                <> 'DRAFT'
                 and  t.activity_type                   <> 'R' || chr(38) || 'D'
                 and  length(nvl(rtrim(ltrim(t.gpms_id)),
                              '-99999'))                = 6 -- filter out SEE projects
                 --
                 and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                       (t.project_status                = 'Draft' and
                        nvl(t.global_nte, '0')          = '1'))*/
                 and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
                 -- filter out blocked projects
                 and  not exists (select 1
                                  from   psnext_unique_projects s
                                  where  s.gpms_id = t.gpms_id
                                    and  s.country = t.country)
                 and  not exists (select 1
                                  from   export_grd_go_live gl
                                  where  gl.gpms_id        = t.gpms_id
                                    and  gl.plw_pjt_id     = t.plw_pjt_id
                                    and  gl.plw_int_number = t.plw_int_number)
               -- Tasks that were created in PLW for non migrated projects
               union
               select count(t.plw_int_number)
               from   export_grd t
               where  t.gpms_id                         is null
                 and  upper(t.region)                   in('EMIA', 'ASIA', 'LATAM')
                 and  t.country                         not in ('JP','IL')
                 and  upper(t.wp_status)                <> 'DRAFT'
                 and  t.activity_type                   <> 'R' || chr(38) || 'D'
                 --
                 and  t.development_site                not in('BDIR', 'BD-L', 'BN', 'RDI')
                 and  t.development_site                not like '%BioG%'
                 /*and  (upper(t.allocation_status)       = 'APPROVED'  or
                       (t.project_status                = 'Draft' and
                        nvl(t.global_nte, '0')          = '1'))*/
                 and  nvl(t.generic_segment, '1')       not like '%Authorized Generic%'
             )q
      ;


   end count_projects_tasks;


   ------------------------------------------------
   -- generate_gpms_exp_id
   ------------------------------------------------
   function generate_gpms_exp_id(l_start_time varchar2) return number is


      l_gpms_exp_id number;

   begin

      -- generate new gpms_exp_id and insert it into ps_gpms_exp
      insert into ps_gpms_exp(gpms_exp_id,
                              exp_date)
      values(ps_env_field_sq.nextval, -- plw_ps_gpms_exp_seq.nextval,
             to_date(l_start_time, 'dd/mm/rrrr hh24:mi:ss'))
      ;

      -- validate gpms_exp_id is inserted into ps_gpms_exp
      begin

         select t.gpms_exp_id
         into   l_gpms_exp_id
         from   ps_gpms_exp t
         where  t.exp_date = to_date(l_start_time, 'dd/mm/rrrr hh24:mi:ss');

      exception
         when no_data_found then
            g_errbuf   := sqlerrm;
            g_sql_code := sqlcode;
            raise e_gpms_exp_id_not_found;

         when too_many_rows then
            g_errbuf   := sqlerrm;
            g_sql_code := sqlcode;
            raise e_gpms_exp_id_multiple_rows;

      end;

   return l_gpms_exp_id;

   exception
      when others then
         g_errbuf   := sqlerrm;
         g_sql_code := sqlcode;
         raise e_generate_gpms_exp_id;

   end generate_gpms_exp_id;


   ------------------------------------------------------------------------------------
   -- create_export_grd_pure_tbl
   ------------------------------------------------------------------------------------
   function create_export_grd_pure_tbl return number is

      l_deleted_records      number;

      l_drop_tbl_stmnt       varchar2(50);
      l_create_tbl_stmnt     varchar2(200);

      cursor c_export_grd_pure is
         select t.object_name
         from   all_objects t
         where  t.object_type                = 'TABLE'
           and  t.object_name                like 'EXPORT_GRD_PURE_%'
           -- a month back
           and  to_date(substr(t.object_name, 17, 8), 'ddmmrrrr') < (trunc(sysdate) - 31)
         ;

   begin

      begin

         -- create daily backup table of export_grd before removal of
         -- projects for country 'FSA' or for project_rationale that includes '(AFS)'
         l_create_tbl_stmnt := 'create table export_grd_pure_' || to_char(trunc(sysdate), 'ddmmrrrr') ||
                              ' as select * from export_grd';

         begin
            execute immediate l_create_tbl_stmnt;

         exception
            when others then
               null;
         end;

         -- drop all export_grd_pure backup tables older than month
         for r_export_grd_pure in c_export_grd_pure loop

            l_drop_tbl_stmnt := 'drop table ' || r_export_grd_pure.object_name;

            begin
               execute immediate l_drop_tbl_stmnt;
            exception
               when others then
                  null;
            end;

         end loop;

         -- remove projects for country 'FSA' or for project_rationale that includes '(AFS)'
         delete from export_grd t
         where  t.country           = 'FSA'
            or  t.project_rationale like '%(AFS)%'
         ;

         l_deleted_records := sql%rowcount;
         commit;

      exception
         when others then
            rollback;
            g_errbuf   := sqlerrm;
            g_sql_code := sqlcode;
            raise e_create_export_grd_pure_tbl;

      end;

      return l_deleted_records;

   end create_export_grd_pure_tbl;

   ------------------------------------------------------------------------------------
   -- FETCH_MIGRATION_LOG_FILE - creates delta projects / tasks in ps_project / ps_task
   ------------------------------------------------------------------------------------
   procedure FETCH_MIGRATION_LOG_FILE(errbuf   in  out varchar2,
                                      retcode  in  out varchar2,
                                      p_site   in      varchar2) is

      l_start_time     varchar2(20) := to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss');
      l_from_env_id    varchar2(6)  := 'GPMS';
      l_plw_projects   number       := 0;
      l_plw_tasks      number       := 0;

      l_gpms_exp_id    number;

   begin

      if(upper(nvl(p_site, '-999')) not in('GRD-IL', 'GRD-US')) then

         errbuf    := 'Completed Unsuccessfully - The parameter site is manadatory and must be populated by the one of the following values: GRD-IL or GRD-US';
         retcode   := C_ERROR;

         dbms_output.put_line('The parameter site is manadatory and must be populated by the one of the following values: GRD-IL or GRD-US');

      else

         errbuf    := 'Completed Successfully ';
         retcode   := C_SUCCESS;

         begin

            /*select max(t.gpms_exp_id)
            into   l_gpms_exp_id
            from   ps_gpms_exp t
            where  t.exp_date = to_date(l_start_time)
            ;*/

            -- fetch gpms_exp_id for last inserted data in ps_project, ps_task
            select distinct
                   t.gpms_exp_id
            into   l_gpms_exp_id
            from   ps_project t
            ;

         exception
            when no_data_found then
               g_errbuf   := sqlerrm;
               g_sql_code := sqlcode;
               raise e_gpms_exp_id_not_found2;

            when too_many_rows then
               g_errbuf   := sqlerrm;
               g_sql_code := sqlcode;
               raise e_gpms_exp_id_multiple_rows2;

         end;

         if(upper(p_site) = 'GRD-IL') then

            select count(*)
            into   l_plw_projects
            from   ps_project t
            where  t.action             is not null
              and  ((t.region           in('EU', 'IL', 'JP') and
                     t.development_site in('BD', 'NA'))      or
                    -- for country projects (INT, LATAM) region is not transfered
                    (nvl(t.region, '-999')  not in('EU', 'IL', 'JP', 'US', 'CA') and
                     t.development_site in('BD', 'NA'))                      or
                    (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                           'ZR','ZG','BN','KR','UL','GO','ML',
                                           'MU','TYK','UL-CoDev','RM','NAG')))
              and  t.action <> 'E'
            ;

            select count(*)
            into   l_plw_tasks
            from   ps_task t
            where  t.action             is not null
              and  ((t.region           in('EU', 'IL', 'JP', 'INT', 'LATAM') and
                     t.development_site in('BD', 'NA'))                      or
                    (t.development_site in('DB','HL','GD','RN','WF','IL','SA',
                                           'ZR','ZG','BN','KR','UL','GO','ML',
                                           'MU','TYK','UL-CoDev','RM','NAG')))
              and  t.action <> 'E'
            ;

            -- create log for update fields
            /*report_updated_records(l_gpms_exp_id,
                                   upper(p_site),
                                   l_start_time);*/

            report_updated_projects(l_gpms_exp_id,
                                    upper(p_site),
                                    l_start_time);

            report_updated_tasks(l_gpms_exp_id,
                                 upper(p_site),
                                 l_start_time);

            print_log(l_start_time,
                      l_plw_projects,
                      l_plw_tasks,
                      null,
                      null,
                      null,
                      'PLW', -- l_from_env_id,
                      upper(p_site),
                      l_gpms_exp_id)
                      ;

         elsif(upper(p_site) = 'GRD-US') then

            select count(*)
            into   l_plw_projects
            from   ps_project t
            where  t.action   is not null
              and  ((t.region           in('US', 'CA')  and
                     t.development_site in('BD', 'NA')) or
                    (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                           'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                           'PO','AR','CL','VE','PE','KW','NVD')))
              and  t.action <> 'E'
            ;

            select count(*)
            into   l_plw_tasks
            from   ps_task t
            where  t.action             is not null
              and  ((t.region           in('US', 'CA')  and
                     t.development_site in('BD', 'NA')) or
                    (t.development_site in('SV','NV','TO','IR','OP','MC','MB',
                                           'MI', 'BDPO', 'BDJP','BDAM','BDCA', 'BP','BDIR',
                                           'PO','AR','CL','VE','PE','KW','NVD')))
              and  t.action <> 'E'
            ;

            -- create log for update fileds
            /*report_updated_records(l_gpms_exp_id,
                                   upper(p_site),
                                   l_start_time);*/

            report_updated_projects(l_gpms_exp_id,
                                    upper(p_site),
                                    l_start_time);

            report_updated_tasks(l_gpms_exp_id,
                                 upper(p_site),
                                 l_start_time);

            print_log(l_start_time,
                      l_plw_projects,
                      l_plw_tasks,
                      null,
                      null,
                      null,
                      'PLW', -- l_from_env_id,
                      upper(p_site),
                      l_gpms_exp_id)
                      ;

         end if;

      end if;

   exception

      when e_print_log then
         dbms_output.put_line('ERROR in PRINT_LOG procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in PRINT_LOG procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      when e_report_updated_records then
         dbms_output.put_line('ERROR in FETCH_MIGRATION_LOG_FILE procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in FETCH_MIGRATION_LOG_FILE procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      /*when others then
         dbms_output.put_line('ERROR in REPORT_UPDATED_RECORDS procedure - ' || sqlcode || ':' || sqlerrm);
         errbuf  := 'ERROR in REPORT_UPDATED_RECORDS procedure - ' || sqlcode || ':' || sqlerrm;
         retcode := C_ERROR;*/

   end FETCH_MIGRATION_LOG_FILE;

   ----------------------------------------------------------------------------------------------
   -- CREATE_DELTA_PROJECTS_TASKS - creates log file of migrated projects / tasks in java program
   ----------------------------------------------------------------------------------------------
   procedure CREATE_DELTA_PROJECTS_TASKS(errbuf   in  out varchar2,
                                         retcode  in  out varchar2) is

      l_deleted_project_rows     number := 0;
      l_deleted_task_rows        number := 0;

      l_gpms_exp_id              number;
      l_from_env_id              varchar2(6) := 'GPMS';
      l_to_env_id                varchar2(6) := 'GRD';
      l_site                     varchar2(6) := 'ALL';

      l_start_time               varchar2(20) := to_char(sysdate, 'dd/mm/rrrr hh24:mi:ss');

      l_plw_projects             number := 0;
      l_plw_tasks                number := 0;
      l_null_val_projects        number := 0;
      l_null_val_tasks           number := 0;
      l_null_projects_message    clob;

      l_failed_projects          number := 0;
      l_failed_tasks             number := 0;

      l_java_updated             varchar2(1);

      l_deleted_records          number;

   begin

      errbuf    := 'Completed Successfully ';
      retcode   := C_SUCCESS;

      -- count deleted records from export_grd table (country = 'FSA' or project_rationale like '%(AFS)%'
      l_deleted_records := create_export_grd_pure_tbl;

      l_gpms_exp_id := generate_gpms_exp_id(l_start_time);

      count_projects_tasks(l_plw_projects,
                           l_plw_tasks);

      -- if no projects exist in export_grd - report a warning and exit the program
      if(l_plw_projects = 0) then

         g_errbuf           := 'WARNING: No Projects exist in the table EXPORT_GRD.';
         g_retcode          := C_WARNING;
         g_interface_status := 'WARNING';

         raise e_export_grd_empty;

      end if;

      validate_null_values(l_null_val_projects,
                           l_null_val_tasks,
                           l_null_projects_message);

      if((l_null_val_projects  + l_null_val_tasks ) > 0) then

         g_errbuf           := 'WARNING in VALIDATE_NULL_VALUES function - there are blank fields';
         g_retcode          := C_WARNING;
         g_interface_status := 'WARNING';

         /*print_log(l_start_time,
                   l_commited_projects_err,
                   l_plw_projects,
                   l_commited_tasks_err);*/

         --raise e_validate_null_values;

      end if;

      update_export_grd_backup_actin(l_java_updated);
      commit;

      -- delete from ps_project / ps_task before insert
      begin

         delete from ps_task;
         l_deleted_task_rows    := sql%rowcount;
         commit;

         delete from ps_project;
         l_deleted_project_rows := sql%rowcount;
         commit;

         --dbms_output.put_line('Deleted projects from PS_PROJECT: ' || l_deleted_project_rows);
         --dbms_output.put_line('Deleted projects from PS_TASK: '    || l_deleted_task_rows);

      exception
         when others then
            rollback;
            g_errbuf   := sqlerrm;
            g_sql_code := sqlcode;
            raise e_delete_ps_tables;

      end;

      -- populate tables: ps_project, ps_tasks
      create_grd_projects_tasks(l_gpms_exp_id,
                                l_failed_projects, -- out
                                l_failed_tasks);   -- out
      commit;

      l_failed_projects := l_failed_projects + l_null_val_projects;
      l_failed_tasks    := l_failed_tasks    + l_null_val_tasks;

      create_backup_projects_tasks(l_gpms_exp_id);
      commit;

      -- before start the delta we need to update some fields in the gpms level (for pram & mop)
      -- diff_all(); -- this procedure is rewritten here
      ps_manipulation;

      -- delete data created in comparison data process from all tables: ps_project, ps_task, ps_diff_err
      delete_diff_data(l_gpms_exp_id);

      diff_level('PROJECT',
                 l_gpms_exp_id,
                 l_from_env_id,
                 l_to_env_id);

      diff_level('TASK',
                 l_gpms_exp_id,
                 l_from_env_id,
                 l_to_env_id);

      -- Update Levels After Initial Status
      UpdateLevelStatus(l_to_env_id);

      -- create projects / tasks that were failed in the previous migration
      create_failed_projects_tasks(l_java_updated);
      commit;

      -- update action of the project, which tasks were updated / deleted
      update_country_projects_status;

      -- populate export_grd_backup table by data from export_grd
      update_export_grd_backup_tbl;
      commit;

      -- create log for update fileds
      /*report_updated_records(l_gpms_exp_id,
                             l_site,
                             l_start_time);*/

      print_log(l_start_time,
                l_plw_projects,
                l_plw_tasks,
                l_failed_projects,
                l_failed_tasks,
                l_null_projects_message,
                'PLW',  -- l_from_env_id,
                l_site, -- ALL
                l_gpms_exp_id)
                ;

      errbuf  := g_errbuf;
      retcode := g_retcode;

   exception

      when e_export_grd_empty then
         dbms_output.put_line(g_errbuf);
         errbuf  := g_errbuf;
         retcode := g_retcode;

      when e_create_export_grd_pure_tbl then
         dbms_output.put_line('ERROR in CREATE_EXPORT_GRD_PURE_TBL procedure - ' || g_errbuf);
         errbuf  := 'ERROR in CREATE_EXPORT_GRD_PURE_TBL procedure - ' || g_errbuf;
         retcode := C_ERROR;

      when e_print_log then
         dbms_output.put_line('ERROR in PRINT_LOG procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in PRINT_LOG procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      when e_report_updated_records then
         dbms_output.put_line('ERROR in REPORT_UPDATED_RECORDS procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in REPORT_UPDATED_RECORDS procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      when e_count_migrated_projcts_tasks then
         dbms_output.put_line('ERROR in COUNT_MIGRATED_PROJECTS_TASKS procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in COUNT_MIGRATED_PROJECTS_TASKS procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      when e_count_delta_projects_tasks then
         dbms_output.put_line('ERROR in COUNT_DELTA_PROJECTS_TASKS procedure - ' || g_retcode || ':' || g_errbuf);
         errbuf  := 'ERROR in COUNT_DELTA_PROJECTS_TASKS procedure - ' || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      /*when e_wrong_interface_site_code then

         dbms_output.put_line(g_errbuf);
         errbuf  := g_errbuf;
         retcode := C_ERROR;*/


      when e_validate_null_values then
         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                        'ERROR in MAIN procedure, delete from ps tables failed - ' || g_errbuf);*/
         errbuf  := g_errbuf;
         retcode := C_ERROR;

      when e_delete_ps_tables then
         dbms_output.put_line('ERROR in MAIN procedure, delete from ps tables failed - ' || g_errbuf);

         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                        'ERROR in MAIN procedure, delete from ps tables failed - ' || g_errbuf);*/

         errbuf  := 'ERROR in MAIN procedure, delete from ps tables failed - ' || g_errbuf;
         retcode := C_ERROR;

      when e_create_grd_projects_tasks then
         dbms_output.put_line('ERROR in CREATE_GRD_PROJECTS_TASKS - ' || g_errbuf);

         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                        'ERROR in create_grd_projects_tasks - ' || g_errbuf);*/

         errbuf  := 'ERROR in CREATE_GRD_PROJECTS_TASKS - ' || g_errbuf;
         retcode := C_ERROR;

      when e_create_backup_projects_tasks then
         dbms_output.put_line('ERROR in CREATE_BACKUP_PROJECTS_TASKS - ' || g_errbuf);

         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in create_backup_projects_tasks - ' || g_errbuf);*/

         errbuf  := 'ERROR in CREATE_BACKUP_PROJECTS_TASKS - ' || g_errbuf;
         retcode := C_ERROR;

      when e_ps_manipulation then
         dbms_output.put_line('ERROR in PS_MANIPULATION ' || g_errbuf);

         write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in PS_MANIPULATION ' || g_errbuf);

         errbuf  := 'ERROR in PS_MANIPULATION ' || g_errbuf;
         retcode := C_ERROR;

      when e_delete_diff_data then
         dbms_output.put_line('ERROR in DELETE_DIFF_DATA ' || g_errbuf);

         write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in DELETE_DIFF_DATA ' || g_errbuf);

         errbuf  := 'ERROR in DELETE_DIFF_DATA ' || g_errbuf;
         retcode := C_ERROR;

      when e_diff_level then
         dbms_output.put_line('ERROR in DIFF_LEVEL '  || g_retcode || ':' || g_errbuf);

         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in DIFF_LEVEL ' || g_errbuf);*/

         errbuf  := 'ERROR in DIFF_LEVEL '  || g_retcode || ':' || g_errbuf;
         retcode := C_ERROR;

      when e_get_env_activity_id then
         dbms_output.put_line('ERROR in get_env_activity_id - ' || g_errbuf);
         errbuf  := 'ERROR in get_env_activity_id - ' || g_errbuf;
         retcode := C_ERROR;

      when e_write_diff_err then
         dbms_output.put_line('ERROR in write_diff_err - ' || g_errbuf);
         errbuf  := 'ERROR in write_diff_err - ' || g_errbuf;
         retcode := C_ERROR;

      when e_UpdateLevelStatus then
         dbms_output.put_line('ERROR in UpdateLevelStatu ' || g_errbuf);

         write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in UpdateLevelStatu ' || g_errbuf);

         errbuf  := 'ERROR in UpdateLevelStatu ' || g_errbuf;
         retcode := C_ERROR;

      when e_create_failed_projects_tasks then
         dbms_output.put_line('ERROR in CREATE_FAILED_PROJECTS_TASKS ' || g_errbuf);

         /*write_diff_err(l_gpms_exp_id,
                        l_to_env_id,
                        10,
                        null,
                        null,
                        null,
                        null,
                        g_sql_code,
                       'ERROR in CREATE_FAILED_PROJECTS_TASKS ' || g_errbuf);*/

         errbuf  := 'ERROR in CREATE_FAILED_PROJECTS_TASKS ' || g_errbuf;
         retcode := C_ERROR;

      when e_update_country_projct_status then
         dbms_output.put_line('ERROR in UPDATE_COUNTRY_PROJECTS_STATUS - ' || g_errbuf);

         errbuf  := 'ERROR in UPDATE_COUNTRY_PROJECTS_STATUS - ' || g_errbuf;
         retcode := C_ERROR;

      when e_update_export_grd_backup_tbl then
         dbms_output.put_line('ERROR in UPDATE_EXPORT_GRD_BACKUP_TBL - ' || g_errbuf);

         errbuf  := 'ERROR in UPDATE_EXPORT_GRD_BACKUP_TBL - ' || g_errbuf;
         retcode := C_ERROR;

      when e_gpms_exp_id_not_found then
         dbms_output.put_line('ERROR in GENERATE_GPMS_EXP_ID. No GPMS_EXP_ID exists for the data_time in the table PS_GPMS_EXP - ' || g_errbuf);

         errbuf  := 'ERROR in GENERATE_GPMS_EXP_ID. No GPMS_EXP_ID exists for the data_time in the table PS_GPMS_EXP - ' || g_errbuf;
         retcode := G_RETCODE;

      when e_gpms_exp_id_multiple_rows then
         dbms_output.put_line('ERROR in GENERATE_GPMS_EXP_ID. Multiple GPMS_EXP_ID values exist for the data_time in the table PS_GPMS_EXP - ' || g_errbuf);

         errbuf  := 'ERROR in GENERATE_GPMS_EXP_ID. Multiple GPMS_EXP_ID values exist for the data_time in the table PS_GPMS_EXP - ' || g_errbuf;
         retcode := G_RETCODE;

      when e_gpms_exp_id_not_found2 then
         dbms_output.put_line('ERROR in FETCH_MIGRATION_LOG_FILE. No GPMS_EXP_ID exists in the table PS_PROJECT - ' || g_errbuf);

         errbuf  := 'ERROR in FETCH_MIGRATION_LOG_FILE. No GPMS_EXP_ID exists in the table PS_PROJECT - ' || g_errbuf;
         retcode := G_RETCODE;

      when e_gpms_exp_id_multiple_rows2 then
         dbms_output.put_line('ERROR in GENERATE_GPMS_EXP_ID. Multiple GPMS_EXP_ID values exist in the table PS_PROJECT - ' || g_errbuf);

         errbuf  := 'ERROR in GENERATE_GPMS_EXP_ID. Multiple GPMS_EXP_ID values exist in the table PS_PROJECT - ' || g_errbuf;
         retcode := G_RETCODE;

      when e_generate_gpms_exp_id then
         dbms_output.put_line('ERROR in GENERATE_GPMS_EXP_ID - ' || g_errbuf);

         errbuf  := 'ERROR in GENERATE_GPMS_EXP_ID - ' || g_errbuf;
         retcode := G_RETCODE;

      when e_fetch_migration_log_file then
         dbms_output.put_line('ERROR in FETCH_MIGRATION_LOG_FILE - ' || g_errbuf);

         errbuf  := 'ERROR in FETCH_MIGRATION_LOG_FILE - ' || g_errbuf;
         retcode := G_RETCODE;

      /*when others then
         dbms_output.put_line('ERROR in CREATE_DELTA_PROJECTS_TASKS - ' || sqlerrm);
         errbuf  := 'ERROR in CREATE_DELTA_PROJECTS_TASKS - ' || sqlerrm;
         retcode := C_ERROR;*/

   end CREATE_DELTA_PROJECTS_TASKS;

end plw_rnd_data_migration;
/
