create or replace package plw_rnd_data_migration is
/*
 $Id: plw_rnd_data_migration.pks,v 1.2 2013/10/15 12:29:56 olando Exp $
 $Log: plw_rnd_data_migration.pks,v $
 Revision 1.2  2013/10/15 12:29:56  olando
 Ticket# -999:
 1. Procedure report_updated_records is removed from spec.
 2. Out parameter p_log is removed from the procedures FETCH_MIGRATION_LOG_FILE, CREATE_DELTA_PROJECTS_TASKS
     since log files are saved in the table ps_log to be fetched by BPEL.
 3. Appropriate change to save log files and not tp use out and global parameters for log file.

 Revision 1.1  2013/10/10 17:05:21  olando
 Ticket# -999:
 The pck file was splitted into pks (spec) and pkb (body) files.

*/
   -- creates log file of migrated projects / tasks in java program
   procedure FETCH_MIGRATION_LOG_FILE(errbuf     in  out varchar2,
                                      retcode    in  out varchar2,
                                      p_site     in      varchar2);
   
   -- creates delta projects / tasks in ps_project / ps_task
   procedure CREATE_DELTA_PROJECTS_TASKS(errbuf  in  out varchar2,
                                         retcode in  out varchar2);

end plw_rnd_data_migration;
/
