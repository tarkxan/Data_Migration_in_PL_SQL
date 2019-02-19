create or replace package PLW_CREATE_RAW_RND_DATA_TBLS is

   procedure Create_Raw_Tables(p_message out varchar2);

   procedure Drop_Raw_Tables(p_message out varchar2);

end PLW_CREATE_RAW_RND_DATA_TBLS;
/
create or replace package body PLW_CREATE_RAW_RND_DATA_TBLS is

/*
 $Id:  $
 $Log:  $
*/

   -------------------------------------------------------
   -- Create_Raw_Tables
   -------------------------------------------------------
   procedure Create_Raw_Tables(p_message out varchar2) is

      l_create_tbl_stmnt  varchar2(2000);

   begin

      l_create_tbl_stmnt := 'create table raw_notes_project as (select * from notes_project)';
      execute immediate l_create_tbl_stmnt;

      l_create_tbl_stmnt := 'create table raw_notes_task as (select * from notes_task)';
      execute immediate l_create_tbl_stmnt;

      commit;

   exception

      when others then

         p_message := sqlerrm;

   end Create_Raw_Tables;

   -------------------------------------------------------
   -- Drop_Raw_Tables
   -------------------------------------------------------
   procedure Drop_Raw_Tables(p_message out varchar2) is

      l_drop_tbl_stmnt  varchar2(2000);

   begin

      l_drop_tbl_stmnt := 'drop table raw_notes_project';
      execute immediate l_drop_tbl_stmnt;

      l_drop_tbl_stmnt := 'drop table raw_notes_task';
      execute immediate l_drop_tbl_stmnt;

      commit;

   exception

      when others then

         p_message := sqlerrm;

   end Drop_Raw_Tables;

end PLW_CREATE_RAW_RND_DATA_TBLS;
/
