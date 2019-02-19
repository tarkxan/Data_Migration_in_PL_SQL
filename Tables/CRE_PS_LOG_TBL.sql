/*$Id: CRE_PS_LOG_TBL.sql,v 1.4 2014/09/17 21:53:26 olando Exp $*/
create table ps_log
(
   gpms_exp_id   number,
   date_of_log   date,
   dev_site      varchar2(6),
   log_type      varchar2(7),
   report_level  varchar2(8),
   log_file      clob
)
/
