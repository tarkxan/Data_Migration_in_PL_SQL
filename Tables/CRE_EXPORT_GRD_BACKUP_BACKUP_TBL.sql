/*$Id: CRE_EXPORT_GRD_BACKUP_BACKUP_TBL.sql,v 1.4 2013/09/17 12:57:33 olando Exp $*/
create table EXPORT_GRD_BACKUP_BACKUP as
select * 
from   EXPORT_GRD_BACKUP 
where  romnum < 1
/
alter table EXPORT_GRD_BACKUP_BACKUP
/*drop
(
   STATUS,          
   ACTION,       
   ERROR_MESSAGE
)
/*/
add
(
   PRODUCT_STATUS                VARCHAR2(200),
   STATUS                        VARCHAR2(5),
   ACTION                        VARCHAR2(5),
   ERROR_MESSAGE                 VARCHAR2(3999)
)
/
