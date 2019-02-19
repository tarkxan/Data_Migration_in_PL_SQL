create table export_grd_failed
( /*$Id: CRE_EXPORT_GRD_FAILED_TBL.sql,v 1.2 2013/04/25 13:28:48 olando Exp $*/
   gpms_id  varchar2(6),
   region   varchar2(10),
   country  varchar2(3),
   status   varchar2(1),
   message  varchar2(2000)
)
/
select * from export_grd_failed for update
/
select q.* 
from   export_grd q
where  (q.gpms_id,
        q.country) in (
                       select t.gpms_id,
                              t.country
                       from   export_grd_failed t
                      )
/
delete from export_grd_1
/
insert into export_grd_1
select q.* 
from   export_grd q
where  (q.gpms_id,
        q.country) not in(
                          select t.gpms_id,
                                 t.country
                          from   export_grd_failed t
                         )
/

-- ('302404', '301504')
