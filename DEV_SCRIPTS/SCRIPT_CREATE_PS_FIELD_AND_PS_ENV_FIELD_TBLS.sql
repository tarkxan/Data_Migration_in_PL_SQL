/*$Id: SCRIPT_CREATE_PS_FIELD_AND_PS_ENV_FIELD_TBLS.sql,v 1.1 2013/04/09 14:29:04 olando Exp $*/
------------------------------
--  1. ps_field_1           --
------------------------------
create table ps_field_1 as
select * 
from   ps_field t
;

alter table ps_field_1 add field_id_new number;

select t.field_id_new,
       t.level_name,
       t.field_name,
       t.field_type,
       t.uk_part
from   ps_field_1 t-- for update
order by t.field_id_new
;

-- drop and re-create trigger --
create sequence PS_FIELD_SQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;


------------------------------
--  2. ps_env_field_1       --
------------------------------
create table ps_env_field_1 as
select * 
from   ps_env_field t
--order by t.field_id
;

alter table ps_env_field_1 add field_id_new number;

select t.env_field_id,
       t.env_id,
       t.field_id_new,
       t.env_field_name
from   ps_env_field_1 t
;

-- drop and re-create trigger --
create sequence PS_ENV_FIELD_SQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;
