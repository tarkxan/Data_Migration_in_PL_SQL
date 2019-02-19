create or replace trigger GRD_SOURCING_TR
before insert /*$Id: CRE_GRD_SOURCING_TRG.sql,v 1.1 2013/07/01 10:24:47 olando Exp $*/

   on INTERFACES.GRD_SOURCING
   referencing new as new old as old
   for each row

declare

begin

   :new.APICOSTLASTMODIFY  := trunc(:new.APICOSTLASTMODIFY);
   
end;
