/*$Id: CRE_GRD_SOURCING_TBL.sql,v 1.1 2013/07/01 10:03:25 olando Exp $*/
create table GRD_SOURCING
(
  ID                 NUMBER,
  GPMS_ID            VARCHAR2(100),
  PLWPROJECT_ID      VARCHAR2(100),
  PLWR_DW_P_ID       VARCHAR2(100),
  TARGETMARKET       VARCHAR2(100),
  DEV_SITE           VARCHAR2(100),
  API_NAME           VARCHAR2(100),
  CATALOGNO_PRIMARYF VARCHAR2(100),
  MANUFACTURERGLOBAL VARCHAR2(100),
  PURCHASEDQTYFORM_1 FLOAT,
  PURCHASEDQTYFORM_2 FLOAT,
  PURCHASEDQTYFORMMR FLOAT,
  SHORTSUPPLIERNAMEF VARCHAR2(100),
  PRICEFORPURCHASE_1 FLOAT,
  PRICEPERKGFOREAC_1 VARCHAR2(100),
  NAME               VARCHAR2(100),
  COMPANYGLOBALIDFOR VARCHAR2(100),
  SUPPLLIERNAMEFORPL VARCHAR2(100),
  GLOBALIDFORMULAFIE VARCHAR2(100),
  MANUFACTURERFACILI VARCHAR2(100),
  LASTUPDATEDATEBYTH VARCHAR2(100),
  LASTUPDATEBY       VARCHAR2(100),
  INTERFACE_STATUS   VARCHAR2(10),
  INTERFACE_MESSAGE  VARCHAR2(1000),
  INTERFACE_DATE     DATE,
  PLW_ID_APINAME     VARCHAR2(100),
  APICostLastModify  DATE
)
/