/*$Id: CRE_EXPORT_GRD_BACKUP_TBL.sql,v 1.7 2013/09/17 12:52:59 olando Exp $*/
create table EXPORT_GRD_BACKUP
(
  GPMS_ID                       VARCHAR2(200 CHAR),
  PLW_PJT_ID                    VARCHAR2(200 CHAR),
  PLW_INT_NUMBER                VARCHAR2(200 CHAR),
  GAIN                          VARCHAR2(200 CHAR),
  GLOBAL_DOSAGE_FORM            VARCHAR2(200 CHAR),
  DEVELOPMENT_SITE              VARCHAR2(200 CHAR),
  DEV_TYPE                      VARCHAR2(200 CHAR),
  LAUNCHING_SITE                VARCHAR2(200 CHAR),
  PROJECT_NAME                  VARCHAR2(200 CHAR),
  PROJECT_RATIONALE             VARCHAR2(200 CHAR),
  RATIONALE_COMMENTS            VARCHAR2(200 CHAR),
  PROJECT_TECHNOLOGY            VARCHAR2(200 CHAR),
  PROJECT_STATUS                VARCHAR2(200 CHAR),
  WP_STATUS                     VARCHAR2(200 CHAR),
  PROJECT_PROGRESS              VARCHAR2(200 CHAR),
  BUSINESS                      VARCHAR2(200 CHAR),
  REGION                        VARCHAR2(200 CHAR),
  COUNTRY                       VARCHAR2(200 CHAR),
  PROJECT_PRIORITY              VARCHAR2(200 CHAR),
  ALLOCATION_STATUS             VARCHAR2(200 CHAR),
  FTF                           VARCHAR2(200 CHAR),
  FTF_REASON                    VARCHAR2(200 CHAR),
  HANDLING_CATEGORY             VARCHAR2(200 CHAR),
  IL_PLMD                       VARCHAR2(200 CHAR),
  REASON_FOR_ALLOCATION         VARCHAR2(200 CHAR),
  INNOVATOR_COMPANY             VARCHAR2(200 CHAR),
  INNOVATOR_SHELF_LIFE          VARCHAR2(200 CHAR),
  IP_MANAGER                    VARCHAR2(200 CHAR),
  PLFD                          DATE,
  PLFD_REMARK                   VARCHAR2(200 CHAR),
  PLMD                          DATE,
  PLMD_ASAP                     VARCHAR2(200 CHAR),
  FILING_STRATEGY               VARCHAR2(200 CHAR),
  PRODUCT_TYPE                  VARCHAR2(200 CHAR),
  SALES_CHANNEL                 VARCHAR2(200 CHAR),
  GENERIC_SEGMENT               VARCHAR2(200 CHAR),
  THERAPEUTIC_CLASS             VARCHAR2(200 CHAR),
  SOURCE_PROJECT_ID             VARCHAR2(200 CHAR),
  TYPE_OF_PROJECT               VARCHAR2(200 CHAR),
  PIPELINE_MANAGER              VARCHAR2(200 CHAR),
  GO_NO_GO_PIVOTAL              NUMBER(1),
  GO_NO_GO_BIOSTUDY             NUMBER(1),
  GO_NO_GO_SUBMISSION           NUMBER(1),
  GO_NO_GO_LAUNCH               NUMBER(1),
  ACTIVITY_TYPE                 VARCHAR2(200 CHAR),
  NAME                          VARCHAR2(255 CHAR) not null,
  SUBMISSION_TO_AUTHORITIES     DATE,
  IMS_SALES                     VARCHAR2(200 CHAR),
  IMS_UNITS_                    VARCHAR2(200 CHAR),
  LCM                           NUMBER(1),
  NPV                           FLOAT,
  TLD                           DATE,
  NCE_DATE                      DATE,
  PRELIMINARY_ALLOCATION        NUMBER(1),
  APPROVAL_DATE                 DATE,
  ALLOCATION_COMMITTEE_DATE     DATE,  -- First_Allocation_Approval_Date -- added in FDS 53
  LAUNCH_DATE                   DATE,
  MARS_DATE                     DATE,
  STRENGTH_FILL_VOLUME          VARCHAR2(200),
  IMS_PERCENT_GROWTH            VARCHAR2(200 CHAR),
  STRENGTH_UOM                  VARCHAR2(200),
  FILL_VOLUME_UOM               VARCHAR2(200),
  INIT_PROJ_STRAT_APPRO         DATE,
  PRODUCT_RATIONALE             VARCHAR2(200),
  IMS_SALES_PERCENTAGE          VARCHAR2(200),
  IMS_UNITS_PERCENTAGE          VARCHAR2(200),
  SUBM_TO_AUTHORITIES_ACT_START DATE,
  GLOBAL_NTE                    VARCHAR2(255),
  AFW                           VARCHAR2(255),
  TAPI_AFW                      NUMBER(1),
  SUBMISSION_A_P                VARCHAR2(255 CHAR),
  APPROVAL_A_P                  VARCHAR2(255 CHAR),
  LAUNCH_A_P                    VARCHAR2(255 CHAR),
  VALUE_FOR_TEVA                FLOAT,
  PLFD_LATE                     DATE,
  PLMD_MAIN                     DATE,
  LMD_MAIN                      DATE,
  LMD_EARLY                     DATE,
  PLFD_ASAP                     VARCHAR2(200 CHAR),
  PLFD_LATE_ASAP                VARCHAR2(200 CHAR),
  PLMD_MAIN_ASAP                VARCHAR2(200 CHAR),
  LMD_MAIN_ASAP                 VARCHAR2(200 CHAR),
  LMD_EARLY_ASAP                VARCHAR2(200 CHAR),
  PRODUCT_STATUS                VARCHAR2(200),
  STATUS                        VARCHAR2(5),
  ACTION                        VARCHAR2(5),
  ERROR_MESSAGE                 VARCHAR2(3999)
)
alter table EXPORT_GRD_BACKUP
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
tablespace INTERFACES_DATA --RDPM_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 4M
    minextents 1
    maxextents unlimited
  );
create table EXPORT_GRD_BACKUP_BACKUP as 
select * from EXPORT_GRD_BACKUP
where  rownum < 1
/
