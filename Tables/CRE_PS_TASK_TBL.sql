/*$Id: CRE_PS_TASK_TBL.sql,v 1.8 2013/11/05 15:35:19 olando Exp $*/
create table PS_TASK
(
   -----------------------------------------------------
   -- PREVIOUSLY EXISTED                              --
   -----------------------------------------------------
   TASK_ID                         NUMBER       not null,
   PROJECT_ID                      NUMBER       not null,
   GPMS_ID                         VARCHAR(6)   not null,
   COUNTRY_ID                      VARCHAR2(50) not null,
   ACTION                          VARCHAR2(20),
   PLMD_DATE                       DATE,
   PLMD_ASAP                       VARCHAR2(20),
   INNOVATOR_BRAND_NAME            VARCHAR2(50),
   BRAND_SALES                     NUMBER(20,4),
   BRAND_SALES_UNITS               NUMBER(20,4),
   STRENGTHS                       VARCHAR2(200),
   DEVELOPMENT_SITE                VARCHAR2(20),
   HU_FLFD                         DATE,
   STATUS                          VARCHAR2(50),
   PLMD_TEXT                       VARCHAR2(20),
   PLMD_IL_DATE                    DATE,
   PLMD_IL_ASAP                    VARCHAR2(20),
   PLMD_CA_DATE                    DATE,
   PLMD_CA_ASAP                    VARCHAR2(20),
   PLMD_EARLYLATE                  VARCHAR2(200),
   SPC                             VARCHAR2(200),
   LMD                             DATE,
   TASK_PRIORITY                   VARCHAR2(200),
   SALES_CHANNEL                   VARCHAR2(200),
   COUNTRY_COMMENTS                VARCHAR2(300),
   MARS_DATE                       DATE,
   REGION                          VARCHAR2(100),
   GRD                             VARCHAR2(1),
   PRISMA                          VARCHAR2(1),
   MARKET_TYPE                     VARCHAR2(50),
   PLFD_DATE                       DATE,
   PLFD_ASAP                       VARCHAR2(200)
)
tablespace INTERFACES_DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )
/
alter table PS_TASK
add(
   -----------------------------------------------------
   -- ADDED FOLLOWING THE FDS                         --
   -----------------------------------------------------
   --TASK_ID                         NUMBER       NOT NULL,
   --PROJECT_ID                      NUMBER       NOT NULL,
   GPMS_EXP_ID                     NUMBER not null,
   ENV_ACTIVITY_ID                 NUMBER not null,
   PRJ_ID                          VARCHAR2(20)  NOT NULL,
   --
   --GPMS_ID                         VARCHAR2(20),
   PLW_PJT_ID                      VARCHAR2(20),
   PLW_INT_NUMBER                  VARCHAR2(20),
   FILL_VOLUME_UOM                 VARCHAR2(200), -- in prj or wp for latam ???
   STRENGTH_UOM                    VARCHAR2(200), -- STRENGTH
   STRENGTH_FILL_VOLUME            VARCHAR2(200), -- request by email - in prj or wp for latam ???
   --DEVELOPMENT_SITE                VARCHAR2(20),
   --TARGET_MARKET                   VARCHAR2(20),  -- does not exist in export_grd, is transfered from region
   COUNTRY                         VARCHAR2(200) not null, --COUNTRY_ID is used instead
   ACTIVITY_TYPE                   VARCHAR2(200),
   LAUNCHING_SITE                  VARCHAR2(50),
   PROJECT_NAME                    VARCHAR2(350),
   PROJECT_RATIONALE               VARCHAR2(200),
   RATIONALE_COMMENTS              VARCHAR2(200),
   PROJECT_TECHNOLOGY              VARCHAR2(200),
   PRODUCT_RATIONALE               VARCHAR2(200),
   PROJECT_STATUS                  VARCHAR2(200),
   WP_STATUS                       VARCHAR2(200),
   PROJECT_PROGRESS                VARCHAR2(200),
   BUSINESS                        VARCHAR2(200),
   IMS_SALES                       VARCHAR2(200),
   IMS_SALES_PERCENTAGE            VARCHAR2(200),
   IMS_UNITS_                      VARCHAR2(200),
   IMS_UNITS_PERCENTAGE            VARCHAR2(200),
   FIRST_ALLOCATION_APPROVAL_DATE  DATE,          -- does not exist in EXPORT_GRD table !!!
   --REGION                          VARCHAR2(100),  
   APPROVAL_DATE                   DATE,
   PROJECT_PRIORITY                VARCHAR2(20), 
   ALLOCATION_STATUS               VARCHAR2(200),
   FTF                             VARCHAR2(200),
   FTF_REASON                      VARCHAR2(200),
   HANDLING_CATEGORY               VARCHAR2(20),
   IL_PLMD                         VARCHAR2(200), -- project or wp level ???
   INIT_PROJ_STRAT_APPRO           DATE,
   INNOVATOR_COMPANY               VARCHAR2(200),
   INNOVATOR_SHELF_LIFE            VARCHAR2(100),
   IP_MANAGER                      VARCHAR2(200),
   LCM                             VARCHAR2(5),
   LAUNCH_DATE                     DATE,
   --MARS_DATE                       DATE,
   NCE_DATE                        DATE,
   PLFD                            DATE,
   PLFD_REMARK                     VARCHAR2(200),
   PLMD                            DATE,
   --PLMD_ASAP                       VARCHAR2(20), 
   FILING_STRATEGY                 VARCHAR2(100),
   PRODUCT_TYPE                    VARCHAR2(200),
   --SALES_CHANNEL                   VARCHAR2(200),
   GENERIC_SEGMENT                 VARCHAR2(20),
   SUBMISSION_TO_AUTHORTIES_FNISH  DATE,
   SUBMISSION_TO_AUTHRT_ACTL_STRT  DATE,
   THERAPEUTIC_CLASS               VARCHAR2(200),
   SOURCE_PROJECT_ID               VARCHAR2(50),
   TYPE_OF_PROJECT                 VARCHAR2(200),
   PIPELINE_MANAGER                VARCHAR2(200),
   GO_NO_GO_PIVOTAL                VARCHAR2(1),
   GO_NO_GO_BIOSTUDY               VARCHAR2(1),
   GO_NO_GO_SUBMISSION             VARCHAR2(1),
   GO_NO_GO_LAUNCH                 VARCHAR2(1),
   PRELIMINARY_ALLOCATION          VARCHAR2(1),
   REASON_FOR_ALLOCATION           VARCHAR2(200),
   DEV_TYPE                        VARCHAR2(200), -- in wp ???  
   NPV                             FLOAT,         -- number in FDS ???
   TLD                             DATE,
   --
   SUBMISSION_A_P                  VARCHAR2(255),
   APPROVAL_A_P                    VARCHAR2(255),
   LAUNCH_A_P                      VARCHAR2(255),
   VALUE_FOR_TEVA                  FLOAT,
   -- added 08/09/2013
   PLFD_LATE                       DATE,
   PLMD_MAIN                       DATE,
   LMD_MAIN                        DATE,
   LMD_EARLY                       DATE,
   -- added 15/09/2013
   PLFD_LATE_ASAP                  VARCHAR2(200),
   PLMD_MAIN_ASAP                  VARCHAR2(200),
   LMD_MAIN_ASAP                   VARCHAR2(200),
   LMD_EARLY_ASAP                  VARCHAR2(200),
   PRODUCT_STATUS                  VARCHAR2(200)
)
/
alter table PS_TASK
modify 
(
   COUNTRY_ID VARCHAR2(50) null, /*not null*/
   --COUNTRY    VARCHAR2(50) not null,
   GPMS_ID    VARCHAR(6)   null
)
/
-- Add comments to the columns 
comment on column PS_TASK.ACTION
  is 'I=INSERT U=UPDATE T=TERMINATE E= ERROR N=NONE';
-- 1. PK 
alter table PS_TASK
  add constraint PS_TASK_DEV_PK primary key (TASK_ID)
  using index 
  tablespace INTERFACES_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 1040K
    next 1M
    minextents 1
    maxextents unlimited
  )
/
-- 2. UK
alter table PS_TASK
  add constraint PS_TASK_DEV_UK unique (/*PROJECT_ID*/ PRJ_ID, /*COUNTRY_ID*/ COUNTRY, ENV_ACTIVITY_ID)
  using index 
  tablespace INTERFACES_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 1040K
    next 1M
    minextents 1
    maxextents unlimited
  )
/
-- 3. FK
alter table PS_TASK
  add constraint PS_TASK_PROJECT_FK foreign key(PROJECT_ID)
  references PS_PROJECT (PROJECT_ID)
/
-- 4. Create/Recreate indexes 
create index PS_TASK_IND1 on PS_TASK (/*GPMS_ID*/ PRJ_ID, /*COUNTRY_ID*/ COUNTRY, PROJECT_ID)
  tablespace INTERFACES_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  )
/
