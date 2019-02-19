/*$Id: CRE_PS_PROJECT_1_TBL.sql,v 1.5 2013/04/30 12:24:41 olando Exp $*/
create table PS_PROJECT_1
(
   -----------------------------------------------------
   -- PREVIOUSLY EXISTED                              --
   -----------------------------------------------------
   PROJECT_ID                NUMBER not null,
   GPMS_EXP_ID               NUMBER not null,
   ENV_ACTIVITY_ID           NUMBER not null,
   GPMS_ID                   VARCHAR2(20), -- not null,
   ACTION                    VARCHAR2(10),
   PROJECT_NAME              VARCHAR2(500),
   GAIN                      VARCHAR2(200),
   GLOBAL_DOSAGE_FORM        VARCHAR2(200),
   STRENGTHS                 VARCHAR2(200),
   MEASURE_UNIT              VARCHAR2(20),
   EXTENSION                 VARCHAR2(200),
   GPMS_EXCEPTION            VARCHAR2(200),
   DEVELOPMENT_SITE          VARCHAR2(20),
   TARGET_MARKET             VARCHAR2(20),
   THERAPEUTIC_CLASS         VARCHAR2(200),
   INDICATIONS               VARCHAR2(500),
   BRAND_NAME                VARCHAR2(200),
   INNOVATOR_COMPANY         VARCHAR2(200),
   INNOVATOR_BRAND_NAME      VARCHAR2(200),
   BRAND_SALES               NUMBER(20,4),
   BRAND_SALES_UNITS         NUMBER(20,4),
   INNOVATOR_APPROVAL        DATE,
   FIRST_TO_FILE             VARCHAR2(100),
   MARKET_TYPE               VARCHAR2(20),
   PLFD_DATE                 DATE,
   PLMD_DATE                 DATE,
   FILING_STRATEGY           VARCHAR2(500), --VARCHAR2(100),
   PROJECT_PRIORITY          VARCHAR2(20),
   PERCENT_CHANGE_SALES      NUMBER(8,4),
   PERCENT_CHANGE_UNITS      NUMBER(8,4),
   EXCLUSIVITIES             VARCHAR2(500),
   TEN_YEAR_DATE             DATE,
   SIX_YEAR_DATE             DATE,
   DISTRIBUTOR               VARCHAR2(200),
   IL_REF_PRODUCT            VARCHAR2(100),
   INNOVATOR_SHELF_LIFE      VARCHAR2(100),
   STATUS                    VARCHAR2(50),
   FILL_VOLUMES              VARCHAR2(200),
   PLMD_ASAP                 VARCHAR2(20),
   PLFD_ASAP                 VARCHAR2(20),
   RESPIRATORY               VARCHAR2(5),
   INNOVATOR_COUNTRY         VARCHAR2(100),
   SALES_DATE                DATE,
   MFG_SITES                 VARCHAR2(50),
   MFG2_SITES                VARCHAR2(50),
   FIRST_MRTG_APPROVAL_DATE  DATE,
   PRODUCTION_SITE           VARCHAR2(20),
   PS8_IDS                   VARCHAR2(100),
   BIOLOGIC                  VARCHAR2(10),
   TOP30                     VARCHAR2(200),
   STAR                      VARCHAR2(100),
   HISTORY_APP_DATE          DATE,
   NO_SALES_DATA             VARCHAR2(10),
   TIME_SENSITIVE            VARCHAR2(20),
   TIME_SENSITIVE_REASON     VARCHAR2(200),
   DROP_DEAD_START_DATE      DATE,
   LATE                      VARCHAR2(20),
   LATE_REASON               VARCHAR2(200),
   MARKET_ACTIVATED          VARCHAR2(20),
   IS_GENERIC                VARCHAR2(20),
   BPEL_SITE_FLAG            VARCHAR2(1),
   ALMD                      DATE,
   ALMD_REMARK               VARCHAR2(200),
   JOINT_MARKET              VARCHAR2(50),
   JOINT_MARKET_OLD          VARCHAR2(50),
   MARS                      DATE,
   REFERENCE_PRODUCT         VARCHAR2(500),
   E_PLMD                    DATE,
   L_PLMD                    DATE,
   POTENTIAL_FILING_STRATEGY VARCHAR2(20),
   BU_PRIORITY               VARCHAR2(10),
   NCE                       DATE,
   GENERIC                   VARCHAR2(5),
   NOT_FTF_REASON            VARCHAR2(500),
   LCM                       VARCHAR2(5),
   PLFD_SIXYRDT              DATE,
   PLFD_TENYRDT              DATE,
   PLMD_CA                   DATE,
   PLMD_CA_ASAP              VARCHAR2(200),
   PLMD_IL                   DATE,
   PLMD_IL_ASAP              VARCHAR2(200),
   PLMD_MX                   DATE,
   PLMD_XM_ASAP              VARCHAR2(200),
   PLMD_EARLY_ASAP           VARCHAR2(200),
   PLMD_LATE_ASAP            VARCHAR2(200),
   PLMD_EARLY                DATE,
   PLMD_LATE                 DATE,
   INSURANCE                 VARCHAR2(200),
   PLFD_HU                   DATE,
   PLFD_HU_ASAP              VARCHAR2(200),
   PLFD_CH                   DATE,
   PLFD_CH_ASAP              VARCHAR2(200),
   PARTNER_WORK_TYPE         VARCHAR2(10),
   PARTNER_DEAL_TYPE         VARCHAR2(10),
   TRANSFER_TYPE             VARCHAR2(20),
   PLFD_8_YRS                VARCHAR2(3),
   OTC                       VARCHAR2(3),
   LCEURO                    NUMBER,
   HANDLING_CATEGORY         VARCHAR2(20),
   BUSINESS_PARTNER          VARCHAR2(10),
   MARS_COMMENT              VARCHAR2(250),
   PRODUCT_DOC_ID            VARCHAR2(32),
   SOURCE_PROJECT_ID         VARCHAR2(50),
   ALLOCATION_DECISION_DATE  DATE,
   PEDIATRIC_EXCLUSIVITY     VARCHAR2(50),
   PARTNER_DEAL_TYPE1        VARCHAR2(10),
   PARTNER_WORK_TYPE1        VARCHAR2(10),
   BUSINESS_PARTNER1         VARCHAR2(10),
   TEVA_FILE_APPROVAL_DATE   DATE,
   PLMD_ZR_ASAP              VARCHAR2(50),
   PLMD_ZR                   DATE,
   LSD                       DATE,
   JAPAN_JV                  VARCHAR2(10),
   JOINTSDEVSITES            VARCHAR2(100),
   MFG_REASON                VARCHAR2(100),
   TAPI                      VARCHAR2(100),
   BD_SIGNED_AGREEMENT       VARCHAR2(100),
   BD_BUSINESS_PARTNER       VARCHAR2(100),
   JOINT_COUNTRIES           VARCHAR2(500),
   MFG_COMMENTS              VARCHAR2(100),
   IP_OWNERSHIP              VARCHAR2(100),
   LIVE_LAUNCH_DATE          DATE,
   ATC4                      VARCHAR2(200),
   LSD_REMARKS               VARCHAR2(200),
   PLFD_JA                   DATE,
   PLFD_JA_ASAP              VARCHAR2(200),
   PLFD_IL                   DATE,
   PLFD_IL_ASAP              VARCHAR2(200),
   PLFD_BR                   DATE,
   PLFD_BR_ASAP              VARCHAR2(200),
   PLFD_MX                   DATE,
   PLFD_MX_ASAP              VARCHAR2(200),
   JAPAN_PROJECT_STRATEGY    VARCHAR2(200),
   LAUNCHING_SITE            VARCHAR2(500), -- VARCHAR2(50),
   SHARED_LAUNCHING          VARCHAR2(50),
   TRANSFERRING_SITE         VARCHAR2(50),
   RECEIVING_SITE            VARCHAR2(50),
   SITE_HISTORY              VARCHAR2(50),
   FRANCHISE                 VARCHAR2(50),
   CYTOTOXIC                 VARCHAR2(10),
   VAP_PROJECT               VARCHAR2(3),
   RND_REASON                VARCHAR2(250),
   RND_COMMENTS              VARCHAR2(250),
   GLOBAL_NTE                VARCHAR2(3),
   AFW                       VARCHAR2(240),
   -----------------------------------------------------
   -- ADDED FOLLOWING THE FDS                         --
   -----------------------------------------------------
   --PROJECT_ID                      NUMBER       NOT NULL,
   PRJ_ID                          VARCHAR2(20) NOT NULL, -- to ask Galit to add to FDS
   ------------
   --GPMS_EXP_ID                     NUMBER,
   --ENV_ACTIVITY_ID                 NUMBER,
   ------------
   --GPMS_ID                         VARCHAR2(20),  --
   PLW_PJT_ID                      VARCHAR2(20),  --
   PLW_INT_NUMBER                  VARCHAR2(20),  -- PLW WP_ID wp level
   --GAIN                            VARCHAR2(200), --
   --GLOBAL_DOSAGE_FORM              VARCHAR2(200), --
   FILL_VOLUME_UOM                 VARCHAR2(200), --
   STRENGTH_UOM                    VARCHAR2(200), -- STRENGTH
   STRENGTH_FILL_VOLUME            VARCHAR2(200), -- request by email
   --DEVELOPMENT_SITE                VARCHAR2(200), -- wp level
   COUNTRY                         VARCHAR2(200), -- wp level
   ACTIVITY_TYPE                   VARCHAR2(200), -- wp level
   --LAUNCHING_SITE                  VARCHAR2(200), --
   --PROJECT_NAME                    VARCHAR2(200), --
   PROJECT_RATIONALE               VARCHAR2(200), --
   RATIONALE_COMMENTS              VARCHAR2(200), --
   PROJECT_TECHNOLOGY              VARCHAR2(200), --
   PRODUCT_RATIONALE               VARCHAR2(200), --
   PROJECT_STATUS                  VARCHAR2(200), --
   WP_STATUS                       VARCHAR2(200), -- only wp
   PROJECT_PROGRESS                VARCHAR2(200), --
   BUSINESS                        VARCHAR2(200), -- R&D WP Level
   IMS_SALES                       VARCHAR2(200), --
   IMS_SALES_PERCENTAGE            VARCHAR2(200), --
   IMS_UNITS_                      VARCHAR2(200), -- 
   IMS_UNITS_PERCENTAGE            VARCHAR2(200), --
   FIRST_ALLOCATION_APPROVAL_DATE  DATE,          -- does not exist in EXPORT_GRD table !!!
   REGION                          VARCHAR2(200), --
   APPROVAL_DATE                   DATE,          -- 
   --PROJECT_PRIORITY                VARCHAR2(200), --
   ALLOCATION_STATUS               VARCHAR2(200), -- wp level
   FTF                             VARCHAR2(200), --
   FTF_REASON                      VARCHAR2(200), --
   --HANDLING_CATEGORY               VARCHAR2(200), --
   IL_PLMD                         VARCHAR2(200), --
   INIT_PROJ_STRAT_APPRO           DATE,          --
   --INNOVATOR_COMPANY               VARCHAR2(200), --
   --INNOVATOR_SHELF_LIFE            NUMBER,        -- Stan - VARCHAR2(200) ???
   IP_MANAGER                      VARCHAR2(200), --
   --LCM                             VARCHAR2(1),   -- NUMBER(1) in EXPORT_GRD
   LAUNCH_DATE                     DATE,          --
   MARS_DATE                       DATE,          -- 
   NCE_DATE                        DATE,          --
   PLFD                            DATE,          --
   PLFD_REMARK                     VARCHAR2(200), --
   PLMD                            DATE,          --
   --PLMD_ASAP                       VARCHAR2(200), --
   --FILING_STRATEGY                 VARCHAR2(200), --
   PRODUCT_TYPE                    VARCHAR2(200), --
   SALES_CHANNEL                   VARCHAR2(200), --
   GENERIC_SEGMENT                 VARCHAR2(200), --
   SUBMISSION_TO_AUTHORTIES_FNISH  DATE,          -- SUBMISSION_TO_AUTHORITIES     in EXPORT_GRD
   SUBMISSION_TO_AUTHRT_ACTL_STRT  DATE,          -- SUBM_TO_AUTHORITIES_ACT_START in EXPORT_GRD
   --THERAPEUTIC_CLASS               VARCHAR2(200), --
   --SOURCE_PROJECT_ID               VARCHAR2(200), --
   TYPE_OF_PROJECT                 VARCHAR2(200), --
   PIPELINE_MANAGER                VARCHAR2(200), -- wp level
   GO_NO_GO_PIVOTAL                NUMBER(1),     -- wp level
   GO_NO_GO_BIOSTUDY               NUMBER(1),     -- wp level
   GO_NO_GO_SUBMISSION             NUMBER(1),     -- wp level
   GO_NO_GO_LAUNCH                 NUMBER(1),     -- wp level
   PRELIMINARY_ALLOCATION          NUMBER(1),     -- wp level
   REASON_FOR_ALLOCATION           VARCHAR2(200), -- only wp ?
   DEV_TYPE                        VARCHAR2(200), -- in wp only???                         
   NPV                             FLOAT,         -- number in FDS ???
   TLD                             DATE
)
;
-- Add comments to the columns 
comment on column PS_PROJECT_1.ACTION
  is 'I=INSERT U=UPDATE T=TERMINATE E= ERROR N=NONE';
comment on column PS_PROJECT_1.TIME_SENSITIVE
  is 'Y/N/Blank';
comment on column PS_PROJECT_1.LATE
  is 'Y/N/Blank';
comment on column PS_PROJECT_1.MARKET_ACTIVATED
  is 'Y/N/Blank';
comment on column PS_PROJECT_1.IS_GENERIC
  is 'Y/N/Blank';
-- Create/Recreate primary, unique and foreign key constraints 
-- 1. PK
alter table PS_PROJECT_1
  add constraint PS_PROJECT_PK_1 primary key (PROJECT_ID)
  using index 
  tablespace INTERFACES_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 280K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- 2. UK
alter table PS_PROJECT_1
  add constraint PS_PROJECT_UK_1 unique (PRJ_ID/*GPMS_ID*/, GPMS_EXP_ID, ENV_ACTIVITY_ID)
  using index 
  tablespace INTERFACES_DATA
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 680K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- 3.1. FK 1
alter table PS_PROJECT_1
  add constraint PS_PROJECT_ENV_FK_1 foreign key (ENV_ACTIVITY_ID)
  references PS_ENV_ACTIVITY (ENV_ACTIVITY_ID);
-- 3.2. FK 2
alter table PS_PROJECT_1
  add constraint PS_PROJECT_EXP_FK_1 foreign key (GPMS_EXP_ID)
  references PS_GPMS_EXP (GPMS_EXP_ID);
