create or replace trigger GRD_PROJECTS_TR
before insert /*$Id: CRE_GRD_PROJECTS_TRG.trg,v 1.1 2013/06/27 15:37:48 olando Exp $*/

   on INTERFACES.GRD_PROJECTS
   referencing new as new old as old
   for each row

declare

begin

   :new.DEVELOPMENTSS        := trunc(:new.DEVELOPMENTSS);
   :new.DEVELOPMENTF         := trunc(:new.DEVELOPMENTF);
   :new.DEVELOPMENTSF        := trunc(:new.DEVELOPMENTSF);
   :new.DEVELOPMENTSTARTAF   := trunc(:new.DEVELOPMENTSTARTAF);
   :new.DEVELOPMENTSTART_S   := trunc(:new.DEVELOPMENTSTART_S);
   :new.DEVELOPMENTSTART_M   := trunc(:new.DEVELOPMENTSTART_M);
   :new.PROVIDEPRODUCTS      := trunc(:new.PROVIDEPRODUCTS);
   :new.PROVIDEPRODUCTSFI    := trunc(:new.PROVIDEPRODUCTSFI);
   :new.PROVIDEPRODUCTSAS    := trunc(:new.PROVIDEPRODUCTSAS);
   :new.PROVIDEPRODUCTSAF    := trunc(:new.PROVIDEPRODUCTSAF);
   :new.PROVIDEPRODUCTSST    := trunc(:new.PROVIDEPRODUCTSST);
   :new.PROVIDEPRODUCTSMU    := trunc(:new.PROVIDEPRODUCTSMU);
   :new.PROVIDEPRIMARYPACK   := trunc(:new.PROVIDEPRIMARYPACK);
   :new.PROVIDEPRIMARYPA_1   := trunc(:new.PROVIDEPRIMARYPA_1);
   :new.PROVIDEPRIMARYPA_2   := trunc(:new.PROVIDEPRIMARYPA_2);
   :new.PROVIDEPRIMARYPA_3   := trunc(:new.PROVIDEPRIMARYPA_3);
   :new.PROVIDEPRIMARYPA_5   := trunc(:new.PROVIDEPRIMARYPA_5);
   :new.PROVIDEPRIMARYPA_4   := trunc(:new.PROVIDEPRIMARYPA_4);
   :new.PROVIDEPRODUCTFORC   := trunc(:new.PROVIDEPRODUCTFORC);
   :new.PROVIDEPRODUCTFO_1   := trunc(:new.PROVIDEPRODUCTFO_1);
   :new.PROVIDEPRODUCTFO_2   := trunc(:new.PROVIDEPRODUCTFO_2);
   :new.PROVIDEPRODUCTFO_3   := trunc(:new.PROVIDEPRODUCTFO_3);
   :new.PROVIDEPRODUCTFO_4   := trunc(:new.PROVIDEPRODUCTFO_4);
   :new.PROVIDEPRODUCTFO_5   := trunc(:new.PROVIDEPRODUCTFO_5);
   :new.SUBMISSIONBATCHMST   := trunc(:new.SUBMISSIONBATCHMST);
   :new.SUBMISSIONBATCHMFI   := trunc(:new.SUBMISSIONBATCHMFI);
   :new.SUBMISSIONBATCHM_3   := trunc(:new.SUBMISSIONBATCHM_3);
   :new.SUBMISSIONBATCHMAN   := trunc(:new.SUBMISSIONBATCHMAN);
   :new.SUBMISSIONBATCHM_1   := trunc(:new.SUBMISSIONBATCHM_1);
   :new.SUBMISSIONBATCHM_2   := trunc(:new.SUBMISSIONBATCHM_2);
   :new.STABILITYSTART       := trunc(:new.STABILITYSTART);
   :new.STABILITYFINISH      := trunc(:new.STABILITYFINISH);
   :new.STABILITYSTART_EXP   := trunc(:new.STABILITYSTART_EXP);
   :new.STABILITY_ACTUALFI   := trunc(:new.STABILITY_ACTUALFI);
   :new.STABILITY_STARTNOE   := trunc(:new.STABILITY_STARTNOE);
   :new.STABILITY_MUSTSTAR   := trunc(:new.STABILITY_MUSTSTAR);
   :new.BIOSTUDYSTART        := trunc(:new.BIOSTUDYSTART);
   :new.BIOSTUDYFINISH       := trunc(:new.BIOSTUDYFINISH);
   :new.BIOSTUDYSTART_EX_    := trunc(:new.BIOSTUDYSTART_EX_);
   :new.BIOSTUDYSTART_EX_A   := trunc(:new.BIOSTUDYSTART_EX_A);
   :new.BIOSTUDY_STARTNOEA   := trunc(:new.BIOSTUDY_STARTNOEA);
   :new.BIOSTUDY_MUSTSTART   := trunc(:new.BIOSTUDY_MUSTSTART);
   :new.SITETRANSFERSTART    := trunc(:new.SITETRANSFERSTART);
   :new.SITETRANSFERFINISH   := trunc(:new.SITETRANSFERFINISH);
   :new.SITETRANSFERCOMP_2   := trunc(:new.SITETRANSFERCOMP_2);
   :new.SITETRANSFERCOMP_1   := trunc(:new.SITETRANSFERCOMP_1);
   :new.SITETRANSFERCOMPLE   := trunc(:new.SITETRANSFERCOMPLE);
   :new.SITETRANSFERCOMP_3   := trunc(:new.SITETRANSFERCOMP_3);
   :new.FILEM3CMCSENT        := trunc(:new.FILEM3CMCSENT);
   :new.FILEM3CMCSENTFINI    := trunc(:new.FILEM3CMCSENTFINI);
   :new.FILE_M3_CMC_SENTTO   := trunc(:new.FILE_M3_CMC_SENTTO);
   :new.FILE_M3_CMC_SENT_1   := trunc(:new.FILE_M3_CMC_SENT_1);
   :new.FILE_M3_CMC_SENT_2   := trunc(:new.FILE_M3_CMC_SENT_2);
   :new.FILE_M3_CMC_SENT_3   := trunc(:new.FILE_M3_CMC_SENT_3);
   :new.SUBMISSION_TO_ASTA   := trunc(:new.SUBMISSION_TO_ASTA);
   :new.SUBMISSION_TO_AFIN   := trunc(:new.SUBMISSION_TO_AFIN);
   :new.SUBMISSION_TO_AAS    := trunc(:new.SUBMISSION_TO_AAS);
   :new.SUBMISSION_TO_AAF    := trunc(:new.SUBMISSION_TO_AAF);
   :new.SUBMISSION_TO_ASET   := trunc(:new.SUBMISSION_TO_ASET);
   :new.SUBMISSION_TO_AMSO   := trunc(:new.SUBMISSION_TO_AMSO);
   :new.APISUITABILITYSTAR   := trunc(:new.APISUITABILITYSTAR);
   :new.APISUITABILITYFINI   := trunc(:new.APISUITABILITYFINI);
   :new.APISUITABILITY_A_1   := trunc(:new.APISUITABILITY_A_1);
   :new.APISUITABILITY_ACT   := trunc(:new.APISUITABILITY_ACT);
   :new.APISUITABILITY_STA   := trunc(:new.APISUITABILITY_STA);
   :new.APISUITABILITY_MUS   := trunc(:new.APISUITABILITY_MUS);
   :new.MODULE1_5COMPLETED   := trunc(:new.MODULE1_5COMPLETED);
   :new.MODULE1_5COMPLET_1   := trunc(:new.MODULE1_5COMPLET_1);
   :new.MODULE1_5COMPLET_2   := trunc(:new.MODULE1_5COMPLET_2);
   :new.MODULE1_5COMPLET_3   := trunc(:new.MODULE1_5COMPLET_3);
   :new.MODULE1_5COMPLET_4   := trunc(:new.MODULE1_5COMPLET_4);
   :new.MODULE1_5COMPLET_5   := trunc(:new.MODULE1_5COMPLET_5);
   :new.INTERFACE_DATE       := trunc(:new.INTERFACE_DATE);
   
end;
/
