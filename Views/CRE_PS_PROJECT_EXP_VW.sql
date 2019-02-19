create or replace view PS_PROJECT_EXP_VW as
select /*$Id: CRE_PS_PROJECT_EXP_VW.sql,v 1.7 2014/08/04 19:25:10 olando Exp $*/
       pe.env_id,
       pp.project_id,
       pp.gpms_exp_id,
       pp.env_activity_id,
       pp.gpms_id,
       pp.action,
       pp.project_name,
       pp.gain,
       pp.global_dosage_form,
       pp.strengths,
       pp.measure_unit,
       pp.extension,
       pp.gpms_exception,
       pp.development_site,
       pp.target_market,
       pp.therapeutic_class,
       pp.indications,
       pp.brand_name,
       pp.innovator_company,
       pp.innovator_brand_name,
       pp.brand_sales,
       pp.brand_sales_units,
       pp.innovator_approval,
       pp.first_to_file,
       pp.market_type,
       pp.plfd_date,
       pp.plmd_date,
       pp.filing_strategy,
       pp.project_priority,
       pp.percent_change_sales,
       pp.percent_change_units,
       pp.exclusivities,
       pp.ten_year_date,
       pp.six_year_date,
       pp.distributor,
       pp.il_ref_product,
       pp.innovator_shelf_life,
       pp.status,
       pp.fill_volumes,
       pp.plmd_asap,
       pp.plfd_asap,
       pp.respiratory,
       pp.innovator_country,
       pp.sales_date,
       pp.mfg_sites,
       pp.mfg2_sites,
       pp.first_mrtg_approval_date,
       pp.production_site,
       pp.ps8_ids,
       pp.biologic,
       pp.top30,
       pp.star,
       pp.history_app_date,
       pp.no_sales_data,
       pp.time_sensitive,
       pp.time_sensitive_reason,
       pp.drop_dead_start_date,
       pp.late,
       pp.late_reason,
       pp.market_activated,
       pp.is_generic,
       nvl(ps.terminated, 0) terminated,
       pp.almd,
       pp.almd_remark,
       pp.joint_market,
       pp.joint_market_old,
       pp.mars,
       pp.reference_product,
       pp.e_plmd,
       pp.l_plmd,
       pp.potential_filing_strategy,
       pp.bu_priority,
       pp.nce,
       pp.generic,
       pp.not_ftf_reason,
       pp.lcm,
       pp.plfd_sixyrdt,
       pp.plfd_tenyrdt,
       pp.plmd_ca,
       pp.plmd_ca_asap,
       pp.plmd_il,
       pp.plmd_il_asap,
       pp.plmd_mx,
       pp.plmd_xm_asap,
       pp.plmd_early_asap,
       pp.plmd_late_asap,
       pp.plmd_early,
       pp.plmd_late,
       pp.insurance,
       pp.plfd_hu,
       pp.plfd_hu_asap,
       pp.plfd_ch,
       pp.plfd_ch_asap,
       pp.partner_work_type,
       pp.partner_deal_type,
       pp.transfer_type,
       pp.plfd_8_yrs,
       pp.otc,
       pp.lceuro,
       pp.handling_category,
       pp.business_partner,
       pp.mars_comment,
       pp.product_doc_id,
       pp.source_project_id,
       pp.allocation_decision_date,
       pp.pediatric_exclusivity,
       pp.partner_deal_type1,
       pp.partner_work_type1,
       pp.business_partner1,
       pp.teva_file_approval_date,
       pp.plmd_zr_asap,
       pp.plmd_zr,
       pp.lsd,
       pp.japan_jv,
       pp.jointsdevsites,
       pp.mfg_reason,
       pp.tapi,
       pp.bd_signed_agreement,
       pp.bd_business_partner,
       pp.joint_countries,
       pp.mfg_comments,
       pp.ip_ownership,
       pp.live_launch_date,
       pp.atc4,
       pp.lsd_remarks,
       pp.plfd_ja,
       pp.plfd_ja_asap,
       pp.plfd_il,
       pp.plfd_il_asap,
       pp.plfd_br,
       pp.plfd_br_asap,
       pp.plfd_mx,
       pp.plfd_mx_asap,
       pp.japan_project_strategy,
       pp.launching_site,
       pp.shared_launching,
       pp.transferring_site,
       pp.receiving_site,
       pp.site_history,
       pp.franchise,
       pp.cytotoxic,
       pp.vap_project,
       pp.rnd_reason,
       pp.rnd_comments,
       pp.global_nte,
       pp.afw,
       -----------------------------------------------------
       -- ADDED FOLLOWING THE FDS                         --
       -----------------------------------------------------
       pp.prj_id,
       pp.plw_pjt_id,
       pp.plw_int_number,
       pp.fill_volume_uom,
       pp.strength_uom,
       pp.strength_fill_volume,
       pp.country,
       pp.activity_type,
       pp.project_rationale,
       pp.rationale_comments,
       pp.project_technology,
       pp.product_rationale,
       pp.project_status,
       pp.wp_status,
       pp.project_progress,
       pp.business,
       pp.ims_sales,
       pp.ims_sales_percentage,
       pp.ims_units_,
       pp.ims_units_percentage,
       pp.first_allocation_approval_date,
       pp.region,
       pp.approval_date,
       pp.allocation_status,
       pp.ftf,
       pp.ftf_reason,
       pp.il_plmd,
       pp.init_proj_strat_appro,
       pp.ip_manager,
       pp.launch_date,
       pp.mars_date,
       pp.nce_date,
       pp.plfd,
       pp.plfd_remark,
       pp.plmd,
       pp.product_type,
       pp.sales_channel,
       pp.generic_segment,
       pp.submission_to_authorties_fnish,
       pp.submission_to_authrt_actl_strt,
       pp.type_of_project,
       pp.pipeline_manager,
       pp.go_no_go_pivotal,
       pp.go_no_go_biostudy,
       pp.go_no_go_submission,
       pp.go_no_go_launch,
       pp.preliminary_allocation,
       pp.reason_for_allocation,
       pp.dev_type,
       pp.npv,
       pp.tld,
       pp.tapi_afw,
       pp.submission_a_p,
       pp.approval_a_p,
       pp.launch_a_p,
       pp.value_for_teva,
       pp.plfd_late,
       pp.plmd_main,
       pp.lmd_main,
       pp.lmd_early,
       pp.plfd_late_asap,
       pp.plmd_main_asap,
       pp.lmd_main_asap,
       pp.lmd_early_asap,
       pp.product_status,
       -- added om 04/08/2014
       --pp.INNOVATOR_BRAND_NAME, -- already exists
       pp.STABILITY_RISK,
       pp.FORMULATION_COMPLEXITY,
       pp.DEV_WITHOUT_RLD,
       pp.API_COMPLEXITY,
       pp.COMPLEX_TECHNOLOGY,
       pp.STERILE_Y_N,
       pp.AGENT_COMPANY,
       pp.MFG_SITE,
       pp.PRODUCT_TECHNOLOGY ,
       pp.PRODUCT_DOSAGE_FORM,
       pp.OVERALL_BIO_RISK,
       pp.FINAL_CATEGORIZATION,
       pp.DO_YOU_AGREE_WITH_RESULT,
       pp.CALCULATED_CATEGORIZATION,
       pp.GLOBAL_DEVELOPMENT_STATUS
from   ps_project       pp,
       ps_env_activity  pe,
       ps_status        ps
where  pp.env_activity_id = pe.env_activity_id
  and  pp.status          = ps.status(+)
  and  pe.activity_id     = 1
/