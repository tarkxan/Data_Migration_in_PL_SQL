create or replace view PS_TASK_EXP_VW_1 as
select /*$Id: CRE_PS_TASK_EXP_VW_1.sql,v 1.2 2013/04/30 08:28:20 olando Exp $*/
       pa.env_id,
       pp.gpms_exp_id,
       pt.task_id,
       pt.project_id,
       pt.gpms_id,
       pt.country_id,
       pt.action,
       pt.plmd_date,
       pt.plmd_asap,
       pt.innovator_brand_name,
       pt.brand_sales,
       pt.brand_sales_units,
       pt.strengths,
       pt.development_site,
       pt.hu_flfd,
       pt.status,
       pt.plmd_text,
       pt.plmd_il_date,
       pt.plmd_il_asap,
       pt.plmd_ca_date,
       pt.plmd_ca_asap,
       pt.plmd_earlylate ,
       pt.spc ,
       pt.lmd,
       pt.task_priority,
       pt.sales_channel,
       pt.country_comments,
       pt.mars_date,
       pt.region,
       pt.grd,
       pt.prisma,
       pt.market_type,
       pt.plfd_date,
       pt.plfd_asap,
       nvl(ps.terminated, 0) terminated,
       -----------------------------------------------------
       -- ADDED FOLLOWING THE FDS                         --
       -----------------------------------------------------
       pt.prj_id, 
       pt.plw_pjt_id,   
       pt.plw_int_number,
       pt.fill_volume_uom,
       pt.strength_uom,    
       pt.strength_fill_volume, 
       pt.country, 
       pt.activity_type,  
       pt.launching_site, 
       pt.project_name,   
       pt.project_rationale,     
       pt.rationale_comments,    
       pt.project_technology,     
       pt.product_rationale,     
       pt.project_status,  
       pt.wp_status,
       pt.project_progress,
       pt.business,
       pt.ims_sales,
       pt.ims_sales_percentage,  
       pt.ims_units_,     
       pt.ims_units_percentage,   
       pt.first_allocation_approval_date,
       pt.approval_date,  
       pt.project_priority,
       pt.allocation_status,    
       pt.ftf,      
       pt.ftf_reason,    
       pt.handling_category,      
       pt.il_plmd, 
       pt.init_proj_strat_appro,  
       pt.innovator_company,     
       pt.innovator_shelf_life,  
       pt.ip_manager,      
       pt.lcm,     
       pt.launch_date,    
       pt.nce_date,
       pt.plfd, 
       pt.plfd_remark, 
       pt.plmd,  
       pt.filing_strategy,
       pt.product_type,
       pt.generic_segment,
       pt.submission_to_authorties_fnish,
       pt.submission_to_authrt_actl_strt,
       pt.therapeutic_class,   
       pt.source_project_id,   
       pt.type_of_project,
       pt.pipeline_manager,
       pt.go_no_go_pivotal,
       pt.go_no_go_biostudy,
       pt.go_no_go_submission,
       pt.go_no_go_launch,
       pt.preliminary_allocation,
       pt.reason_for_allocation,
       pt.dev_type,
       pt.npv,
       pt.tld
from   ps_project_1      pp,
       ps_task_1         pt,
       ps_env_activity   pa,
       ps_status         ps
where  pt.project_id        = pp.project_id
  and  pp.env_activity_id   = pa.env_activity_id
  and  decode(pt.status, 'Historic', null) = ps.status(+)
  and  pa.activity_id       = 1
/