DROP VIEW IF EXISTS VW_IFRS_ECL_MODEL_DETAIL_LGD;

CREATE VIEW VW_IFRS_ECL_MODEL_DETAIL_LGD
AS

 SELECT a.pkid,
    a.syscode_ecl_configuration AS ecl_model_id,
    a.syscode_segmentation AS segmentation_id,
    a.syscode_lgd_config AS lgd_model_id,
    NULL::bigint AS me_model_id,
    a.effective_date AS eff_date_option,
    a.effective_period AS eff_date,
    0 AS is_delete,
    a.created_by AS createdby,
    a.created_date AS createddate,
    a.created_host AS createdhost,
    a.updated_by AS updatedby,
    a.updated_date AS updateddate,
    a.updated_host AS updatedhost
   FROM ( SELECT eclpdmodel.pkid,
            eclpdmodel.syscode_ecl_configuration,
            eclpdmodel.syscode_segmentation,
            eclpdmodel.syscode_lgd_config,
            eclpdmodel.effective_date,
            eclpdmodel.effective_period,
            eclpdmodel.created_by,
            eclpdmodel.created_date,
            eclpdmodel.created_host,
            eclpdmodel.updated_by,
            eclpdmodel.updated_date,
            eclpdmodel.updated_host
           FROM dblink('workflow_ntt_impairment'::text, '
			select A.pkid,
					max(B.pkid) as SYSCODE_ECL_CONFIGURATION,
					max(C.pkid) as SYSCODE_SEGMENTATION,
					max(D.pkid) as SYSCODE_LGD_CONFIG,
					MAX(A.effective_date) as effective_date,
					max(A.effective_period) as effective_period,
					A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST
			FROM "EclLgdModel" A
			LEFT JOIN "EclConfiguration" B ON A.SYSCODE_ECL_CONFIGURATION = B.SYSCODE_ECL_CONFIGURATION
			LEFT JOIN "Segmentation" C ON A.CODE_SEGMENTATION = C.SYSCODE_SEGMENTATION
			LEFT JOIN "LgdConfiguration" D ON A.CODE_LGD_CONFIGURATION = D.SYSCODE_LGD_CONFIG
			group by A.pkid,
					A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST'::text) eclpdmodel(pkid bigint, syscode_ecl_configuration bigint, syscode_segmentation bigint, syscode_lgd_config bigint, effective_date character varying, effective_period date, created_by character varying, created_date timestamp without time zone, created_host character varying, updated_by character varying, updated_date timestamp without time zone, updated_host character varying)) a;