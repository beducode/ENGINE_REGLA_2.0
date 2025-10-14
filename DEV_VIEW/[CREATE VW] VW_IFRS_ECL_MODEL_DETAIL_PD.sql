DROP VIEW IF EXISTS VW_IFRS_ECL_MODEL_DETAIL_PD;

CREATE VIEW VW_IFRS_ECL_MODEL_DETAIL_PD
AS

 SELECT a.pkid,
    a.syscode_ecl_configuration AS ecl_model_id,
    a.syscode_segmentation AS segmentation_id,
    a.syscode_pd_config AS pd_model_id,
    NULL::bigint AS me_model_id,
    a.effective_date AS eff_date_option,
    a.effective_period AS eff_date,
    0 AS is_delete,
    a.created_by AS createdby,
    a.created_date AS createddate,
    a.created_host AS createdhost,
    a.updated_by AS updatedby,
    a.updated_date AS updateddate,
    a.updated_host AS updatedhost,
    NULL::text AS scalar_eff_date_option,
    CURRENT_DATE AS scalar_eff_date
   FROM ( SELECT eclpdmodel.pkid,
            eclpdmodel.syscode_ecl_configuration,
            eclpdmodel.syscode_segmentation,
            eclpdmodel.syscode_pd_config,
            eclpdmodel.effective_date,
            eclpdmodel.effective_period,
            eclpdmodel.created_by,
            eclpdmodel.created_date,
            eclpdmodel.created_host,
            eclpdmodel.updated_by,
            eclpdmodel.updated_date,
            eclpdmodel.updated_host
           FROM dblink('workflow_ntt_impairment'::text, '
			SELECT A.PKID,
				max(B.PKID) AS SYSCODE_ECL_CONFIGURATION,
				max(C.PKID) AS SYSCODE_SEGMENTATION,
				max(D.PKID) AS SYSCODE_PD_CONFIG,
				max(A.EFFECTIVE_DATE),
				max(A.EFFECTIVE_PERIOD),
				A.CREATED_BY,
				A.CREATED_DATE, 
				A.CREATED_HOST, 
				A.UPDATED_BY, 
				A.UPDATED_DATE, 
				A.UPDATED_HOST
			FROM "EclPdModel" A
			LEFT JOIN "EclConfiguration" B ON A.SYSCODE_ECL_CONFIGURATION = B.SYSCODE_ECL_CONFIGURATION
			LEFT JOIN "Segmentation" C ON A.CODE_SEGMENTATION = c.SYSCODE_SEGMENTATION
			LEFT JOIN "PdConfiguration" D ON A.CODE_PD_CONFIGURATION = D.SYSCODE_PD_CONFIG
			group by A.PKID,
				A.CREATED_BY,
				A.CREATED_DATE, 
				A.CREATED_HOST, 
				A.UPDATED_BY, 
				A.UPDATED_DATE, 
				A.UPDATED_HOST'::text) eclpdmodel(pkid bigint, syscode_ecl_configuration bigint, syscode_segmentation bigint, syscode_pd_config bigint, effective_date character varying, effective_period date, created_by character varying, created_date timestamp without time zone, created_host character varying, updated_by character varying, updated_date timestamp without time zone, updated_host character varying)) a;