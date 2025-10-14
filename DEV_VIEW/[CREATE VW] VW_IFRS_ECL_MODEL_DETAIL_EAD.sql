DROP VIEW IF EXISTS VW_IFRS_ECL_MODEL_DETAIL_EAD;

CREATE VIEW VW_IFRS_ECL_MODEL_DETAIL_EAD
AS
 SELECT a.pkid,
    a.syscode_ecl_configuration AS ecl_model_id,
    a.code_segmentation AS segmentation_id,
    a.code_lgd_config AS ead_model_id,
    0 AS is_delete,
    a.created_by AS createdby,
    a.created_date AS createddate,
    a.created_host AS createdhost,
    a.updated_by AS updatedby,
    a.updated_date AS updateddate,
    a.updated_host AS updatedhost,
    NULL::bigint AS ccf_model_id,
    NULL::text AS ccf_eff_date_option,
    a.ccf_date AS ccf_eff_date
   FROM ( SELECT eclpdmodel.pkid,
            eclpdmodel.syscode_ecl_configuration,
            eclpdmodel.code_segmentation,
            eclpdmodel.code_lgd_config,
            eclpdmodel.created_by,
            eclpdmodel.created_date,
            eclpdmodel.created_host,
            eclpdmodel.updated_by,
            eclpdmodel.updated_date,
            eclpdmodel.updated_host,
            eclpdmodel.ccf_date
           FROM dblink('workflow_ntt_impairment'::text, '
			select A.pkid,
					MAX(B.pkid) as SYSCODE_ECL_CONFIGURATION,
					MAX(C.pkid) as CODE_SEGMENTATION,
					MAX(D.pkid) as CODE_EAD_CONFIG,
					A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST,
					A.ccf_date
			FROM "EclEadModel" A
			LEFT JOIN "EclConfiguration" B ON A.SYSCODE_ECL_CONFIGURATION = B.SYSCODE_ECL_CONFIGURATION
			LEFT JOIN "Segmentation" C ON A.CODE_SEGMENTATION = C.SYSCODE_SEGMENTATION
			LEFT JOIN "EadConfiguration" D ON A.CODE_EAD_CONFIGURATION = D.SYSCODE_EAD_CONFIG
			group by A.pkid,A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST,
					A.ccf_date'::text) eclpdmodel(pkid bigint, syscode_ecl_configuration bigint, code_segmentation bigint, code_lgd_config bigint, created_by character varying, created_date timestamp without time zone, created_host character varying, updated_by character varying, updated_date timestamp without time zone, updated_host character varying, ccf_date timestamp without time zone)) a;