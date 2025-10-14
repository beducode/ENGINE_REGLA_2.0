DROP VIEW IF EXISTS VW_IFRS_ECL_MODEL_DETAIL_PF;

CREATE VIEW VW_IFRS_ECL_MODEL_DETAIL_PF
AS

 SELECT a.pkid,
    a.syscode_ecl_configuration AS ecl_model_id,
    a.code_segmentation AS segmentation_id,
    NULL::bigint AS lt_rule_id,
    NULL::bigint AS sicr_rule_id,
    NULL::double precision AS upside_value,
    NULL::double precision AS base_value,
    NULL::double precision AS downside_value,
    NULL::double precision AS upside_prob_outcome,
    NULL::double precision AS base_prob_outcome,
    NULL::double precision AS downside_prob_outcome,
    NULL::double precision AS overlay_amount,
    0 AS is_delete,
    a.created_by AS createdby,
    a.created_date AS createddate,
    a.created_host AS createdhost,
    a.updated_by AS updatedby,
    a.updated_date AS updateddate,
    a.updated_host AS updatedhost,
    a.bucket_code AS bucket_group,
    a.default_criteria_code AS default_rule_id
   FROM ( SELECT eclpdmodel.pkid,
            eclpdmodel.syscode_ecl_configuration,
            eclpdmodel.code_segmentation,
            eclpdmodel.created_by,
            eclpdmodel.created_date,
            eclpdmodel.created_host,
            eclpdmodel.updated_by,
            eclpdmodel.updated_date,
            eclpdmodel.updated_host,
            eclpdmodel.bucket_code,
            eclpdmodel.default_criteria_code
           FROM dblink('workflow_ntt_impairment'::text, '
			select A.pkid,
					MAX(B.pkid) as SYSCODE_ECL_CONFIGURATION,
					MAX(C.pkid) as CODE_SEGMENTATION,
					A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST,
					MAX(E.unique_code) as bucket_code,
					MAX(F.pkid) as default_criteria_code
			FROM "EclPortfolio" A
			LEFT JOIN "EclConfiguration" B ON A.SYSCODE_ECL_CONFIGURATION = B.SYSCODE_ECL_CONFIGURATION
			LEFT JOIN "Segmentation" C ON A.CODE_SEGMENTATION = C.SYSCODE_SEGMENTATION
			LEFT JOIN "PdConfiguration" D ON A.CODE_SEGMENTATION = D.segment_code
			LEFT JOIN "GroupBucket" E ON D.bucket_code = E.syscode_group_bucket
			LEFT JOIN "DefaultCriteria" F ON D.default_criteria_code = F.syscode_default_criteria
			group by A.pkid,A.CREATED_BY,
					A.CREATED_DATE, 
					A.CREATED_HOST, 
					A.UPDATED_BY, 
					A.UPDATED_DATE, 
					A.UPDATED_HOST'::text) eclpdmodel(pkid bigint, syscode_ecl_configuration bigint, code_segmentation bigint, created_by character varying, created_date timestamp without time zone, created_host character varying, updated_by character varying, updated_date timestamp without time zone, updated_host character varying, bucket_code character varying, default_criteria_code bigint)) a;