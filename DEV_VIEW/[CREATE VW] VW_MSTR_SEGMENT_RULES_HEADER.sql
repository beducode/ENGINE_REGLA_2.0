DROP VIEW IF EXISTS VW_MSTR_SEGMENT_RULES_HEADER CASCADE;

CREATE VIEW VW_MSTR_SEGMENT_RULES_HEADER 
AS
SELECT * FROM dblink('workflow_ntt_impairment','SELECT A.pkid, 
A.segment_name_lv1 AS group_segment,
A.segment_name_lv2 AS segment,
A.segment_name_lv3 AS sub_segment,
(SELECT UPPER(source_table_conditions) 
FROM "Segmentation" B 
WHERE B.syscode_segmentation = A.syscode_segmentation_lv1 
LIMIT 1) AS table_name,
UPPER(merge_sql_conditions) AS sql_condition,
1 AS active_flag,
(SELECT CASE WHEN C.SEGMENT_TYPE IS NULL THEN ''PORTFOLIO_SEGMENT'' ELSE C.SEGMENT_TYPE END 
FROM "Segmentation" C 
WHERE C.syscode_segmentation = A.syscode_segmentation_lv1 
LIMIT 1) AS segment_type,
1 AS sequence,
1 AS is_new,
CASE WHEN A.IS_DELETED = FALSE THEN 0 ELSE 1 END is_delete,
A.created_by AS createdby,
A.created_date AS createddate,
A.created_host AS createdhost
FROM "SegmentationMapping" A') AS MSTR_SEGMENT_RULES_HEADER(
PKID BIGINT
,GROUP_SEGMENT VARCHAR(50)
,SEGMENT VARCHAR(50)
,SUB_SEGMENT VARCHAR(60)
,TABLE_NAME VARCHAR(100)
,SQL_CONDITION TEXT
,ACTIVE_FLAG INT
,SEGMENT_TYPE VARCHAR(50)
,SEQUENCE INT
,IS_NEW INT
,IS_DELETE INT
,CREATEDBY CHARACTER VARYING(36)
,CREATEDDATE TIMESTAMP WITHOUT TIME ZONE
,CREATEDHOST CHARACTER VARYING(30));