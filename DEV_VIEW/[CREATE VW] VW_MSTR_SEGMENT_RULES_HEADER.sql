 DROP VIEW IF EXISTS VW_MSTR_SEGMENT_RULES_HEADER;

CREATE VIEW VW_MSTR_SEGMENT_RULES_HEADER AS

-- combine data with only level 0 (don't have parent) and data only parent (without level 0)
WITH Grp_seg AS (
	select * from dblink('workflow_ntt_impairment','
					SELECT * FROM "Segmentation"  where level <> 0 and is_active = true
					Union all
				    select * from "Segmentation" where level = 0 and parent_code_level_0 not in (
						select distinct parent_code_level_0 from "Segmentation" where parent_code_level_1 is not null and is_active = true)')
						Segmentation(pkid bigint, 
                              syscode_segmentation character varying, 
                              version character varying, 
                              parent_code_level_0 character varying, 
                              parent_code_level_1 character varying, 
                              parent_code character varying, 
                              segment_name character varying, 
                              segment_type character varying, 
                              description character varying, 
                              level integer, 
                              effective_start_date timestamp without time zone,
                              effective_end_date timestamp without time zone,
                              is_active boolean,
                              is_publish boolean, 
                              is_last_child boolean, 
                              json_conditions text, 
                              sql_conditions text, 
                              created_by character varying, 
                              created_date timestamp without time zone, 
                              created_host character varying, 
                              updated_by character varying, 
                              updated_date timestamp without time zone, 
                              updated_host character varying, 
                              merge_sql_conditions text)
), List_Sub AS (
   select * from dblink('workflow_ntt_impairment','SELECT distinct SYSCODE_SEGMENTATION,SEGMENT_NAME FROM "Segmentation"')
	list_segment(syscode_segmentation character varying, segment_name character varying)
)


select A.pkid,
      B.SEGMENT_NAME as group_segment,
      C.SEGMENT_NAME as segment,
      D.SEGMENT_NAME as sub_segment, 
      A.is_active as active_flag, 
		COALESCE(A.segment_type,'PORTFOLIO_SEGMENT') as segment_type, 
      A.level as sequence, 
      1 as is_new,
		0 As is_delete,
      A.created_by As createdby,
      created_date As createddate,
      created_host As createdhost,
      updated_by As updatedby,
      updated_date As updateddate,
		updated_host As updatedhost 
		from Grp_seg A 
		left join List_Sub B ON A.parent_code_level_0 = B.SYSCODE_SEGMENTATION
		left join List_Sub C ON A.parent_code_level_1 = C.SYSCODE_SEGMENTATION
		left join List_Sub D ON A.SYSCODE_SEGMENTATION = D.SYSCODE_SEGMENTATION;