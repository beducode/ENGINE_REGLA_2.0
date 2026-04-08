CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_EIL_MODEL_DETAIL_PD
BEGIN
    
	         --====>> EilPdModel <<=======
     MERGE INTO IFRS_EIL_MODEL_DETAIL_PD a
	    USING (
			selecT A."pkid",
				       B.PKID AS EIL_MODEL_ID ,C.PKID AS SEGMENTATION_ID,
				       D.PKID AS PD_RULE_ID,NVL(A."workflow_header_draft_pkid",0) AS PD_MODEL_ID, A."effective_date",A."pd_date",A."is_override_pd", A."is_deleted"
				    from "EilPdModel"@DBCONFIGLINK A
				    INNER JOIN "Segmentation"@DBCONFIGLINK seg ON A."code_segmentation" = seg."syscode_segmentation"
					INNER JOIN IFRS_SEGMENTATION_MAPPING C on CASE WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
					    											 WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
					    											 WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 END = A."code_segmentation"
					INNER JOIN IFRS_EIL_MODEL_HEADER B on A."syscode_eil_configuration" = B.SYSCODE_EIL
				    INNER JOIN IFRS_PD_RULES_CONFIG D on A."code_pd_configuration" = D.SYSCODE_PD
	    ) b
	    ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID)  
	     
	    WHEN MATCHED THEN
	        UPDATE SET  
	        	a.IS_OVERRIDE			 = b."is_override_pd",
	            a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_3',
	            a.UPDATEDDATE            = sysdate,
	            a.IS_DELETE              = b."is_deleted",
	            a.PD_MODEL_ID			 = b.PD_MODEL_ID
	 
	    WHEN NOT MATCHED THEN
	        INSERT (
	            EIL_MODEL_ID, SEGMENTATION_ID,
	            PD_RULE_ID,PD_MODEL_ID,EFF_DATE_OPTION,EFF_DATE ,IS_OVERRIDE,IS_DELETE,  
	            UPDATEDBY, UPDATEDDATE
	        )
	        VALUES (
	            b.EIL_MODEL_ID, b.SEGMENTATION_ID,
	            b.PD_RULE_ID,b.PD_MODEL_ID,b."effective_date",b."pd_date",b."is_override_pd",nvl(b."is_deleted",0), 'SP_IFRS_EIL_CONFIG_MAPPING_3', sysdate 
	        );
	        
	        commit;
	     
END