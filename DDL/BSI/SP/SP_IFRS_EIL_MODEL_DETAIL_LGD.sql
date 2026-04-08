CREATE OR REPLACE PROCEDURE PSAK413."SP_IFRS_EIL_MODEL_DETAIL_LGD" 
AS 
    
BEGIN
    
	     --====>> EilLgdModel <<=======
     MERGE INTO IFRS_EIL_MODEL_DETAIL_LGD a
	    USING (
	    
		    selecT A."pkid",
		       B.PKID AS EIL_MODEL_ID ,C.PKID AS SEGMENTATION_ID,
		       D.PKID AS LGD_RULE_ID,A."lgd_date", A."effective_date",A."is_override_lgd", A."is_deleted"
		    from "EilLgdModel"@DBCONFIGLINK A
		    INNER JOIN "Segmentation"@DBCONFIGLINK seg ON A."code_segmentation" = seg."syscode_segmentation"
			INNER JOIN IFRS_SEGMENTATION_MAPPING C on CASE WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
			    											 WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
			    											 WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 END = A."code_segmentation"
			INNER JOIN IFRS_EIL_MODEL_HEADER B on A."syscode_eil_configuration" = B.SYSCODE_EIL
		    INNER JOIN IFRS_LGD_RULES_CONFIG D on A."code_lgd_configuration" = D.SYSCODE_LGD
	    ) b
	    ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID )  
	     
	    WHEN MATCHED THEN
	        UPDATE SET  
	        	a.IS_OVERRIDE			 = b."is_override_lgd",
	            a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_2',
	            a.UPDATEDDATE            = sysdate,
	            a.IS_DELETE              = b."is_deleted"
	 
	    WHEN NOT MATCHED THEN
	        INSERT (
	            EIL_MODEL_ID, SEGMENTATION_ID,
	            LGD_RULE_ID,EFF_DATE ,EFF_DATE_OPTION,IS_OVERRIDE, IS_DELETE,  
	            UPDATEDBY, UPDATEDDATE
	        )
	        VALUES (
	            b.EIL_MODEL_ID, b.SEGMENTATION_ID,
	            b.LGD_RULE_ID,b."lgd_date", b."effective_date",b."is_override_lgd",nvl(b."is_deleted",0), 'SP_IFRS_EIL_CONFIG_MAPPING_2', sysdate 
	        );
	        
	        commit;
	     
END;
/
