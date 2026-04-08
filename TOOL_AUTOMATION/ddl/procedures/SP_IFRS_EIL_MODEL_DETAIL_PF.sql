CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_EIL_MODEL_DETAIL_PF
BEGIN
    
	          --====>> EilPortfolio <<=======
     MERGE INTO IFRS_EIL_MODEL_DETAIL_PF a
	    USING (
	    
		    selecT A."pkid",
			       B.PKID AS EIL_MODEL_ID ,C.PKID AS SEGMENTATION_ID,
			       D.PKID AS LT_RULE_ID, A."is_deleted"
			    from "EilPortfolio"@DBCONFIGLINK A
			    INNER JOIN "Segmentation"@DBCONFIGLINK seg ON A."code_segmentation" = seg."syscode_segmentation"
			    INNER JOIN IFRS_SEGMENTATION_MAPPING C on CASE WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
			    											 WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
			    											 WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 END = A."code_segmentation"
			    INNER JOIN IFRS_EIL_MODEL_HEADER B on A."syscode_eil_configuration" = B.SYSCODE_EIL
			    LEFT  JOIN IFRS_LIFETIME_RULES_CONFIG D on C.PKID  = D.SEGMENTATION_ID
			    
		    ) b
		    ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID) 
		    WHEN MATCHED THEN
		        UPDATE SET  
		            a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_4',
		            a.UPDATEDDATE            = sysdate,
		            a.IS_DELETE              = b."is_deleted"
		    WHEN NOT MATCHED THEN
		        INSERT (
		            EIL_MODEL_ID, SEGMENTATION_ID,
		            LT_RULE_ID, IS_DELETE,
		            UPDATEDBY, UPDATEDDATE
		        )
		        VALUES (
		            b.EIL_MODEL_ID, b.SEGMENTATION_ID,
		            b.LT_RULE_ID,b."is_deleted", 'SP_IFRS_EIL_CONFIG_MAPPING_4', sysdate 
	        );
	        
	        commit;
	     
END