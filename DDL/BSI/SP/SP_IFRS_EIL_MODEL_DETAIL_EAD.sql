CREATE OR REPLACE PROCEDURE PSAK413."SP_IFRS_EIL_MODEL_DETAIL_EAD" 
AS 
    
BEGIN

	--====>> EilEadModel <<=======
     MERGE INTO IFRS_EIL_MODEL_DETAIL_EAD a
	    USING (
	    
		    selecT A."pkid",
		       B.PKID AS EIL_MODEL_ID ,C.PKID AS SEGMENTATION_ID,
		       D.PKID AS EAD_RULE_ID,A."lifetime_date" ,A."is_override_lifetime", A."is_deleted", A."lifetime_effective_date"
		   from "EilEadModel"@DBCONFIGLINK A
			 INNER JOIN "Segmentation"@DBCONFIGLINK seg ON A."code_segmentation" = seg."syscode_segmentation"
			 INNER JOIN IFRS_SEGMENTATION_MAPPING C on CASE WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
			    											 WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
			    											 WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 END = A."code_segmentation"
			 INNER JOIN IFRS_EIL_MODEL_HEADER B on A."syscode_eil_configuration" = B.SYSCODE_EIL
			 INNER JOIN IFRS_EAD_RULES_CONFIG D on A."code_ead_configuration" = D.SYSCODE_EAD
			    
	    ) b
	    ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID )  
	    WHEN MATCHED THEN
	        UPDATE SET  
	        	a.IS_OVERRIDE_LIFETIME	 = b."is_override_lifetime",
	            a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_1',
	            a.UPDATEDDATE            = sysdate,
	            a.IS_DELETE              = b."is_deleted" 
	 
	    WHEN NOT MATCHED THEN
	        INSERT (
	            EIL_MODEL_ID, SEGMENTATION_ID,
	            EAD_RULE_ID,LIFETIME_DATE,LIFETIME_EFF_DATE_OPTION,IS_OVERRIDE_LIFETIME, IS_DELETE,  
	            UPDATEDBY, UPDATEDDATE
	        )
	        VALUES (
	            b.EIL_MODEL_ID, b.SEGMENTATION_ID,
	            b.EAD_RULE_ID,b."lifetime_date" ,B."lifetime_effective_date",nvl(b."is_override_lifetime",0),nvl(b."is_deleted",0), 'SP_IFRS_EIL_CONFIG_MAPPING_1', sysdate 
	        );
	        
	        commit;
       
END;
/
