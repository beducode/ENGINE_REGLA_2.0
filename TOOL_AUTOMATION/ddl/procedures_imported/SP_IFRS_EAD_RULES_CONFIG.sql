CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_EAD_RULES_CONFIG" 
AS 
 
BEGIN
     
    MERGE INTO IFRS_EAD_RULES_CONFIG a
    USING (
    
    	 SELECT * 
		 FROM (
		 
				SELECT a."ead_name",A."syscode_ead_config" ,
					B.PKID AS SEGMENTATION_ID, A."balance_source",  
					NVL(C.PKID,0) AS LIFETIME_RULE_ID,
					a."is_ead_parameter_lifetime", A."include_accrued_interest", A."include_unamortized_fee_cost", a."include_interest_margin_past_due",
					NVL(A."is_deleted",0) AS IS_DELETE,
					ROW_NUMBER() OVER (PARTITION BY a."syscode_ead_config" ORDER BY a."pkid" desc) RN
					 FROM "EadConfiguration"@DBCONFIGLINK a 
					JOIN IFRS_SEGMENTATION_MAPPING B ON a."segment_code" = b.SYSCODE_SEGMENTATION
					LEFT JOIN IFRS_LIFETIME_RULES_CONFIG C ON a."lifetime_code" = C.SYSCODE_LIFETIME
						) x 
		WHERE x.rn = 1 
    ) b
    ON (a.SYSCODE_EAD = b."syscode_ead_config" )  
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATEDBY              = 'SP_IFRS_EAD_RULES_CONFIG',
            a.UPDATEDDATE            = sysdate,
            a.IS_DELETE              = B.IS_DELETE,
            a.EAD_BALANCE			 = B."balance_source",
            A.UNAMORTIZED_FEE_COST_FLAG = B."include_unamortized_fee_cost",
            A.MARGIN_ACCRUED_FLAG	 = B."include_accrued_interest",
            A.MARGIN_PAST_DUE_FLAG	 = B."include_interest_margin_past_due"
    
    WHEN NOT MATCHED THEN
        INSERT (
            EAD_RULE_NAME,
		    SYSCODE_EAD,
		    SEGMENTATION_ID,
		    EAD_BALANCE,
		    LIFETIME_RULE_ID,
		    LIFETIME_FLAG,
		    UNAMORTIZED_FEE_COST_FLAG,
		    MARGIN_ACCRUED_FLAG,
		    MARGIN_PAST_DUE_FLAG,
		    IS_DELETE,
		    CREATEDBY,
		    CREATEDDATE
		    )
        VALUES (
            b."ead_name",b."syscode_ead_config" ,
		b.SEGMENTATION_ID,b."balance_source", 
		b.LIFETIME_RULE_ID, b."is_ead_parameter_lifetime",B."include_unamortized_fee_cost",B."include_accrued_interest",B."include_interest_margin_past_due", B.IS_DELETE,
		'SP_IFRS_EAD_RULES_CONFIG',sysdate
        );
        
        commit;
     
END