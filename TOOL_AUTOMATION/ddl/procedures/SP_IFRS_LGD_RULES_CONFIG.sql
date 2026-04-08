CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_LGD_RULES_CONFIG
BEGIN
     
    
    MERGE INTO IFRS_LGD_RULES_CONFIG a
    USING (
    
    	 SELECT * 
	 FROM (
	 
SELECT A."syscode_lgd_config" ,
	A."lgd_name", A."lgd_method", B.PKID AS SEGMENTATION_ID,
	"start_historical_date", "calculation_method", "historical_data",
	"workout_period", C.PKID AS DEFAULT_RULE_ID, NVL(A."is_deleted",1) AS IS_DELETED,
	ROW_NUMBER() OVER (PARTITION BY a."syscode_lgd_config" ORDER BY a."pkid" desc) RN
FROM "LgdConfiguration"@DBCONFIGLINK a 
	JOIN IFRS_SEGMENTATION_MAPPING b ON a."segment_code" = b.SYSCODE_SEGMENTATION
	JOIN IFRS_DEFAULT_CRITERIA  C ON A."default_criteria_code" = c.SYSCODE_DEFAULT_CRITERIA ) x 
	WHERE x.rn = 1 
    ) b
    ON (a.SYSCODE_LGD = b."syscode_lgd_config" )  
    
      
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_LGD_RULES_CONFIG',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = B.IS_DELETED 
    
    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_LGD, 
		    LGD_RULE_NAME, 
		    LGD_METHOD, 
		    SEGMENTATION_ID, 
		    START_HISTORICAL_DATE, 
		    CALCULATION_METHOD, 
		    HISTORICAL_DATA, 
		    WORKOUT_PERIOD, 
		    DEFAULT_RULE_ID, 
		    IS_DELETED, 
		    CREATED_BY, 
		    CREATED_DATE)
        VALUES (
            b."syscode_lgd_config", b."lgd_name", b."lgd_method",
		b.segmentation_id, b."start_historical_date", b."calculation_method", b."historical_data", 
		b."workout_period", b.default_rule_id,   B.IS_DELETED,
		'SP_IFRS_LGD_RULES_CONFIG',sysdate
        );
        
        commit;
     
END