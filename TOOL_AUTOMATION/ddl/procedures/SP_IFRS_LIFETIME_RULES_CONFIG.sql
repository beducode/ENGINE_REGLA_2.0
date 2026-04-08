CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_LIFETIME_RULES_CONFIG
BEGIN
     
    
    MERGE INTO IFRS_LIFETIME_RULES_CONFIG a
    USING (
    
    	 SELECT * 
	 FROM (
	 

SELECT a."lifetime_name",A."syscode_lifetime_config" ,
	B.PKID AS SEGMENTATION_ID,A."method", 
	a."start_historical_date", a."calculation_method", a."historical_month",
	NVL(A."is_deleted",0) AS IS_DELETED,
	ROW_NUMBER() OVER (PARTITION BY a."syscode_lifetime_config" ORDER BY a."pkid" desc) RN
FROM "LifetimeConfiguration"@DBCONFIGLINK a 
	JOIN IFRS_SEGMENTATION_MAPPING b ON a."segment_code" = b.SYSCODE_SEGMENTATION  ) x 
	WHERE x.rn = 1 
    ) b
    ON (a.SYSCODE_Lifetime = b."syscode_lifetime_config" )  
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_LIFETIME_RULES_CONFIG',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = B.IS_DELETED
    
    WHEN NOT MATCHED THEN
        INSERT (
            LIFETIME_RULE_NAME,
		    SYSCODE_LIFETIME,
		    SEGMENTATION_ID,
		    LIFETIME_METHOD,
		    START_HISTORICAL_DATE,
		    CALCULATION_METHOD,
		    HISTORICAL_MONTH,
		    IS_DELETED,
		    CREATED_BY,
		    CREATED_DATE
		    )
        VALUES (
            b."lifetime_name",b."syscode_lifetime_config" ,
		b.SEGMENTATION_ID,b."method", 
	b."start_historical_date", b."calculation_method", b."historical_month",   B.IS_DELETED,
		'SP_IFRS_LIFETIME_RULES_CONFIG',sysdate
        );
        
        commit;
     
END