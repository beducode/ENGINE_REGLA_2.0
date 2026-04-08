CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_RULES_CONFIG
AS 
 
BEGIN
     
    
    MERGE INTO IFRS_PD_RULES_CONFIG a
    USING (
    
    	 SELECT * 
	 FROM (
	 SELECT a."syscode_pd_config", a."pd_name", 
		b.pkid AS segmentation_id, a."pd_method", a."start_historical_date", a."calculation_method", a."historical_data", 
		a."expected_lifetime", a."windows_moving", 
		a."include_individual_account", a."include_wo", a."include_close", 
		a."default_ratio_by", d.pkid AS DEFAULT_RULE_ID, C.BUCKET_GROUP, a."mev_code",a."period_start_date",a."period_end_date", a."is_deleted",
		ROW_NUMBER() OVER (PARTITION BY a."syscode_pd_config" ORDER BY a."pkid" desc) rn 
	 FROM "NTT_PSAK413_IMPAIRMENT"."PdConfiguration"@DBCONFIGLINK  a
		JOIN IFRS_SEGMENTATION_MAPPING  B ON a."segment_code" = b.SYSCODE_SEGMENTATION 
		JOIN IFRS_BUCKET_HEADER  C ON a."bucket_code" = c.SYSCODE_GROUP_BUCKET 
		JOIN IFRS_DEFAULT_CRITERIA D ON A."default_criteria_code" = d.SYSCODE_DEFAULT_CRITERIA ) x 
	WHERE x.rn = 1 
    ) b
    ON (a.SYSCODE_PD = b."syscode_pd_config" )  
     WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_PD_RULES_CONFIG',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = NVL(b."is_deleted",0)
    
    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_PD, PD_RULE_NAME, SEGMENTATION_ID, PD_METHOD, 
		    START_HISTORICAL_DATE, CALC_METHOD, HISTORICAL_DATA, EXPECTED_LIFE, INCREMENT_PERIOD,
		    INCLUDE_INDIVIDUAL_ACCOUNT, INCLUDE_WO, INCLUDE_CLOSE, 
		    DEFAULT_RATIO_BY, 
		    DEFAULT_RULE_ID, BUCKET_GROUP, ME_CODE, 
		    PERIOD_START_DATE, PERIOD_END_DATE, IS_DELETED,
		    CREATED_BY, CREATED_DATE)
        VALUES (
            b."syscode_pd_config", b."pd_name", 
		b.segmentation_id, b."pd_method", b."start_historical_date", b."calculation_method", b."historical_data", 
		b."expected_lifetime", b."windows_moving", 
		b."include_individual_account", b."include_wo", b."include_close", 
		b."default_ratio_by", b.DEFAULT_RULE_ID, b.BUCKET_GROUP, b."mev_code",b."period_start_date",b."period_end_date", NVL(b."is_deleted",0),
		'SP_IFRS_PD_RULES_CONFIG',sysdate
        );
        
        commit;
     
END;
/
