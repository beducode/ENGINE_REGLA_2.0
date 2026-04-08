CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_LGD_WORKOUT_RULES_CONFIG
AS 
 
BEGIN
     
    MERGE INTO IFRS_LGD_WORKOUT_RULES_CONFIG a
    USING (
    
    	 SELECT * 
	 FROM (
	 

SELECT a."workout_period_configuration_name",A."syscode_workout_period_config" ,
	B.PKID AS SEGMENTATION_ID, 
	a."start_period",  a."historical_month",
	NVL(A."is_deleted",0) AS IS_DELETED,
	ROW_NUMBER() OVER (PARTITION BY a."syscode_workout_period_config" ORDER BY a."pkid" desc) RN
	FROM "NTT_PSAK413_IMPAIRMENT"."WorkoutPeriodConfiguration"@DBCONFIGLINK a 
	JOIN IFRS_SEGMENTATION_MAPPING b ON a."segment_code" = b.SYSCODE_SEGMENTATION  ) x 
	WHERE x.rn = 1 
    ) b
    ON (a.SYSCODE_workout = b."syscode_workout_period_config" )  
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_LGD_WORKOUT_RULES_CONFIG',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = B.IS_DELETED
    
    WHEN NOT MATCHED THEN
        INSERT ( 
		    syscode_workout,
		    workout_rule_name,
		    segmentation_id,
		    historical_month,
		    start_period,
		    is_deleted,
		      created_by ,
		    created_date
		  
		    )
        VALUES (
            b."syscode_workout_period_config",b."workout_period_configuration_name" ,
		b.SEGMENTATION_ID, 
	b."historical_month", b."start_period",     B.IS_DELETED,
		'SP_IFRS_LIFETIME_RULES_CONFIG',sysdate
        );
        
        commit;
     
END;
/
