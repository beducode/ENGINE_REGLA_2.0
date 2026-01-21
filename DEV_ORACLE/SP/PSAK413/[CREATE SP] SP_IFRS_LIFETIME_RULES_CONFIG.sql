CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_LIFETIME_RULES_CONFIG
AUTHID CURRENT_USER
AS
BEGIN
    /* =========================================================
       STEP 1 : MERGE DATA KE IFRS_LIFETIME_RULES_CONFIG
       ========================================================= */
	MERGE INTO IFRS_LIFETIME_RULES_CONFIG T
	USING (
	    SELECT
	        A."lifetime_name"            	AS LIFETIME_RULE_NAME,
	        A."syscode_lifetime_config"  	AS SYSCODE_LIFETIME,
	        B."pkid"                     	AS SEGMENTATION_ID,
	        A."method"                   	AS LIFETIME_METHOD,
	        A."start_historical_date"    	AS START_HISTORICAL_DATE,
	        A."calculation_method"       	AS CALCULATION_METHOD,
	        A."historical_month"         	AS HISTORICAL_MONTH,
	        'SP_IFRS_LIFETIME_RULES_CONFIG' AS CREATED_BY,
			NVL(A."created_date", SYSDATE)  AS CREATED_DATE,
	        NULL                            AS UPDATED_BY,
	        NULL                            AS UPDATED_DATE,
	        NVL(A."is_deleted", 0)          AS IS_DELETED
	    FROM "LifetimeConfiguration"@DBCONFIGLINK A
	    LEFT JOIN "SegmentationMapping"@DBCONFIGLINK B
	        ON A."segment_code" = B."syscode_segmentation_lv3"
	) S
	ON (T.SYSCODE_LIFETIME = S.SYSCODE_LIFETIME)
	WHEN MATCHED THEN
	    UPDATE SET
	        T.LIFETIME_RULE_NAME    = S.LIFETIME_RULE_NAME,
	        T.LIFETIME_METHOD       = S.LIFETIME_METHOD,
	        T.SEGMENTATION_ID 		= S.SEGMENTATION_ID,
	        T.START_HISTORICAL_DATE = S.START_HISTORICAL_DATE,
	        T.CALCULATION_METHOD    = S.CALCULATION_METHOD,
	        T.HISTORICAL_MONTH      = S.HISTORICAL_MONTH,
	        T.UPDATED_BY            = 'SP_IFRS_LIFETIME_RULES_CONFIG',
	        T.UPDATED_DATE          = SYSDATE,
	        T.IS_DELETED            = S.IS_DELETED
	WHEN NOT MATCHED THEN
	    INSERT (
	        LIFETIME_RULE_NAME,
	        SYSCODE_LIFETIME,
	        SEGMENTATION_ID,
	        LIFETIME_METHOD,
	        START_HISTORICAL_DATE,
	        CALCULATION_METHOD,
	        HISTORICAL_MONTH,
	        CREATED_BY,
	        CREATED_DATE,
	        UPDATED_BY,
	        UPDATED_DATE,
	        IS_DELETED
	    )
	    VALUES (
	        S.LIFETIME_RULE_NAME,
	        S.SYSCODE_LIFETIME,
	        S.SEGMENTATION_ID,
	        S.LIFETIME_METHOD,
	        S.START_HISTORICAL_DATE,
	        S.CALCULATION_METHOD,
	        S.HISTORICAL_MONTH,
	        S.CREATED_BY,
	        S.CREATED_DATE,
	        NULL,
	        NULL,
	        S.IS_DELETED
	    );
	   
	   
       /* ========================================================================
       	  STEP 2 : INSERT KE IFRS_LIFETIME_OVERRIDE (HANYA DATA AKTIF & BELUM ADA)
       	  ======================================================================== */
	   INSERT INTO IFRS_LIFETIME_OVERRIDE (
	        LIFETIME_CONFIGURATION,
	        SYSCODE_LIFETIME,
	        DOWNLOAD_DATE,
	        LIFETIME_RATE,
	        LIFETIME_OVERRIDE,
	        CREATED_BY,
	        CREATED_DATE,
	        CREATED_HOST,
	        UPDATED_BY,
	        UPDATED_DATE,
	        UPDATED_HOST
	    )
		    SELECT
                R.LIFETIME_RULE_NAME        	AS LIFETIME_CONFIGURATION,
		        R.SYSCODE_LIFETIME 				AS SYSCODE_LIFETIME,
		        TRUNC(SYSDATE)               	AS DOWNLOAD_DATE,
		        0                          		AS LIFETIME_RATE, /* INI NANTI NYA AKAN DI JOIN KE TABLE IMA UNTUK DAPET NILAI LIFETIME */
		        0                          		AS LIFETIME_OVERRIDE,
		        'SP_IFRS_LIFETIME_RULES_CONFIG' AS CREATED_BY,
		        SYSDATE                       	AS CREATED_DATE,
		        'LOCALHOST' 					AS CREATED_HOST,
		        NULL                          	AS UPDATED_BY,
	    		NULL                          	AS UPDATED_DATE,
		        NULL                          	AS UPDATED_HOST
		    FROM IFRS_LIFETIME_RULES_CONFIG R
		    WHERE NVL(R.IS_DELETED, 0) = 0
		      AND NOT EXISTS (
		            SELECT 1
		            FROM IFRS_LIFETIME_OVERRIDE O
		            WHERE O.SYSCODE_LIFETIME = R.SYSCODE_LIFETIME
	      );
	   
	   	COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
	  
END;