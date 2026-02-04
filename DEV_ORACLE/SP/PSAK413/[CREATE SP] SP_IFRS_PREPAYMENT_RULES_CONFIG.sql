CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PREPAYMENT_RULES_CONFIG(
	P_DOWNLOAD_DATE IN DATE
)
AUTHID CURRENT_USER
AS
BEGIN
    /* =========================================================
       STEP 1 : MERGE DATA KE IFRS_PREPAYMENT_RULES_CONFIG
       ========================================================= */
	MERGE INTO IFRS_PREPAYMENT_RULES_CONFIG T
	USING (
	    SELECT
        	A."prepayment_name"          		AS PREPAYMENT_RULE_NAME,
	        A."syscode_prepayment_config"  		AS SYSCODE_PREPAYMENT,
	        B."pkid"                     		AS SEGMENTATION_ID,
	        A."is_active"                   	AS ACTIVE_FLAG,
	        NVL(A."is_deleted", 0)        		AS IS_DELETE,
	        'SP_IFRS_PREPAYMENT_RULES_CONFIG' 	AS CREATED_BY,
	        NVL(A."created_date", SYSDATE)      AS CREATED_DATE,
	        NULL                          		AS UPDATED_BY,
	        NULL                          		AS UPDATED_DATE
	    FROM "NTT_PSAK413_IMPAIRMENT"."PrepaymentConfiguration"@DBCONFIGLINK A
	    LEFT JOIN "NTT_PSAK413_IMPAIRMENT"."SegmentationMapping"@DBCONFIGLINK B
	        ON A."segment_code" = B."syscode_segmentation_lv3"
	    WHERE B."pkid" IS NOT NULL
	) S
	ON (
	    T.SYSCODE_PREPAYMENT = S.SYSCODE_PREPAYMENT
	)
	WHEN MATCHED THEN
	    UPDATE SET
	        T.PREPAYMENT_RULE_NAME	= S.PREPAYMENT_RULE_NAME,
            T.SEGMENTATION_ID 		= S.SEGMENTATION_ID,
	        T.UPDATEDBY           	= 'SP_IFRS_PREPAYMENT_RULES_CONFIG',
	        T.ACTIVE_FLAG		   	= S.ACTIVE_FLAG,
	        T.UPDATEDDATE         	= SYSDATE,
	        T.IS_DELETE           	= S.IS_DELETE
	WHEN NOT MATCHED THEN
	    INSERT (
	        PREPAYMENT_RULE_NAME,
	        SYSCODE_PREPAYMENT,
	        SEGMENTATION_ID,
	        ACTIVE_FLAG,
	        IS_DELETE,
	        CREATEDBY,
	        CREATEDDATE,
	        UPDATEDBY,
	        UPDATEDDATE
	    )
	    VALUES (
	        S.PREPAYMENT_RULE_NAME,
	        S.SYSCODE_PREPAYMENT,
	        S.SEGMENTATION_ID,
	        S.ACTIVE_FLAG,
	        S.IS_DELETE,
	        S.CREATED_BY,
	        S.CREATED_DATE,
	        NULL,
	        NULL
	    );
	   
	   
       /* ========================================================================
       	  STEP 2 : INSERT KE IFRS_PREPAYMENT_OVERRIDE (HANYA DATA AKTIF & BELUM ADA)
       	  ======================================================================== */
	   INSERT INTO IFRS_PREPAYMENT_OVERRIDE (
		PREPAYMENT_CONFIGURATION,
		SYSCODE_PREPAYMENT,
		DOWNLOAD_DATE,
		PREPAYMENT_RATE,
		PREPAYMENT_OVERRIDE,
		CREATEDBY,
		CREATEDDATE,
		CREATEDHOST,
		UPDATEDBY,
		UPDATEDDATE,
		UPDATEDHOST
		
	    )
		    SELECT
	        	R.PREPAYMENT_RULE_NAME          AS PREPAYMENT_CONFIGURATION,
	        	R.SYSCODE_PREPAYMENT 			AS SYSCODE_PREPAYMENT,
		        TRUNC(SYSDATE)               	AS DOWNLOAD_DATE,
		        0                          		AS PREPAYMENT_RATE, /* INI NANTI NYA AKAN DI JOIN KE TABLE IMA UNTUK DAPET NILAI PREPAYMENT */
		        0                          		AS PREPAYMENT_OVERRIDE,
		        'SP_IFRS_PREPAYMENT_RULES_CONFIG' AS CREATED_BY,
		        SYSDATE                       	AS CREATED_DATE,
		        'LOCALHOST' 					AS CREATED_HOST,
		        NULL                          	AS UPDATED_BY,
	    		NULL                          	AS UPDATED_DATE,
		        NULL                          	AS UPDATED_HOST
		    FROM IFRS_PREPAYMENT_RULES_CONFIG R
		    WHERE NVL(R.IS_DELETE, 0) = 0
		      AND NOT EXISTS (
		            SELECT 1
		            FROM IFRS_PREPAYMENT_OVERRIDE O
		            WHERE O.SYSCODE_PREPAYMENT = R.SYSCODE_PREPAYMENT
	      );
	   
	   	COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
	  
END;