CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_CCF_RULES_CONFIG(
	P_DOWNLOAD_DATE IN DATE
)
AUTHID CURRENT_USER
AS
BEGIN
    /* =========================================================
       STEP 1 : MERGE DATA KE IFRS_CCF_RULES_CONFIG
       ========================================================= */
	MERGE INTO IFRS_CCF_RULES_CONFIG T
	USING (
		SELECT
			A."ccf_name"          				AS CCF_RULE_NAME,
		    A."syscode_ccf_config"  			AS SYSCODE_CCF,
		    B."pkid"                     		AS SEGMENTATION_ID,
		    A."calculation_method"				AS CALC_METHOD,
		    A."method"							AS AVERAGE_METHOD,
		    0									AS DEFAULT_RULE_ID,
		    0									AS CCF_OVERRIDE,
		    A."is_active"                   	AS ACTIVE_FLAG,
		    NVL(A."is_deleted", 0)        		AS IS_DELETE,
		    'SP_IFRS_PREPAYMENT_RULES_CONFIG' 	AS CREATED_BY,
		    NVL(A."created_date", SYSDATE)      AS CREATED_DATE,
		    NULL                          		AS UPDATED_BY,
		    NULL                          		AS UPDATED_DATE
		FROM "NTT_PSAK413_IMPAIRMENT"."CcfConfiguration"@DBCONFIGLINK A
		LEFT JOIN "NTT_PSAK413_IMPAIRMENT"."SegmentationMapping"@DBCONFIGLINK B
    		ON A."segment_code" = B."syscode_segmentation_lv3"
		WHERE B."pkid" IS NOT NULL
	) S
	ON (
	    T.SYSCODE_CCF = S.SYSCODE_CCF
	)
	WHEN MATCHED THEN
	    UPDATE SET
	        T.CCF_RULE_NAME			= S.CCF_RULE_NAME,
            T.SEGMENTATION_ID 		= S.SEGMENTATION_ID,
	        T.UPDATEDBY           	= 'SP_IFRS_CCF_RULES_CONFIG',
	        T.ACTIVE_FLAG		   	= S.ACTIVE_FLAG,
	        T.UPDATEDDATE         	= SYSDATE,
	        T.IS_DELETE           	= S.IS_DELETE
	WHEN NOT MATCHED THEN
	    INSERT (
	        CCF_RULE_NAME,
	        SYSCODE_CCF,
	        SEGMENTATION_ID,
	        CALC_METHOD,
	        AVERAGE_METHOD,
	        DEFAULT_RULE_ID,
	        CCF_OVERRIDE,
	        ACTIVE_FLAG,
	        IS_DELETE,
	        CREATEDBY,
	        CREATEDDATE,
	        UPDATEDBY,
	        UPDATEDDATE
	    )
	    VALUES (
	        S.CCF_RULE_NAME,
	        S.SYSCODE_CCF,
	        S.SEGMENTATION_ID,
	        S.CALC_METHOD,
	        S.AVERAGE_METHOD,
	        S.DEFAULT_RULE_ID,
	        S.CCF_OVERRIDE,
	        S.ACTIVE_FLAG,
	        S.IS_DELETE,
	        S.CREATED_BY,
	        S.CREATED_DATE,
	        NULL,
	        NULL
	    );
	   
	   
       /* ========================================================================
       	  STEP 2 : INSERT KE IFRS_CCF_OVERRIDE (HANYA DATA AKTIF & BELUM ADA)
       	  ======================================================================== */
			INSERT INTO IFRS_CCF_OVERRIDE (
				CCF_CONFIGURATION,
				SYSCODE_CCF,
				DOWNLOAD_DATE,
				CCF_RATE,
				CCF_OVERRIDE,
				CREATEDBY,
				CREATEDDATE,
				CREATEDHOST,
				UPDATEDBY,
				UPDATEDDATE,
				UPDATEDHOST
				)
					SELECT
						R.CCF_RULE_NAME          		AS CCF_CONFIGURATION,
						R.SYSCODE_CCF 					AS SYSCODE_CCF,
						TRUNC(SYSDATE)               	AS DOWNLOAD_DATE,
						0                          		AS CCF_RATE, /* INI NANTI NYA AKAN DI JOIN KE TABLE IMA UNTUK DAPET NILAI CCF */
						0                          		AS CCF_OVERRIDE,
						'SP_IFRS_CCF_RULES_CONFIG' 		AS CREATEDBY,
						SYSDATE                       	AS CREATEDDATE,
						'LOCALHOST' 					AS CREATEDHOST,
						NULL                          	AS UPDATEDBY,
						NULL                          	AS UPDATEDDATE,
						NULL                          	AS UPDATEDHOST
					FROM IFRS_CCF_RULES_CONFIG R
					WHERE NVL(R.IS_DELETE, 0) = 0
					AND NOT EXISTS (
							SELECT 1
							FROM IFRS_CCF_OVERRIDE O
							WHERE O.SYSCODE_CCF = R.SYSCODE_CCF
				);
		
			COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
	  
END;