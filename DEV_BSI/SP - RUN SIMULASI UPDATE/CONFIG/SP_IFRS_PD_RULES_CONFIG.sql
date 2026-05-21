CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_RULES_CONFIG';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);


    -- MISC
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);

    -- RESULT QUERY
    V_QUERYS        CLOB;
BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM PSAK413.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    V_TABLEINSERT1 := 'IFRS_PD_RULES_CONFIG';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
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
			
	COMMIT;

    ------------------------------------------------------------------

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;