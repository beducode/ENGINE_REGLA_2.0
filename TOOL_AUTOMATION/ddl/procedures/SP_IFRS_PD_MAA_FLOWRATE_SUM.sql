CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_PD_MAA_FLOWRATE_SUM
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AS
    -- Dates / counters
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    V_STR_QUERY     CLOB;

    -- Table names (simple strings)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- misc
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- result query (might be long)
    V_QUERYS        CLOB;

BEGIN
    V_SP_NAME := 'SP_IFRS_PD_MAA_FLOWRATE_SUM';

    -- determine current date
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

    IF P_SYSCODE <> '0' THEN
   		V_MODEL_ID := '1';
    ELSE
    	V_MODEL_ID := '0';
    END IF;

    IF P_PRC = 'S' THEN
        V_TABLEINSERT1 := 'GTMP_PD_FLOW_SUM_CONFIG_' || P_RUNID;
        V_TABLEINSERT2 := 'IFRS_PD_MAA_FLOWRATE_SUM_' || P_RUNID;
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR_SUM_' || P_RUNID;
        V_TABLEPDCONFIG := 'GTMP_IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_TABLEINSERT1 := 'GTMP_PD_FLOW_SUM_CONFIG';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_FLOWRATE_SUM';
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR_SUM';
        V_TABLEPDCONFIG := 'GTMP_IFRS_PD_RULES_CONFIG';
    END IF;

    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
	
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLEPDCONFIG || '' ; 
	 
    V_STR_QUERY := 'INSERT INTO ' || V_TABLEPDCONFIG || ' (PKID, SYSCODE_PD, PD_RULE_NAME, SEGMENTATION_ID, PD_METHOD, START_HISTORICAL_DATE,' || 
						'CALC_METHOD, HISTORICAL_DATA, EXPECTED_LIFE, INCLUDE_INDIVIDUAL_ACCOUNT, INCLUDE_WO, INCLUDE_CLOSE, DEFAULT_RATIO_BY, ' ||
						'DEFAULT_RULE_ID, BUCKET_GROUP, ME_CODE, PERIOD_START_DATE, PERIOD_END_DATE, ' ||
					 	'IS_DELETED, INCREMENT_PERIOD, IS_ACTIVE, CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE)' ||
               'SELECT PKID, SYSCODE_PD, PD_RULE_NAME, SEGMENTATION_ID, PD_METHOD, START_HISTORICAL_DATE,' || 
						'CALC_METHOD, HISTORICAL_DATA, EXPECTED_LIFE, INCLUDE_INDIVIDUAL_ACCOUNT, INCLUDE_WO, INCLUDE_CLOSE, DEFAULT_RATIO_BY, ' ||
						'DEFAULT_RULE_ID, BUCKET_GROUP, ME_CODE, PERIOD_START_DATE, PERIOD_END_DATE, ' ||
					 	'IS_DELETED, INCREMENT_PERIOD, IS_ACTIVE, CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE' || 
					 	' FROM IFRS_PD_RULES_CONFIG ' ||
               'WHERE PD_METHOD = ''MAA'' AND NVL(IS_DELETED,0) = 0 AND (  
                 UPPER(TRIM(SYSCODE_PD)) IN ( 
                   SELECT UPPER(TRIM(REGEXP_SUBSTR(:p1, ''[^;]+'', 1, LEVEL)))
                   FROM DUAL
                   CONNECT BY REGEXP_SUBSTR(:p1, ''[^;]+'', 1, LEVEL) IS NOT NULL
                 )
                 OR :p2 = ''0'' 
               )';
              
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    EXECUTE IMMEDIATE V_STR_QUERY USING P_SYSCODE, P_SYSCODE, P_SYSCODE;
    ----------------------------------------------------------------
    -- PRE-SIMULATION TABLES (only in simulation mode P_PRC='S')
    -- drop/create structure-only copies (caller must have privileges)
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        -- drop/create V_TABLEINSERT1
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.GTMP_PD_FLOW_SUM_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- drop/create V_TABLEINSERT2
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_MAA_FLOWRATE_SUM WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- drop/create V_TABLESELECT1
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLESELECT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLESELECT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_MAA_ENR_SUM WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        
        -- drop/create V_TABLEPDCONFIG
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEPDCONFIG);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.GTMP_IFRS_PD_RULES_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- Ensure V_TABLEINSERT1 exists and clear it
    ----------------------------------------------------------------
   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
    COMMIT;

    ----------------------------------------------------------------
    -- Insert into config temp table (V_TABLEINSERT1)
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                   ' (PD_RULE_ID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, CURR_DATE, PREV_DATE, TRANSITION_START_DATE, CUT_OFF_DATE) ' ||
                   ' SELECT PKID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * INCREMENT_PERIOD)), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * HISTORICAL_DATA)), START_HISTORICAL_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||' ';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- Delete logic on target summary table
    ----------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    ELSE
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' A' ||
                       ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                       ' AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B WHERE B.PD_RULE_ID = A.PD_RULE_ID )';
    END IF;

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Delete executed: ' || SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- Clear session tmp tables TMP_PD_BUCKET_SUM & TMP_PD_TOTAL_SUM PAKAI TABLE SAMA.
    ----------------------------------------------------------------
   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PSAK413.TMP_PD_BUCKET';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PSAK413.TMP_PD_TOTAL';
   
    COMMIT;

    ----------------------------------------------------------------
    -- Populate TMP_PD_BUCKET_SUM
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO PSAK413.TMP_PD_BUCKET (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, CALC_AMOUNT) ' ||
                   ' SELECT A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END AS BUCKET_TO, SUM(A.CALC_AMOUNT) ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLESELECT1 || ' A ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' C ON A.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END ';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);

    ----------------------------------------------------------------
    -- Populate TMP_PD_TOTAL_SUM
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO PSAK413.TMP_PD_TOTAL (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, TOTAL, PREV_DATE) ' ||
                   ' SELECT X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, SUM(X.CALC_AMOUNT) AS TOTAL, C.PREV_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLESELECT1 || ' X ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' C ON X.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE (X.BUCKET_TO <> 0 OR (X.BUCKET_TO = 0 AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||' Y WHERE Y.BUCKET_GROUP = X.BUCKET_GROUP AND Y.INCLUDE_CLOSE = 1))) ' ||
                   ' AND X.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, C.PREV_DATE';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);

    ----------------------------------------------------------------
    -- Insert aggregated flowrate into target
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' (EFF_DATE, BASE_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, FLOWRATE,FLOWRATE_ORI, CREATEDBY, CREATEDDATE) ' ||
                   ' SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), B.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE (A.CALC_AMOUNT / B.TOTAL) END, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE (A.CALC_AMOUNT / B.TOTAL) END, ' ||
                   '''SP_IFRS_PD_MAA_FLOWRATE_SUM'', SYSDATE ' ||
                   ' FROM PSAK413.TMP_PD_BUCKET A ' ||
                   ' INNER JOIN PSAK413.TMP_PD_TOTAL B ON A.BUCKET_GROUP = B.BUCKET_GROUP AND A.PD_RULE_ID = B.PD_RULE_ID AND A.BUCKET_FROM = B.BUCKET_FROM ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.VW_IFRS_MAX_BUCKET D ON A.BUCKET_GROUP = D.BUCKET_GROUP ' ||
                   ' ORDER BY A.PD_RULE_ID, A.BUCKET_FROM, A.BUCKET_TO';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    ----------------------------------------------------------------
    -- LOG: call exec_and_log
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLEINSERT2;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT preview
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (' || CASE WHEN V_MODEL_ID = '0' THEN '1=1' ELSE 'PD_RULE_ID = ' || V_MODEL_ID END || ')';

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20003, 'SP_IFRS_PD_MAA_FLOWRATE_SUM failed: ' || SQLERRM);
END