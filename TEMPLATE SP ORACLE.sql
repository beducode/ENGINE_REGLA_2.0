CREATE OR REPLACE PROCEDURE IFRS9.XXXX (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AS
    -- DATES / COUNTERS
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'IFRS9';
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- RESULT QUERY
    V_QUERYS        CLOB;

    -- helper to print long text
    PROCEDURE PRINT_LONG(p_txt CLOB) IS
        v_pos INTEGER := 1;
        v_len INTEGER := DBMS_LOB.getlength(p_txt);
        v_step INTEGER := 30000;
        v_part VARCHAR2(32767);
    BEGIN
        WHILE v_pos <= v_len LOOP
            v_part := DBMS_LOB.SUBSTR(p_txt, v_step, v_pos);
            DBMS_OUTPUT.PUT_LINE(v_part);
            v_pos := v_pos + v_step;
        END LOOP;
    END PRINT_LONG;

BEGIN
    V_SP_NAME := 'SP_IFRS_PD_MAA_FLOWRATE';

    -- DETERMINE CURRENT DATE
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_MODEL_ID := NVL(P_SYSCODE, '0');

    IF P_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'GTMP_PD_FLOWRATE_CONFIG_' || P_RUNID;
        V_TABLEINSERT2 := 'IFRS_PD_MAA_FLOWRATE_' || P_RUNID;
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'GTMP_PD_FLOWRATE_CONFIG';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_FLOWRATE';
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    IFRS9.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    -- SIMULATION
    IF P_PRC = 'S' THEN
        -- CREATE/SELECT COPY FOR CONFIG
    	SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEPDCONFIG);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_RULES_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        
        
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLESELECT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLESELECT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_MAA_ENR WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- CREATE V_TABLEINSERT1
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.GTMP_PD_FLOWRATE_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- CREATE V_TABLEINSERT2
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_MAA_FLOWRATE WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

        
    BEGIN
        V_STR_QUERY := 'SELECT COUNT(*) FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
        EXECUTE IMMEDIATE V_STR_QUERY INTO V_COUNT;
    EXCEPTION
        WHEN OTHERS THEN
            V_COUNT := 0; 
    END;

    IF V_COUNT > 0 THEN
        V_STR_QUERY := 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- INSERT INTO GTMP_PD_RUNNING_CONFIG_<RUNID> (CONFIG DATA)
    
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                   ' (PD_RULE_ID, BUCKET_GROUP, HISTORICAL_DATA, CURR_DATE, PREV_DATE, TRANSITION_START_DATE, CUT_OFF_DATE) ' ||
                   ' SELECT PKID, BUCKET_GROUP, HISTORICAL_DATA, TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * INCREMENT_PERIOD)), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * HISTORICAL_DATA)), ' ||
                   ' CUT_OFF_DATE' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||
                   ' WHERE PD_METHOD = ''MAA'' AND PKID = ' || TO_NUMBER(V_MODEL_ID) || ' AND NVL(ACTIVE_FLAG,0) = 1';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- DELETE LOGIC ON TARGET FLOWRATE TABLE
    ----------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    ELSE
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' A ' ||
                       ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                       ' AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B WHERE B.SYSCODE_PD = A.SYSCODE_PD)';
    END IF;

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Delete on ' || V_TABLEINSERT2 || ' executed: ' || SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- POPULATE TMP_PD_BUCKET (ASSUMES TMP TABLE EXISTS AND ACCESSIBLE)
    ----------------------------------------------------------------
    V_STR_QUERY := 'TRUNCATE TABLE TMP_PD_BUCKET';
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    V_STR_QUERY := 'INSERT INTO TMP_PD_BUCKET (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, CALC_AMOUNT, PREV_DATE) ' ||
                   ' SELECT A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, SUM(A.CALC_AMOUNT), C.PREV_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLESELECT1 || ' A ' ||
                   ' JOIN ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' C ON A.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, C.PREV_DATE';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    -- NOTE: KEEP THE EXISTS SUBQUERY FORMATTED FOR ORACLE
    V_STR_QUERY := 'INSERT INTO TMP_PD_TOTAL (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, TOTAL) ' ||
                   ' SELECT X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, SUM(X.CALC_AMOUNT) ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLESELECT1 || ' X ' ||
                   ' JOIN ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' C ON X.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE (X.BUCKET_TO <> 0 OR (X.BUCKET_TO = 0 AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.IFRS_BUCKET_HEADER Y WHERE Y.BUCKET_GROUP = X.BUCKET_GROUP AND Y.INCLUDE_FULLYPAID_FLAG = 1))) ' ||
                   ' AND X.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Inserted into TMP_PD_TOTAL');

    ----------------------------------------------------------------
    -- INSERT INTO IFRS_PD_MAA_FLOWRATE (COMPUTE FLOWRATE)
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' (' ||
                   ' EFF_DATE, BASE_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, FLOWRATE, CREATEDBY, CREATEDDATE, CREATEDHOST) ' ||
                   ' SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' A.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE A.CALC_AMOUNT / B.TOTAL END, ' ||
                   '''SP_IFRS_PD_MAA_FLOWRATE'', SYSDATE, ''LOCALHOST'' ' ||
                   ' FROM TMP_PD_BUCKET A ' ||
                   ' JOIN TMP_PD_TOTAL B ON A.BUCKET_GROUP = B.BUCKET_GROUP AND A.PD_RULE_ID = B.PD_RULE_ID AND A.BUCKET_FROM = B.BUCKET_FROM ' ||
                   ' JOIN ' || V_TAB_OWNER || '.VW_IFRS_MAX_BUCKET D ON A.BUCKET_GROUP = D.BUCKET_GROUP ' ||
                   ' ORDER BY A.PD_RULE_ID, A.BUCKET_FROM, A.BUCKET_TO';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Inserted into ' || V_TABLEINSERT2);

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLEINSERT2;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (' || CASE WHEN V_MODEL_ID = '0' THEN '1=1' ELSE 'PD_RULE_ID = ' || V_MODEL_ID END || ')';

    IFRS9.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20002, 'SP_IFRS_PD_MAA_FLOWRATE FAILED: ' || SQLERRM);
END;