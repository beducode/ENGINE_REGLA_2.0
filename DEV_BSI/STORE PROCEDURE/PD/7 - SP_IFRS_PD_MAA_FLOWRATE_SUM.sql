CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_MAA_FLOWRATE_SUM (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    -- Dates / counters
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    V_STR_QUERY     CLOB;

    -- Table names (simple strings)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_GTMP_PD_FLOW_SUM_CONFIG   VARCHAR2(100);
    V_IFRS_PD_MAA_FLOWRATE_SUM   VARCHAR2(100);
    V_IFRS_PD_MAA_ENR_SUM       VARCHAR2(100);
    V_TMP_PD_BUCKET             VARCHAR2(100);
    V_TMP_PD_TOTAL              VARCHAR2(100);
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
        V_GTMP_PD_FLOW_SUM_CONFIG := 'GTMP_PD_FLOW_SUM_CONFIG_' || P_RUNID;
        V_IFRS_PD_MAA_FLOWRATE_SUM := 'IFRS_PD_MAA_FLOWRATE_SUM_' || P_RUNID;
        V_IFRS_PD_MAA_ENR_SUM := 'IFRS_PD_MAA_ENR_SUM_' || P_RUNID;
        V_TMP_PD_BUCKET := 'TMP_PD_BUCKET_' || P_RUNID;
        V_TMP_PD_TOTAL := 'TMP_PD_TOTAL_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_GTMP_PD_FLOW_SUM_CONFIG := 'GTMP_PD_FLOW_SUM_CONFIG';
        V_IFRS_PD_MAA_FLOWRATE_SUM := 'IFRS_PD_MAA_FLOWRATE_SUM';
        V_IFRS_PD_MAA_ENR_SUM := 'IFRS_PD_MAA_ENR_SUM';
        V_TMP_PD_BUCKET := 'TMP_PD_BUCKET';
        V_TMP_PD_TOTAL := 'TMP_PD_TOTAL';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    ----------------------------------------------------------------
    -- SIMULATION
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('GTMP_PD_FLOW_SUM_CONFIG', V_GTMP_PD_FLOW_SUM_CONFIG);
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_FLOWRATE_SUM', V_IFRS_PD_MAA_FLOWRATE_SUM);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_ENR_SUM', V_IFRS_PD_MAA_ENR_SUM);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_RULES_CONFIG', V_TABLEPDCONFIG);
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('TMP_PD_BUCKET', V_TMP_PD_BUCKET);
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('TMP_PD_TOTAL', V_TMP_PD_TOTAL);
    END IF;
    COMMIT;

    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    ----------------------------------------------------------------
    -- ENSURE V_TABLEINSERT1 EXISTS AND CLEAR IT
    ----------------------------------------------------------------
   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOW_SUM_CONFIG;
    COMMIT;

    ----------------------------------------------------------------
    -- INSERT INTO CONFIG TEMP TABLE (V_TABLEINSERT1)
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOW_SUM_CONFIG ||
                   ' (PD_RULE_ID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, CURR_DATE, PREV_DATE, TRANSITION_START_DATE, CUT_OFF_DATE) ' ||
                   ' SELECT PKID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * INCREMENT_PERIOD)), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * HISTORICAL_DATA)), START_HISTORICAL_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||' WHERE PD_METHOD = ''MAA'' AND (  
                        UPPER(TRIM(SYSCODE_PD)) IN ( 
                        SELECT UPPER(TRIM(REGEXP_SUBSTR(:1, ''[^;]+'', 1, LEVEL)))
                        FROM DUAL
                        CONNECT BY REGEXP_SUBSTR(:2, ''[^;]+'', 1, LEVEL) IS NOT NULL
                        )
                        OR :3 = ''0'' 
                    )';

    EXECUTE IMMEDIATE V_STR_QUERY USING P_SYSCODE, P_SYSCODE, P_SYSCODE;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- Delete logic on target summary table
    ----------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE_SUM ||
                       ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    ELSE
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE_SUM || ' A' ||
                       ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                       ' AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOW_SUM_CONFIG || ' B WHERE B.PD_RULE_ID = A.PD_RULE_ID )';
    END IF;

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Delete executed: ' || SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- CLEAR SESSION TMP TABLES TMP_PD_BUCKET_SUM & TMP_PD_TOTAL_SUM PAKAI TABLE SAMA.
    ----------------------------------------------------------------
   
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_TMP_PD_BUCKET;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_TMP_PD_TOTAL;
   
    COMMIT;

    ----------------------------------------------------------------
    -- POPULATE TMP_PD_BUCKET_SUM
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TMP_PD_BUCKET || ' (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, CALC_AMOUNT) ' ||
                   ' SELECT A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END AS BUCKET_TO, SUM(A.CALC_AMOUNT) ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ENR_SUM || ' A ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOW_SUM_CONFIG || ' C ON A.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END ';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);

    ----------------------------------------------------------------
    -- POPULATE TMP_PD_TOTAL_SUM
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TMP_PD_TOTAL || ' (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, TOTAL, PREV_DATE) ' ||
                   ' SELECT X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, SUM(X.CALC_AMOUNT) AS TOTAL, C.PREV_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ENR_SUM || ' X ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOW_SUM_CONFIG || ' C ON X.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE (X.BUCKET_TO <> 0 OR (X.BUCKET_TO = 0 AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||' Y WHERE Y.BUCKET_GROUP = X.BUCKET_GROUP AND Y.INCLUDE_CLOSE = 1))) ' ||
                   ' AND X.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, C.PREV_DATE';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);

    ----------------------------------------------------------------
    -- INSERT AGGREGATED FLOWRATE INTO TARGET
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE_SUM || ' (EFF_DATE, BASE_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, FLOWRATE,FLOWRATE_ORI, CREATEDBY, CREATEDDATE) ' ||
                   ' SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), B.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE (A.CALC_AMOUNT / B.TOTAL) END, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE (A.CALC_AMOUNT / B.TOTAL) END, ' ||
                   '''SP_IFRS_PD_MAA_FLOWRATE_SUM'', SYSDATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TMP_PD_BUCKET || ' A ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_TMP_PD_TOTAL || ' B ON A.BUCKET_GROUP = B.BUCKET_GROUP AND A.PD_RULE_ID = B.PD_RULE_ID AND A.BUCKET_FROM = B.BUCKET_FROM ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.VW_IFRS_MAX_BUCKET D ON A.BUCKET_GROUP = D.BUCKET_GROUP ' ||
                   ' ORDER BY A.PD_RULE_ID, A.BUCKET_FROM, A.BUCKET_TO';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    ----------------------------------------------------------------
    -- LOG:
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE_SUM;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE_SUM;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;