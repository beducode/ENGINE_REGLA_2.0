CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_MAA_FLOWRATE (
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

    -- Dynamic SQL (use VARCHAR2 large)
    V_STR_QUERY     VARCHAR2(32767);

    -- Table names (unqualified parts)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_GTMP_PD_FLOWRATE_CONFIG VARCHAR2(100);
    V_IFRS_PD_MAA_FLOWRATE VARCHAR2(100);
    V_IFRS_PD_MAA_ENR VARCHAR2(100);
    V_TABLEPDCONFIG   VARCHAR2(100); 

    -- misc
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- result query
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
        V_GTMP_PD_FLOWRATE_CONFIG := 'GTMP_PD_FLOWRATE_CONFIG_' || P_RUNID;
        V_IFRS_PD_MAA_FLOWRATE := 'IFRS_PD_MAA_FLOWRATE_' || P_RUNID;
        V_IFRS_PD_MAA_ENR := 'IFRS_PD_MAA_ENR_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE 
        V_GTMP_PD_FLOWRATE_CONFIG := 'GTMP_PD_FLOWRATE_CONFIG';
        V_IFRS_PD_MAA_FLOWRATE := 'IFRS_PD_MAA_FLOWRATE';
        V_IFRS_PD_MAA_ENR := 'IFRS_PD_MAA_ENR';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;
   
    -- SIMULATION
    IF P_PRC = 'S' THEN
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('GTMP_PD_FLOWRATE_CONFIG', V_GTMP_PD_FLOWRATE_CONFIG);
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_FLOWRATE', V_IFRS_PD_MAA_FLOWRATE);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_ENR', V_IFRS_PD_MAA_ENR);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_RULES_CONFIG', V_TABLEPDCONFIG);
    END IF;
    COMMIT;

    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOWRATE_CONFIG;
   
    COMMIT;

    ----------------------------------------------------------------
    -- INSERT INTO GTMP_PD_RUNNING_CONFIG_<RUNID> (CONFIG DATA)
    
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOWRATE_CONFIG ||
                   ' (PD_RULE_ID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, CURR_DATE, PREV_DATE, TRANSITION_START_DATE, CUT_OFF_DATE) ' ||
                   ' SELECT PKID,INCLUDE_CLOSE, BUCKET_GROUP, HISTORICAL_DATA, TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * INCREMENT_PERIOD)), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * HISTORICAL_DATA)), ' ||
                   ' START_HISTORICAL_DATE' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || ' WHERE PD_METHOD = ''MAA'' AND (  
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
    -- DELETE LOGIC ON TARGET FLOWRATE TABLE
    ----------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE ||
                       ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    ELSE
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE || ' A ' ||
                       ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                       ' AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOWRATE_CONFIG || ' B WHERE B.PD_RULE_ID = A.PD_RULE_ID)';
    END IF;

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Delete on ' || V_IFRS_PD_MAA_FLOWRATE || ' executed: ' || SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- POPULATE TMP_PD_BUCKET (ASSUMES TMP TABLE EXISTS AND ACCESSIBLE)
    ----------------------------------------------------------------
    V_STR_QUERY := 'TRUNCATE TABLE TMP_PD_BUCKET';
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    V_STR_QUERY := 'INSERT INTO TMP_PD_BUCKET (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, CALC_AMOUNT, PREV_DATE) ' ||
                   ' SELECT A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END AS BUCKET_TO, SUM(A.CALC_AMOUNT), C.PREV_DATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ENR || ' A ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOWRATE_CONFIG || ' C ON A.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN C.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END, C.PREV_DATE';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));
   
    V_STR_QUERY := 'TRUNCATE TABLE TMP_PD_TOTAL';
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO TMP_PD_TOTAL (PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, TOTAL) ' ||
                   ' SELECT X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM, SUM(X.CALC_AMOUNT) ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ENR || ' X ' ||
                   ' INNER JOIN ' || V_TAB_OWNER || '.' || V_GTMP_PD_FLOWRATE_CONFIG || ' C ON X.PD_RULE_ID = C.PD_RULE_ID ' ||
                   ' WHERE (X.BUCKET_TO <> 0 OR (X.BUCKET_TO = 0 AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || ' Y WHERE Y.BUCKET_GROUP = X.BUCKET_GROUP AND Y.INCLUDE_CLOSE = 1))) ' ||
                   ' AND X.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') ' ||
                   ' GROUP BY X.PD_RULE_ID, X.BUCKET_GROUP, X.BUCKET_FROM';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Inserted into TMP_PD_TOTAL');

    ----------------------------------------------------------------
    -- INSERT INTO IFRS_PD_MAA_FLOWRATE (COMPUTE FLOWRATE)
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE || ' (' ||
                   ' EFF_DATE, BASE_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, FLOWRATE,FLOWRATE_ORI , CREATEDBY, CREATEDDATE, CREATEDHOST) ' ||
                   ' SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' A.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, A.BUCKET_TO, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE A.CALC_AMOUNT / B.TOTAL END AS FLOWRATE, ' ||
                   ' CASE WHEN B.TOTAL = 0 THEN 0 WHEN A.BUCKET_FROM = D.MAX_BUCKET_ID AND A.BUCKET_TO = D.MAX_BUCKET_ID THEN 1 ELSE A.CALC_AMOUNT / B.TOTAL END AS FLOWRATE_ORI, ' ||
                   '''SP_IFRS_PD_MAA_FLOWRATE'', SYSDATE, ''LOCALHOST'' ' ||
                   ' FROM TMP_PD_BUCKET A ' ||
                   ' JOIN TMP_PD_TOTAL B ON A.BUCKET_GROUP = B.BUCKET_GROUP AND A.PD_RULE_ID = B.PD_RULE_ID AND A.BUCKET_FROM = B.BUCKET_FROM ' ||
                   ' JOIN ' || V_TAB_OWNER || '.VW_IFRS_MAX_BUCKET D ON A.BUCKET_GROUP = D.BUCKET_GROUP ' ||
                   ' ORDER BY A.PD_RULE_ID, A.BUCKET_FROM, A.BUCKET_TO';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('INSERTED INTO ' || V_IFRS_PD_MAA_FLOWRATE);

    ----------------------------------------------------------------
    -- LOGGING
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_FLOWRATE;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;