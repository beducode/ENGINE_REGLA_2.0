CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_PD_MAA_ENR_SUM" (
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

    -- Dynamic SQL
    V_STR_QUERY     VARCHAR2(32767);

    -- Table names (unqualified parts)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- misc
    V_RETURNROWS    NUMBER;
    V_RETURNROWS2   NUMBER;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- result query
    V_QUERYS        CLOB;

    -- helper to print long SQL
    PROCEDURE PRINT_CLOB(p_clob CLOB) IS
        v_pos  INTEGER := 1;
        v_len  INTEGER := DBMS_LOB.getlength(p_clob);
        v_step INTEGER := 30000;
        v_part VARCHAR2(32767);
    BEGIN
        WHILE v_pos <= v_len LOOP
            v_part := DBMS_LOB.SUBSTR(p_clob, v_step, v_pos);
            DBMS_OUTPUT.PUT_LINE(v_part);
            v_pos := v_pos + v_step;
        END LOOP;
    END PRINT_CLOB;

    -- helper to build model filter
    FUNCTION MODEL_FILTER_RETURN(p_model_id VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF p_model_id IS NULL OR p_model_id = '0' THEN
            RETURN '1=1';
        ELSE
            -- PKID is numeric, so don't quote
            RETURN 'PKID = ' || TO_NUMBER(p_model_id);
        END IF;
    EXCEPTION
        WHEN VALUE_ERROR THEN
            -- if p_model_id not numeric, fallback to false condition
            RETURN '1=0';
    END MODEL_FILTER_RETURN;

BEGIN
    V_SP_NAME := 'SP_IFRS_PD_MAA_ENR_SUM';

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
        V_TABLEINSERT1 := 'GTMP_PD_RUNNING_ENR_SUM_' || P_RUNID;
        V_TABLEINSERT2 := 'IFRS_PD_MAA_ENR_SUM_' || P_RUNID;
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR_' || P_RUNID;
        V_TABLEPDCONFIG := 'GTMP_IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'GTMP_PD_RUNNING_ENR_SUM';
        V_TABLEINSERT2 := 'IFRS_PD_MAA_ENR_SUM';
        V_TABLESELECT1 := 'IFRS_PD_MAA_ENR';
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
               'WHERE PD_METHOD = ''MAA'' AND (  
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
    -- PRE-SIMULATION TABLES: create/drop temp tables if P_PRC = 'S'
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        -- DROP/Create V_TABLESELECT1 (copy structure only)
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

        -- DROP/Create V_TABLEINSERT1
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.GTMP_PD_RUNNING_ENR_SUM WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        
        -- DROP/Create V_TABLEPDCONFIG
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

        -- DROP/Create V_TABLEINSERT2
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_MAA_ENR_SUM WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    ELSE 
    	V_STR_QUERY := 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    
    COMMIT;

    ----------------------------------------------------------------
    -- Insert into temp config table (dynamic INSERT...SELECT)
    ----------------------------------------------------------------
     V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                   ' (PD_RULE_ID, BUCKET_GROUP, HISTORICAL_DATA, ' ||
                   ' CURR_DATE, INCREMENT_PERIOD, PREV_DATE, TRANSITION_START_DATE, CUT_OFF_DATE,INCLUDE_CLOSE) ' ||
                   ' SELECT PKID, BUCKET_GROUP, HISTORICAL_DATA,  ' ||
                   ' TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), ' ||
                   ' INCREMENT_PERIOD, LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * INCREMENT_PERIOD)), ' ||
                   ' LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -1 * HISTORICAL_DATA)), ' ||
                   ' START_HISTORICAL_DATE , INCLUDE_CLOSE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||
                   ' WHERE PD_METHOD = ''MAA'' AND NVL(IS_DELETED,0) = 0';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- Delete logic on target
    ----------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
                       ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    ELSE
        V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' A ' ||
                       ' WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                       ' AND EXISTS (SELECT 1 FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B WHERE B.PD_RULE_ID = A.PD_RULE_ID)';
    END IF;

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- Insert aggregated data into target summary
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' ( ' ||
                   ' EFF_DATE, BASE_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_FROM, BUCKET_TO, CALC_AMOUNT, CREATEDBY, CREATEDDATE) ' ||
                   ' SELECT B.CURR_DATE, B.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN B.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END AS BUCKET_TO, SUM(A.CALC_AMOUNT), ' ||
                   '''SP_IFRS_PD_MAA_ENR_SUM'', SYSDATE ' ||
                   ' FROM ' || V_TAB_OWNER || '.' || V_TABLESELECT1 || ' A ' ||
                   ' JOIN ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B ON A.PD_RULE_ID = B.PD_RULE_ID ' ||
                   ' AND A.EFF_DATE BETWEEN B.TRANSITION_START_DATE AND B.CURR_DATE ' ||
                   ' WHERE A.EFF_DATE >= B.CUT_OFF_DATE ' ||
                   ' GROUP BY B.CURR_DATE, B.PREV_DATE, A.PD_RULE_ID, A.BUCKET_GROUP, A.BUCKET_FROM, CASE WHEN B.INCLUDE_CLOSE = 1 AND A.BUCKET_TO = 0 THEN 1 ELSE A.BUCKET_TO END';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- LOG: call exec_and_log (assumed signature)
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
            ' AND (PD_RULE_ID IN (SELECT PKID FROM ' || V_TABLEPDCONFIG || ') )';
    
    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20001, 'SP_IFRS_PD_MAA_ENR_SUM failed: ' || SQLERRM);
END