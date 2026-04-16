CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_VAS_TTC (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
) IS
    -- DATE
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_COUNT         NUMBER;
	V_MODEL_ID      VARCHAR2(22);

    -- DYNAMIC NAMES / QUERY
    V_STR_QUERY     CLOB;

    -- TABLE NAMES
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_GTMP_PD_TTC_CONFIG  VARCHAR2(100);
    V_IFRS_PD_VAS_TTC  VARCHAR2(100);
    V_TBLU_IFRS_TTC_PD  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- LOG / RESULT
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME        VARCHAR2(100) := 'SP_IFRS_PD_VAS_TTC';
    V_OPERATION     VARCHAR2(100);
    V_QUERYS        CLOB;
    V_RETURNROWS2   NUMBER;
BEGIN
    -- HANDLE DEFAULT DOWNLOAD DATE
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_PREVDATE := LAST_DAY(ADD_MONTHS(V_CURRDATE, -1));
    V_MODEL_ID := NVL(P_SYSCODE,'0');

    -- CHOOSE TABLE NAMES BASED ON MODE
    IF P_PRC = 'S' THEN
        V_GTMP_PD_TTC_CONFIG  := 'GTMP_PD_TTC_CONFIG_' || P_RUNID;
        V_IFRS_PD_VAS_TTC  := 'IFRS_PD_VAS_TTC_' || P_RUNID;
        V_TBLU_IFRS_TTC_PD  := 'TBLU_IFRS_TTC_PD_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_GTMP_PD_TTC_CONFIG  := 'GTMP_PD_TTC_CONFIG';
        V_IFRS_PD_VAS_TTC  := 'IFRS_PD_VAS_TTC';
        V_TBLU_IFRS_TTC_PD  := 'TBLU_IFRS_TTC_PD';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    BEGIN
        SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, 0, SYSTIMESTAMP);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: SP_IFRS_RUNNING_LOG FAILED: ' || SQLERRM);
    END;
    COMMIT;

    ----------------------------------------------------------------
    -- PREPARE: DROP+CREATE META TABLES (IF P_PRC = 'S' CREATE RUN-SPECIFIC TABLES)
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        -- DROP TABLE IF EXISTS (OWNED BY CURRENT SCHEMA)
        SELECT COUNT(*) INTO V_COUNT
        FROM USER_TABLES
        WHERE TABLE_NAME = UPPER(V_GTMP_PD_TTC_CONFIG);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TTC_CONFIG;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TTC_CONFIG || ' AS SELECT * FROM GTMP_PD_TTC_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        SELECT COUNT(*) INTO V_COUNT
        FROM USER_TABLES
        WHERE TABLE_NAME = UPPER(V_IFRS_PD_VAS_TTC);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC || ' AS SELECT * FROM IFRS_PD_VAS_TTC WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        COMMIT;
    ELSE
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TTC_CONFIG;
        
    --------------------------------------------------------
    -- DELETE OLD DATA
    --------------------------------------------------------
    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC || ' 
             WHERE EFF_DATE = ' || V_CURRDATE || ' 
             AND (PD_RULE_ID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    END IF;

    --------------------------------------------------------
    -- CREATE TEMP TABLE (GLOBAL TEMP OR NORMAL TABLE)
    --------------------------------------------------------
    EXECUTE IMMEDIATE '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_PD_TTC_CONFIG || ' (PD_RULE_ID,BUCKET_GROUP,BUCKET_GROUP_PD,HISTORICAL_DATA,CURR_DATE,PREV_DATE
				,TRANSITION_START_DATE, CUT_OFF_DATE)
        SELECT 
            A.PKID AS PD_RULE_ID,
            A.BUCKET_GROUP,
            A.BUCKET_GROUP_PD,
            A.HISTORICAL_DATA,
            TO_DATE(''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS CURR_DATE,
            LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -A.INCREMENT_PERIOD)) AS PREV_DATE,
            LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''',''YYYY-MM-DD''), -A.HISTORICAL_DATA)) AS TRANSITION_START_DATE,
            A.CUT_OFF_DATE
        FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || ' A
        WHERE (A.PKID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)
          AND A.PD_METHOD = ''VAS''
    ';
    --------------------------------------------------------
    -- INSERT TTC DATA USING CTE + RANK
    --------------------------------------------------------
	V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC || ' 
	    (
	        EFF_DATE, PD_RULE_ID, BUCKET_GROUP, BUCKET_ID, TTC_PD,
	        CORRELATION, VAS_RATING, INT_RATING, CREATEDBY, CREATEDDATE
	    )
	    WITH CTE AS (
	        SELECT 
	            A.EFFECTIVE_DATE, A.RATING, A.TTC_PD,
	            RANK() OVER (ORDER BY A.EFFECTIVE_DATE DESC) AS RN
	        FROM ' || V_TAB_OWNER || '.' || V_TBLU_IFRS_TTC_PD || ' A
	        WHERE A.EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
	    )
	    SELECT DISTINCT
	        ' || V_CURRDATE || ' AS EFF_DATE,
	        D.PD_RULE_ID,
	        D.BUCKET_GROUP_PD,
	        E.BUCKET_ID,
	        (CASE WHEN A.TTC_PD = 0 THEN 0.03 ELSE A.TTC_PD END) / 100 AS TTC_PD,
	        0.12 * (1 - EXP(-50 * (CASE WHEN A.TTC_PD = 0 THEN 0.03 ELSE A.TTC_PD END) / 100))
	            / (1 - EXP(-50))
	        + 0.24 *
	          (1 - (1 - EXP(-50 * (CASE WHEN A.TTC_PD = 0 THEN 0.03 ELSE A.TTC_PD END) / 100))
	                    / (1 - EXP(-50))) AS CORRELATION,
	        A.RATING AS EXT_RATING,
	        CASE WHEN C.BUCKET_ID >= 14 THEN ''DEF'' ELSE C.BUCKET_NAME END AS INT_RATING,
	        ''SP_IFRS_PD_VAS_TTC1'' AS CREATEDBY,
	        SYSTIMESTAMP AS CREATEDDATE
	    FROM ' || V_TAB_OWNER || '.' || V_GTMP_PD_TTC_CONFIG || ' D
	         JOIN CTE A ON 1 = 1
	         LEFT JOIN TBLM_COMMONCODEDETAIL B
	                ON B.COMMONCODE = ''S1045''
	               AND B.VALUE1 = ''Moody''''s''
	               AND A.RATING = B.VALUE2
	         JOIN IFRS_BUCKET_DETAIL C
	                ON D.BUCKET_GROUP = C.BUCKET_GROUP
	               AND C.BUCKET_NAME = B.VALUE3
	         JOIN IFRS_BUCKET_DETAIL E
	                ON D.BUCKET_GROUP_PD = E.BUCKET_GROUP
	               AND A.RATING = E.BUCKET_NAME
	    WHERE A.RN = 1';
    
	EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    -----------------------------
    -- Log & insert final data
    -----------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
   
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (PD_RULE_ID = ''' || V_MODEL_ID || ''' OR ''' || V_MODEL_ID || ''' = ''0'')';

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_PD_VAS_TTC: ' || SQLERRM);
        RAISE;
END;
/
