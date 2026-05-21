CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_TERM_STRUCTURE_VAS (
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
	
    V_BASE_DATE     DATE;
    V_MAX_DATE      DATE;
    V_SEQ           NUMBER;
    V_MAX_SEQ       NUMBER;
    V_MAX_SEQ1      NUMBER;
    V_MONTH_MAX     NUMBER;

    -- DYNAMIC NAMES / QUERY
    V_STR_QUERY     CLOB;

    -- TABLE NAMES
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_IFRS_PD_VAS_MPD  		 VARCHAR2(100);
    V_IFRS_PD_TERM_STRUCTURE  		 VARCHAR2(100);
    V_GTMP_TERM_STRUCTURE 	 VARCHAR2(100);
    V_TABLEPDCONFIG 		 VARCHAR2(100);
	V_GTMP_LOOP				VARCHAR2(100);
    
    -- LOG / RESULT
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME        VARCHAR2(100) := 'SP_IFRS_PD_TERM_STRUCTURE_VAS';
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
        V_IFRS_PD_TERM_STRUCTURE  := 'IFRS_PD_TERM_STRUCTURE_' || P_RUNID;
        V_GTMP_TERM_STRUCTURE  := 'GTMP_TERM_STRUCTURE_' || P_RUNID;
    	V_IFRS_PD_VAS_MPD := 'IFRS_PD_VAS_MPD_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    	V_GTMP_LOOP := 'GTMP_LOOP_' || P_RUNID;
    ELSE
        V_IFRS_PD_TERM_STRUCTURE  := 'IFRS_PD_TERM_STRUCTURE';
        V_GTMP_TERM_STRUCTURE  := 'GTMP_TERM_STRUCTURE';
    	V_IFRS_PD_VAS_MPD := 'IFRS_PD_VAS_MPD';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    	V_GTMP_LOOP := 'GTMP_LOOP';
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
        WHERE TABLE_NAME = UPPER(V_GTMP_TERM_STRUCTURE);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE || ' AS SELECT * FROM GTMP_TERM_STRUCTURE WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        
        -- DROP TABLE IF EXISTS (OWNED BY CURRENT SCHEMA)
        SELECT COUNT(*) INTO V_COUNT
        FROM USER_TABLES
        WHERE TABLE_NAME = UPPER(V_GTMP_LOOP);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_GTMP_LOOP;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_LOOP || ' AS SELECT * FROM GTMP_LOOP WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        COMMIT;
    ELSE
	    --------------------------------------------------------
	    -- DELETE OLD DATA
	    --------------------------------------------------------
	    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' 
	             WHERE EFF_DATE = ' || V_CURRDATE || ' 
				 AND PD_METHOD = ''VAS''
	             AND (PD_RULE_ID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)';
	
	    EXECUTE IMMEDIATE V_STR_QUERY;
	    
	    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TERM_STRUCTURE';
	    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_LOOP';
	    
    END IF;
	COMMIT;
    -----------------------------------------------------------------
    -- Populate GTMP_TERM_STRUCTURE (equiv of SELECT ... INTO #TMP_TERM_STRUCTURE)
    -----------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE ||' (
            EFF_DATE, BASE_DATE, MODEL_ID, PD_RULE_ID, FL_SEQ, INCREMENT_PERIOD,
            BUCKET_GROUP, BUCKET_ID, PD, PD_METHOD, PD_RULE_NAME, MAX_BUCKET_ID
        )
        SELECT
            A.EFF_DATE, A.BASE_DATE, 0 AS MODEL_ID, A.PD_RULE_ID, A.FL_SEQ,
            B.INCREMENT_PERIOD, A.BUCKET_GROUP, A.BUCKET_ID, SUM(A.PD * (E.ADJUSTMENT_VALUE / 100)),
            B.PD_METHOD, B.PD_RULE_NAME,  C.MAX_BUCKET_ID
        FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD ||' A
        JOIN ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||' B 
          ON A.PD_RULE_ID = B.PKID
        JOIN ' || V_TAB_OWNER || '.VW_IFRS_MAX_BUCKET C 
          ON A.BUCKET_GROUP = C.BUCKET_GROUP
        JOIN ' || V_TAB_OWNER || '.IFRS_ME_SCALAR_HEADER D 
          ON A.PD_RULE_ID = D.RULE_ID AND D.MODEL_ID = 0
        JOIN ' || V_TAB_OWNER || '.IFRS_ME_SCALAR_DETAIL E 
          ON D.PKID = E.SCALAR_ID AND A.SCENARIO_NO = E.SCENARIO_NO
        WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
          AND B.PD_METHOD = ''VAS''
          AND (A.PD_RULE_ID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)
        GROUP BY A.EFF_DATE, A.BASE_DATE, A.PD_RULE_ID, A.FL_SEQ,
                 B.INCREMENT_PERIOD, A.BUCKET_GROUP, A.BUCKET_ID,
                 B.PD_METHOD, B.PD_RULE_NAME, C.MAX_BUCKET_ID ';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    -----------------------------------------------------------------
    -- Build GTMP_LOOP using recursive WITH (CTE) and insert results
    -----------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_LOOP ||' (PERIOD, END_DATE, SEQ, SEQ1)
        WITH cte_date (period, end_date) AS (
            SELECT 
                ADD_MONTHS(TRUNC(:v_currdate, ''YYYY''), 12) AS period,
                (SELECT MAX(BASE_DATE) FROM ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE ||') AS end_date
            FROM dual
            UNION ALL
            SELECT 
                ADD_MONTHS(period, 1), 
                end_date
            FROM cte_date
            WHERE ADD_MONTHS(period, 1) <= end_date
        )
        SELECT
            period,
            end_date,
            ROW_NUMBER() OVER (
                PARTITION BY EXTRACT(YEAR FROM period) 
                ORDER BY period
            ) AS seq,
            ROW_NUMBER() OVER (ORDER BY period) AS seq1
        FROM cte_date ';

    EXECUTE IMMEDIATE V_STR_QUERY
        USING V_CURRDATE;
    -----------------------------------------------------------------
    -- Extract max values and initialize loop counters
    -----------------------------------------------------------------
    V_STR_QUERY := 'SELECT MAX(BASE_DATE), MAX(FL_SEQ)
    FROM ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE ||'';

	EXECUTE IMMEDIATE V_STR_QUERY
  	INTO V_MAX_DATE , V_MAX_SEQ ;

    V_STR_QUERY := '
    SELECT MIN(PERIOD), MAX(SEQ), MAX(SEQ1)
    FROM ' || V_TAB_OWNER || '.' || V_GTMP_LOOP ||'';

	EXECUTE IMMEDIATE V_STR_QUERY
  	INTO V_BASE_DATE , V_MONTH_MAX, V_MAX_SEQ1 ;

    V_SEQ := 0;  -- start sequence counter per original logic
	COMMIT;
    -----------------------------------------------------------------
    -- Main loop: iterate period by period
    -----------------------------------------------------------------
    WHILE V_BASE_DATE <= V_MAX_DATE AND V_SEQ <= V_MAX_SEQ1 LOOP

        V_STR_QUERY := '
    INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE ||' (
        EFF_DATE, BASE_DATE, SCENARIO_NO, PD_RULE_ID, PD_METHOD,
        PD_RULE_NAME, MODEL_ID, BUCKET_GROUP, BUCKET_ID,
        FL_SEQ, FL_YEAR, FL_MONTH, PD, PD_OVERRIDE, CREATEDDATE, CREATEDBY
    )
    SELECT
        B.EFF_DATE,
        :V_BASE_DATE AS BASE_DATE,
        0 AS SCENARIO_NO,
        B.PD_RULE_ID,
        B.PD_METHOD,
        B.PD_RULE_NAME,
        B.MODEL_ID,
        B.BUCKET_GROUP,
        B.BUCKET_ID,
        A.SEQ1 - 1 AS FL_SEQ,
        TRUNC(:V_SEQ / 12) + 1 AS FL_YEAR,
        A.SEQ AS FL_MONTH,
        B.PD / 12 AS PD,
        B.PD / 12 AS PD_OVERRIDE,
        SYSDATE AS CREATEDDATE,
        ''SP_IFRS_PD_TERM_STRUCTURE_VAS1'' AS CREATEDBY
    FROM ' || V_TAB_OWNER || '.' || V_GTMP_LOOP ||' A
    JOIN ' || V_TAB_OWNER || '.' || V_GTMP_TERM_STRUCTURE ||' B
      ON A.PERIOD = :V_BASE_DATE
     AND B.FL_SEQ = TRUNC(:V_SEQ / B.INCREMENT_PERIOD)';

	EXECUTE IMMEDIATE V_STR_QUERY
    USING V_BASE_DATE, V_SEQ, V_BASE_DATE, V_SEQ;
	COMMIT;
        -- next period
        V_BASE_DATE := ADD_MONTHS(V_BASE_DATE, 1);
        V_SEQ := V_SEQ + 1;
    END LOOP;
    
    -----------------------------
    -- Log & insert final data
    -----------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
   
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (PD_RULE_ID = ''' || V_MODEL_ID || ''' OR ''' || V_MODEL_ID || ''' = ''0'')';

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_PD_TERM_STRUCTURE_VAS: ' || SQLERRM);
        RAISE;
END;
/
