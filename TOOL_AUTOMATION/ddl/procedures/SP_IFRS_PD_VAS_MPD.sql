CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_PD_VAS_MPD
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
	V_MAX_FL		NUMBER := 0;
	V_FL_PRC		NUMBER := 0;

    -- DYNAMIC NAMES / QUERY
    V_STR_QUERY     CLOB;

    -- TABLE NAMES
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_IFRS_PD_VAS_MPD  		 VARCHAR2(100);
    V_IFRS_PD_VAS_TTC  		 VARCHAR2(100);
    V_IFRS_PD_VAS_ZSCORE 	 VARCHAR2(100);
    V_TABLEPDCONFIG 		 VARCHAR2(100);

    -- LOG / RESULT
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME        VARCHAR2(100) := 'SP_IFRS_PD_VAS_MPD';
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
        V_IFRS_PD_VAS_MPD  := 'IFRS_PD_VAS_MPD_' || P_RUNID;
        V_IFRS_PD_VAS_TTC  := 'IFRS_PD_VAS_TTC_' || P_RUNID;
    	V_IFRS_PD_VAS_ZSCORE := 'IFRS_PD_VAS_ZSCORE_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_IFRS_PD_VAS_MPD  := 'IFRS_PD_VAS_MPD';
        V_IFRS_PD_VAS_TTC  := 'IFRS_PD_VAS_TTC';
    	V_IFRS_PD_VAS_ZSCORE := 'IFRS_PD_VAS_ZSCORE';
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
        WHERE TABLE_NAME = UPPER(V_IFRS_PD_VAS_MPD);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' AS SELECT * FROM IFRS_PD_VAS_MPD WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        COMMIT;
    ELSE
    
	    --------------------------------------------------------
	    -- DELETE OLD DATA
	    --------------------------------------------------------
	    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' 
	             WHERE EFF_DATE = ' || V_CURRDATE || ' 
	             AND (PD_RULE_ID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)';
	
	    EXECUTE IMMEDIATE V_STR_QUERY;
	    COMMIT;
    
    END IF;
	
     V_STR_QUERY :=
        'SELECT MAX(EXPECTED_LIFE)
           FROM ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || '
          WHERE PKID = :p1
             OR (:p1 = 0 AND PD_METHOD = ''VAS'')';

    EXECUTE IMMEDIATE V_STR_QUERY
        INTO V_MAX_FL
        USING V_MODEL_ID, V_MODEL_ID;
    
	--------------------------------------------------------------------
	-- MAIN LOOP
	--------------------------------------------------------------------
	WHILE V_FL_PRC <= V_MAX_FL LOOP

    ----------------------------------------------------------------
    -- INSERT IFRS_PD_VAS_MPD (Dynamic SQL)
    ----------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || '
        (
            EFF_DATE, PD_RULE_ID, FL_SEQ, SCENARIO_NO, BASE_DATE,
            BUCKET_GROUP, BUCKET_ID, TTC_PD, CORRELATION, ZSCORE,
            FPD, CPD, PD, CREATEDDATE, CREATEDBY
        )
        SELECT 
            A.EFF_DATE, A.PD_RULE_ID, B.FL_SEQ, B.SCENARIO_NO, B.BASE_DATE, A.BUCKET_GROUP, A.BUCKET_ID,
            A.TTC_PD, A.CORRELATION, B.ZSCORE,
            FUTIL_NORMDIST(
                (FUTIL_NORMSINV1(A.TTC_PD) - (SQRT(A.CORRELATION) * B.ZSCORE))
                / SQRT(1 - A.CORRELATION),
                0, 1, 1 ), 0, 0, SYSDATE,
            ''SP_IFRS_PD_VAS_MPD1''
        FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_TTC || ' A
        JOIN ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_ZSCORE || ' B
          ON A.EFF_DATE = B.EFF_DATE
         AND B.PD_RULE_ID = A.PD_RULE_ID
        WHERE A.EFF_DATE = ' || V_CURRDATE || '
          AND (A.PD_RULE_ID = ' || V_MODEL_ID || ' OR ' || V_MODEL_ID || ' = 0)
          AND B.FL_SEQ = ' || V_FL_PRC || '';

    EXECUTE IMMEDIATE V_STR_QUERY;
	----------------------------------------------------
    -- UPDATE PD (Dynamic SQL)
    ----------------------------------------------------------------
    V_STR_QUERY := '
        UPDATE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' A
           SET A.PD =
                CASE 
                    WHEN A.FL_SEQ = 0 THEN A.FPD
                    ELSE NVL((
                            SELECT B.FPD FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' B
                             WHERE B.EFF_DATE = A.EFF_DATE
                               AND B.PD_RULE_ID = A.PD_RULE_ID
                               AND B.BUCKET_ID = A.BUCKET_ID
                               AND B.SCENARIO_NO = A.SCENARIO_NO
                               AND B.FL_SEQ = :flseq2
                        ), A.FPD) 
                    * (1 - (
                            SELECT C.CPD FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' C
                             WHERE C.EFF_DATE = A.EFF_DATE
                               AND C.PD_RULE_ID = A.PD_RULE_ID
                               AND C.BUCKET_ID = A.BUCKET_ID
                               AND C.SCENARIO_NO = A.SCENARIO_NO
                               AND C.FL_SEQ = 0
                       ))
                END
         WHERE A.EFF_DATE = :vdate
           AND A.FL_SEQ = :flseq1 ';

        EXECUTE IMMEDIATE V_STR_QUERY
            USING V_FL_PRC + 1, V_CURRDATE, V_FL_PRC;

    ----------------------------------------------------------------
    -- UPDATE CPD (Dynamic SQL)
    ----------------------------------------------------------------
    V_STR_QUERY := '
        UPDATE ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' A
           SET A.CPD =
                CASE 
                    WHEN A.FL_SEQ = 0 THEN NVL(A.FPD, 0)
                    ELSE (
                            SELECT C.CPD 
                              FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD || ' C
                             WHERE C.EFF_DATE = A.EFF_DATE
                               AND C.PD_RULE_ID = A.PD_RULE_ID
                               AND C.BUCKET_ID = A.BUCKET_ID
                               AND C.SCENARIO_NO = A.SCENARIO_NO
                               AND C.FL_SEQ = 0
                         ) + A.PD
                END
         WHERE A.EFF_DATE = :vdate
           AND A.FL_SEQ = :flseq
        ';

        EXECUTE IMMEDIATE V_STR_QUERY
            USING V_CURRDATE, V_FL_PRC;
    V_FL_PRC := V_FL_PRC + 1;

	END LOOP;
    -----------------------------
    -- Log & insert final data
    -----------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
   
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_VAS_MPD ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (PD_RULE_ID = ''' || V_MODEL_ID || ''' OR ''' || V_MODEL_ID || ''' = ''0'')';

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_PD_VAS_MPD: ' || SQLERRM);
        RAISE;
END