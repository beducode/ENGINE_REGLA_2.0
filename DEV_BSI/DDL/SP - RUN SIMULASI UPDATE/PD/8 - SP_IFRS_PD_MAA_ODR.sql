CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_MAA_ODR (
    P_RUNID         VARCHAR2,
    P_DOWNLOAD_DATE DATE,
    P_SYSCODE       VARCHAR2 DEFAULT '0',
    P_PRC           VARCHAR2 DEFAULT 'S'
) 
AUTHID CURRENT_USER
AS
    -- DATE
    V_CURRDATE   DATE;
    V_PREVDATE   DATE;
    V_MODEL_ID   VARCHAR2(22);
    V_COUNT      NUMBER;

    -- QUERY
    V_STR_QUERY  CLOB;

    -- TABLE LIST
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_GTMP_PD_ODR_CONFIG VARCHAR2(100);
    V_IFRS_PD_MAA_ODR VARCHAR2(100);
    V_IFRS_PD_MAA_ENR VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- CONDITION / LOG
    V_RETURNROWS  NUMBER;
    V_RETURNROWS2 NUMBER;
    V_TABLEDEST   VARCHAR2(100);
    V_COLUMNDEST  VARCHAR2(100);
    V_SPNAME      VARCHAR2(100);
    V_OPERATION   VARCHAR2(100);

    V_QUERYS      CLOB;
BEGIN
	-- set procedure name
    V_SPNAME := 'SP_IFRS_PD_MAA_ODR';
    
    ------------------------------------------------------------------------
    -- SET CURRDATE
    ------------------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_PREVDATE := LAST_DAY(ADD_MONTHS(V_CURRDATE, -1));

    ------------------------------------------------------------------------
    -- SET TABLE NAMES
    ------------------------------------------------------------------------
    IF P_PRC = 'S' THEN
        V_GTMP_PD_ODR_CONFIG := 'GTMP_PD_ODR_CONFIG_' || P_RUNID;
        V_IFRS_PD_MAA_ODR := 'IFRS_PD_MAA_ODR_' || P_RUNID;
        V_IFRS_PD_MAA_ENR := 'IFRS_PD_MAA_ENR_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_GTMP_PD_ODR_CONFIG := 'GTMP_PD_ODR_CONFIG';
        V_IFRS_PD_MAA_ODR := 'IFRS_PD_MAA_ODR';
        V_IFRS_PD_MAA_ENR := 'IFRS_PD_MAA_ENR';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;
    
    ----------------------------------------------------------------
    -- SIMULATION
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('GTMP_PD_ODR_CONFIG', V_GTMP_PD_ODR_CONFIG);
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_ODR', V_IFRS_PD_MAA_ODR);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_MAA_ENR', V_IFRS_PD_MAA_ENR);
        -- PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_PD_RULES_CONFIG', V_TABLEPDCONFIG);
    END IF;
    COMMIT;

    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SPNAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
    ------------------------------------------------------------------------
    -- TRUNCATE CONFIG TABLE IF NOT EMPTY
    ----------------------------------------------------------------
    
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM '|| V_TAB_OWNER || '.' || V_GTMP_PD_ODR_CONFIG
        INTO V_COUNT;

    IF V_COUNT > 0 THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE '|| V_TAB_OWNER || '.' || V_GTMP_PD_ODR_CONFIG;
    END IF;
    COMMIT;
    ------------------------------------------------------------------------
    -- INSERT GTPC CONFIG
    ------------------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_PD_ODR_CONFIG || ' (
            PD_RULE_ID, BUCKET_GROUP, HISTORICAL_DATA, 
            CURR_DATE, CUT_OFF_DATE, DEFAULT_RULE_ID, CALC_METHOD, PERIOD_START, INCLUDE_CLOSE
        )
        SELECT PKID, BUCKET_GROUP, HISTORICAL_DATA, 
               TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''),
               START_HISTORICAL_DATE, DEFAULT_RULE_ID, CALC_METHOD,
               LAST_DAY(ADD_MONTHS(
                    TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), 
                    -1 * HISTORICAL_DATA)), INCLUDE_CLOSE
        FROM '|| V_TAB_OWNER || '.' || V_TABLEPDCONFIG || '
        WHERE PD_METHOD = ''MAA'' AND (  
            UPPER(TRIM(SYSCODE_PD)) IN ( 
            SELECT UPPER(TRIM(REGEXP_SUBSTR(:1, ''[^;]+'', 1, LEVEL)))
            FROM DUAL
            CONNECT BY REGEXP_SUBSTR(:2, ''[^;]+'', 1, LEVEL) IS NOT NULL
            )
            OR :3 = ''0'' 
        )';
	EXECUTE IMMEDIATE V_STR_QUERY USING P_SYSCODE, P_SYSCODE, P_SYSCODE;
    COMMIT;
    ------------------------------------------------------------------------
    -- DELETE EXISTING ODR
    ------------------------------------------------------------------------
    IF P_SYSCODE = '0' THEN
        V_STR_QUERY := '
            DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR || '
            WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        ';
    ELSE
        V_STR_QUERY := '
            DELETE FROM '|| V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR || ' A
            WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
              AND EXISTS (
                    SELECT 1 FROM '|| V_TAB_OWNER || '.' || V_GTMP_PD_ODR_CONFIG || ' B 
                    WHERE A.PD_RULE_ID = B.PD_RULE_ID
              )
        ';
    END IF;
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    ------------------------------------------------------------------------
    -- INSERT ODR DATA
    ------------------------------------------------------------------------
     V_STR_QUERY := '
        INSERT INTO '|| V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR || ' (
            EFF_DATE, BASE_DATE, PD_RULE_ID,
            TOT_DEFAULT, NON_DEFAULT, ODR, ODR_NUMBER,
            CREATEDBY, CREATEDDATE
        )
        SELECT A.EFF_DATE, A.BASE_DATE, A.PD_RULE_ID,
               SUM(CASE WHEN A.BUCKET_TO = C.MAX_BUCKET_ID
                        AND A.BUCKET_FROM <> C.MAX_BUCKET_ID THEN CALC_AMOUNT ELSE 0 END),
               SUM(CASE WHEN A.BUCKET_FROM <> C.MAX_BUCKET_ID AND (A.BUCKET_TO <> 0  OR (A.BUCKET_TO = 0 AND B.INCLUDE_CLOSE = 1 ))THEN CALC_AMOUNT ELSE 0 END),
               0,0,
               ''SP_IFRS_PD_MAA_ODR'',
               SYSDATE
        FROM '|| V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ENR || ' A
        JOIN '|| V_TAB_OWNER || '.' || V_GTMP_PD_ODR_CONFIG || ' B ON A.PD_RULE_ID = B.PD_RULE_ID
        JOIN VW_IFRS_MAX_BUCKET C ON A.BUCKET_GROUP = C.BUCKET_GROUP
        WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        GROUP BY A.EFF_DATE, A.BASE_DATE, A.PD_RULE_ID
    ';
	EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY, 1, 30000));

    ------------------------------------------------------------------------
    -- UPDATE ODR VALUE
    ------------------------------------------------------------------------
    V_STR_QUERY := '
	    MERGE INTO '|| V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR || ' A
	    USING (
	        SELECT EFF_DATE, PD_RULE_ID, ' ||
	               'CASE WHEN NON_DEFAULT = 0 THEN 0 ELSE 
	               					CASE WHEN (TOT_DEFAULT / NON_DEFAULT)> 1 
	               						THEN 1 ELSE (TOT_DEFAULT / NON_DEFAULT) END 
	               						END AS ODR
	        FROM '|| V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR || '
	        WHERE EFF_DATE = :P_EFFDATE
	    ) B
	    ON (A.EFF_DATE = B.EFF_DATE AND A.PD_RULE_ID = B.PD_RULE_ID)
	    WHEN MATCHED THEN
	        UPDATE SET A.ODR = B.ODR,
					A.ODR_NUMBER = B.ODR
	';
	EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
	COMMIT;


    ----------------------------------------------------------------
    -- LOG
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

    ------------------------------------------------------------------------
    -- RESULT QUERY
    ------------------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_ODR;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SPNAME || ' : ' || SQLERRM);
END;