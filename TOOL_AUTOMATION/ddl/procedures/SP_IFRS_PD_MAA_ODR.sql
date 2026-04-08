CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_PD_MAA_ODR
    P_RUNID         VARCHAR2,
    P_DOWNLOAD_DATE DATE,
    P_SYSCODE       VARCHAR2 DEFAULT '0',
    P_PRC           VARCHAR2 DEFAULT 'S'
) AS
    -- DATE
    V_CURRDATE   DATE;
    V_PREVDATE   DATE;
    V_MODEL_ID   VARCHAR2(22);
    V_COUNT      NUMBER;

    -- QUERY
    V_STR_QUERY  CLOB;

    -- TABLE LIST
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_TABLEINSERT1   VARCHAR2(100);
    V_TABLEINSERT2   VARCHAR2(100);
    V_TABLESELECT1   VARCHAR2(100);
    V_TABLEPDCONFIG  VARCHAR2(100);

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
    V_MODEL_ID := NVL(P_SYSCODE, '0');

    ------------------------------------------------------------------------
    -- SET TABLE NAMES
    ------------------------------------------------------------------------
    IF P_PRC = 'S' THEN
        V_TABLEINSERT1  := 'GTMP_PD_ODR_CONFIG_' || P_RUNID;
        V_TABLEINSERT2  := 'IFRS_PD_MAA_ODR_' || P_RUNID;
        V_TABLESELECT1  := 'IFRS_PD_MAA_ENR_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_TABLEINSERT1  := 'GTMP_PD_ODR_CONFIG';
        V_TABLEINSERT2  := 'IFRS_PD_MAA_ODR';
        V_TABLESELECT1  := 'IFRS_PD_MAA_ENR';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;
    ------------------------------------------------------------------------
    -- LOG RUN_ID → Oracle tidak bisa CALL, cukup prosedur langsung
    ------------------------------------------------------------------------
    PSAK413.SP_IFRS_RUNNING_LOG(
        V_CURRDATE,
        V_SPNAME,
        P_RUNID,
        SYS_CONTEXT('USERENV','SID'),
        SYSDATE
    );
    COMMIT;
    ------------------------------------------------------------------------
    -- DROP + CREATE TEMP TABLE 1
    ------------------------------------------------------------------------
    IF P_PRC = 'S' THEN
        SELECT COUNT(*)
        INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = 'PSAK413'
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE ' || V_TABLEINSERT1;
        END IF;

        EXECUTE IMMEDIATE
            'CREATE TABLE ' || V_TABLEINSERT1 ||
            ' AS SELECT * FROM GTMP_PD_ODR_CONFIG WHERE 1=0';

        COMMIT;
    END IF;
    ------------------------------------------------------------------------
    -- DROP + CREATE TEMP TABLE 2
    ------------------------------------------------------------------------
    IF P_PRC = 'S' THEN
        SELECT COUNT(*)
        INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = 'PSAK413'
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLE ' || V_TABLEINSERT2;
        END IF;

        EXECUTE IMMEDIATE
            'CREATE TABLE ' || V_TABLEINSERT2 ||
            ' AS SELECT * FROM IFRS_PD_MAA_ODR WHERE 1=0';
        
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
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_RULES_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        COMMIT;
    END IF;
    ------------------------------------------------------------------------
    -- TRUNCATE CONFIG TABLE IF NOT EMPTY
    ------------------------------------------------------------------------
    
    EXECUTE IMMEDIATE
        'SELECT COUNT(*) FROM '|| V_TAB_OWNER || '.' || V_TABLEINSERT1
        INTO V_COUNT;

    IF V_COUNT > 0 THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE '|| V_TAB_OWNER || '.' || V_TABLEINSERT1;
    END IF;
    COMMIT;
    ------------------------------------------------------------------------
    -- INSERT GTPC CONFIG
    ------------------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' (
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
        WHERE PD_METHOD = ''MAA''
          AND (''' || V_MODEL_ID || ''' = ''0'' OR PKID = ''' || V_MODEL_ID || ''')
          AND IS_DELETED = 0 ';
	EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    ------------------------------------------------------------------------
    -- DELETE EXISTING ODR
    ------------------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := '
            DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT2 || '
            WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        ';
    ELSE
        V_STR_QUERY := '
            DELETE FROM '|| V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' A
            WHERE A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
              AND EXISTS (
                    SELECT 1 FROM '|| V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B 
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
        INSERT INTO '|| V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' (
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
        FROM '|| V_TAB_OWNER || '.' || V_TABLESELECT1 || ' A
        JOIN '|| V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' B ON A.PD_RULE_ID = B.PD_RULE_ID
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
	    MERGE INTO '|| V_TAB_OWNER || '.' || V_TABLEINSERT2 || ' A
	    USING (
	        SELECT EFF_DATE, PD_RULE_ID, ' ||
	               'CASE WHEN NON_DEFAULT = 0 THEN 0 ELSE 
	               					CASE WHEN (TOT_DEFAULT / NON_DEFAULT)> 1 
	               						THEN 1 ELSE (TOT_DEFAULT / NON_DEFAULT) END 
	               						END AS ODR
	        FROM '|| V_TAB_OWNER || '.' || V_TABLEINSERT2 || '
	        WHERE EFF_DATE = :P_EFFDATE
	    ) B
	    ON (A.EFF_DATE = B.EFF_DATE AND A.PD_RULE_ID = B.PD_RULE_ID)
	    WHEN MATCHED THEN
	        UPDATE SET A.ODR = B.ODR,
					A.ODR_NUMBER = B.ODR
	';
	EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
	COMMIT;


    ------------------------------------------------------------------------
    -- LOGGING
    ------------------------------------------------------------------------
    V_TABLEDEST := V_TABLEINSERT2;
    V_COLUMNDEST := '-';
    V_SPNAME := 'SP_IFRS_PD_MAA_ODR';
    V_OPERATION := 'INSERT';

    SP_IFRS_EXEC_AND_LOG(
        V_CURRDATE, V_TABLEDEST, V_COLUMNDEST,
        V_SPNAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID
    );
    COMMIT;
    ------------------------------------------------------------------------
    -- RESULT QUERY
    ------------------------------------------------------------------------
    V_QUERYS :=
        'SELECT * FROM '|| V_TAB_OWNER || '.' || V_TABLEINSERT2 ||
        ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
          AND (PD_RULE_ID = ''' || V_MODEL_ID || ''' OR ''' || V_MODEL_ID || ''' = ''0'')';

    SP_IFRS_RESULT_PREV(
        V_CURRDATE, V_QUERYS, V_SPNAME, NVL(V_RETURNROWS2,0), P_RUNID
    );
    COMMIT;

END