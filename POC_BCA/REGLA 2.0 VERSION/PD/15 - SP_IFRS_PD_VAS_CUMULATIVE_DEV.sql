CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_VAS_CUMULATIVE_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_VAS_CUMULATIVE_DEV';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_DETAIL_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_MINYEAR     NUMBER;
    V_MAXYEAR     NUMBER;
    V_YEAR        NUMBER;


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);
    V_SYSCODE      VARCHAR2(10);
    V_PRC         VARCHAR2(5);

    -- RESULT QUERY
    V_QUERYS        CLOB;


BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');
    V_SYSCODE := NVL(P_SYSCODE, '0');
    V_MODEL_ID := V_SYSCODE;
    V_PRC := NVL(P_PRC, 'P');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_PD_VAS_CUMULATIVE_' || V_RUNID;
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_PD_VAS_CUMULATIVE';
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    ----------------------------------------------------------------
    -- PRE-PROCESSING SIMULATION TABLES
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN
        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_VAS_CUMULATIVE';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLESELECT1;

    ----------------------------------------------------------------
    -- BUILD DYNAMIC QUERY
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || '
	(
        EFF_DATE,
        PD_RULE_ID,
        BUCKET_GROUP
	)
	SELECT
	    TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP
	FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A
	WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD =''VAS''
	AND DERIVED_PD_MODEL IS NULL';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'DELETE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
	WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
	AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    EXECUTE IMMEDIATE 'SELECT MIN(FL_YEAR), MAX(FL_YEAR)
    FROM ' || V_OWNER || '.IFRS_PD_VAS_PIT
    WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' INTO V_MINYEAR, V_MAXYEAR;

    V_YEAR := V_MINYEAR;
    
    WHILE V_YEAR <= V_MAXYEAR LOOP
        IF V_YEAR = V_MINYEAR THEN

            V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            (
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                CUMULATIVE_PD
            )
            (
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                CUMULATIVE_PD
            )
            SELECT EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                PIT
            FROM ' || V_OWNER || '.IFRS_PD_VAS_PIT
            WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
            AND FL_YEAR = ' || V_YEAR || ';

            COMMIT;
        ELSE
            INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            (
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                CUMULATIVE_PD
            )
            SELECT A.EFF_DATE,
                A.PD_RULE_ID,
                A.BUCKET_GROUP,
                A.BUCKET_ID,
                B.FL_YEAR,
                A.CUMULATIVE_PD + (1-A.CUMULATIVE_PD)*B.PIT CUMULATIVE_PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
            JOIN ' || V_OWNER || '.IFRS_PD_VAS_PIT B
            ON A.EFF_DATE = B.EFF_DATE
            AND A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
            AND A.PD_RULE_ID = B.PD_RULE_ID
            AND A.BUCKET_ID = B.BUCKET_ID
            AND A.FL_YEAR = ' || V_YEAR || ' - 1
            AND B.FL_YEAR = ' || V_YEAR || ' ';

            EXECUTE IMMEDIATE V_STR_QUERY;
            COMMIT;

        END IF;

        V_YEAR := V_YEAR + 1;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF C_RULE%ISOPEN THEN
            CLOSE C_RULE;
        END IF;

        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;