CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_MIG_ENR_DEV (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_MIG_ENR_DEV';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLEINSERT3  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLESELECT2  VARCHAR2(100);
    V_TABLESELECT3  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_DETAIL_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);


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
        V_TABLEINSERT1 := 'TMP_IFRS_PD_MIG_AVG_MONTH_' || V_RUNID;
        V_TABLEINSERT2 := 'TMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLEINSERT3 := 'TMP_IFRS_PD_SCENARIO_DATA_' || V_RUNID;
        V_TABLESELECT1 := 'IFRS_PD_SCENARIO_DATA_' || V_RUNID;
        V_TABLESELECT2 := 'IFRS_PD_MIGRATION_DETAIL_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT_MONTHLY_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'TMP_IFRS_PD_MIG_AVG_MONTH';
        V_TABLEINSERT2 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLEINSERT3 := 'TMP_IFRS_PD_SCENARIO_DATA';
        V_TABLESELECT1 := 'IFRS_PD_SCENARIO_DATA';
        V_TABLESELECT2 := 'IFRS_PD_MIGRATION_DETAIL';
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT_MONTHLY';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_MIG_AVG_MONTH';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT3);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT3;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT3 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_SCENARIO_DATA';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLESELECT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLESELECT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_MIGRATION_DETAIL';
        EXECUTE IMMEDIATE V_STR_QUERY;

    END IF;
    COMMIT;

    --------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    --------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT1;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT2;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT3;

    --------------------------------------------------------------
    -- BUILD DYNAMIC QUERY
    --------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' (EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        HISTORICAL_DATA,
        POPULATION_MONTH,
        TRANSITION_START_DATE
        )
        SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
               LAST_DAY (ADD_MONTHS (TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), A.INCREMENT_PERIOD * -1)) AS BASE_DATE,
               A.PKID AS PD_RULE_ID,
               A.BUCKET_GROUP,
               A.HISTORICAL_DATA,
               A.POPULATION_MONTH,
               LAST_DAY (ADD_MONTHS (TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), A.TRANSITION_DATA * -1)) AS TRANSITION_START_DATE
          FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A
         WHERE NVL (A.ACTIVE_FLAG, 0) = 1
               AND IS_DELETED = 0
               AND PD_METHOD = ''MIG''
               AND DERIVED_PD_MODEL IS NULL';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
          SELECT PD_RULE_ID, POPULATIONMONTH
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
          CROSS APPLY (SELECT REGEXP_SUBSTR (POPULATION_MONTH,''[^,]+'', 1, LEVEL) AS POPULATIONMONTH FROM DUAL
          CONNECT BY REGEXP_SUBSTR (POPULATION_MONTH, ''[^,]+'',1, LEVEL) IS NOT NULL)';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || '
          SELECT A.EFF_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.PD_UNIQUE_ID,
                 A.CALC_METHOD,
                 MAX (CUSTOMER_NAME) AS CUSTOMER_NAME,
                 SUM (CALC_AMOUNT) AS CALC_AMOUNT,
                 MAX (BUCKET_ID) AS BUCKET_ID,
                 SUM (OUTSTANDING) AS OUTSTANDING,
                 TO_CHAR (A.PD_RULE_ID || A.PD_UNIQUE_ID) AS KEY_TMP
            FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
                 JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
                     ON EXTRACT (MONTH FROM A.EFF_DATE) = B.POPULATION_MONTH
                        AND A.PD_RULE_ID = B.PD_RULE_ID
        GROUP BY A.EFF_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.PD_UNIQUE_ID,
                 A.CALC_METHOD';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'DELETE ' || V_OWNER || '.' || V_TABLESELECT2 || '
     WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
           AND PD_RULE_ID IN (SELECT DISTINCT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    
    SELECT COUNT (*)
    INTO V_COUNT
    FROM USER_INDEXES
    WHERE INDEX_NAME = 'IDX_IFRS_PD_MIGRATION_DETAIL';

    IF V_COUNT > 0
    THEN
        EXECUTE IMMEDIATE 'DROP INDEX IDX_IFRS_PD_MIGRATION_DETAIL';
    END IF;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' 
        (EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        PD_UNIQUE_ID,
        BUCKET_FROM,
        BUCKET_TO,
        CALC_AMOUNT,
        CUSTOMER_NAME,
        OUTSTANDING)
        SELECT B.EFF_DATE AS EFF_DATE,
               A.EFF_DATE AS BASE_DATE,
               A.PD_RULE_ID,
               A.BUCKET_GROUP,
               A.PD_UNIQUE_ID,
               A.BUCKET_ID AS BUCKET_FROM,
               B.BUCKET_ID AS BUCKET_TO,
               A.CALC_AMOUNT,
               B.CUSTOMER_NAME,
               A.OUTSTANDING
          FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || ' A
               JOIN ' || V_OWNER || '.' || V_TABLEINSERT3 || ' B
                   ON     A.PD_RULE_ID = B.PD_RULE_ID
                      AND A.PD_UNIQUE_ID = B.PD_UNIQUE_ID
               JOIN ' || V_OWNER || '.' || V_TABLEINSERT2 || ' C
                   ON     A.PD_RULE_ID = C.PD_RULE_ID
                      AND B.PD_RULE_ID = C.PD_RULE_ID
                      AND A.EFF_DATE = C.BASE_DATE
                      AND B.EFF_DATE = C.EFF_DATE
               JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET D
                   ON     D.BUCKET_GROUP = A.BUCKET_GROUP
                      AND D.BUCKET_GROUP = B.BUCKET_GROUP';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' UPD
     USING (SELECT B.NEW_PD_RULE_ID, A.*
              FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' A
                   JOIN
                   (SELECT CASE WHEN PD.PD_RULE_ID = 4 AND UPPER (NVL (FLAG, ''BARU'')) = ''BEKAS''
                                THEN 56
                                WHEN PD.PD_RULE_ID = 4 AND UPPER (NVL (FLAG, ''BARU'')) <> ''BEKAS''
                                THEN 55
                                WHEN PD.PD_RULE_ID = 5 AND UPPER (NVL (FLAG, ''BARU'')) = ''BEKAS''
                                THEN 58
                                WHEN PD.PD_RULE_ID = 5 AND UPPER (NVL (FLAG, ''BARU'')) <> ''BEKAS''
                                THEN 57
                           END NEW_PD_RULE_ID,
                           FLAG,
                           PD.*
                      FROM (SELECT DET.*
                              FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' DET
                             WHERE     DET.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                                   AND PD_RULE_ID IN (4, 5)) PD
                           LEFT JOIN
                           (SELECT ''KKB'' AS SOURCE,
                                   DET1.PD_UNIQUE_ID ACCOUNT_NUMBER,
                                   UPPER (FLAG) AS FLAG
                              FROM ' || V_OWNER || '.IFRS_KKB_FLAG KKB
                                   JOIN
                                   (  SELECT ACCOUNT_NUMBER,
                                             MAX(DOWNLOAD_DATE) DOWNLOAD_DATE
                                        FROM ' || V_OWNER || '.IFRS_KKB_FLAG
                                    GROUP BY ACCOUNT_NUMBER) NX
                                      ON     NX.ACCOUNT_NUMBER = KKB.ACCOUNT_NUMBER
                                         AND NX.DOWNLOAD_DATE = KKB.DOWNLOAD_DATE
                                   FULL JOIN
                                   (SELECT PD_UNIQUE_ID, BASE_DATE
                                      FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
                                     WHERE     PD_RULE_ID IN (4, 5)
                                           AND EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')) DET1
                                      ON KKB.ACCOUNT_NUMBER = SUBSTR (DET1.PD_UNIQUE_ID, 1, 16)
                             WHERE DET1.PD_UNIQUE_ID IS NOT NULL
                            UNION
                            SELECT ''IMAM'' SOURCE,
                                   ACCOUNT_NUMBER,
                                   CASE WHEN RESERVED_VARCHAR_6 IN (''O'', ''0'', ''1'', ''2'')
                                        THEN ''BARU''
                                        WHEN RESERVED_VARCHAR_6 IN (''3'', ''4'')
                                        THEN ''BEKAS''
                                   END FLAG
                              FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' IMAM
                                   JOIN
                                   (SELECT PD_UNIQUE_ID, BASE_DATE
                                      FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
                                     WHERE     PD_RULE_ID IN (4, 5)
                                           AND EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')) PD
                                      ON PD.PD_UNIQUE_ID = IMAM.ACCOUNT_NUMBER
                             WHERE     NOT EXISTS
                                          (SELECT 1
                                             FROM ' || V_OWNER || '.IFRS_KKB_FLAG KKB
                                            WHERE KKB.ACCOUNT_NUMBER = SUBSTR (IMAM.ACCOUNT_NUMBER, 1, 16))
                                   AND IMAM.DOWNLOAD_DATE >= ''31-OCT-2024''
                                   AND IMAM.DOWNLOAD_DATE = PD.BASE_DATE) KK
                              ON KK.ACCOUNT_NUMBER = PD.PD_UNIQUE_ID
                     WHERE PD.PD_UNIQUE_ID IS NOT NULL) B
                         ON A.PD_UNIQUE_ID = B.PD_UNIQUE_ID
                         AND A.EFF_DATE = B.EFF_DATE
                         AND A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                         AND A.PD_RULE_ID IN (55, 56, 57, 58)
                         AND A.PD_RULE_ID <> B.NEW_PD_RULE_ID) DAT
        ON (DAT.PKID = UPD.PKID
            AND DAT.PD_UNIQUE_ID = UPD.PD_UNIQUE_ID
            AND DAT.EFF_DATE = UPD.EFF_DATE)
            WHEN MATCHED THEN UPDATE SET PD_RULE_ID = NEW_PD_RULE_ID
           WHERE UPD.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                 AND UPD.PD_RULE_ID IN (''55'', ''56'', ''57'', ''58'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' UPD USING (
    SELECT PREV.PD_RULE_ID PREV_PD_RULE_ID,CURR.* FROM (
    SELECT * FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
    WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND PD_RULE_ID IN (55,56,57,58)) CURR JOIN (
    SELECT * FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
    WHERE EFF_DATE = (SELECT MAX(EFF_DATE) AS EFF_DATE FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
    WHERE EFF_DATE < TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND PD_RULE_ID IN (55,56,57,58))
    AND PD_RULE_ID IN (55,56,57,58)) PREV ON CURR.PD_UNIQUE_ID = PREV.PD_UNIQUE_ID
    WHERE CURR.PD_RULE_ID <> PREV.PD_RULE_ID) DAT ON (UPD.PD_UNIQUE_ID = DAT.PD_UNIQUE_ID
    AND UPD.EFF_DATE = DAT.EFF_DATE)
    WHEN MATCHED THEN UPDATE SET PD_RULE_ID = PREV_PD_RULE_ID
    WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND PD_RULE_ID IN (55,56,57,58)';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'DELETE ' || V_OWNER || '.IFRS_PD_MIG_ENR
     WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
           AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_MIG_ENR (EFF_DATE,
          BASE_DATE,
          PD_RULE_ID,
          BUCKET_GROUP,
          BUCKET_FROM,
          BUCKET_TO,
          CALC_AMOUNT)
          SELECT B.EFF_DATE,
                 B.BASE_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.BUCKET_FROM,
                 A.BUCKET_TO,
                 SUM (A.CALC_AMOUNT)
            FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' A
                 JOIN ' || V_OWNER || '.' || V_TABLEINSERT2 || ' B
                     ON A.PD_RULE_ID = B.PD_RULE_ID AND A.EFF_DATE = B.EFF_DATE
        GROUP BY B.EFF_DATE,
                 B.BASE_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.BUCKET_FROM,
                 A.BUCKET_TO';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_MIG_ENR (EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        BUCKET_FROM,
        BUCKET_TO,
        CALC_AMOUNT)
        SELECT DISTINCT A.EFF_DATE,
                        A.BASE_DATE,
                        PD_RULE_ID,
                        A.BUCKET_GROUP,
                        B.BUCKET_ID AS BUCKET_FROM,
                        D.BUCKET_ID AS BUCKET_TO,
                        0 AS CALC_AMOUNT
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '  A
               JOIN ' || V_OWNER || '.IFRS_BUCKET_DETAIL B ON A.BUCKET_GROUP = B.BUCKET_GROUP
               JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET C
                   ON     B.BUCKET_GROUP = C.BUCKET_GROUP
                      AND B.BUCKET_ID <= C.MAX_BUCKET_ID
               CROSS JOIN ' || V_OWNER || '.IFRS_BUCKET_DETAIL D
         WHERE     D.BUCKET_GROUP = C.BUCKET_GROUP
               AND D.BUCKET_ID <= C.MAX_BUCKET_ID
               AND NOT EXISTS
                       (SELECT 1
                          FROM ' || V_OWNER || '.IFRS_PD_MIG_ENR E
                         WHERE E.PD_RULE_ID = A.PD_RULE_ID
                               AND E.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                               AND E.BUCKET_FROM = B.BUCKET_ID
                               AND E.BUCKET_TO = D.BUCKET_ID)';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

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