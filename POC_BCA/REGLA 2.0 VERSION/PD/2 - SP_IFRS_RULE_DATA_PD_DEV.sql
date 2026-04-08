CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_RULE_DATA_PD_DEV (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_RULE_DATA_PD_DEV';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);
    V_STR_QUERY_CSR VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLEINSERT3  VARCHAR2(100);
    V_TABLEINSERT4  VARCHAR2(100);
    V_TABLEINSERT5  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLESELECT2  VARCHAR2(100);
    V_TABLESELECT3  VARCHAR2(100);
    V_TABLESELECT4  VARCHAR2(100);
    V_TABLESELECT5  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_GROUP_SEGMENT VARCHAR2(50);
    V_SEGMENT VARCHAR2(50);
    V_SUB_SEGMENT VARCHAR2(100);
    V_SEGMENT_TYPE VARCHAR2(25);
    V_SEQUENCE NUMBER(10);
    V_TABLE_NAME  VARCHAR2(100);
    V_STR_SQL_RULE VARCHAR2 (4000);


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID         VARCHAR2(30);
    V_SYSCODE       VARCHAR2(10);
    V_PRC           VARCHAR2(5);
    V_MAX_COUNT     NUMBER;
    V_EOM           DATE;
    V_EOM_LOOP      DATE;

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
        V_TABLEINSERT1 := 'GTMP_IFRS_MASTER_ACCOUNT_PREV_' || V_RUNID;
        V_TABLEINSERT2 := 'GTMP_IFRS_MSTR_CUSTOMER_RATING_' || V_RUNID;
        V_TABLEINSERT3 := 'GTMP_IFRS_MASTER_ACCOUNT_'  || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_DATA_' || V_RUNID;
        V_TABLESELECT2 := 'GTMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLESELECT3 := 'GTMP_SCENARIO_SEGMENT_GENQUERY_' || V_RUNID;
        V_TABLESELECT4 := 'IFRS_MASTER_CUSTOMER_RATING' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_DEV_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'GTMP_IFRS_MASTER_ACCOUNT_PREV';
        V_TABLEINSERT2 := 'GTMP_IFRS_MSTR_CUSTOMER_RATING';
        V_TABLEINSERT3 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_DATA';
        V_TABLESELECT2 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLESELECT3 := 'GTMP_SCENARIO_SEGMENT_GENQUERY';
        V_TABLESELECT4 := 'IFRS_MASTER_CUSTOMER_RATING';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_IFRS_MASTER_ACCOUNT_PREV';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_IFRS_MSTR_CUSTOMER_RATING';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_IFRS_MASTER_ACCOUNT';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLESELECT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLESELECT1 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_IFRS_SCENARIO_DATA';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_RUNNING_DATE';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT3);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLESELECT3;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLESELECT3 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_SCENARIO_SEGMENT_GENQUERY';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLESELECT4);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLESELECT4;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLESELECT4 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_MASTER_CUSTOMER_RATING';
        EXECUTE IMMEDIATE V_STR_QUERY;


        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEPDCONFIG);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEPDCONFIG;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEPDCONFIG ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_RULES_CONFIG';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    V_STR_QUERY_CSR := 'SELECT RULE_ID,
               TABLE_NAME,
               CONDITION,
               GROUP_SEGMENT,
               SEGMENT,
               SUB_SEGMENT
    FROM GTMP_SCENARIO_SEGMENT_GENQUERY';

    ----------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLESELECT2 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLESELECT2 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT1;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT2;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLESELECT1;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLESELECT2;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' (EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        HISTORICAL_DATA)
        SELECT EFF_DATE,
               BASE_DATE,
               0 AS PD_RULE_ID,
               ROW_NUMBER () OVER (ORDER BY BASE_DATE) AS HISTORICAL_DATA
          FROM (SELECT DISTINCT
                       TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
                       ADD_MONTHS (TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), A2.INCREMENT_PERIOD * -1)    AS BASE_DATE
                  FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A2
                       JOIN ' || V_OWNER || '.' || V_TABLESELECT3 || ' B2
                           ON A2.SEGMENTATION_ID = B2.RULE_ID
                              AND A2.ACTIVE_FLAG = 1
                              AND A2.PD_METHOD = ''MIG''
                              AND A2.IS_DELETED = 0
                UNION
                SELECT TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE, 
                TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS BASE_DATE 
                FROM DUAL) A';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' 
        (
        CUSTOMER_NUMBER,
        RATING_TYPE_1,
        RATING_CODE_1
        )
        SELECT DISTINCT A.CUSTOMER_NUMBER, A.RATING_TYPE_1, A.RATING_CODE_1
          FROM ' || V_OWNER || '.' || V_TABLESELECT4 || ' A
         WHERE     1 = 1
               AND A.DOWNLOAD_DATE =
                   (SELECT MAX (C.DOWNLOAD_DATE)
                      FROM ' || V_OWNER || '.' || V_TABLESELECT4 || ' C
                     WHERE C.CUSTOMER_NUMBER = A.CUSTOMER_NUMBER
                           AND C.DOWNLOAD_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))
               AND A.RATING_TYPE_1 = ''1''
               AND A.RATING_CODE_1 != ''UNK''';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    V_COUNT := 1;

    
    EXECUTE IMMEDIATE 'SELECT COUNT (*) 
    FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '' INTO V_MAX_COUNT;

    WHILE V_COUNT <= V_MAX_COUNT
    LOOP

        EXECUTE IMMEDIATE 'SELECT BASE_DATE
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
        WHERE HISTORICAL_DATA = :1' INTO V_EOM USING V_COUNT;

        IF (V_EOM = V_CURRDATE)
        THEN

            EXECUTE IMMEDIATE 'SELECT ADD_MONTHS (BASE_DATE, 1)
            FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
            WHERE HISTORICAL_DATA = 1' INTO V_EOM_LOOP;

            WHILE V_EOM_LOOP <= V_CURRDATE
            LOOP

                V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_INSERT_GTMP_FROM_IMA_M_DEV(:1, :2, :3, :4, ''ILS''); END;';

                EXECUTE IMMEDIATE V_STR_QUERY
                USING V_RUNID, V_EOM_LOOP, V_SYSCODE, V_PRC;
                COMMIT;

                V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' (
                                PKID,
                                DOWNLOAD_DATE,
                                MASTERID,
                                MASTER_ACCOUNT_CODE,
                                CUSTOMER_NUMBER,
                                ACCOUNT_NUMBER,
                                OUTSTANDING,
                                RESERVED_VARCHAR_2)
                    SELECT PKID,
                           DOWNLOAD_DATE,
                           MASTERID,
                           MASTER_ACCOUNT_CODE,
                           CUSTOMER_NUMBER,
                           ACCOUNT_NUMBER,
                           OUTSTANDING,
                           RESERVED_VARCHAR_2
                      FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
                     WHERE CUSTOMER_NUMBER IN
                                   (SELECT A.CUSTOMER_NUMBER
                                      FROM ' || V_OWNER || '.' || V_TABLESELECT1 || '  A
                                           JOIN
                                           ' || V_OWNER || '.' || V_TABLESELECT3 || ' B
                                               ON     A.RULE_ID = B.RULE_ID
                                                  AND B.SEGMENT IN
                                                          (''CORPORATE'',
                                                           ''COMMERCIAL'',
                                                           ''SME''))
                           AND PRODUCT_CODE NOT IN (''BSL'',
                                                    ''BPC'',
                                                    ''BGR'',
                                                    ''BGP'',
                                                    ''BGL'',
                                                    ''BGB'',
                                                    ''KLG'',
                                                    ''KFX'',
                                                    ''KBR'',
                                                    ''KXT'')';
                
                EXECUTE IMMEDIATE V_STR_QUERY;
                COMMIT;

                V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' (
                                PKID,
                                DOWNLOAD_DATE,
                                MASTERID,
                                MASTER_ACCOUNT_CODE,
                                CUSTOMER_NUMBER,
                                ACCOUNT_NUMBER,
                                OUTSTANDING,
                                RESERVED_VARCHAR_2)
                    SELECT A.PKID,
                           A.DOWNLOAD_DATE,
                           A.MASTERID,
                           A.MASTER_ACCOUNT_CODE,
                           A.CUSTOMER_NUMBER,
                           A.ACCOUNT_NUMBER,
                           A.OUTSTANDING,
                           A.RESERVED_VARCHAR_2
                      FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '  A
                           JOIN
                           (SELECT A2.CUSTOMER_NUMBER, A2.ACCOUNT_NUMBER
                              FROM ' || V_OWNER || '.' || V_TABLESELECT1 || '  A2
                                   JOIN ' || V_OWNER || '.' || V_TABLESELECT3 || ' B2
                                       ON     A2.RULE_ID = B2.RULE_ID
                                          AND B2.SEGMENT IN
                                                  (''CORPORATE'',
                                                   ''COMMERCIAL'',
                                                   ''SME'')) B
                               ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                   AND A.CUSTOMER_NUMBER != B.CUSTOMER_NUMBER)';

                EXECUTE IMMEDIATE V_STR_QUERY;
                COMMIT;

                V_EOM_LOOP := ADD_MONTHS (V_EOM_LOOP, 1);
            END LOOP;
        END IF;

        V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_INSERT_GTMP_FROM_IMA_M_DEV(:1, :2, :3, :4); END;';

        EXECUTE IMMEDIATE V_STR_QUERY
        USING V_RUNID, V_EOM, V_SYSCODE, V_PRC;
        COMMIT;

        -- OPEN C_RULE FOR V_STR_QUERY_CSR;

        -- LOOP
        --     FETCH C_RULE INTO
        --         V_RULE_ID,
        --          V_TABLE_NAME,
        --          V_STR_SQL_RULE,
        --          V_GROUP_SEGMENT,
        --          V_SEGMENT,
        --          V_SUB_SEGMENT;

        --     EXIT WHEN C_RULE%NOTFOUND;

        --     IF (V_EOM != V_CURRDATE)
        --     THEN

        --         V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || ' (
        --             DOWNLOAD_DATE,
        --             RULE_ID,
        --             MASTERID,
        --             GROUP_SEGMENT,
        --             SEGMENT,
        --             SUB_SEGMENT,
        --             RATING_CODE,
        --             DAY_PAST_DUE,
        --             BI_COLLECTABILITY,
        --             WRITEOFF_FLAG,
        --             ACCOUNT_NUMBER,
        --             ACCOUNT_STATUS,
        --             CUSTOMER_NUMBER,
        --             CUSTOMER_NAME,
        --             EXCHANGE_RATE,
        --             IMPAIRED_FLAG,
        --             OUTSTANDING,
        --             KEY_TMP_IMA
        --             )
        --           SELECT  DOWNLOAD_DATE,
        --                   '''
        --             || V_RULE_ID
        --             || ''',
        --                   MASTERID,
        --                   '''
        --             || V_GROUP_SEGMENT
        --             || ''' GROUP_SEGMENT,
        --                   '''
        --             || V_SEGMENT
        --             || ''' SEGMENT,
        --                   '''
        --             || V_SUB_SEGMENT
        --             || ''' SUB_SEGMENT,
        --                   RATING_CODE,
        --                   ORIGINAL_DAY_PAST_DUE,
        --                   BI_COLLECTABILITY,
        --                   WRITEOFF_FLAG,
        --                   ACCOUNT_NUMBER,
        --                   ACCOUNT_STATUS,
        --                   CUSTOMER_NUMBER,
        --                   CUSTOMER_NAME,
        --                   EXCHANGE_RATE,
        --                   IMPAIRED_FLAG,
        --                   OUTSTANDING,
        --                   '' '' KEY_TMP_IMA
        --             FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || ' A
        --             WHERE  A.DOWNLOAD_DATE =  '''
        --             || V_EOM
        --             || ''' AND A.OUTSTANDING > 0 '
        --             || ' AND (A.ACCOUNT_STATUS = ''A'' OR (A.ACCOUNT_STATUS = ''C'' AND A.DATA_SOURCE = ''CRD'')) '
        --             || ' AND ('
        --             || RTRIM (NVL (V_STR_SQL_RULE, ''))
        --             || ')';

        --         EXECUTE IMMEDIATE V_STR_QUERY;
        --         COMMIT;

        --     ELSIF (V_GROUP_SEGMENT NOT IN ('CORPORATE', 'COMMERCIAL', 'SME') AND V_EOM = V_CURRDATE)
        --     THEN

        --         V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || ' (DOWNLOAD_DATE,
        --             RULE_ID,
        --             MASTERID,
        --             GROUP_SEGMENT,
        --             SEGMENT,
        --             SUB_SEGMENT,
        --             RATING_CODE,
        --             DAY_PAST_DUE,
        --             BI_COLLECTABILITY,
        --             WRITEOFF_FLAG,
        --             ACCOUNT_NUMBER,
        --             ACCOUNT_STATUS,
        --             CUSTOMER_NUMBER,
        --             CUSTOMER_NAME,
        --             EXCHANGE_RATE,
        --             IMPAIRED_FLAG,
        --             OUTSTANDING,
        --             KEY_TMP_IMA)
        --             SELECT TO_DATE(''' || TO_CHAR(V_EOM,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS DOWNLOAD_DATE,
        --                    A.RULE_ID,
        --                    A.MASTERID,
        --                    A.GROUP_SEGMENT,
        --                    A.SEGMENT,
        --                    A.SUB_SEGMENT,
        --                    B.RATING_CODE,
        --                    B.ORIGINAL_DAY_PAST_DUE,
        --                    B.BI_COLLECTABILITY,
        --                    B.WRITEOFF_FLAG,
        --                    A.ACCOUNT_NUMBER,
        --                    B.ACCOUNT_STATUS,
        --                    A.CUSTOMER_NUMBER,
        --                    A.CUSTOMER_NAME,
        --                    A.EXCHANGE_RATE,
        --                    A.IMPAIRED_FLAG,
        --                    0 AS OUTSTANDING,
        --                    KEY_TMP_IMA
        --               FROM ' || V_OWNER || '.' || V_TABLESELECT1 || '  A
        --                    JOIN ' || V_OWNER || '.' || V_TABLEINSERT3 || ' B
        --                        ON A.MASTERID = B.MASTERID
        --                        AND A.RULE_ID = :1
        --                        AND B.OUTSTANDING > 0';
                
        --         EXECUTE IMMEDIATE V_STR_QUERY USING V_RULE_ID;
        --         COMMIT;
        --     ELSE

        --         V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || ' (DOWNLOAD_DATE,
        --             RULE_ID,
        --             MASTERID,
        --             GROUP_SEGMENT,
        --             SEGMENT,
        --             SUB_SEGMENT,
        --             RATING_CODE,
        --             DAY_PAST_DUE,
        --             BI_COLLECTABILITY,
        --             WRITEOFF_FLAG,
        --             ACCOUNT_NUMBER,
        --             ACCOUNT_STATUS,
        --             CUSTOMER_NUMBER,
        --             CUSTOMER_NAME,
        --             EXCHANGE_RATE,
        --             IMPAIRED_FLAG,
        --             OUTSTANDING,
        --             KEY_TMP_IMA)
        --             SELECT TO_DATE(''' || TO_CHAR(V_EOM,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS DOWNLOAD_DATE,
        --                    RULE_ID,
        --                    MASTERID,
        --                    GROUP_SEGMENT,
        --                    SEGMENT,
        --                    SUB_SEGMENT,
        --                    RATING_CODE,
        --                    DAY_PAST_DUE,
        --                    BI_COLLECTABILITY,
        --                    WRITEOFF_FLAG,
        --                    ACCOUNT_NUMBER,
        --                    ACCOUNT_STATUS,
        --                    CUSTOMER_NUMBER,
        --                    CUSTOMER_NAME,
        --                    EXCHANGE_RATE,
        --                    IMPAIRED_FLAG,
        --                    0 AS OUTSTANDING,
        --                    KEY_TMP_IMA
        --               FROM ' || V_OWNER || '.' || V_TABLESELECT1 || '
        --              WHERE RULE_ID = :1';

        --         EXECUTE IMMEDIATE V_STR_QUERY USING V_RULE_ID;
        --         COMMIT;

        --         V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
        --              USING (SELECT DISTINCT A2.CUSTOMER_NUMBER, A2.ACCOUNT_NUMBER
        --                       FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '  A2
        --                            JOIN
        --                            (  SELECT ACCOUNT_NUMBER,
        --                                      MAX (DOWNLOAD_DATE)    MAX_DOWNLOAD_DATE
        --                                 FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
        --                             GROUP BY ACCOUNT_NUMBER) B2
        --                                 ON A2.DOWNLOAD_DATE = B2.MAX_DOWNLOAD_DATE
        --                                    AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER) B
        --                 ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        --                     AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
        --         WHEN MATCHED
        --         THEN
        --             UPDATE SET A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER';

        --         EXECUTE IMMEDIATE V_STR_QUERY;
        --         COMMIT;

        --         V_STR_QUERY := 'DELETE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
        --          WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        --                AND RULE_ID = :1
        --                AND CUSTOMER_NUMBER IN
        --                        (SELECT A2.CUSTOMER_NUMBER
        --                           FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '  A2
        --                                JOIN
        --                                (  SELECT CUSTOMER_NUMBER,
        --                                          MAX (DOWNLOAD_DATE) AS MAX_DOWNLOAD_DATE
        --                                     FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
        --                                 GROUP BY CUSTOMER_NUMBER) B2
        --                                    ON A2.DOWNLOAD_DATE = B2.MAX_DOWNLOAD_DATE
        --                                       AND A2.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER
        --                                       AND NVL (A2.RESERVED_VARCHAR_2,
        --                                                ''-'') NOT IN
        --                                               (''L'',
        --                                                ''M'',
        --                                                ''S'',
        --                                                ''-''))';

        --         EXECUTE IMMEDIATE V_STR_QUERY USING V_RULE_ID;
        --         COMMIT;

        --         /*==============================================================================================================================
        --             BCA'S RULE
        --             USE LATEST RATING_CODE FROM IFRS_MASTER_CUSTOMER_RATING FOR CORPORATE OR COMMERCIAL OR SME SEGMENT (PD_RULE_ID 1 - 3)
        --         ==============================================================================================================================*/

        --         V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
        --              USING ' || V_OWNER || '.' || V_TABLEINSERT2 || ' B
        --                 ON (    A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_EOM,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        --                     AND A.GROUP_SEGMENT = :1
        --                     AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
        --         WHEN MATCHED
        --         THEN
        --             UPDATE SET A.RATING_CODE = B.RATING_CODE_1';

        --         EXECUTE IMMEDIATE V_STR_QUERY USING V_GROUP_SEGMENT;
        --         COMMIT;
        --     END IF;

        -- END LOOP;

        -- CLOSE C_RULE;

        V_COUNT := V_COUNT + 1;
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