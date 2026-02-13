CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_RESULT_CALC_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_RESULT_CALC_BCA';
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
    V_TABLESELECT2  VARCHAR2(100);
    V_TABLESELECT3  VARCHAR2(100);
    V_TABLECONFIG   VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_RULE_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_PD_RULES_QRY_RESULT CLOB;


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID         VARCHAR2(30);
    V_SYSCODE       VARCHAR2(10);
    V_PRC           VARCHAR2(5);
    V_CALCMODE      VARCHAR2(50);
    V_INTERPOLATIONNMETHOD NUMBER(10);
    V_DATASOURCE   VARCHAR2(30) := 'ALL';

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
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE_AMORT;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE_AMORT has no CURRDATE row');
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
        V_TABLEINSERT1 := 'IFRS_ECL_RESULT_DETAIL_CALC_' || V_RUNID;
        V_TABLEINSERT2 := 'IFRS_ECL_RESULT_DETAIL_' || V_RUNID;
        V_TABLECONFIG := 'IFRS_ECL_MODEL_CONFIG_' || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLESELECT2 := 'IFRS_EAD_RESULT_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_EAD_RESULT_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ECL_RESULT_DETAIL_CALC';
        V_TABLEINSERT2 := 'IFRS_ECL_RESULT_DETAIL';
        V_TABLECONFIG := 'IFRS_ECL_MODEL_CONFIG';
        V_TABLESELECT1 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLESELECT2 := 'IFRS_EAD_RESULT';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_ECL_RESULT_DETAIL_CALC WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_ECL_RESULT_DETAIL WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ' 
    WHERE ECL_MODEL_ID = ' || V_MODEL_ID || ' AND DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' 
    WHERE ECL_MODEL_ID = ' || V_MODEL_ID || ' AND DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    BEGIN
        V_STR_QUERY := 'SELECT COMMONUSAGE 
                        FROM TBLM_COMMONCODEHEADER 
                        WHERE COMMONCODE = ''B140''';
        EXECUTE IMMEDIATE V_STR_QUERY INTO V_CALCMODE;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_CALCMODE := NULL; -- JIKA TIDAK ADA DATA
    END;

    -- SET DEFAULT JIKA NULL ATAU KOSONG
    IF V_CALCMODE IS NULL OR TRIM(V_CALCMODE) = '' THEN
        V_CALCMODE := 'YEAR';
    END IF;

    BEGIN
        V_STR_QUERY := 'SELECT COMMONUSAGE 
                        FROM TBLM_COMMONCODEHEADER 
                        WHERE COMMONCODE = ''B141''';
        EXECUTE IMMEDIATE V_STR_QUERY INTO V_INTERPOLATIONNMETHOD;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_INTERPOLATIONNMETHOD := NULL; -- JIKA TIDAK ADA DATA
    END;

    -- SET DEFAULT JIKA NULL ATAU KOSONG
    IF V_INTERPOLATIONNMETHOD IS NULL THEN
        V_INTERPOLATIONNMETHOD := 1;
    END IF;

    V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_INSERT_GTMP_FROM_IMA_BCA(:1, :2, :3, :4, :5); END;';

    EXECUTE IMMEDIATE V_STR_QUERY
    USING V_RUNID, V_CURRDATE, V_SYSCODE, V_PRC, V_DATASOURCE;
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_OWNER || '.TMP_PD_TERM_STRUCTURE';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_OWNER || '.TMP_IFRS_EAD_RESULT';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_OWNER || '.TMP_SBLC_OVERRIDE';


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.TMP_PD_TERM_STRUCTURE
        SELECT TS.EFF_DATE,
               TS.BASE_DATE,
               TS.PD_RULE_ID,
               TS.MODEL_ID,
               TS.BUCKET_GROUP,
               TS.BUCKET_ID,
               TS.FL_SEQ,
               TS.FL_YEAR,
               TS.FL_DATE,
               12 AS FL_MONTH,
               TS.OVERRIDE_PD AS PD
          FROM ' || V_OWNER || '.IFRS_PD_TERM_STRUCTURE TS
          WHERE TS.TM_TYPE = ''YEAR''
               AND EXISTS
                (SELECT 1
                    FROM ' || V_OWNER || '.' || V_TABLECONFIG || ' CF
                    WHERE TS.PD_RULE_ID = CF.PD_MODEL_ID
                        AND CF.ECL_MODEL_ID = ' || V_MODEL_ID || '
                        AND CF.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                        AND CF.PD_EFF_DATE = TS.EFF_DATE
                        AND TS.MODEL_ID =
                            CASE
                                WHEN CF.PD_FL_FLAG = 1
                                THEN
                                    CF.PD_FL_MODEL
                                ELSE 0 
                                END)
        UNION ALL

        SELECT TS.EFF_DATE,
               TS.BASE_DATE,
               TS.PD_RULE_ID,
               TS.MODEL_ID,
               TS.BUCKET_GROUP,
               C.BUCKET_ID,
               TS.FL_SEQ,
               TS.FL_YEAR,
               TS.FL_DATE,
               12 AS FL_MONTH,
               TS.OVERRIDE_PD AS PD
          FROM ' || V_OWNER || '.IFRS_PD_TERM_STRUCTURE  TS
               JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET B
                   ON TS.BUCKET_GROUP = B.BUCKET_GROUP
                   AND TS.BUCKET_ID = B.MAX_BUCKET_ID
               JOIN
               (SELECT A2.BUCKET_GROUP, C2.BUCKET_ID
                  FROM (  SELECT BUCKET_GROUP,
                                 COUNT (BUCKET_ID)     TOTAL_BUCKET_ID
                            FROM ' || V_OWNER || '.IFRS_BUCKET_DETAIL
                        GROUP BY BUCKET_GROUP) A2
                       JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET B2
                           ON     A2.BUCKET_GROUP = B2.BUCKET_GROUP
                              AND A2.TOTAL_BUCKET_ID != B2.MAX_BUCKET_ID
                       JOIN ' || V_OWNER || '.IFRS_BUCKET_DETAIL C2
                           ON     A2.BUCKET_GROUP = C2.BUCKET_GROUP
                              AND C2.BUCKET_ID > B2.MAX_BUCKET_ID
                              AND C2.BUCKET_NAME != ''BR12'') C
                   ON TS.BUCKET_GROUP = C.BUCKET_GROUP
         WHERE TM_TYPE = ''YEAR''
               AND EXISTS
                (SELECT 1
                    FROM ' || V_OWNER || '.' || V_TABLECONFIG || ' CF
                    WHERE     TS.PD_RULE_ID = CF.PD_MODEL_ID
                        AND CF.ECL_MODEL_ID = ' || V_MODEL_ID || '
                        AND CF.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                        AND CF.PD_EFF_DATE = TS.EFF_DATE
                        AND TS.MODEL_ID =
                            CASE
                                WHEN CF.PD_FL_FLAG = 1
                                THEN
                                    CF.PD_FL_MODEL
                                ELSE
                                    0
                            END)';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.TMP_IFRS_EAD_RESULT 
                    (DOWNLOAD_DATE,
                    MASTERID,
                    CURRENCY,
                    EXCHANGE_RATE,
                    ECL_MODEL_ID,
                    CR_STAGE,
                    EAD_RULE_ID,
                    EAD_SEGMENT,
                    CCF_RULE_ID,
                    PREPAYMENT_RULE_ID,
                    UNUSED_AMOUNT,
                    REVOLVING_FLAG,
                    MARKET_AMOUNT,
                    INTEREST_ACCRUED,
                    CCF_RATE,
                    CCF_AMOUNT,
                    PREPAYMENT_RATE,
                    PREPAYMENT_AMOUNT,
                    OUTSTANDING,
                    UNAMORT_FEE_COST,
                    FAIRVALUE,
                    PMTDATE,
                    EAD_AMOUNT,
                    COUNTER,
                    MAX_COUNTER,
                    SOURCE)
        SELECT
               DOWNLOAD_DATE,
               MASTERID,
               CURRENCY,
               EXCHANGE_RATE,
               ECL_MODEL_ID,
               CR_STAGE,
               EAD_RULE_ID,
               EAD_SEGMENT,
               CCF_RULE_ID,
               PREPAYMENT_RULE_ID,
               UNUSED_AMOUNT,
               REVOLVING_FLAG,
               MARKET_AMOUNT,
               INTEREST_ACCRUED,
               CCF_RATE,
               CCF_AMOUNT,
               PREPAYMENT_RATE,
               PREPAYMENT_AMOUNT,
               OUTSTANDING,
               UNAMORT_FEE_COST,
               FAIRVALUE,
               PMTDATE,
               EAD_AMOUNT,
               COUNTER,
               MAX_COUNTER,
               SOURCE
          FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' 
          WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
               AND ECL_MODEL_ID = ' || V_MODEL_ID || '
               AND MOD(COUNTER, 12) = 1';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.TMP_SBLC_OVERRIDE 
                (ACCOUNT_NUMBER,
                CURRENCY,
                RATING,
                BUCKET_GROUP,
                BUCKET_ID,
                PD_RULE_ID,
                LGD_RULE_ID)
        SELECT A.ACCOUNT_NUMBER,
               A.CURRENCY,
               B.RATING,
               ''BR9_1'',
               1,
               C.PD_MODEL_ID,
               C.LGD_MODEL_ID
          FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
               JOIN ' || V_OWNER || '.TBLU_SBLC_OVERRIDE B
                   ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                      AND B.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
               JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' C
                   ON C.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                   AND C.PF_SEGMENT_ID = 436
                   AND C.ECL_MODEL_ID = ' || V_MODEL_ID || '';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.TMP_SBLC_OVERRIDE A
         USING ' || V_OWNER || '.IFRS_BUCKET_DETAIL B
            ON (B.BUCKET_GROUP = ''BR9_1'' AND A.RATING = B.IMPAIRMENT_BUCKET)
    WHEN MATCHED
    THEN
        UPDATE SET A.BUCKET_ID = B.BUCKET_ID';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.TMP_SBLC_OVERRIDE A
         USING ' || V_OWNER || '.IFRS_BUCKET_DETAIL B
            ON (A.CURRENCY = ''IDR''
                AND B.BUCKET_GROUP = ''PEFFI22_1''
                AND A.RATING = B.IMPAIRMENT_BUCKET)
    WHEN MATCHED
    THEN
        UPDATE SET
            A.BUCKET_GROUP = ''PEFFI22_1'',
            A.BUCKET_ID = B.BUCKET_ID,
            A.PD_RULE_ID = 43';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.TMP_SBLC_OVERRIDE A
         USING ' || V_OWNER || '.IFRS_BUCKET_DETAIL B
            ON (A.CURRENCY != ''IDR''
                AND B.BUCKET_GROUP = ''SNPFI22_1''
                AND A.RATING = B.IMPAIRMENT_BUCKET)
    WHEN MATCHED
    THEN
        UPDATE SET
            A.BUCKET_GROUP = ''SNPFI22_1'',
            A.BUCKET_ID = B.BUCKET_ID,
            A.PD_RULE_ID = 42';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_ECL_RESULT_DETAIL_CALC (DOWNLOAD_DATE,
                    ECL_MODEL_ID,
                    MASTERID,
                    BUCKET_GROUP,
                    BUCKET_ID,
                    CR_STAGE,
                    LIFETIME_PERIOD,
                    FL_YEAR,
                    FL_MONTH,
                    FL_SEQ,
                    PD_RATE,
                    LGD_RATE,
                    DISCOUNT_RATE,
                    COUNTER_PAYSCHD,
                    EAD_AMOUNT,
                    FAIR_VALUE_AMOUNT,
                    INTEREST_ACCRUED,
                    UNUSED_AMOUNT,
                    PREPAYMENT_RATE,
                    PREPAYMENT_AMOUNT,
                    CCF_RATE,
                    CCF_AMOUNT,
                    ECL_AMOUNT)
        SELECT A.DOWNLOAD_DATE,
               G.ECL_MODEL_ID,
               A.MASTERID,
               B.BUCKET_GROUP,
               B.BUCKET_ID,
               A.CR_STAGE,
               CASE
                   WHEN    (    A.CR_STAGE = ''1''
                            AND (   NVL (A.REVOLVING_FLAG, 0) = 1
                                 OR A.SEGMENT_RULE_ID = 448))
                        OR A.DATA_SOURCE = ''LIMIT''
                   THEN
                       12
                   ELSE
                       LEAST (NVL (C.MAX_COUNTER, 120),
                              NVL (L.LIFETIME_PERIOD, 120))
               END AS LIFETIME_PERIOD,
               B.FL_YEAR,
               B.FL_MONTH,
               B.FL_SEQ,
               CASE
                   WHEN    (    A.CR_STAGE = ''1''
                            AND (   NVL (A.REVOLVING_FLAG, 0) = 1
                                 OR A.SEGMENT_RULE_ID = 448))
                        OR A.DATA_SOURCE = ''LIMIT''
                   THEN
                       NVL (B.PD, 0)
                   WHEN A.CR_STAGE = ''3'' AND B.FL_YEAR = 1
                   THEN
                       1
                   WHEN A.CR_STAGE = ''3'' AND B.FL_YEAR > 1
                   THEN
                       0
                   WHEN B.FL_YEAR =
                        CEIL (
                            LEAST ((NVL (C.MAX_COUNTER, 120) / 12),
                                   (NVL (L.LIFETIME_PERIOD, 120) / 12)))
                   THEN
                         NVL (B.PD, 0)
                       * CASE
                             WHEN MOD (
                                      LEAST (NVL (C.MAX_COUNTER, 120),
                                             NVL (L.LIFETIME_PERIOD, 120)),
                                      12) =
                                  0
                             THEN
                                 12
                             ELSE
                                 MOD (
                                     LEAST (NVL (C.MAX_COUNTER, 120),
                                            NVL (L.LIFETIME_PERIOD, 120)),
                                     12)
                         END
                       / 12
                   ELSE
                       NVL (B.PD, 0)
               END                          AS PD_RATE,
               D.OVERRIDE_LGD               AS LGD_RATE,
               CASE
                   WHEN B.FL_YEAR = 1  --OR (A.DATA_SOURCE IN (''BTRD'', ''RKN''))
                   THEN
                       1
                   WHEN A.EIR < 0
                   THEN
                       (  1
                        / POWER (
                                1
                              +   NVL ((INTEREST_RATE / 100),
                                       A.INTEREST_RATE)
                                / 100,
                              B.FL_YEAR - 1))
                   ELSE
                       (  1
                        / POWER (1 + NVL (A.EIR, A.INTEREST_RATE) / 100,
                                 B.FL_YEAR - 1))
               END AS DISCOUNT_RATE,
               C.COUNTER AS COUNTER_PAYSCHD,
               C.EAD_AMOUNT,
               C.FAIRVALUE,
               C.INTEREST_ACCRUED,
               C.UNUSED_AMOUNT,
               C.PREPAYMENT_RATE,
               C.PREPAYMENT_AMOUNT,
               C.CCF_RATE,
               C.CCF_AMOUNT,
               C.EAD_AMOUNT
               * (CASE
                      WHEN B.FL_YEAR = 1
                      THEN
                          1
                      WHEN A.EIR < 0
                      THEN
                          (  1
                           / POWER (
                                   1
                                 +   NVL ((INTEREST_RATE / 100),
                                          A.INTEREST_RATE)
                                   / 100,
                                 B.FL_YEAR - 1))
                      ELSE
                          (  1
                           / POWER (1 + NVL (A.EIR, A.INTEREST_RATE) / 100,
                                    B.FL_YEAR - 1))
                  END)
               * CASE
                     WHEN    (    A.CR_STAGE = ''1''
                              AND (   NVL (A.REVOLVING_FLAG, 0) = 1
                                   OR A.SEGMENT_RULE_ID = 448))
                          OR A.DATA_SOURCE = ''LIMIT''
                     THEN
                         NVL (B.PD, 0)
                     WHEN A.CR_STAGE = ''3'' AND B.FL_YEAR = 1
                     THEN
                         1
                     WHEN A.CR_STAGE = ''3'' AND B.FL_YEAR > 1
                     THEN
                         0
                     WHEN B.FL_YEAR =
                          CEIL (
                              LEAST ((NVL (C.MAX_COUNTER, 120) / 12),
                                     (NVL (L.LIFETIME_PERIOD, 120) / 12)))
                     THEN
                           NVL (B.PD, 0)
                         * CASE
                               WHEN MOD (
                                        LEAST (NVL (C.MAX_COUNTER, 120),
                                               NVL (L.LIFETIME_PERIOD, 120)),
                                        12) =
                                    0
                               THEN
                                   12
                               ELSE
                                   MOD (
                                       LEAST (NVL (C.MAX_COUNTER, 120),
                                              NVL (L.LIFETIME_PERIOD, 120)),
                                       12)
                           END
                         / 12
                     ELSE
                         NVL (B.PD, 0)
                 END
               * NVL (D.OVERRIDE_LGD, 1) AS ECL_AMOUNT
          FROM (SELECT * FROM IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE=TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))  A
               INNER JOIN IFRS_ECL_MODEL_CONFIG G
                   ON A.DOWNLOAD_DATE = G.DOWNLOAD_DATE
                      AND A.SEGMENT_RULE_ID = G.PF_SEGMENT_ID
               LEFT JOIN IFRS_LIFETIME_HEADER L
                   ON     G.LT_RULE_ID = L.LIFETIME_CONFIG_ID
                      AND L.DOWNLOAD_DATE = G.LT_EFF_DATE
               LEFT JOIN TMP_SBLC_OVERRIDE E
                   ON A.ACCOUNT_NUMBER = E.ACCOUNT_NUMBER
               INNER JOIN TMP_PD_TERM_STRUCTURE B
                   ON     B.PD_RULE_ID =
                          CASE
                              WHEN     NVL (E.BUCKET_ID, 0) = 0
                                   AND A.BUCKET_GROUP != ''BR9_1''
                              THEN
                                  G.PD_MODEL_ID
                              WHEN NVL (E.BUCKET_ID, 0) > 0
                              THEN
                                  E.PD_RULE_ID
                              ELSE
                                  23
                          END
                      AND B.BUCKET_ID = NVL (E.BUCKET_ID, A.BUCKET_ID)
                      AND B.EFF_DATE = G.PD_EFF_DATE
               INNER JOIN TMP_IFRS_EAD_RESULT C
                   ON     A.MASTERID = C.MASTERID
                      AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
                      AND C.ECL_MODEL_ID = G.ECL_MODEL_ID
                      AND B.FL_YEAR = CEIL (C.COUNTER / 12)
                      AND B.FL_YEAR <=
                          CASE
                              WHEN A.CR_STAGE = 1
                                   OR A.CR_STAGE IS NULL
                                   OR A.DATA_SOURCE = ''LIMIT''
                              THEN
                                  1
                              ELSE
                                  CEIL (
                                      LEAST (
                                          (NVL (C.MAX_COUNTER, 120) / 12),
                                          (NVL (L.LIFETIME_PERIOD, 120) / 12)))
                          END
               LEFT JOIN IFRS_LGD_TERM_STRUCTURE D
                   ON     D.LGD_RULE_ID = NVL (E.LGD_RULE_ID, G.LGD_MODEL_ID)
                      AND D.EFF_DATE = G.LGD_EFF_DATE --YTA: PERLU DIUPDATE DENGAN LOGIC EFFECTIVE DATE HASIL ECL CONFIG
                      AND B.FL_SEQ = D.FL_SEQ
                      AND D.FL_YEAR <=
                          CASE
                              WHEN    A.CR_STAGE = 1
                                   OR A.CR_STAGE IS NULL
                                   OR A.DATA_SOURCE = ''LIMIT''
                              THEN
                                  1
                              ELSE
                                  CEIL (
                                      LEAST (
                                          (NVL (C.MAX_COUNTER, 120) / 12),
                                          (NVL (L.LIFETIME_PERIOD, 120) / 12)))
                          END
         WHERE G.ECL_MODEL_ID = ' || V_MODEL_ID || '
               AND NVL (A.IFRS9_CLASS, '' '') != ''FVTPL''';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO IFRS_ECL_RESULT_DETAIL (DOWNLOAD_DATE,
                    ECL_MODEL_ID,
                    MASTERID,
                    PF_SEGMENT_ID,
                    GROUP_SEGMENT,
                    SEGMENT,
                    SUB_SEGMENT,
                    ACCOUNT_NUMBER,
                    CUSTOMER_NUMBER,
                    CUSTOMER_NAME,
                    DATA_SOURCE,
                    PRODUCT_GROUP,
                    PRODUCT_TYPE,
                    PRODUCT_CODE,
                    CURRENCY,
                    EXCHANGE_RATE,
                    EIR,
                    INTEREST_RATE,
                    OUTSTANDING,
                    FAIR_VALUE_AMOUNT,
                    INTEREST_ACCRUED,
                    UNUSED_AMOUNT,
                    BI_COLLECTABILITY,
                    DAY_PAST_DUE,
                    BUCKET_GROUP,
                    BUCKET_ID,
                    CR_STAGE,
                    IMPAIRED_FLAG,
                    LIFETIME_PERIOD,
                    PREPAYMENT_AMOUNT,
                    CCF_AMOUNT,
                    ECL_AMOUNT,
                    SPECIAL_REASON,
                    MULTIPLIER)
        SELECT A.DOWNLOAD_DATE,
               B.ECL_MODEL_ID,
               B.MASTERID,
               A.SEGMENT_RULE_ID AS PF_SEGMENT_ID,
               A.GROUP_SEGMENT,
               A.SEGMENT,
               A.SUB_SEGMENT,
               A.ACCOUNT_NUMBER,
               A.CUSTOMER_NUMBER,
               A.CUSTOMER_NAME,
               A.DATA_SOURCE,
               A.PRODUCT_GROUP,
               A.PRODUCT_TYPE,
               A.PRODUCT_CODE,
               A.CURRENCY,
               A.EXCHANGE_RATE,
               A.EIR,
               A.INTEREST_RATE,
               NVL (A.OUTSTANDING, 0) AS OUTSTANDING,
               NVL (B.FAIR_VALUE_AMOUNT, 0) AS FAIR_VALUE_AMOUNT,
               NVL (A.INTEREST_ACCRUED, 0) AS INTEREST_ACCRUED,
               B.UNUSED_AMOUNT,
               A.BI_COLLECTABILITY,
               A.DAY_PAST_DUE,
               A.BUCKET_GROUP,
               B.BUCKET_ID,
               B.CR_STAGE,
               ''C'' AS IMPAIRED_FLAG,
               B.LIFETIME_PERIOD,
               B.PREPAYMENT_AMOUNT,
               B.CCF_AMOUNT,
               CASE
                   WHEN NVL (B.EAD_AMOUNT, 0) > 0
                        AND NVL (B.ECL_AMOUNT, 0) > NVL (B.EAD_AMOUNT, 0)
                   THEN
                       NVL (B.EAD_AMOUNT, 0)
                   ELSE
                       NVL (B.ECL_AMOUNT, 0)
               END AS ECL_AMOUNT,
               '' AS SPECIAL_REASON,
               NVL (RESERVED_RATE_8, 1)
          FROM GTMP_IFRS_MASTER_ACCOUNT  A
               JOIN
               (  SELECT DOWNLOAD_DATE,
                         ECL_MODEL_ID,
                         MASTERID,
                         BUCKET_ID,
                         CR_STAGE,
                         LIFETIME_PERIOD,
                         MAX (FAIR_VALUE_AMOUNT) AS FAIR_VALUE_AMOUNT,
                         UNUSED_AMOUNT,
                         SUM (
                             CASE
                                 WHEN COUNTER_PAYSCHD = 1
                                 THEN
                                     NVL (EAD_AMOUNT, 0)
                                 ELSE
                                     0
                             END) AS EAD_AMOUNT,
                         SUM (
                             CASE
                                 WHEN COUNTER_PAYSCHD = 1
                                 THEN
                                     NVL (PREPAYMENT_AMOUNT, 0)
                                 ELSE
                                     0
                             END) AS PREPAYMENT_AMOUNT,
                         SUM (
                             CASE
                                 WHEN COUNTER_PAYSCHD = 1
                                 THEN
                                     NVL (CCF_AMOUNT, 0)
                                 ELSE
                                     0
                             END) AS CCF_AMOUNT,
                         SUM (NVL (ECL_AMOUNT, 0)) AS ECL_AMOUNT
                    FROM IFRS_ECL_RESULT_DETAIL_CALC
                   WHERE     DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                         AND ECL_MODEL_ID = ' || V_MODEL_ID || '
                GROUP BY DOWNLOAD_DATE,
                         ECL_MODEL_ID,
                         MASTERID,
                         BUCKET_ID,
                         CR_STAGE,
                         LIFETIME_PERIOD,
                         UNUSED_AMOUNT) B
                   ON A.MASTERID = B.MASTERID';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    
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
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1 ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (' || CASE WHEN V_MODEL_ID = '0' THEN '1=1' ELSE 'PD_RULE_ID = ' || V_MODEL_ID END || ')';

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