CREATE OR REPLACE PROCEDURE      SP_IFRS_RULE_DATA_PD (V_EFF_DATE DATE)
AS
    V_EOM             DATE;
    V_EOM_LOOP        DATE;
    v_TABLE_NAME      VARCHAR2 (30);
    V_STR_SQL         VARCHAR2 (4000);
    V_STR_SQL_RULE    VARCHAR2 (4000);
    V_RULE_ID         VARCHAR2 (250);
    v_Group_Segment   VARCHAR2 (50);
    v_Segment         VARCHAR2 (50);
    v_Sub_Segment     VARCHAR2 (100);
    V_COUNT           NUMBER (10);
    V_MAX_COUNT       NUMBER (10);

    CURSOR i IS
        SELECT RULE_ID,
               TABLE_NAME,
               CONDITION,
               GROUP_SEGMENT,
               SEGMENT,
               SUB_SEGMENT
          FROM GTMP_SCENARIO_SEGMENT_GENQUERY;
BEGIN

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';


    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_SCENARIO_DATA';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT_PREV';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MSTR_CUSTOMER_RATING';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_RUNNING_DATE';

    INSERT /*+ PARALLEL(8) */  INTO TMP_IFRS_PD_RUNNING_DATE (EFF_DATE,
                                          BASE_DATE,
                                          PD_RULE_ID,
                                          HISTORICAL_DATA)
        SELECT EFF_DATE,
               BASE_DATE,
               0                                           PD_RULE_ID,
               ROW_NUMBER () OVER (ORDER BY BASE_DATE)     HISTORICAL_DATA
          FROM (SELECT DISTINCT
                       V_EFF_DATE                                           EFF_DATE,
                       ADD_MONTHS (V_EFF_DATE, A2.INCREMENT_PERIOD * -1)    AS BASE_DATE
                  FROM IFRS_PD_RULES_CONFIG  A2
                       JOIN GTMP_SCENARIO_SEGMENT_GENQUERY B2
                           ON     A2.SEGMENTATION_ID = B2.RULE_ID
                              AND A2.ACTIVE_FLAG = 1
                              AND A2.PD_METHOD = 'MIG'
                              AND A2.IS_DELETED = 0
                UNION
                SELECT V_EFF_DATE EFF_DATE, V_EFF_DATE BASE_DATE FROM DUAL) A;

    COMMIT;

    INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_MSTR_CUSTOMER_RATING (CUSTOMER_NUMBER,
                                                RATING_TYPE_1,
                                                RATING_CODE_1)
        SELECT distinct a.CUSTOMER_NUMBER, a.RATING_TYPE_1, a.RATING_CODE_1
          FROM IFRS_MASTER_CUSTOMER_RATING a
         WHERE     1 = 1
               AND a.DOWNLOAD_DATE =
                   (SELECT MAX (c.download_date)
                      FROM IFRS_MASTER_CUSTOMER_RATING c
                     WHERE     c.customer_number = a.customer_number
                           AND c.DOWNLOAD_DATE <= V_EFF_DATE)
               AND a.RATING_TYPE_1 = '1'
               AND a.RATING_CODE_1 != 'UNK';

    COMMIT;

    V_COUNT := 1;

    SELECT COUNT (*) INTO V_MAX_COUNT FROM TMP_IFRS_PD_RUNNING_DATE;

    WHILE V_COUNT <= V_MAX_COUNT
    LOOP
        SELECT BASE_DATE
          INTO V_EOM
          FROM TMP_IFRS_PD_RUNNING_DATE
         WHERE HISTORICAL_DATA = V_COUNT;

        IF (V_EOM = V_EFF_DATE)
        THEN
            SELECT ADD_MONTHS (BASE_DATE, 1)
              INTO V_EOM_LOOP
              FROM TMP_IFRS_PD_RUNNING_DATE
             WHERE HISTORICAL_DATA = 1;

            WHILE V_EOM_LOOP <= V_EFF_DATE
            LOOP
                SP_IFRS_INSERT_GTMP_FROM_IMA_M (V_EOM_LOOP, 'ILS');

                INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_MASTER_ACCOUNT_PREV (
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
                      FROM GTMP_IFRS_MASTER_ACCOUNT
                     WHERE     CUSTOMER_NUMBER IN
                                   (SELECT A.CUSTOMER_NUMBER
                                      FROM GTMP_IFRS_SCENARIO_DATA  A
                                           JOIN
                                           GTMP_SCENARIO_SEGMENT_GENQUERY B
                                               ON     A.RULE_ID = B.RULE_ID
                                                  AND B.SEGMENT IN
                                                          ('CORPORATE',
                                                           'COMMERCIAL',
                                                           'SME'))
                           AND PRODUCT_CODE NOT IN ('BSL',
                                                    'BPC',
                                                    'BGR',
                                                    'BGP',
                                                    'BGL',
                                                    'BGB',
                                                    'KLG',
                                                    'KFX',
                                                    'KBR',
                                                    'KXT');

                COMMIT;

                INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_MASTER_ACCOUNT_PREV (
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
                      FROM GTMP_IFRS_MASTER_ACCOUNT  A
                           JOIN
                           (SELECT A2.CUSTOMER_NUMBER, A2.ACCOUNT_NUMBER
                              FROM GTMP_IFRS_SCENARIO_DATA  A2
                                   JOIN GTMP_SCENARIO_SEGMENT_GENQUERY B2
                                       ON     A2.RULE_ID = B2.RULE_ID
                                          AND B2.SEGMENT IN
                                                  ('CORPORATE',
                                                   'COMMERCIAL',
                                                   'SME')) B
                               ON (    A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                   AND A.CUSTOMER_NUMBER != B.CUSTOMER_NUMBER);

                COMMIT;
                V_EOM_LOOP := ADD_MONTHS (V_EOM_LOOP, 1);
            END LOOP;
        END IF;

        SP_IFRS_INSERT_GTMP_FROM_IMA_M (V_EOM);

        OPEN i;

        FETCH i
            INTO V_RULE_ID,
                 v_TABLE_NAME,
                 V_STR_SQL_RULE,
                 V_Group_Segment,
                 V_Segment,
                 V_Sub_Segment;

        WHILE i%FOUND
        LOOP
            IF (V_EOM != V_EFF_DATE)
            THEN
                v_TABLE_NAME := 'GTMP_IFRS_MASTER_ACCOUNT';

                V_STR_SQL :=
                       '  INSERT INTO  GTMP_IFRS_SCENARIO_DATA (
                                                   DOWNLOAD_DATE,
                                                   RULE_ID,
                                                   MASTERID,
                                                   GROUP_SEGMENT,
                                                   SEGMENT,
                                                   SUB_SEGMENT,
                                                   RATING_CODE,
                                                   DAY_PAST_DUE,
                                                   BI_COLLECTABILITY,
                                                   WRITEOFF_FLAG,
                                                   ACCOUNT_NUMBER,
                                                   ACCOUNT_STATUS,
                                                   CUSTOMER_NUMBER,
                                                   CUSTOMER_NAME,
                                                   EXCHANGE_RATE,
                                                   IMPAIRED_FLAG,
                                                   OUTSTANDING,
                                                   KEY_TMP_IMA
                                                   )
                  SELECT  DOWNLOAD_DATE,
                          '''
                    || V_RULE_ID
                    || ''',
                          MASTERID,
                          '''
                    || V_Group_Segment
                    || ''' GROUP_SEGMENT,
                          '''
                    || V_Segment
                    || ''' SEGMENT,
                          '''
                    || V_Sub_Segment
                    || ''' SUB_SEGMENT,
                          RATING_CODE,
                          ORIGINAL_DAY_PAST_DUE,
                          BI_COLLECTABILITY,
                          WRITEOFF_FLAG,
                          ACCOUNT_NUMBER,
                          ACCOUNT_STATUS,
                          CUSTOMER_NUMBER,
                          CUSTOMER_NAME,
                          EXCHANGE_RATE,
                          IMPAIRED_FLAG,
                          OUTSTANDING,
                          '' '' KEY_TMP_IMA
                    FROM  '
                    || v_TABLE_NAME
                    || ' A
                   WHERE  A.DOWNLOAD_DATE =  '''
                    || V_EOM
                    || ''' AND A.OUTSTANDING > 0 '
                    || ' AND (A.ACCOUNT_STATUS = ''A'' OR (A.ACCOUNT_STATUS = ''C'' AND A.DATA_SOURCE = ''CRD'')) '
                    || ' AND ('
                    || RTRIM (NVL (V_STR_SQL_RULE, ''))
                    || ')';

                EXECUTE IMMEDIATE V_STR_SQL;

                COMMIT;
            ELSIF (    V_Group_Segment NOT IN
                           ('CORPORATE', 'COMMERCIAL', 'SME')
                   AND V_EOM = V_EFF_DATE)
            THEN
                INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_SCENARIO_DATA (DOWNLOAD_DATE,
                                                     RULE_ID,
                                                     MASTERID,
                                                     GROUP_SEGMENT,
                                                     SEGMENT,
                                                     SUB_SEGMENT,
                                                     RATING_CODE,
                                                     DAY_PAST_DUE,
                                                     BI_COLLECTABILITY,
                                                     WRITEOFF_FLAG,
                                                     ACCOUNT_NUMBER,
                                                     ACCOUNT_STATUS,
                                                     CUSTOMER_NUMBER,
                                                     CUSTOMER_NAME,
                                                     EXCHANGE_RATE,
                                                     IMPAIRED_FLAG,
                                                     OUTSTANDING,
                                                     KEY_TMP_IMA)
                    SELECT V_EOM     DOWNLOAD_DATE,
                           A.RULE_ID,
                           A.MASTERID,
                           A.GROUP_SEGMENT,
                           A.SEGMENT,
                           A.SUB_SEGMENT,
                           B.RATING_CODE,
                           B.ORIGINAL_DAY_PAST_DUE,
                           B.BI_COLLECTABILITY,
                           B.WRITEOFF_FLAG,
                           A.ACCOUNT_NUMBER,
                           B.ACCOUNT_STATUS,
                           A.CUSTOMER_NUMBER,
                           A.CUSTOMER_NAME,
                           A.EXCHANGE_RATE,
                           A.IMPAIRED_FLAG,
                           0         OUTSTANDING,
                           KEY_TMP_IMA
                      FROM GTMP_IFRS_SCENARIO_DATA  A
                           JOIN GTMP_IFRS_MASTER_ACCOUNT B
                               ON     A.MASTERID = B.MASTERID
                                  AND A.RULE_ID = V_RULE_ID
                                  AND B.OUTSTANDING > 0;

                COMMIT;
            ELSE
                INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_SCENARIO_DATA (DOWNLOAD_DATE,
                                                     RULE_ID,
                                                     MASTERID,
                                                     GROUP_SEGMENT,
                                                     SEGMENT,
                                                     SUB_SEGMENT,
                                                     RATING_CODE,
                                                     DAY_PAST_DUE,
                                                     BI_COLLECTABILITY,
                                                     WRITEOFF_FLAG,
                                                     ACCOUNT_NUMBER,
                                                     ACCOUNT_STATUS,
                                                     CUSTOMER_NUMBER,
                                                     CUSTOMER_NAME,
                                                     EXCHANGE_RATE,
                                                     IMPAIRED_FLAG,
                                                     OUTSTANDING,
                                                     KEY_TMP_IMA)
                    SELECT V_EOM     DOWNLOAD_DATE,
                           RULE_ID,
                           MASTERID,
                           GROUP_SEGMENT,
                           SEGMENT,
                           SUB_SEGMENT,
                           RATING_CODE,
                           DAY_PAST_DUE,
                           BI_COLLECTABILITY,
                           WRITEOFF_FLAG,
                           ACCOUNT_NUMBER,
                           ACCOUNT_STATUS,
                           CUSTOMER_NUMBER,
                           CUSTOMER_NAME,
                           EXCHANGE_RATE,
                           IMPAIRED_FLAG,
                           0         OUTSTANDING,
                           KEY_TMP_IMA
                      FROM GTMP_IFRS_SCENARIO_DATA
                     WHERE RULE_ID = V_RULE_ID;

                COMMIT;

                MERGE /*+ PARALLEL(8) */ INTO GTMP_IFRS_SCENARIO_DATA A
                     USING (SELECT DISTINCT A2.CUSTOMER_NUMBER, A2.ACCOUNT_NUMBER
                              FROM GTMP_IFRS_MASTER_ACCOUNT_PREV  A2
                                   JOIN
                                   (  SELECT ACCOUNT_NUMBER,
                                             MAX (DOWNLOAD_DATE)    MAX_DOWNLOAD_DATE
                                        FROM GTMP_IFRS_MASTER_ACCOUNT_PREV
                                    GROUP BY ACCOUNT_NUMBER) B2
                                       ON     A2.DOWNLOAD_DATE =
                                              B2.MAX_DOWNLOAD_DATE
                                          AND A2.ACCOUNT_NUMBER =
                                              B2.ACCOUNT_NUMBER) B
                        ON (    A.DOWNLOAD_DATE = V_EFF_DATE
                            AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
                WHEN MATCHED
                THEN
                    UPDATE SET A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER;

                COMMIT;

                DELETE /*+ PARALLEL(8) */ GTMP_IFRS_SCENARIO_DATA
                 WHERE     DOWNLOAD_DATE = V_EFF_DATE
                       AND RULE_ID = V_RULE_ID
                       AND CUSTOMER_NUMBER IN
                               (SELECT A2.CUSTOMER_NUMBER
                                  FROM GTMP_IFRS_MASTER_ACCOUNT_PREV  A2
                                       JOIN
                                       (  SELECT CUSTOMER_NUMBER,
                                                 MAX (DOWNLOAD_DATE)    MAX_DOWNLOAD_DATE
                                            FROM GTMP_IFRS_MASTER_ACCOUNT_PREV
                                        GROUP BY CUSTOMER_NUMBER) B2
                                           ON     A2.DOWNLOAD_DATE =
                                                  B2.MAX_DOWNLOAD_DATE
                                              AND A2.CUSTOMER_NUMBER =
                                                  B2.CUSTOMER_NUMBER
                                              AND NVL (A2.RESERVED_VARCHAR_2,
                                                       '-') NOT IN
                                                      ('L',
                                                       'M',
                                                       'S',
                                                       '-'));

                COMMIT;

                /*==============================================================================================================================
                    BCA's Rule
                    Use latest Rating_Code from ifrs_master_customer_rating for Corporate or Commercial or SME segment (PD_Rule_ID 1 - 3)
                ==============================================================================================================================*/

                MERGE /*+ PARALLEL(8) */ INTO GTMP_IFRS_SCENARIO_DATA A
                     USING GTMP_IFRS_MSTR_CUSTOMER_RATING B
                        ON (    A.DOWNLOAD_DATE = V_EOM
                            AND A.GROUP_SEGMENT = V_Group_Segment
                            AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
                WHEN MATCHED
                THEN
                    UPDATE SET A.RATING_CODE = B.RATING_CODE_1;

                COMMIT;
            END IF;

            FETCH i
                INTO V_RULE_ID,
                     v_TABLE_NAME,
                     V_STR_SQL_RULE,
                     V_Group_Segment,
                     V_Segment,
                     V_Sub_Segment;
        END LOOP;

        CLOSE i;

        V_COUNT := V_COUNT + 1;
    END LOOP;
END;