CREATE OR REPLACE PROCEDURE SP_IFRS_CCF_MIGRASI
AS
    V_CURRDATE DATE;
    V_PREVDATE DATE;

    BEGIN

        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_DATE_DAY1;
        SELECT ADD_MONTHS(CURRDATE,-12) INTO V_PREVDATE FROM IFRS_DATE_DAY1;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

        INSERT INTO GTMP_IFRS_MASTER_ACCOUNT
                SELECT * FROM IFRS_MASTER_ACCOUNT_MONTHLY WHERE DOWNLOAD_DATE IN (V_CURRDATE,V_PREVDATE)
                          AND DATA_SOURCE IN ('ILS','CRD','LIMIT');
        COMMIT;

        DELETE IFRS_CCF_HEADER WHERE DOWNLOAD_DATE >= V_CURRDATE;
        DELETE IFRS_CCF_DETAIL WHERE DOWNLOAD_DATE >= V_CURRDATE;
        DELETE IFRS_CCF_DETAIL_PROCESS WHERE DOWNLOAD_DATE >= V_CURRDATE;
        DELETE IFRS_CCF_DETAIL_UNPROCESS WHERE DOWNLOAD_DATE >= V_CURRDATE;

        --ILS
        INSERT INTO IFRS_CCF_DETAIL
          (
                 DOWNLOAD_DATE,
                 CURRENT_DATE,
                 ACCOUNT_NUMBER,
                 FACILITY_NUMBER,
                 ACCOUNT_STATUS,
                 FIRST_NPL_DATE,
                 OS_CUR,
                 OS_PREV,
                 LIMIT_CUR,
                 USED_AMOUNT_CUR,
                 AVAILABLE_AMT_CUR,
                 REVOLVING_FLAG_L,
                 REVOLVING_FLAG_I,
                 PRODUCT_CODE_L,
                 PRODUCT_CODE_I,
                 OS_SELISIH,
                 LIMIT_SELISIH,
                 SEGMENTATION_ID,
                 CUSTOMER_NAME,
                 CUSTOMER_NUMBER
            )
                  SELECT DISTINCT
                    Z.DOWNLOAD_DATE,
                    Z.CURRENT_DATE,
                    B.ACCOUNT_NUMBER,
                    B.FACILITY_NUMBER,
                    B.ACCOUNT_STATUS,
                    B.FIRST_NPL_DATE,
                    B.OS_CUR,
                    B.OS_PREV,
                    Z.LIMIT_CUR,
                    Z.USED_AMOUNT_CUR,
                    Z.AVAILABLE_AMT_CUR,
                    Z.REVOLVING_FLAG,
                    B.REVOLVING_FLAG,
                    Z.PRODUCT_CODE,
                    B.PRODUCT_CODE,
                    (OS_CUR - OS_PREV) OS_SELISIH,
                    (LIMIT_CUR - OS_PREV) LIMIT_SELISIH,
                    K.SEGMENTATION_ID,
                    B.CUSTOMER_NAME,
                    B.CUSTOMER_NUMBER
             FROM
             (
                          SELECT
                          X.DOWNLOAD_DATE,
                          X.DOWNLOAD_DATE AS CURRENT_DATE,
                          X.ACCOUNT_NUMBER,
                          X.FACILITY_NUMBER,
                          X.INITIAL_OUTSTANDING  AS LIMIT_CUR,
                          X.RESERVED_AMOUNT_13  AS USED_AMOUNT_CUR,
                          X.RESERVED_AMOUNT_15  AS AVAILABLE_AMT_CUR,
                          x.REVOLVING_FLAG,
                          X.PRODUCT_CODE,
                          X.CCF_RULE_ID
                         FROM
                         (
                                SELECT
                                        DOWNLOAD_DATE,
                                        ACCOUNT_NUMBER,
                                        FACILITY_NUMBER,
                                        CCF_RULE_ID,
                                        SUM (INITIAL_OUTSTANDING) AS INITIAL_OUTSTANDING,
                                        SUM (RESERVED_AMOUNT_13) AS RESERVED_AMOUNT_13,
                                        SUM (RESERVED_AMOUNT_14) AS RESERVED_AMOUNT_14,
                                        SUM (RESERVED_AMOUNT_15) AS RESERVED_AMOUNT_15,
                                        REVOLVING_FLAG,
                                        PRODUCT_CODE
                                  FROM GTMP_IFRS_MASTER_ACCOUNT
                                  WHERE DOWNLOAD_DATE = V_CURRDATE
                                        AND DATA_SOURCE = 'LIMIT'
                               GROUP BY DOWNLOAD_DATE,
                                        ACCOUNT_NUMBER,
                                        FACILITY_NUMBER,
                                        REVOLVING_FLAG,
                                        PRODUCT_CODE,
                                        CCF_RULE_ID
                          ) X
                  )Z
                  JOIN
                  (
                               SELECT
                               XX.RESERVED_DATE_3 AS FIRST_NPL_DATE,
                               XX.ACCOUNT_NUMBER,
                               XX.OUTSTANDING AS OS_CUR,
                               XX.ACCOUNT_STATUS,
                               XX.FACILITY_NUMBER,
                               YY.OUTSTANDING AS OS_PREV,
                               YY.RESERVED_VARCHAR_2,
                               xx.REVOLVING_FLAG,
                               XX.PRODUCT_CODE,
                               XX.CCF_RULE_ID,
                               XX.CUSTOMER_NAME,
                               XX.CUSTOMER_NUMBER
                          FROM
                              (
                                  SELECT
                                        DOWNLOAD_DATE,
                                         ACCOUNT_NUMBER,
                                         RESERVED_DATE_3,
                                         OUTSTANDING,
                                         ACCOUNT_STATUS,
                                         FACILITY_NUMBER,
                                         PRODUCT_CODE,
                                         RESERVED_VARCHAR_2,
                                         REVOLVING_FLAG,
                                         CCF_RULE_ID,
                                         CUSTOMER_NAME,
                                         CUSTOMER_NUMBER
                                   FROM GTMP_IFRS_MASTER_ACCOUNT
                                   WHERE DATA_SOURCE = 'ILS'
                                         AND LAST_DAY (RESERVED_DATE_3) IS NOT NULL
                                         AND DOWNLOAD_DATE = V_CURRDATE
                                         AND ACCOUNT_STATUS = 'A'
                                ) XX
                               JOIN
                                (
                                      SELECT
                                              DOWNLOAD_DATE,
                                              ACCOUNT_NUMBER,
                                              RESERVED_DATE_3,
                                              OUTSTANDING,
                                              ACCOUNT_STATUS,
                                              FACILITY_NUMBER,
                                              PRODUCT_CODE,
                                              RESERVED_VARCHAR_2
                                        FROM GTMP_IFRS_MASTER_ACCOUNT
                                        WHERE DATA_SOURCE = 'ILS'
                                              AND LAST_DAY (DOWNLOAD_DATE) = V_PREVDATE
                                  ) YY
                                  ON XX.FACILITY_NUMBER = YY.FACILITY_NUMBER
                                     AND XX.ACCOUNT_NUMBER = YY.ACCOUNT_NUMBER
                    ) B
                   ON Z.ACCOUNT_NUMBER = B.FACILITY_NUMBER
                   JOIN IFRS_CCF_RULES_CONFIG K ON B.CCF_RULE_ID = K.PKID;
        COMMIT;

        --INSERT CARD




        INSERT INTO IFRS_CCF_DETAIL_PROCESS
        (
             PKID,
             DOWNLOAD_DATE,
             CURRENT_DATE,
             PREVIOUS_DATE,
             FACILITY_NUMBER,
             ACCOUNT_STATUS,
             SEGMENTATION_ID,
             FIRST_NPL_DATE,
             OS_CUR,
             OS_PREV,
             LIMIT_CUR,
             LIMIT_PREV,
             USED_AMOUNT_CUR,
             USED_AMOUNT_PREV,
             AVAILABLE_AMT_CUR,
             AVAILABLE_AMT_PREV,
             REVOLVING_FLAG_L,
             REVOLVING_FLAG_I,
             PRODUCT_CODE_L,
             PRODUCT_CODE_I
          )
              SELECT
                    0,
                    DOWNLOAD_DATE,
                    CURRENT_DATE,
                    PREVIOUS_DATE,
                    FACILITY_NUMBER,
                    ACCOUNT_STATUS,
                    SEGMENTATION_ID,
                    FIRST_NPL_DATE,
                    SUM (OS_CUR),
                    SUM (OS_PREV),
                    LIMIT_CUR,
                    LIMIT_PREV,
                    USED_AMOUNT_CUR,
                    USED_AMOUNT_PREV,
                    AVAILABLE_AMT_CUR,
                    AVAILABLE_AMT_PREV,
                    REVOLVING_FLAG_L,
                    REVOLVING_FLAG_I,
                    PRODUCT_CODE_L,
                    PRODUCT_CODE_I
              FROM IFRS_CCF_DETAIL
              WHERE DOWNLOAD_DATE = V_CURRDATE AND FACILITY_NUMBER NOT IN (SELECT FACILITY_NUMBER FROM IFRS_CCF_DETAIL_PROCESS)
              GROUP BY
                        DOWNLOAD_DATE,
                        "CURRENT_DATE",
                        PREVIOUS_DATE,
                        FACILITY_NUMBER,
                        ACCOUNT_STATUS,
                        SEGMENTATION_ID,
                        FIRST_NPL_DATE,
                        LIMIT_CUR,
                        LIMIT_PREV,
                        USED_AMOUNT_CUR,
                        USED_AMOUNT_PREV,
                        AVAILABLE_AMT_CUR,
                        AVAILABLE_AMT_PREV,
                        REVOLVING_FLAG_L,
                        REVOLVING_FLAG_I,
                        PRODUCT_CODE_L,
                        PRODUCT_CODE_I;
            COMMIT;

           --UNTUK UPDATE NILAI OS_SELISIH , LIMIT_SELISIH
           UPDATE IFRS_CCF_DETAIL_PROCESS
           SET OS_SELISIH = OS_CUR - OS_PREV,
               LIMIT_SELISIH = LIMIT_CUR - OS_PREV;
           COMMIT;

           INSERT INTO IFRS_CCF_DETAIL_UNPROCESS
               SELECT *
                  FROM IFRS_CCF_DETAIL_PROCESS
                  WHERE DOWNLOAD_DATE = V_CURRDATE
                        AND
                        (
                          (LIMIT_CUR - OS_PREV) <= 0
                          OR OS_CUR > LIMIT_CUR
                          OR OS_PREV > LIMIT_CUR
                        );
           COMMIT;

           DELETE IFRS_CCF_DETAIL_PROCESS
           WHERE DOWNLOAD_DATE = V_CURRDATE
                 AND (
                       (LIMIT_CUR - OS_PREV) <= 0
                       OR OS_CUR > LIMIT_CUR
                       OR OS_PREV > LIMIT_CUR
                    );
           COMMIT;

           INSERT INTO IFRS_CCF_DETAIL_UNPROCESS
               SELECT * FROM IFRS_CCF_DETAIL_PROCESS
               WHERE DOWNLOAD_DATE = V_CURRDATE
                  AND (LIMIT_SELISIH < 0 OR OS_SELISIH < 0);
           COMMIT;

           DELETE IFRS_CCF_DETAIL_PROCESS
                WHERE DOWNLOAD_DATE = V_CURRDATE
                AND (LIMIT_SELISIH < 0 OR OS_SELISIH < 0);
           COMMIT;

           UPDATE IFRS_CCF_DETAIL_PROCESS
                  SET CCF_RESULT =
                            CASE WHEN (OS_CUR - OS_PREV)/(LIMIT_CUR - OS_PREV) > 1 THEN 1
                                 WHEN (OS_CUR - OS_PREV)/(LIMIT_CUR - OS_PREV) < 0 THEN 0
                                 ELSE (OS_CUR - OS_PREV)/(LIMIT_CUR - OS_PREV) END
                  WHERE DOWNLOAD_DATE = V_CURRDATE;
           COMMIT;

            UPDATE IFRS_CCF_DETAIL_PROCESS
                   SET CCF_RESULT =
                            CASE WHEN (OS_CUR - OS_PREV) <= 0 THEN 0
                                 ELSE CCF_RESULT END;
            COMMIT;


            INSERT INTO IFRS_CCF_HEADER
            (
                 PKID,
                 DOWNLOAD_DATE,
                 CCF_MODEL_ID,
                 SEGMENTATION,
                 SEGMENTATION_ID,
                 CCF_RATE,
                 AVERAGE_METHOD
            )
               SELECT
                      0,
                      V_CURRDATE,
                      B.PKID,
                      B.CCF_RULE_NAME,
                      A.SEGMENTATION_ID,
                      CASE WHEN B.AVERAGE_METHOD = 'Simple'
                            THEN C1 ELSE C2
                      END,
                      B.AVERAGE_METHOD
                 FROM (
                          SELECT
                                SEGMENTATION_ID,
                                C1,C2
                                FROM
                                (
                                        SELECT
                                          SEGMENTATION_ID,
                                          AVG(CCF_RESULT)C1,
                                          (SUM(OS_CUR)-SUM(OS_PREV))/(SUM(LIMIT_CUR)- SUM(OS_PREV)) C2
                                        FROM IFRS_CCF_DETAIL_PROCESS
                                        GROUP BY SEGMENTATION_ID
                                )
                                GROUP BY SEGMENTATION_ID,C1,C2
                       ) A
                       JOIN IFRS_CCF_RULES_CONFIG B
                            ON A.SEGMENTATION_ID = B.SEGMENTATION_ID
                            AND V_CURRDATE >= B.CUT_OFF_DATE;
                COMMIT;
        COMMIT;
    END;