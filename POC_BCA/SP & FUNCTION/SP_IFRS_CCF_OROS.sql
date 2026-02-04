CREATE OR REPLACE PROCEDURE SP_IFRS_CCF_OROS (
   v_DOWNLOADDATE DATE DEFAULT '01 JAN 1990')
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
BEGIN

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';


   IF v_DOWNLOADDATE = '01 JAN 1990'
   THEN
      SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
   ELSE
      V_CURRDATE := v_DOWNLOADDATE;
   END IF;


   SELECT ADD_MONTHS (V_CURRDATE, -12) INTO V_PREVDATE FROM DUAL;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

       SP_IFRS_INSERT_GTMP_FROM_IMA_M(V_CURRDATE, 'ILS,CRD');
       SP_IFRS_INSERT_GTMP_IMA_M_PREV(V_PREVDATE, 'ILS,CRD');

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS_CCF_HEADER_BU2024
    WHERE DOWNLOAD_DATE >= V_CURRDATE;
    commit;

    DELETE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_BU2024
    WHERE DOWNLOAD_DATE >= V_CURRDATE;
	COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
    WHERE DOWNLOAD_DATE >= V_CURRDATE;

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_UNP_BU2024
    WHERE DOWNLOAD_DATE >= V_CURRDATE;

   COMMIT;

       INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_DETAIL_BU2024
       (DOWNLOAD_DATE,
        CURRENT_DATE,
        ACCOUNT_NUMBER,
        CUSTOMER_NAME,
        CUSTOMER_NUMBER,
        FACILITY_NUMBER,
        ACCOUNT_STATUS,
        FIRST_NPL_DATE,
        OS_CUR,
        OS_PREV,
        LIMIT_CUR,
        LIMIT_PREV,
        REVOLVING_FLAG_L,
        PRODUCT_CODE_L,
        OS_SELISIH,
        LIMIT_SELISIH,
        CCF_RULE_ID,
        SEGMENTATION_ID,
        CURRENCY,
        USED_AMOUNT_CUR,
        USED_AMOUNT_PREV)
       SELECT A.DOWNLOAD_DATE,
              A.DOWNLOAD_DATE,
              A.ACCOUNT_NUMBER,
              A.CUSTOMER_NAME,
              A.CUSTOMER_NUMBER,
              A.FACILITY_NUMBER,
              A.ACCOUNT_STATUS,
              A.RESERVED_DATE_3,
              NVL(A.OUTSTANDING, 0)                              OS_CUR,
              NVL(B.OUTSTANDING, 0)                              OS_PREV,
              NVL(A.PLAFOND, 0)                                  LIMIT_CUR,
              NVL(B.PLAFOND, 0)                                  LIMIT_PREV,
              A.REVOLVING_FLAG,
              A.PRODUCT_CODE,
              (NVL(A.OUTSTANDING, 0) - NVL(B.OUTSTANDING, 0)) AS OS_SELISIH,
              (NVL(A.PLAFOND, 0) - NVL(B.OUTSTANDING, 0))     AS LIMIT_SELISIH,
              C.PKID                                          AS CCF_RULE_ID,
              C.SEGMENTATION_ID                               AS SEGMENTATION_ID,
              A.CURRENCY                                      AS CURRENCY,
              A.RATE_AMOUNT                                   AS USED_AMOUNT_CUR,
              B.RATE_AMOUNT                                   AS USED_AMOUNT_PREV
       FROM (SELECT A.DOWNLOAD_DATE,
                    A.MASTERID,
                    A.ACCOUNT_NUMBER,
                    FACILITY_NUMBER,
                    ACCOUNT_STATUS,
                    A.CURRENCY,
                    RESERVED_DATE_3,
                    (OUTSTANDING) OUTSTANDING,
                    (PLAFOND)PLAFOND,
                    REVOLVING_FLAG,
                    PRODUCT_CODE,
                    CUSTOMER_NAME,
                    CUSTOMER_NUMBER,
                    CCF_RULE_ID,
                    SEGMENT_RULE_ID,
                    RATE_AMOUNT
             FROM GTMP_IFRS_MASTER_ACCOUNT A
                      JOIN IFRS_MASTER_EXCHANGE_RATE D
                           ON A.CURRENCY = D.CURRENCY AND A.DOWNLOAD_DATE = D.DOWNLOAD_DATE
             WHERE A.RESERVED_DATE_3 IS NOT NULL
               AND A.ACCOUNT_STATUS = 'A') A
                JOIN (SELECT B.DOWNLOAD_DATE,
                             B.MASTERID,
                             B.ACCOUNT_NUMBER,
                             FACILITY_NUMBER,
                             ACCOUNT_STATUS,
                             B.CURRENCY,
                             RESERVED_DATE_3,
                             (OUTSTANDING) OUTSTANDING,
                             PLAFOND,
                             REVOLVING_FLAG,
                             PRODUCT_CODE,
                             CUSTOMER_NAME,
                             CUSTOMER_NUMBER,
                             CCF_RULE_ID,
                             SEGMENT_RULE_ID,
                             RATE_AMOUNT
                      FROM GTMP_IFRS_MASTER_ACCOUNT_PREV B
                               JOIN IFRS_MASTER_EXCHANGE_RATE D
                                    ON B.CURRENCY = D.CURRENCY AND B.DOWNLOAD_DATE = D.DOWNLOAD_DATE
                      WHERE ACCOUNT_STATUS = 'A') B
                     ON A.MASTERID = B.MASTERID
                JOIN IFRS_CCF_RULES_CONFIG C
                     ON A.CCF_RULE_ID = C.PKID
                         AND C.AVERAGE_METHOD = 'Simple';

       COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_DETAIL_PRC_BU2024 (PKID,
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
                                        PRODUCT_CODE_I,
                                        CCF_RULE_ID)
        SELECT 0,
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
               PRODUCT_CODE_I,
               CCF_RULE_ID
          FROM IFRS_CCF_DETAIL_BU2024
         WHERE DOWNLOAD_DATE = V_CURRDATE
               AND FACILITY_NUMBER NOT IN
                      (SELECT NVL (FACILITY_NUMBER, 0)
                         FROM IFRS_CCF_DETAIL_PRC_BU2024)
               AND PRODUCT_CODE_L <> 'CARDS'
               AND LAST_DAY (FIRST_NPL_DATE) = DOWNLOAD_DATE
      GROUP BY DOWNLOAD_DATE,
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
               PRODUCT_CODE_I,
               CCF_RULE_ID;

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_DETAIL_PRC_BU2024 (PKID,
                                        DOWNLOAD_DATE,
                                        CURRENT_DATE,
                                        PREVIOUS_DATE,
                                        FACILITY_NUMBER,
                                        ACCOUNT_STATUS,
                                        SEGMENTATION_ID,
                                        CCF_RULE_ID,
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
                                        PRODUCT_CODE_I)
        SELECT 0,
               DOWNLOAD_DATE,
               CURRENT_DATE,
               PREVIOUS_DATE,
               ACCOUNT_NUMBER,
               ACCOUNT_STATUS,
               SEGMENTATION_ID,
               CCF_RULE_ID,
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
          FROM IFRS_CCF_DETAIL_BU2024
         WHERE DOWNLOAD_DATE = V_CURRDATE
               AND ACCOUNT_NUMBER NOT IN
                      (SELECT NVL (FACILITY_NUMBER, 0)
                         FROM IFRS_CCF_DETAIL_PRC_BU2024)
               AND PRODUCT_CODE_L = 'CARDS'
               AND LAST_DAY (FIRST_NPL_DATE) = DOWNLOAD_DATE
      GROUP BY DOWNLOAD_DATE,
               "CURRENT_DATE",
               PREVIOUS_DATE,
               ACCOUNT_NUMBER,
               ACCOUNT_STATUS,
               SEGMENTATION_ID,
               CCF_RULE_ID,
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
               PRODUCT_CODE_I,
               CCF_RULE_ID;

   COMMIT;


   UPDATE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
      SET OS_SELISIH =
             CASE
                WHEN (OS_CUR - OS_PREV) < 0 THEN 0
                ELSE OS_CUR - OS_PREV
             END,
          LIMIT_SELISIH = LIMIT_CUR - OS_PREV
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_DETAIL_UNP_BU2024
      SELECT *
        FROM IFRS_CCF_DETAIL_PRC_BU2024
       WHERE DOWNLOAD_DATE = V_CURRDATE AND ( (LIMIT_CUR - OS_PREV) <= 0 --OR OS_CUR > LIMIT_CUR
                                             OR OS_PREV > LIMIT_CUR);

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
    WHERE DOWNLOAD_DATE = V_CURRDATE AND ( (LIMIT_CUR - OS_PREV) <= 0 --OR OS_CUR > LIMIT_CUR
                                          OR OS_PREV > LIMIT_CUR);

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_DETAIL_UNP_BU2024
      SELECT *
        FROM IFRS_CCF_DETAIL_PRC_BU2024
       WHERE DOWNLOAD_DATE = V_CURRDATE AND (LIMIT_SELISIH < 0);

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
    WHERE DOWNLOAD_DATE = V_CURRDATE AND (LIMIT_SELISIH < 0);

   COMMIT;

   UPDATE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
      SET CCF_RESULT =
             CASE
                WHEN (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV) > 1
                THEN
                   1
                WHEN (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV) < 0
                THEN
                   0
                ELSE
                   (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV)
             END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   UPDATE /*+ PARALLEL(12) */ IFRS_CCF_DETAIL_PRC_BU2024
      SET CCF_RESULT =
             CASE WHEN (OS_CUR - OS_PREV) <= 0 THEN 0 ELSE CCF_RESULT END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;


   INSERT /*+ PARALLEL(12) */ INTO IFRS_CCF_HEADER_BU2024 (PKID,
                                DOWNLOAD_DATE,
                                SEGMENTATION,
                                SEGMENTATION_ID,
                                CCF_RULE_ID,
                                CCF_RATE,
                                AVERAGE_METHOD)
      SELECT 0,
             V_CURRDATE,
             B.CCF_RULE_NAME,
             A.SEGMENTATION_ID,
             A.CCF_RULE_ID,
             --CASE WHEN NVL(CCF_OVERRIDE,0) <> 0 THEN CCF_OVERRIDE ELSE
             CASE WHEN B.AVERAGE_METHOD = 'Simple' THEN C1 ELSE C2 --END
             END,
             B.AVERAGE_METHOD
        FROM    (  SELECT SEGMENTATION_ID,
                          CCF_RULE_ID,
                          C1,
                          C2
                     FROM (  SELECT SEGMENTATION_ID,
                                    CCF_RULE_ID,
                                    AVG (CCF_RESULT) C1,
                                    (SUM (CASE WHEN OS_SELISIH > LIMIT_SELISIH THEN LIMIT_SELISIH ELSE OS_SELISIH END))
                                    / (SUM (LIMIT_SELISIH))
                                       C2
                               FROM IFRS_CCF_DETAIL_PRC_BU2024
                           GROUP BY SEGMENTATION_ID, CCF_RULE_ID)
                 GROUP BY SEGMENTATION_ID,
                          CCF_RULE_ID,
                          C1,
                          C2) A
             JOIN
                IFRS_CCF_RULES_CONFIG B
             ON A.CCF_RULE_ID = B.PKID AND V_CURRDATE >= B.CUT_OFF_DATE;

   COMMIT;
--INSERT INTO IFRS_CCF_HEADER_BU2024
--SELECT * FROM IFRS_CEF_HEADER WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

END;