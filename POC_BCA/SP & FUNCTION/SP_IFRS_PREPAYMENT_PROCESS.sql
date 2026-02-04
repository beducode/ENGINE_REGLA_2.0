CREATE OR REPLACE PROCEDURE SP_IFRS_PREPAYMENT_PROCESS (
   v_DOWNLOADDATE DATE DEFAULT ('1-JAN-1900'))
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
   V_START      DATE;
   V_ENDDATE    DATE;
BEGIN
   IF v_DOWNLOADDATE = '1-JAN-1900'
   THEN
      SELECT CURRDATE INTO V_CURRDATE FROM IFRS.IFRS_PRC_DATE;
   ELSE
      V_CURRDATE := v_DOWNLOADDATE;
   END IF;

   V_PREVDATE := ADD_MONTHS (V_CURRDATE, -1);
   V_START := ADD_MONTHS (V_CURRDATE, -1) + 1;
   SELECT LAST_DAY(ADD_MONTHS(CURRDATE,-1))
      INTO V_ENDDATE
   FROM IFRS.IFRS_PRC_DATE;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PREPAYMENTT_KK_DAY1';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PREPAYMENT_MST_PAY_ST';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PREPAYMENT_PROCESS';

   /*----------------------------------------------------------------------------------
   ADD WILLY - POPULASI PREPAYMENT DENGAN DATA IMA
   ----------------------------------------------------------------------------------*/
   INSERT INTO IFRS_PREPAYMENTT_KK_DAY1 (REPORT_DATE,
                                         DEAL_ID,
                                         CURRENCY,
                                         OS,
                                         PREPAYMENT,
                                         SCHEDULED,
                                         ACTUAL,
                                         DEAL_TYPE,
                                         DPD,
                                         KOLEK)
        SELECT A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.CURRENCY,
               A.OUTSTANDING,
               NVL (SUM (C.ORG_CCY_AMT), 0) PREPAYMENT,
               NVL (SUM (D.PRINCIPAL), 0) SCHEDULED,
               (B.OUTSTANDING - NVL (A.OUTSTANDING, 0)) ACTUAL,
               A.PRODUCT_CODE,
               A.DAY_PAST_DUE,
               A.BI_COLLECTABILITY
          FROM IFRS.IFRS_MASTER_ACCOUNT_MONTHLY a
               LEFT JOIN IFRS.IFRS_MASTER_ACCOUNT_MONTHLY b
                  ON     A.MASTERID = B.MASTERID
                     AND B.DOWNLOAD_DATE = V_PREVDATE
                     AND A.DATA_SOURCE = B.DATA_SOURCE
               LEFT JOIN IFRS.IFRS_TRANSACTION_DAILY C
                  ON     A.MASTERID = C.MASTERID
                     AND C.TRX_CODE = 'PP'
                     AND C.DOWNLOAD_DATE BETWEEN V_START AND V_CURRDATE
               LEFT JOIN (SELECT A.*
                            FROM    IFRS.IFRS_PAYM_SCHD_ALL A
                                 JOIN
                                    (  SELECT MASTERID,
                                              MAX (DOWNLOAD_DATE) DOWNLOAD_DATE
                                         FROM IFRS.IFRS_PAYM_SCHD_ALL
                                        WHERE PMTDATE BETWEEN V_START
                                                          AND V_CURRDATE
                                              AND DOWNLOAD_DATE <= V_CURRDATE
                                     GROUP BY MASTERID) B
                                 ON A.MASTERID = B.MASTERID
                                    AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                           WHERE A.PMTDATE BETWEEN V_START AND V_CURRDATE) D
                  ON A.MASTERID = D.MASTERID
                     AND D.PMTDATE BETWEEN V_START AND V_CURRDATE
         WHERE A.DATA_SOURCE = 'ILS' AND A.DOWNLOAD_DATE = V_CURRDATE
      GROUP BY A.DOWNLOAD_DATE,
               A.ACCOUNT_NUMBER,
               A.CURRENCY,
               A.OUTSTANDING,
               A.PRODUCT_CODE,
               (B.OUTSTANDING - NVL (A.OUTSTANDING, 0)),
               A.DAY_PAST_DUE,
               A.BI_COLLECTABILITY;

   COMMIT;


   /* Engine Tarik Data Finsto
   INSERT INTO IFRS_PREPAYMENTT_KK_DAY1
     SELECT DISTINCT * FROM TEST_PREPAYMENT
       WHERE LAST_DAY(REPORT_DATE)=V_CURRDATE;
   COMMIT;
   */


   /*TO COLLECT HISTORY DATA DETAILS*/
   --GET PAYMENT SETTING
   INSERT INTO IFRS.TMP_IFRS_PREPAYMENT_MST_PAY_ST (DOWNLOAD_DATE,
                                               MASTERID,
                                               ACCOUNT_NUMBER,
                                               COMPONENT_TYPE,
                                               INCREMENTS,
                                               DATE_START,
                                               DATE_END)
      SELECT B.DOWNLOAD_DATE,
             B.MASTERID,
             B.ACCOUNT_NUMBER,
             B.COMPONENT_TYPE,
             B.INCREMENTS,
             B.DATE_START,
             B.DATE_END
        FROM    (  SELECT MAX (B2.DOWNLOAD_DATE) DOWNLOAD_DATE,
                          MAX (B2.DATE_START) DATE_START,
                          B2.ACCOUNT_NUMBER
                     FROM    IFRS.IFRS_PREPAYMENTT_KK_DAY1 A2
                          JOIN
                             IFRS.IFRS_MASTER_PAYMENT_SETTING B2
                          ON A2.DEAL_ID = B2.ACCOUNT_NUMBER
                    WHERE DPD >= 0 AND KOLEK = '1' --IF LATE PAYMENT NOT SELECTED
                 GROUP BY B2.ACCOUNT_NUMBER) A
             JOIN
                IFRS.IFRS_MASTER_PAYMENT_SETTING B
             ON     A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                AND A.DATE_START = B.DATE_START;

   COMMIT;

   /*TO COLLECT PRODUCTION DATA DETAILS*/
   /*
   INSERT INTO IFRS_PREPAYMENT_PROCESS
   (
       DOWNLOAD_DATE,
       REPORT_DATE,
       MASTERID,
       ACCOUNT_NUMBER,
       CUSTOMER_NUMBER,
       CUSTOMER_NAME,
       ACCOUNT_STATUS,
       BI_COLLECTABILITY,
       REVOLVING_FLAG,
       PREPAYMENT_SEGMENT,
       CURRENCY,
       OUTSTANDING,
       PREPAYMENT,
       SCHEDULE,
       ACTUAL,
       RATE_AMOUNT,
       SEGMENTATION_ID,
       SEGMENTATION_NAME,
       PREPAYMENT_RULE_ID,
       PREPAYMENT_RULE_NAME,
       PRODUCT_CODE,
       INCREMENTS,
       COMPONENT_TYPE,
       DATA_SOURCE,
       LATE_PAYMENT_FLAG
   )
     SELECT
       A.DOWNLOAD_DATE,
       A.DOWNLOAD_DATE AS REPORT_DATE,
       A.MASTERID,
       A.ACCOUNT_NUMBER,
       A.CUSTOMER_NUMBER,
       A.CUSTOMER_NAME,
       A.ACCOUNT_STATUS,
       A.BI_COLLECTABILITY,
       A.REVOLVING_FLAG,
       A.PREPAYMENT_SEGMENT,
       A.CURRENCY,
       A.OUTSTANDING,
       B.ORG_CCY_AMT AS PREPAYMENT,
       C.PRINCIPAL AS SCHEDULE,
       (B.ORG_CCY_AMT + C.PRINCIPAL) AS ACTUAL,
       F.RATE_AMOUNT,
       E.SEGMENTATION_ID,
       A.PREPAYMENT_SEGMENT,
       A.PREPAYMENT_RULE_ID,
       E.PREPAYMENT_RULE_NAME,
       A.PRODUCT_CODE,
       D.INCREMENTS,
       D.COMPONENT_TYPE,
       A.DATA_SOURCE,
       E.LATE_PAYMENT_FLAG
     FROM IFRS_MASTER_ACCOUNT A
     JOIN IFRS_MASTER_EXCHANGE_RATE F
         ON F.DOWNLOAD_DATE = V_CURRDATE
         AND A.CURRENCY = F.CURRENCY
       JOIN IFRS_TRANSACTION_DAILY B
         ON A.MASTERID = B.MASTERID
       JOIN IFRS_PAYM_SCHD_ALL C
         ON A.MASTERID = C.MASTERID
       JOIN
       (
           SELECT MAX(DOWNLOAD_DATE), MASTERID, INCREMENTS, COMPONENT_TYPE
           FROM IFRS_MASTER_PAYMENT_SETTING
           WHERE V_CURRDATE BETWEEN DATE_START AND DATE_END
           GROUP BY MASTERID, INCREMENTS, COMPONENT_TYPE
       )D ON A.MASTERID = D.MASTERID
       JOIN IFRS_PREPAYMENT_RULES_CONFIG E
         ON A.PREPAYMENT_RULE_ID = E.PKID
     WHERE
           A.DAY_PAST_DUE <= 0
           AND B.TRX_CODE = 'PP'
           AND C.PMTDATE BETWEEN ADD_MONTHS(V_CURRDATE,-INCREMENTS) + 1 AND V_CURRDATE --ADDMONTH DARI AWAL BULAN PROSES SAMPAI, CONTOH TANGGAL 1 SAMPE TGL PROSES
           AND A.DOWNLOAD_DATE BETWEEN (TO_DATE('1'||'-'||EXTRACT(MONTH FROM V_CURRDATE)||'-'||EXTRACT(YEAR FROM V_CURRDATE),'dd-mm-yyyy')) AND V_CURRDATE;
   */

   INSERT INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT (PKID,
                                         DOWNLOAD_DATE,
                                         MASTERID,
                                         MASTER_ACCOUNT_CODE,
                                         ACCOUNT_NUMBER,
                                         CUSTOMER_NUMBER,
                                         CUSTOMER_NAME,
                                         ACCOUNT_STATUS,
                                         BI_COLLECTABILITY,
                                         REVOLVING_FLAG,
                                         PREPAYMENT_SEGMENT,
                                         PREPAYMENT_RULE_ID,
                                         RESTRUCTURE_FLAG,
                                         LOAN_DUE_DATE)
      SELECT PKID,
             DOWNLOAD_DATE,
             MASTERID,
             MASTER_ACCOUNT_CODE,
             ACCOUNT_NUMBER,
             CUSTOMER_NUMBER,
             CUSTOMER_NAME,
             ACCOUNT_STATUS,
             BI_COLLECTABILITY,
             REVOLVING_FLAG,
             PREPAYMENT_SEGMENT,
             PREPAYMENT_RULE_ID,
             RESTRUCTURE_FLAG,
             LOAN_DUE_DATE
        FROM IFRS.IFRS_MASTER_ACCOUNT_MONTHLY G
       WHERE DOWNLOAD_DATE = V_CURRDATE
             AND ACCOUNT_NUMBER IN
                    (SELECT ACCOUNT_NUMBER
                       FROM IFRS.TMP_IFRS_PREPAYMENT_MST_PAY_ST);

   COMMIT;

   INSERT INTO IFRS.IFRS_PREPAYMENT_PROCESS (DOWNLOAD_DATE,
                                        REPORT_DATE,
                                        MASTERID,
                                        ACCOUNT_NUMBER,
                                        CUSTOMER_NUMBER,
                                        CUSTOMER_NAME,
                                        ACCOUNT_STATUS,
                                        BI_COLLECTABILITY,
                                        REVOLVING_FLAG,
                                        PREPAYMENT_SEGMENT,
                                        CURRENCY,
                                        OUTSTANDING,
                                        PREPAYMENT,
                                        SCHEDULE,
                                        ACTUAL,
                                        RATE_AMOUNT,
                                        SEGMENTATION_ID,
                                        SEGMENTATION_NAME,
                                        PREPAYMENT_RULE_ID,
                                        PREPAYMENT_RULE_NAME,
                                        PRODUCT_CODE,
                                        INCREMENTS,
                                        COMPONENT_TYPE,
                                        DATA_SOURCE,
                                        LATE_PAYMENT_FLAG,
                                        RESTRUCTURE_FLAG,
                                        LOAN_DUE_DATE)
        SELECT V_CURRDATE DOWNLOAD_DATE,
               A.REPORT_DATE,
               D.MASTERID,
               A.DEAL_ID ACCOUNT_NUMBER,
               D.CUSTOMER_NUMBER,
               D.CUSTOMER_NAME,
               D.ACCOUNT_STATUS,
               D.BI_COLLECTABILITY,
               NVL (D.REVOLVING_FLAG, 0) REVOLVING_FLAG,
               D.PREPAYMENT_SEGMENT,
               A.CURRENCY,
               A.OS OUTSTANDING,
               A.PREPAYMENT,
               SUM (A.SCHEDULED) SCHEDULE,
               A.ACTUAL,
               B.RATE_AMOUNT,
               D.SEGMENTATION_ID,
               D.SEGMENTATION_NAME,
               D.PREPAYMENT_RULE_ID,
               D.PREPAYMENT_RULE_NAME,
               A.DEAL_TYPE PRODUCT_CODE,
               C.INCREMENTS,
               C.COMPONENT_TYPE,
               'ILS' DATA_SOURCE,
               D.LATE_PAYMENT_FLAG,
               D.RESTRUCTURE_FLAG,
               D.LOAN_DUE_DATE
          FROM IFRS.IFRS_PREPAYMENTT_KK_DAY1 A
               JOIN IFRS.IFRS_MASTER_EXCHANGE_RATE B
                  ON B.DOWNLOAD_DATE = V_CURRDATE AND A.CURRENCY = B.CURRENCY
               JOIN IFRS.TMP_IFRS_PREPAYMENT_MST_PAY_ST C
                  ON A.DEAL_ID = C.ACCOUNT_NUMBER
                     AND C.COMPONENT_TYPE IN (0, 2)
               LEFT JOIN (SELECT A2.*,
                                 B2.PREPAYMENT_RULE_NAME,
                                 B2.LATE_PAYMENT_FLAG,
                                 B2.SEGMENTATION_ID,
                                 C2.SUB_SEGMENT SEGMENTATION_NAME
                            FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT A2
                                 JOIN IFRS.IFRS_PREPAYMENT_RULES_CONFIG B2
                                    ON     A2.PREPAYMENT_RULE_ID = B2.PKID
                                       AND B2.LATE_PAYMENT_FLAG = 0
                                       AND V_CURRDATE >= B2.CUT_OFF_DATE
                                 JOIN IFRS.IFRS_MSTR_SEGMENT_RULES_HEADER C2
                                    ON B2.SEGMENTATION_ID = C2.PKID) D
                  ON A.DEAL_ID = D.ACCOUNT_NUMBER
         WHERE A.ACTUAL > 0
      GROUP BY A.REPORT_DATE,
               D.MASTERID,
               A.DEAL_ID,
               D.CUSTOMER_NUMBER,
               D.CUSTOMER_NAME,
               D.ACCOUNT_STATUS,
               D.BI_COLLECTABILITY,
               D.REVOLVING_FLAG,
               D.PREPAYMENT_SEGMENT,
               A.CURRENCY,
               A.OS,
               A.PREPAYMENT,
               A.ACTUAL,
               B.RATE_AMOUNT,
               D.SEGMENTATION_ID,
               D.SEGMENTATION_NAME,
               D.PREPAYMENT_RULE_ID,
               D.PREPAYMENT_RULE_NAME,
               A.DEAL_TYPE,
               C.INCREMENTS,
               C.COMPONENT_TYPE,
               D.LATE_PAYMENT_FLAG,
               D.RESTRUCTURE_FLAG,
               D.LOAN_DUE_DATE;

   COMMIT;

   DELETE IFRS.IFRS_PREPAYMENT_UNPROCESS
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS.IFRS_PREPAYMENT_UNPROCESS (DOWNLOAD_DATE,
                                          REPORT_DATE,
                                          MASTERID,
                                          ACCOUNT_NUMBER,
                                          CUSTOMER_NUMBER,
                                          CUSTOMER_NAME,
                                          ACCOUNT_STATUS,
                                          PREPAYMENT_SEGMENT,
                                          SEGMENTATION_ID,
                                          SEGMENTATION_NAME,
                                          PREPAYMENT_RULE_ID,
                                          PREPAYMENT_RULE_NAME,
                                          CURRENCY,
                                          BI_COLLECTABILITY,
                                          OUTSTANDING,
                                          PREPAYMENT,
                                          SCHEDULE,
                                          ACTUAL,
                                          RATE_AMOUNT,
                                          SMM,
                                          REVOLVING_FLAG,
                                          PRODUCT_CODE,
                                          DATA_SOURCE,
                                          LATE_PAYMENT_FLAG,
                                          INCREMENTS)
      SELECT DOWNLOAD_DATE,
             REPORT_DATE,
             NVL (MASTERID, 0),
             ACCOUNT_NUMBER,
             NVL (CUSTOMER_NUMBER, ' '),
             CUSTOMER_NAME,
             ACCOUNT_STATUS,
             PREPAYMENT_SEGMENT,
             SEGMENTATION_ID,
             SEGMENTATION_NAME,
             PREPAYMENT_RULE_ID,
             PREPAYMENT_RULE_NAME,
             CURRENCY,
             BI_COLLECTABILITY,
             OUTSTANDING,
             PREPAYMENT,
             SCHEDULE,
             ACTUAL,
             RATE_AMOUNT,
             SMM,
             REVOLVING_FLAG,
             PRODUCT_CODE,
             DATA_SOURCE,
             LATE_PAYMENT_FLAG,
             INCREMENTS
        FROM IFRS.IFRS_PREPAYMENT_PROCESS
       WHERE (   SMM < 0
              OR ( (OUTSTANDING + ACTUAL) - SCHEDULE) <= 0
              OR SCHEDULE < 0)
             AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   DELETE IFRS.IFRS_PREPAYMENT_PROCESS
    WHERE (SMM < 0 OR ( (OUTSTANDING + ACTUAL) - SCHEDULE) <= 0)
          AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   DELETE IFRS.IFRS_PREPAYMENT_PROCESS
    WHERE ACCOUNT_NUMBER IN (SELECT ACCOUNT_NUMBER
                               FROM IFRS.IFRS_PREPAYMENT_UNPROCESS
                              WHERE SCHEDULE < 0);

   COMMIT;

   UPDATE IFRS.IFRS_PREPAYMENT_PROCESS
      SET SMM =
             CASE
                WHEN OUTSTANDING = 0
                THEN
                   CASE
                      WHEN LOAN_DUE_DATE <= V_CURRDATE
                      THEN
                         0
                      ELSE
                         ROUND (
                            ( (ACTUAL * RATE_AMOUNT)
                             - (SCHEDULE * RATE_AMOUNT))
                            / ( ( (OUTSTANDING + ACTUAL) - SCHEDULE)
                               * RATE_AMOUNT),
                            6)
                   END
                ELSE
                   CASE
                      WHEN PREPAYMENT = 0
                      THEN
                         0
                      WHEN ( ( (OUTSTANDING + ACTUAL) - SCHEDULE)
                            * RATE_AMOUNT) = 0
                      THEN
                         0
                      WHEN OUTSTANDING = 0 AND LOAN_DUE_DATE <= V_CURRDATE
                      THEN
                         0
                      WHEN ROUND (
                              ( (ACTUAL * RATE_AMOUNT)
                               - (SCHEDULE * RATE_AMOUNT))
                              / ( ( (OUTSTANDING + ACTUAL) - SCHEDULE)
                                 * RATE_AMOUNT),
                              6) >= 100
                      THEN
                         100
                      ELSE
                         ROUND (
                            ( (ACTUAL * RATE_AMOUNT)
                             - (SCHEDULE * RATE_AMOUNT))
                            / ( ( (OUTSTANDING + ACTUAL) - SCHEDULE)
                               * RATE_AMOUNT),
                            6)
                   END
             END
    WHERE download_date = v_currdate;

   COMMIT;

   DELETE IFRS.IFRS_PREPAYMENT_PROCESS
    WHERE (SMM < 0 OR ( (OUTSTANDING + ACTUAL) - SCHEDULE) <= 0)
          AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   /*
       DELETE IFRS_PREPAYMENT_BY_CUST
     WHERE DOWNLOAD_DATE >= V_CURRDATE;
   COMMIT;
   */

   --UPDATE IFRS_PREPAYMENT_PROCESS
   --  SET SMM = 1
   --  WHERE ACTUAL > OUTSTANDING
   --        AND DOWNLOAD_DATE = V_CURRDATE;
   --COMMIT;

   /*
   INSERT INTO IFRS_PREPAYMENT_BY_CUST
   (
       DOWNLOAD_DATE,
       REPORT_DATE,
       CUSTOMER_NUMBER,
       CUSTOMER_NAME,
       PREPAYMENT_SEGMENT,
       SEGMENTATION_ID,
       SEGMENTATION_NAME,
       PREPAYMENT_RULE_ID,
       PREPAYMENT_RULE_NAME,
       OUTSTANDING,
       PREPAYMENT,
       SCHEDULE,
       ACTUAL,
       DATA_SOURCE
   )
     SELECT
         DOWNLOAD_DATE,
         REPORT_DATE,
         NVL(CUSTOMER_NUMBER,' '),
         CUSTOMER_NAME,
         PREPAYMENT_SEGMENT,
         SEGMENTATION_ID,
         SEGMENTATION_NAME,
         PREPAYMENT_RULE_ID,
         PREPAYMENT_RULE_NAME,
         SUM(OUTSTANDING) OUTSTANDING,
         SUM(PREPAYMENT) PREPAYMENT,
         SUM(SCHEDULE) SCHEDULE,
         SUM(ACTUAL) ACTUAL,
         DATA_SOURCE
     FROM IFRS_PREPAYMENT_PROCESS
       WHERE DOWNLOAD_DATE = V_CURRDATE
       GROUP BY
           DOWNLOAD_DATE,
           REPORT_DATE,
           CUSTOMER_NUMBER,
           CUSTOMER_NAME,
           PREPAYMENT_SEGMENT,
           SEGMENTATION_ID,
           SEGMENTATION_NAME,
           PREPAYMENT_RULE_ID,
           PREPAYMENT_RULE_NAME,
           DATA_SOURCE;
   COMMIT;
   */

   /*------------------------------------------------------------------------------
   Exclude Akun Menunggak
   ------------------------------------------------------------------------------*/

   INSERT INTO IFRS.IFRS_PREPAYMENT_DPD_CHECK
      SELECT DOWNLOAD_DATE, ACCOUNT_NUMBER, DAY_PAST_DUE
        FROM IFRS.IFRS_MASTER_ACCOUNT_MONTHLY
       WHERE (DAY_PAST_DUE > 0 OR BI_COLLECTABILITY <> '1')
             AND ACCOUNT_NUMBER NOT IN
                    (SELECT DISTINCT ACCOUNT_NUMBER
                       FROM IFRS.IFRS_PREPAYMENT_DPD_CHECK)
             AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   /*------------------------------------------------------------------------------
   Finalisasi Populasi Akun
   ------------------------------------------------------------------------------*/

   DELETE IFRS.IFRS_PREPAYMENT_DETAIL
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS.IFRS_PREPAYMENT_DETAIL (DOWNLOAD_DATE,
                                       REPORT_DATE,
                                       MASTERID,
                                       ACCOUNT_NUMBER,
                                       CURRENCY,
                                       OUTSTANDING,
                                       PREPAYMENT,
                                       SCHEDULE,
                                       ACTUAL,
                                       RATE_AMOUNT,
                                       SMM,
                                       PREPAYMENT_SEGMENT,
                                       SEGMENTATION_ID,
                                       INCREMENTS,
                                       DURATION,
                                       COMPONENT_TYPE,
                                       DATA_SOURCE)
      SELECT A.DOWNLOAD_DATE,
             A.REPORT_DATE,
             NVL (A.MASTERID, 0) MASTERID,
             A.ACCOUNT_NUMBER,
             A.CURRENCY,
             A.OUTSTANDING,
             A.PREPAYMENT,
             A.SCHEDULE,
             A.ACTUAL,
             A.RATE_AMOUNT,
             A.SMM,
             NVL (A.PREPAYMENT_SEGMENT, '-') PREPAYMENT_SEGMENT,
             NVL (A.SEGMENTATION_ID, 0) SEGMENTATION_ID,
             A.INCREMENTS,
             CASE
                WHEN A.INCREMENTS = 1 THEN 12
                WHEN A.INCREMENTS = 3 THEN 4
                WHEN A.INCREMENTS = 6 THEN 2
                WHEN A.INCREMENTS = 12 THEN 1
             END
                DURATION,
             A.COMPONENT_TYPE,
             A.DATA_SOURCE
        FROM IFRS.IFRS_PREPAYMENT_PROCESS A
       WHERE     DOWNLOAD_DATE = V_CURRDATE
             AND A.SMM >= 0
             AND A.ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER
                                            FROM IFRS.IFRS_PREPAYMENT_DPD_CHECK
                                           WHERE DOWNLOAD_DATE < V_CURRDATE)
             AND A.RESTRUCTURE_FLAG = 0;

   COMMIT;


   WHILE V_CURRDATE <= V_ENDDATE  LOOP

        ------------------------------------------------------------------
        -- A: seg lama 543/545 -> seg baru 253
        ------------------------------------------------------------------
        INSERT /*+ PARALLEL(IFRS_PREPAYMENT_DETAIL,4) */
        INTO IFRS.IFRS_PREPAYMENT_DETAIL (
            DOWNLOAD_DATE,
            REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            SEGMENTATION_ID,
            PREPAYMENT_SEGMENT,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            INCREMENTS,
            DURATION,
            COMPONENT_TYPE,
            DATA_SOURCE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST,
            UPDATEDBY,
            UPDATEDDATE,
            UPDATEDHOST,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME
        )
        SELECT
            a.DOWNLOAD_DATE,
            a.REPORT_DATE,
            a.MASTERID,
            a.ACCOUNT_NUMBER,
            '253' AS SEGMENTATION_ID,
            a.PREPAYMENT_SEGMENT,
            a.CURRENCY,
            a.OUTSTANDING,
            a.PREPAYMENT,
            a.SCHEDULE,
            a.ACTUAL,
            a.RATE_AMOUNT,
            a.SMM,
            a.INCREMENTS,
            a.DURATION,
            a.COMPONENT_TYPE,
            a.DATA_SOURCE,
            'SYSTEM' AS CREATEDBY,
            SYSDATE  AS CREATEDDATE,
            'SYSTEM' AS CREATEDHOST,
            NULL AS UPDATEDBY,
            NULL AS UPDATEDDATE,
            NULL AS UPDATEDHOST,
            a.CUSTOMER_NUMBER,
            a.CUSTOMER_NAME
        FROM IFRS.IFRS_PREPAYMENT_DETAIL a
        WHERE a.DOWNLOAD_DATE = V_CURRDATE
          AND a.SEGMENTATION_ID IN ('543','545')
          -- hanya account yg muncul SEKALI untuk seg lama pd tanggal ini
          AND a.ACCOUNT_NUMBER IN (
                SELECT ACCOUNT_NUMBER
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE = V_CURRDATE
                  AND b.SEGMENTATION_ID IN ('543','545')
                GROUP BY ACCOUNT_NUMBER
                HAVING COUNT(*) = 1
          )
          -- hindari duplikasi seg baru pd tanggal ini
          AND NOT EXISTS (
                SELECT 1
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE   = V_CURRDATE
                  AND b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
                  AND b.SEGMENTATION_ID = '253'
          );

        ------------------------------------------------------------------
        -- B: seg lama 547/549 -> seg baru 252
        ------------------------------------------------------------------
        INSERT /*+ PARALLEL(IFRS_PREPAYMENT_DETAIL,4) */
        INTO IFRS.IFRS_PREPAYMENT_DETAIL (
            DOWNLOAD_DATE,
            REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            SEGMENTATION_ID,
            PREPAYMENT_SEGMENT,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            INCREMENTS,
            DURATION,
            COMPONENT_TYPE,
            DATA_SOURCE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST,
            UPDATEDBY,
            UPDATEDDATE,
            UPDATEDHOST,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME
        )
        SELECT
            a.DOWNLOAD_DATE,
            a.REPORT_DATE,
            a.MASTERID,
            a.ACCOUNT_NUMBER,
            '252' AS SEGMENTATION_ID,
            a.PREPAYMENT_SEGMENT,
            a.CURRENCY,
            a.OUTSTANDING,
            a.PREPAYMENT,
            a.SCHEDULE,
            a.ACTUAL,
            a.RATE_AMOUNT,
            a.SMM,
            a.INCREMENTS,
            a.DURATION,
            a.COMPONENT_TYPE,
            a.DATA_SOURCE,
            'SYSTEM' AS CREATEDBY,
            SYSDATE  AS CREATEDDATE,
            'SYSTEM' AS CREATEDHOST,
            NULL AS UPDATEDBY,
            NULL AS UPDATEDDATE,
            NULL AS UPDATEDHOST,
            a.CUSTOMER_NUMBER,
            a.CUSTOMER_NAME
        FROM IFRS.IFRS_PREPAYMENT_DETAIL a
        WHERE a.DOWNLOAD_DATE = V_CURRDATE
          AND a.SEGMENTATION_ID IN ('547','549')
          AND a.ACCOUNT_NUMBER IN (
                SELECT ACCOUNT_NUMBER
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE = V_CURRDATE
                  AND b.SEGMENTATION_ID IN ('547','549')
                GROUP BY ACCOUNT_NUMBER
                HAVING COUNT(*) = 1
          )
          AND NOT EXISTS (
                SELECT 1
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE   = V_CURRDATE
                  AND b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
                  AND b.SEGMENTATION_ID = '252'
          );

        COMMIT;

        V_CURRDATE := ADD_MONTHS(V_CURRDATE, 1);
    END LOOP;

    IF v_DOWNLOADDATE = '1-JAN-1900'
   THEN
      SELECT CURRDATE INTO V_CURRDATE FROM IFRS.IFRS_PRC_DATE;
   ELSE
      V_CURRDATE := v_DOWNLOADDATE;
   END IF;

   DELETE IFRS.IFRS_PREPAYMENT_HEADER
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS.IFRS_PREPAYMENT_HEADER (DOWNLOAD_DATE,
                                       SEGMENTATION_ID,
                                       SEGMENTATION_NAME,
                                       PREPAYMENT_RULE_ID,
                                       PREPAYMENT_RULE_NAME,
                                       AVERAGE_SMM,
                                       PREPAYMENT_RATE,
                                       DURATION)
        SELECT V_CURRDATE DOWNLOAD_DATE,
               A.SEGMENTATION_ID,
               NVL (C.SEGMENT, '-') SEGMENTATION_NAME,
               NVL (D.PKID, 0) PREPAYMENT_RULE_ID,
               NVL (D.PREPAYMENT_RULE_NAME, '-') PREPAYMENT_RULE_NAME,
               AVG (A.SMM) AVERAGE_SMM,
               ROUND (
                  1
                  - POWER (
                       (1 - AVG (A.SMM)),
                       CASE
                          WHEN A.INCREMENTS = 1 THEN 12
                          WHEN A.INCREMENTS = 3 THEN 4
                          WHEN A.INCREMENTS = 6 THEN 2
                          WHEN A.INCREMENTS = 12 THEN 1
                       END),
                  4)
                  PREPAYMENT_RATE,
               CASE
                  WHEN A.INCREMENTS = 1 THEN 12
                  WHEN A.INCREMENTS = 3 THEN 4
                  WHEN A.INCREMENTS = 6 THEN 2
                  WHEN A.INCREMENTS = 12 THEN 1
               END
                  DURATION
          FROM IFRS.IFRS_PREPAYMENT_DETAIL A
               LEFT JOIN IFRS.IFRS_MSTR_SEGMENT_RULES_HEADER C
                  ON A.SEGMENTATION_ID = C.PKID
               LEFT JOIN IFRS.IFRS_PREPAYMENT_RULES_CONFIG D
                  ON C.PKID = D.SEGMENTATION_ID
         WHERE     A.DOWNLOAD_DATE <= V_CURRDATE
               AND D.AVERAGE_METHOD = 'Simple'
               AND A.SMM >= 0
               AND A.DOWNLOAD_dATE <= V_CURRDATE
               AND A.DURATION = 12
      GROUP BY A.SEGMENTATION_ID,
               C.SEGMENT,
               D.PKID,
               D.PREPAYMENT_RULE_NAME,
               A.INCREMENTS;

   COMMIT;
END;