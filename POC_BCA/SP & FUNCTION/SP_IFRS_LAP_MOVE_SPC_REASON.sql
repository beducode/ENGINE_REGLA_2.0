CREATE OR REPLACE PROCEDURE SP_IFRS_LAP_MOVE_SPC_REASON
IS
   CURR_DATE   DATE;
   PREV_DATE   DATE;
BEGIN
   SELECT TO_DATE(MAX (REPORT_DATE)) INTO CURR_DATE FROM IFRS_NOMINATIVE;

   SELECT TO_DATE(LAST_DAY (ADD_MONTHS (TRUNC (MAX (report_date), 'mm'), -1)))
     INTO PREV_DATE
     FROM IFRS_NOMINATIVE;

   --MOVEMENT_ADJ_KPR_KKB
   EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LAP_ADJ_KPR_KKB';

   INSERT INTO TMP_LAP_ADJ_KPR_KKB (SUB_SEGMENT,
                                    CURR_EAD_AMOUNT_LCL,
                                    CURR_ECL_TOTAL_FINAL_LCL,
                                    PREV_EAD_AMOUNT_LCL,
                                    PREV_ECL_TOTAL_FINAL_LCL,
                                    PERGERAKAN_EAD_AMOUNT_LCL,
                                    PERGERAKAN_ECL_TOTAL_FINAL_LCL)
        SELECT CURR.sub_segment,
               CURR.EAD_AMOUNT_LCL    AS CURR_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL AS CURR_ECL_TOTAL_FINAL_LCL,
               PREV.EAD_AMOUNT_LCL    AS PREV_EAD_AMOUNT_LCL,
               PREV.ECL_TOTAL_FINAL_LCL AS PREV_ECL_TOTAL_FINAL_LCL,
               CURR.EAD_AMOUNT_LCL - PREV.EAD_AMOUNT_LCL
                  AS PERGERAKAN_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL - PREV.ECL_TOTAL_FINAL_LCL
                  AS PERGERAKAN_ECL_TOTAL_FINAL_LCL
          FROM (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'ADJUSTMENT COVID'
                         AND N.REPORT_DATE = CURR_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) CURR
               LEFT JOIN
               (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'ADJUSTMENT COVID'
                         AND N.REPORT_DATE = PREV_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) PREV
                  ON     curr.sub_segment = prev.sub_segment
                     AND CURR.CURRENCY = PREV.CURRENCY
      ORDER BY SUB_SEGMENT;

   COMMIT;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_MOVE_ADJ_KPR_KKB';

   INSERT INTO IFRS_MOVE_ADJ_KPR_KKB (SUB_SEGMENT,
                                      CURR_EAD_AMOUNT_LCL,
                                      CURR_ECL_TOTAL_FINAL_LCL,
                                      PREV_EAD_AMOUNT_LCL,
                                      PREV_ECL_TOTAL_FINAL_LCL,
                                      PERGERAKAN_EAD_AMOUNT_LCL,
                                      PERGERAKAN_ECL_TOTAL_FINAL_LCL)
      (SELECT * FROM TMP_LAP_ADJ_KPR_KKB
       UNION ALL
       SELECT 'GRAND TOTAL' AS Sub_segment,
              SUM (curr_ead_amount_lcl),
              SUM (curr_ecl_total_final_lcl),
              SUM (prev_ead_amount_lcl),
              SUM (prev_ecl_total_final_lcl),
              SUM (pergerakan_ead_amount_lcl),
              SUM (pergerakan_ecl_total_final_lcl)
         FROM TMP_LAP_ADJ_KPR_KKB);

   COMMIT;


   --MOVEMENT_CKPN_100
   EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LAP_CKPN_100';

   INSERT INTO TMP_LAP_CKPN_100 (SUB_SEGMENT,
                                 CURR_EAD_AMOUNT_LCL,
                                 CURR_ECL_TOTAL_FINAL_LCL,
                                 PREV_EAD_AMOUNT_LCL,
                                 PREV_ECL_TOTAL_FINAL_LCL,
                                 PERGERAKAN_EAD_AMOUNT_LCL,
                                 PERGERAKAN_ECL_TOTAL_FINAL_LCL)
        SELECT CURR.sub_segment,
               CURR.EAD_AMOUNT_LCL    AS CURR_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL AS CURR_ECL_TOTAL_FINAL_LCL,
               PREV.EAD_AMOUNT_LCL    AS PREV_EAD_AMOUNT_LCL,
               PREV.ECL_TOTAL_FINAL_LCL AS PREV_ECL_TOTAL_FINAL_LCL,
               CURR.EAD_AMOUNT_LCL - PREV.EAD_AMOUNT_LCL
                  AS PERGERAKAN_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL - PREV.ECL_TOTAL_FINAL_LCL
                  AS PERGERAKAN_ECL_TOTAL_FINAL_LCL
          FROM (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'CKPN 100%'
                         AND N.ASSESSMENT_IMP <> 'I'
                         AND N.REPORT_DATE = CURR_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) CURR
               LEFT JOIN
               (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'CKPN 100%'
                         AND N.ASSESSMENT_IMP <> 'I'
                         AND N.REPORT_DATE = PREV_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) PREV
                  ON     CURR.SUB_SEGMENT = PREV.SUB_SEGMENT
                     AND CURR.CURRENCY = PREV.CURRENCY
      ORDER BY SUB_SEGMENT;

   COMMIT;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_MOVE_CKPN_100';

   INSERT INTO IFRS_MOVE_CKPN_100 (SUB_SEGMENT,
                                   CURR_EAD_AMOUNT_LCL,
                                   CURR_ECL_TOTAL_FINAL_LCL,
                                   PREV_EAD_AMOUNT_LCL,
                                   PREV_ECL_TOTAL_FINAL_LCL,
                                   PERGERAKAN_EAD_AMOUNT_LCL,
                                   PERGERAKAN_ECL_TOTAL_FINAL_LCL)
      (SELECT * FROM TMP_LAP_CKPN_100
       UNION ALL
       SELECT 'GRAND TOTAL' AS Sub_segment,
              SUM (curr_ead_amount_lcl),
              SUM (curr_ecl_total_final_lcl),
              SUM (prev_ead_amount_lcl),
              SUM (prev_ecl_total_final_lcl),
              SUM (pergerakan_ead_amount_lcl),
              SUM (pergerakan_ecl_total_final_lcl)
         FROM TMP_LAP_CKPN_100);

   COMMIT;


   --MOVEMENT_DISASTER_LOAN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LAP_DISASTER_LOAN';

   INSERT INTO TMP_LAP_DISASTER_LOAN (SUB_SEGMENT,
                                      CURR_EAD_AMOUNT_LCL,
                                      CURR_ECL_TOTAL_FINAL_LCL,
                                      PREV_EAD_AMOUNT_LCL,
                                      PREV_ECL_TOTAL_FINAL_LCL,
                                      PERGERAKAN_EAD_AMOUNT_LCL,
                                      PERGERAKAN_ECL_TOTAL_FINAL_LCL)
        SELECT CURR.sub_segment,
               CURR.EAD_AMOUNT_LCL    AS CURR_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL AS CURR_ECL_TOTAL_FINAL_LCL,
               PREV.EAD_AMOUNT_LCL    AS PREV_EAD_AMOUNT_LCL,
               PREV.ECL_TOTAL_FINAL_LCL AS PREV_ECL_TOTAL_FINAL_LCL,
               CURR.EAD_AMOUNT_LCL - PREV.EAD_AMOUNT_LCL
                  AS PERGERAKAN_EAD_AMOUNT_LCL,
               CURR.ECL_TOTAL_FINAL_LCL - PREV.ECL_TOTAL_FINAL_LCL
                  AS PERGERAKAN_ECL_TOTAL_FINAL_LCL
          FROM (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'DISASTER LOAN'
                         AND N.REPORT_DATE = CURR_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) CURR
               LEFT JOIN
               (  SELECT N.SUB_SEGMENT,
                         N.CURRENCY,
                         SUM (N.EAD_AMOUNT_LCL) AS EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5) AS ECL_TOTAL_FINAL_LCL
                    FROM IFRS_NOMINATIVE N
                   WHERE     1 = 1
                         AND N.SPECIAL_REASON = 'DISASTER LOAN'
                         AND N.REPORT_DATE = PREV_DATE
                         AND (   (    N.DATA_SOURCE = 'BTRD'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.BI_CODE, ' ') <> '0')
                              OR (    N.DATA_SOURCE = 'CRD'
                                  AND (   N.ACCOUNT_STATUS = 'A'
                                       OR N.outstanding_on_bs_ccy > 0))
                              OR (    N.DATA_SOURCE = 'ILS'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'LIMIT'
                                  AND N.account_status = 'A')
                              OR (    N.DATA_SOURCE = 'KTP'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'PBMM'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND UPPER (N.PRODUCT_CODE) <> 'BORROWING')
                              OR (    N.DATA_SOURCE = 'RKN'
                                  AND N.ACCOUNT_STATUS = 'A'
                                  AND NVL (N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
                         AND NOT EXISTS
                                (SELECT 1
                                   FROM IFRS_NOMINATIVE L
                                  WHERE     L.REPORT_DATE = N.REPORT_DATE
                                        AND L.DATA_SOURCE = 'ILS'
                                        AND L.ACCOUNT_STATUS = 'A'
                                        AND N.DATA_SOURCE = 'LIMIT'
                                        AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
                GROUP BY N.SUB_SEGMENT, N.CURRENCY) PREV
                  ON     CURR.SUB_SEGMENT = PREV.SUB_SEGMENT
                     AND CURR.CURRENCY = PREV.CURRENCY
      ORDER BY SUB_SEGMENT;

   COMMIT;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_MOVE_DISASTER_LOAN';

   INSERT INTO IFRS_MOVE_DISASTER_LOAN (SUB_SEGMENT,
                                        CURR_EAD_AMOUNT_LCL,
                                        CURR_ECL_TOTAL_FINAL_LCL,
                                        PREV_EAD_AMOUNT_LCL,
                                        PREV_ECL_TOTAL_FINAL_LCL,
                                        PERGERAKAN_EAD_AMOUNT_LCL,
                                        PERGERAKAN_ECL_TOTAL_FINAL_LCL)
      (SELECT * FROM TMP_LAP_DISASTER_LOAN
       UNION ALL
       SELECT 'GRAND TOTAL' AS Sub_segment,
              SUM (curr_ead_amount_lcl),
              SUM (curr_ecl_total_final_lcl),
              SUM (prev_ead_amount_lcl),
              SUM (prev_ecl_total_final_lcl),
              SUM (pergerakan_ead_amount_lcl),
              SUM (pergerakan_ecl_total_final_lcl)
         FROM TMP_LAP_DISASTER_LOAN);

   COMMIT;
END;