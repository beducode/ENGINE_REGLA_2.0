CREATE OR REPLACE PROCEDURE SP_IFRS_REKON_WC
IS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_NOMI_WC';

    INSERT INTO TMP_NOMI_WC
        SELECT N.CUSTOMER_NUMBER,
               N.CUSTOMER_NAME,
               N.DATA_SOURCE,
               N.ACCOUNT_NUMBER,
               N.EAD_AMOUNT_LCL,
               N.RESERVED_AMOUNT_5,
               N.REPORT_DATE
          FROM GTMP_IFRS_NOMINATIVE N
         WHERE     N.ASSESSMENT_IMP = 'W'
               AND N.SPECIAL_REASON <> 'BACK-T0-BACK, NO IMPAIRMENT'
               AND (   (    N.DATA_SOURCE = 'BTRD'
                        AND N.ACCOUNT_STATUS = 'A'
                        AND NVL (N.BI_CODE, ' ') <> '0')
                    OR (    N.DATA_SOURCE = 'CRD'
                        AND (   N.ACCOUNT_STATUS = 'A'
                             OR N.outstanding_on_bs_ccy > 0))
                    OR (N.DATA_SOURCE = 'ILS' AND N.account_status = 'A')
                    OR (N.DATA_SOURCE = 'LIMIT' AND N.account_status = 'A')
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
                          FROM GTMP_IFRS_NOMINATIVE L
                         WHERE     L.REPORT_DATE = N.REPORT_DATE
                               AND L.DATA_SOURCE = 'ILS'
                               AND L.ACCOUNT_STATUS = 'A'
                               AND N.DATA_SOURCE = 'LIMIT'
                               AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER);

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REKON_WC_DEBITUR';

    INSERT INTO IFRS_REKON_WC_DEBITUR
        SELECT W.CUSTOMER_NUMBER,
               C.CUSTOMER_NAME,
               W.SUM_OF_EAD_AMOUNT_LCL,
               W.ECL_TOTAL_FINAL_LCL,
               W.WORST_CASE,
               W.CEK
          FROM (  SELECT WL.CUSTOMER_NUMBER,
                         SUM (N.EAD_AMOUNT_LCL)       SUM_OF_EAD_AMOUNT_LCL,
                         SUM (N.RESERVED_AMOUNT_5)    ECL_TOTAL_FINAL_LCL,
                         WL.PERCENTAGE                WORST_CASE,
                           ROUND (
                               (  SUM (N.RESERVED_AMOUNT_5)
                                / SUM (N.EAD_AMOUNT_LCL)
                                * 100),
                               2)
                         - WL.PERCENTAGE              CEK
                    FROM TBLU_WORSTCASE_LIST WL
                         LEFT JOIN TMP_NOMI_WC N
                             ON     WL.CUSTOMER_NUMBER = N.CUSTOMER_NUMBER
                                AND WL.DOWNLOAD_DATE = N.REPORT_DATE
                   WHERE WL.DOWNLOAD_DATE =
                         (SELECT CURRDATE FROM IFRS_PRC_DATE)
                GROUP BY WL.CUSTOMER_NUMBER, WL.PERCENTAGE) W
               LEFT JOIN
               (SELECT CUSTOMER_NUMBER, CUSTOMER_NAME
                  FROM (SELECT ROW_NUMBER ()
                                   OVER (PARTITION BY CUSTOMER_NUMBER
                                         ORDER BY CUSTOMER_NUMBER)    NUM,
                               CUSTOMER_NUMBER,
                               CUSTOMER_NAME
                          FROM (SELECT DISTINCT
                                       CUSTOMER_NUMBER, CUSTOMER_NAME
                                  FROM TMP_NOMI_WC W
                                 WHERE LENGTH (NVL (CUSTOMER_NAME, ' ')) =
                                       (SELECT MAX (
                                                   LENGTH (
                                                       NVL (CUSTOMER_NAME,
                                                            ' ')))
                                          FROM TMP_NOMI_WC Z
                                         WHERE W.CUSTOMER_NUMBER =
                                               Z.CUSTOMER_NUMBER)))
                 WHERE NUM = 1) C
                   ON W.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REKON_WC_DETAIL';

    INSERT INTO IFRS_REKON_WC_DETAIL
        SELECT N.DATA_SOURCE,
               N.GOL_DEB,
               N.ACCOUNT_NUMBER,
               N.CUSTOMER_NUMBER,
               N.CUSTOMER_NAME,
               N.PRODUCT_CODE,
               N.SUB_SEGMENT,
               N.BI_COLLECTABILITY,
               WC.WORST_CASE,
               N.ASSESSMENT_IMP,
               N.OUTSTANDING_ON_BS_LCL,
               N.OUTSTANDING_OFF_BS_LCL,
               N.RESERVED_AMOUNT_5     ECL_TOTAL_FINAL_LCL,
               N.SPECIAL_REASON
          FROM IFRS_REKON_WC_DEBITUR  WC
               LEFT JOIN GTMP_IFRS_NOMINATIVE N
                   ON wc.CUSTOMER_NUMBER = n.CUSTOMER_NUMBER
         WHERE     (WC.CEK <> 0 OR WC.CEK IS NULL)
               --   AND N.REPORT_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE)
               --   AND WL.DOWNLOAD_DATE = N.REPORT_DATE
               AND (   (    N.DATA_SOURCE = 'BTRD'
                        AND N.ACCOUNT_STATUS = 'A'
                        AND NVL (N.BI_CODE, ' ') <> '0')
                    OR (    N.DATA_SOURCE = 'CRD'
                        AND (   N.ACCOUNT_STATUS = 'A'
                             OR N.outstanding_on_bs_ccy > 0))
                    OR (N.DATA_SOURCE = 'ILS' AND N.account_status = 'A')
                    OR (N.DATA_SOURCE = 'LIMIT' AND N.account_status = 'A')
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
                          FROM GTMP_IFRS_NOMINATIVE L
                         WHERE     L.REPORT_DATE = N.REPORT_DATE
                               AND L.DATA_SOURCE = 'ILS'
                               AND L.ACCOUNT_STATUS = 'A'
                               AND N.DATA_SOURCE = 'LIMIT'
                               AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER);

    COMMIT;
END;