CREATE OR REPLACE PROCEDURE SP_IFRS_REKON_INDIMP IS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_NOMI_INDIMP';

    INSERT INTO TMP_NOMI_INDIMP
    SELECT N.CUSTOMER_NUMBER,
       N.CUSTOMER_NAME,
       N.DATA_SOURCE,
       N.ACCOUNT_NUMBER,
       N.OUTSTANDING_ON_BS_LCL,
       N.EAD_AMOUNT_LCL,
       N.RESERVED_AMOUNT_5,
       N.REPORT_DATE
FROM GTMP_IFRS_NOMINATIVE N
WHERE N.ASSESSMENT_IMP = 'I'
  AND ((N.DATA_SOURCE = 'BTRD'
    AND N.ACCOUNT_STATUS = 'A'
    AND NVL(N.BI_CODE, ' ') <> '0')
    OR (N.DATA_SOURCE = 'CRD'
        AND (N.ACCOUNT_STATUS = 'A' OR N.outstanding_on_bs_ccy > 0))
    OR (N.DATA_SOURCE = 'ILS' AND N.account_status = 'A')
    OR (N.DATA_SOURCE = 'LIMIT' AND N.account_status = 'A')
    OR (N.DATA_SOURCE = 'KTP'
        AND N.ACCOUNT_STATUS = 'A'
        AND UPPER(N.PRODUCT_CODE) <> 'BORROWING')
    OR (N.DATA_SOURCE = 'PBMM'
        AND N.ACCOUNT_STATUS = 'A'
        AND UPPER(N.PRODUCT_CODE) <> 'BORROWING')
    OR (N.DATA_SOURCE = 'RKN'
        AND N.ACCOUNT_STATUS = 'A'
        AND NVL(N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
  AND NOT EXISTS
    (SELECT 1
     FROM IFRS_NOMINATIVE L
     WHERE L.REPORT_DATE = N.REPORT_DATE
       AND L.DATA_SOURCE = 'ILS'
       AND L.ACCOUNT_STATUS = 'A'
       AND N.DATA_SOURCE = 'LIMIT'
       AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER);
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REKON_INDIMP_DEBITUR';

    INSERT INTO IFRS_REKON_INDIMP_DEBITUR
    SELECT W.CUSTOMER_NUMBER,
       C.CUSTOMER_NAME,
       W.SUM_OF_OS_ON_BS_LCL,
       W.SUM_OF_EAD_AMOUNT_LCL,
       W.ECL_TOTAL_FINAL_LCL,
       W.CEK,
       W.CF_ARK,
       W.CF_REGLA,
       W.CF_ARK - W.CF_REGLA CEK_SELISIH_CF,
       W.TARGET_DATE
FROM (select NVL(WL.CUSTOMER_NUMBER, N.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
             sum(OUTSTANDING_ON_BS_LCL)                                                              SUM_OF_OS_ON_BS_LCL,
             sum(N.EAD_AMOUNT_LCL)                                                                   SUM_OF_EAD_AMOUNT_LCL,
             sum(N.RESERVED_AMOUNT_5)                                                                ECL_TOTAL_FINAL_LCL,
             sum(N.RESERVED_AMOUNT_5) - sum(N.EAD_AMOUNT_LCL)                                        CEK,
             NVL(WL.EXPECTED_CF_PERCENT, '')                                                                  CF_ARK,
             round((1- (sum(N.RESERVED_AMOUNT_5) / sum(N.OUTSTANDING_ON_BS_LCL))) * 100,2) CF_REGLA,
             NVL(WL.EXPECTED_PERIOD, '')                                                                      TARGET_DATE
      from (SELECT * FROM TBLU_DCF_BULK WHERE EFFECTIVE_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE)) WL
               FULL OUTER JOIN TMP_NOMI_INDIMP N
                               ON WL.CUSTOMER_NUMBER = N.CUSTOMER_NUMBER
                                   AND WL.EFFECTIVE_DATE = N.REPORT_DATE
      GROUP BY WL.CUSTOMER_NUMBER, N.CUSTOMER_NUMBER, WL.EXPECTED_CF_PERCENT, WL.EXPECTED_PERIOD) W
         LEFT JOIN (SELECT CUSTOMER_NUMBER, CUSTOMER_NAME
                    FROM (SELECT ROW_NUMBER()
                                         OVER (PARTITION BY CUSTOMER_NUMBER ORDER BY CUSTOMER_NUMBER)
                                     NUM,
                                 CUSTOMER_NUMBER,
                                 CUSTOMER_NAME
                          FROM (SELECT DISTINCT CUSTOMER_NUMBER, CUSTOMER_NAME
                                FROM TMP_NOMI_INDIMP W
                                WHERE LENGTH(NVL(CUSTOMER_NAME, ' ')) =
                                      (SELECT MAX(LENGTH(NVL(CUSTOMER_NAME, ' ')))
                                       FROM TMP_NOMI_INDIMP Z
                                       WHERE W.CUSTOMER_NUMBER = Z.CUSTOMER_NUMBER)))
                    WHERE NUM = 1) C
                   ON W.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER;
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REKON_INDIMP_DETAIL';

    INSERT INTO IFRS_REKON_INDIMP_DETAIL
    SELECT N.DATA_SOURCE,
       N.GOL_DEB,
       N.ACCOUNT_NUMBER,
       N.CUSTOMER_NUMBER,
       N.CUSTOMER_NAME,
       N.PRODUCT_CODE,
       N.SUB_SEGMENT,
       N.BI_COLLECTABILITY,
       CASE
           WHEN I.CF_ARK IS NULL THEN I.CF_REGLA
           WHEN I.CF_REGLA IS NULL THEN I.CF_ARK END             CASH_FLOW,
       I.TARGET_DATE,
       N.ASSESSMENT_IMP,
       N.OUTSTANDING_ON_BS_LCL,
       N.EAD_AMOUNT_LCL,
       N.RESERVED_AMOUNT_5                                       ECL_TOTAL_FINAL_LCL,
       N.SPECIAL_REASON,
       CASE
           WHEN I.CF_ARK IS NULL THEN 'TIDAK ADA DI ARK'
           WHEN I.CF_REGLA IS NULL THEN 'TIDAK ADA DI REGLA' END KETERANGAN
FROM IFRS_REKON_INDIMP_DEBITUR I
         LEFT JOIN GTMP_IFRS_NOMINATIVE N
                   ON I.CUSTOMER_NUMBER = N.CUSTOMER_NUMBER
WHERE (I.CF_ARK IS NULL OR I.CF_REGLA IS NULL)
  AND ((N.DATA_SOURCE = 'BTRD'
    AND N.ACCOUNT_STATUS = 'A'
    AND NVL(N.BI_CODE, ' ') <> '0')
    OR (N.DATA_SOURCE = 'CRD'
        AND (N.ACCOUNT_STATUS = 'A' OR N.outstanding_on_bs_ccy > 0))
    OR (N.DATA_SOURCE = 'ILS' AND N.account_status = 'A')
    OR (N.DATA_SOURCE = 'LIMIT' AND N.account_status = 'A')
    OR (N.DATA_SOURCE = 'KTP'
        AND N.ACCOUNT_STATUS = 'A'
        AND UPPER(N.PRODUCT_CODE) <> 'BORROWING')
    OR (N.DATA_SOURCE = 'PBMM'
        AND N.ACCOUNT_STATUS = 'A'
        AND UPPER(N.PRODUCT_CODE) <> 'BORROWING')
    OR (N.DATA_SOURCE = 'RKN'
        AND N.ACCOUNT_STATUS = 'A'
        AND NVL(N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
  AND NOT EXISTS
    (SELECT 1
     FROM IFRS_NOMINATIVE L
     WHERE L.REPORT_DATE = N.REPORT_DATE
       AND L.DATA_SOURCE = 'ILS'
       AND L.ACCOUNT_STATUS = 'A'
       AND N.DATA_SOURCE = 'LIMIT'
       AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER);
    COMMIT;

END;