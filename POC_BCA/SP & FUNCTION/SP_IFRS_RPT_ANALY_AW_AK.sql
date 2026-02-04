CREATE OR REPLACE PROCEDURE SP_IFRS_RPT_ANALY_AW_AK (
   V_CURRDATE    DATE DEFAULT '1-JAN-2019',
   V_PREVDATE    DATE DEFAULT '1-JAN-2019')
AS
BEGIN

   DELETE IFRS_RPT_MOVE_ANALY_AW_AK
    WHERE REPORT_DATE = V_CURRDATE;

   COMMIT;

   INSERT INTO IFRS_RPT_MOVE_ANALY_AW_AK (REPORT_DATE,
                                          MASTERID,
                                          DATA_SOURCE,
                                          SUB_SEGMENT,
                                          SEQ_NO,
                                          IMP_CHANGE_REASON,
                                          ECL_ON_BS,
                                          ECL_OFF_BS,
                                          ECL_TOTAL,
                                          CARRY_AMOUNT,
                                          UNUSED_AMT,
                                          CURRENCY,
                                          EXCHANGE_RATE,
                                          STAGE,
                                          WRITEOFF_FLAG,
                                          SPECIAL_REASON,
                                          PRODUCT_CODE,
                                          TRA,
                                          ASET_KEUANGAN,
                                          OUTSTANDING_ON_BS,
                                          OUTSTANDING_OFF_BS,
                                          NILAI_TERCATAT_ON_BS)
        SELECT REPORT_DATE,
               MASTERID,
               DATA_SOURCE,
               SUB_SEGMENT,
               SEQ_NO,
               IMP_CHANGE_REASON,
               SUM (ECL_ON_BS),
               SUM (ECL_OFF_BS),
               SUM (ECL_TOTAL)  AS ECL_TOTAL,
               SUM (CARRY_AMOUNT) AS CARRY_AMOUNT,
               SUM (UNUSED_AMT) AS UNUSED_AMT,
               CURRENCY,
               EXCHANGE_RATE,
               STAGE,
               WRITEOFF_FLAG,
               SPECIAL_REASON,
               PRODUCT_CODE,
               TRA,
               ASET_KEUANGAN,
         SUM(OUTSTANDING_ON_BS) AS OUTSTANDING_ON_BS,
               SUM(OUTSTANDING_OFF_BS) AS OUTSTANDING_OFF_BS,
               SUM(NILAI_TERCATAT_ON_BS) AS NILAI_TERCATAT_ON_BS
          FROM (SELECT V_CURRDATE                 AS REPORT_DATE,
                       0                          AS MASTERID,
                       A.DATA_SOURCE              AS DATA_SOURCE,
                       A.SUB_SEGMENT              AS SUB_SEGMENT,
                       0                          AS SEQ_NO,
                       'BALANCE AT 1 ' || TO_CHAR (V_CURRDATE, 'MON YYYY')
                          AS IMP_CHANGE_REASON,
                       NVL (A.RESERVED_AMOUNT_3, 0) AS ECL_ON_BS,
                       NVL (A.ECL_OFF_BS_LCL, 0)  AS ECL_OFF_BS,
                       NVL (A.RESERVED_AMOUNT_5, 0) AS ECL_TOTAL,
                       NVL (A.RESERVED_AMOUNT_6, 0) AS CARRY_AMOUNT,
                       NVL (A.UNUSED_AMT_LCL, 0)  AS UNUSED_AMT,
                       ''                         AS CURRENCY,
                       1                          AS EXCHANGE_RATE,
                       CASE WHEN A.POCI_FLAG = 'Y'
                  THEN 'POCI'
                            ELSE NVL (A.STAGE, '1')
                       END AS STAGE,
                       0                          AS WRITEOFF_FLAG,
                       ''                         AS SPECIAL_REASON,
                       A.PRODUCT_CODE             AS PRODUCT_CODE,
                       A.RESERVED_VARCHAR_2       AS TRA,
                       A.RESERVED_VARCHAR_5       AS ASET_KEUANGAN,
             NVL(OUTSTANDING_ON_BS_LCL, 0) AS OUTSTANDING_ON_BS,
                       NVL(OUTSTANDING_OFF_BS_LCL, 0) AS OUTSTANDING_OFF_BS,
                       (NVL(OUTSTANDING_ON_BS_LCL, 0) - NVL(UNAMORT_FEE_AMT_LCL, 0) + NVL(IA_UNWINDING_INTEREST_LCL, 0)) NILAI_TERCATAT_ON_BS
                  FROM GTMP_NOMINATIVE_CURR_PREV A
                 WHERE A.REPORT_DATE = V_PREVDATE)
      GROUP BY REPORT_DATE,
               MASTERID,
               DATA_SOURCE,
               SUB_SEGMENT,
               SEQ_NO,
               IMP_CHANGE_REASON,
               CURRENCY,
               EXCHANGE_RATE,
               STAGE,
               WRITEOFF_FLAG,
               SPECIAL_REASON,
               PRODUCT_CODE,
               TRA,
               ASET_KEUANGAN;

   COMMIT;



   INSERT INTO IFRS_RPT_MOVE_ANALY_AW_AK (REPORT_DATE,
                                          MASTERID,
                                          DATA_SOURCE,
                                          SUB_SEGMENT,
                                          SEQ_NO,
                                          IMP_CHANGE_REASON,
                                          ECL_ON_BS,
                                          ECL_OFF_BS,
                                          ECL_TOTAL,
                                          CARRY_AMOUNT,
                                          UNUSED_AMT,
                                          CURRENCY,
                                          EXCHANGE_RATE,
                                          STAGE,
                                          WRITEOFF_FLAG,
                                          SPECIAL_REASON,
                                          PRODUCT_CODE,
                                          TRA,
                                          ASET_KEUANGAN,
                                          OUTSTANDING_ON_BS,
                                          OUTSTANDING_OFF_BS,
                                          NILAI_TERCATAT_ON_BS)
        SELECT REPORT_DATE,
               MASTERID,
               DATA_SOURCE,
               SUB_SEGMENT,
               SEQ_NO,
               IMP_CHANGE_REASON,
               SUM (ECL_ON_BS)  AS ECL_ON_BS,
               SUM (ECL_OFF_BS) AS ECL_OFF_BS,
               SUM (ECL_TOTAL)  AS ECL_TOTAL,
               SUM (CARRY_AMOUNT) AS CARRY_AMOUNT,
               SUM (UNUSED_AMT) AS UNUSED_AMT,
               CURRENCY,
               EXCHANGE_RATE,
               STAGE,
               WRITEOFF_FLAG,
               SPECIAL_REASON,
               PRODUCT_CODE,
               TRA,
               ASET_KEUANGAN,
         SUM(OUTSTANDING_ON_BS) AS OUTSTANDING_ON_BS,
               SUM(OUTSTANDING_OFF_BS) AS OUTSTANDING_OFF_BS,
               SUM(NILAI_TERCATAT_ON_BS) AS NILAI_TERCATAT_ON_BS
          FROM (SELECT V_CURRDATE                 AS REPORT_DATE,
                       0                          AS MASTERID,
                       A.DATA_SOURCE              AS DATA_SOURCE,
                       A.SUB_SEGMENT              AS SUB_SEGMENT,
                       99                         AS SEQ_NO,
                          'ENDING AT '
                       || TO_CHAR (LAST_DAY (V_CURRDATE), 'DD MON YYYY')
                          AS IMP_CHANGE_REASON,
                       NVL (A.RESERVED_AMOUNT_3, 0) AS ECL_ON_BS,
                       NVL (A.ECL_OFF_BS_LCL, 0)  AS ECL_OFF_BS,
                       NVL (A.RESERVED_AMOUNT_5, 0) AS ECL_TOTAL,
                       NVL (A.RESERVED_AMOUNT_6, 0) AS CARRY_AMOUNT,
                       NVL (A.UNUSED_AMT_LCL, 0)  AS UNUSED_AMT,
                       ''                         AS CURRENCY,
                       1                          AS EXCHANGE_RATE,
                       CASE WHEN A.POCI_FLAG = 'Y'
                  THEN 'POCI'
                            ELSE NVL (A.STAGE, '1')
                       END AS STAGE,
                       0                          AS WRITEOFF_FLAG,
                       ''                         AS SPECIAL_REASON,
                       A.PRODUCT_CODE             AS PRODUCT_CODE,
                       A.RESERVED_VARCHAR_2       AS TRA,
                       A.RESERVED_VARCHAR_5       AS ASET_KEUANGAN,
             NVL(OUTSTANDING_ON_BS_LCL, 0) AS OUTSTANDING_ON_BS,
                       NVL(OUTSTANDING_OFF_BS_LCL, 0) AS OUTSTANDING_OFF_BS,
                       (NVL(OUTSTANDING_ON_BS_LCL, 0) - NVL(UNAMORT_FEE_AMT_LCL, 0) + NVL(IA_UNWINDING_INTEREST_LCL, 0)) NILAI_TERCATAT_ON_BS
                  FROM GTMP_NOMINATIVE_CURR_PREV A
                 WHERE A.REPORT_DATE = V_CURRDATE)
      GROUP BY REPORT_DATE,
               MASTERID,
               DATA_SOURCE,
               SUB_SEGMENT,
               SEQ_NO,
               IMP_CHANGE_REASON,
               CURRENCY,
               EXCHANGE_RATE,
               STAGE,
               WRITEOFF_FLAG,
               SPECIAL_REASON,
               PRODUCT_CODE,
               TRA,
               ASET_KEUANGAN;

   COMMIT;
END;