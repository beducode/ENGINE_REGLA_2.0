CREATE OR REPLACE PROCEDURE SP_IFRS_NOMI_TO_FINMART
AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_NOMINATIVE_TO_FINMART';

    INSERT INTO IFRS_NOMINATIVE_TO_FINMART
    SELECT N.REPORT_DATE,
           N.SOURCE_TYPE,
           N.DATA_SOURCE,
           N.BRANCH_CODE,
           N.LBU_FORM,
           N.NOREK_LBU,
           N.FACILITY_NUMBER,
           N.ACCOUNT_NUMBER,
           N.CUSTOMER_NUMBER,
           N.CUSTOMER_NAME,
           N.SECTOR_ECONOMIC,
           N.SECTOR_COMMODITY,
           N.GOL_DEB,
           N.PRODUCT_GROUP,
           N.PRODUCT_TYPE,
           N.PRODUCT_CODE,
           N.PRODUCT_DESC,
           N.PRODUCT_CODE_GL,
           N.INSTRUMENT,
           N.INV_TYPE,
           N.SEC_NAME,
           N.PORTFOLIO,
           N.CONTRACT_ID,
           N.START_DATE,
           N.MATURITY_DATE,
           N.SETTLE_DATE,
           N.DELINQUENCY,
           N.NEXT_PAYMENT_DATE,
           N.DAY_PAST_DUE,
           N.RATING_CODE,
           N.EXTERNAL_RATING,
           N.SWIFT_CODE,
           N.IMP_RATING,
           N.GROUP_SEGMENT,
           N.SEGMENT,
           N.SUB_SEGMENT,
           N.BUCKET_NAME,
           N.BI_COLLECTABILITY,
           N.STAGE,
           N.LIFETIME_PERIOD,
           N.ASSESSMENT_IMP,
           N.NPL_FLAG,
           N.POCI_FLAG,
           N.BTB_FLAG,
           N.REVOLVING_FLAG,
           N.COMMITMENT_FLAG,
           N.IFRS9_CLASS,
           N.CURRENCY,
           N.EXCHANGE_RATE,
           N.MARKET_RATE,
           N.INTEREST_CALCULATION_CODE,
           N.CONTRACTUAL_INTEREST_RATE,
           N.EIR,
           N.COA_BAL,
           N.PRINCIPAL_AMOUNT_CCY,
           N.PRINCIPAL_AMOUNT_LCL,
           N.INTEREST_RECEIVABLE_CCY,
           N.INTEREST_RECEIVABLE_LCL,
           N.PREMI_DISCOUNT_AMOUNT_CCY,
           N.PREMI_DISCOUNT_AMOUNT_LCL,
           CASE
               WHEN N.DATA_SOURCE = 'KTP'
                   THEN
                   N.PRINCIPAL_AMOUNT_CCY
               WHEN N.DATA_SOURCE = 'RKN'
                   THEN
                   N.OUTSTANDING_PRINCIPAL_CCY
               ELSE
                   N.OUTSTANDING_ON_BS_CCY
               END
                  OUTSTANDING_ON_BS_CCY,
           CASE
               WHEN N.DATA_SOURCE = 'KTP'
                   THEN
                   N.PRINCIPAL_AMOUNT_LCL
               WHEN N.DATA_SOURCE = 'RKN'
                   THEN
                   N.OUTSTANDING_PRINCIPAL_LCL
               ELSE
                   N.OUTSTANDING_ON_BS_LCL
               END
                  OUTSTANDING_ON_BS_LCL,
           CASE
               WHEN N.DATA_SOURCE IN ('KTP', 'RKN') THEN 0
               ELSE N.OUTSTANDING_OFF_BS_CCY
               END
                  OUTSTANDING_OFF_BS_CCY,
           CASE
               WHEN N.DATA_SOURCE IN ('KTP', 'RKN') THEN 0
               ELSE N.OUTSTANDING_OFF_BS_LCL
               END
                  OUTSTANDING_OFF_BS_LCL,
           N.CARRYING_AMOUNT_CCY,
           N.CARRYING_AMOUNT_LCL,
           NVL(N.MARKET_RATE, 0)
                  MARKET_VALUE_CCY,
           NVL(N.MARKET_RATE, 0) * NVL(N.EXCHANGE_RATE, 1)
                  MARKET_VALUE_LCL,
           N.SALDO_YADIT_CCY,
           N.SALDO_YADIT_LCL,
           N.INITIAL_FEE_CCY,
           N.INITIAL_FEE_LCL,
           N.INITIAL_COST_CCY,
           N.INITIAL_COST_LCL,
           N.AMORT_FEE_CCY,
           N.AMORT_FEE_LCL,
           N.AMORT_COST_CCY,
           N.AMORT_COST_LCL,
           N.UNAMORT_FEE_AMT_CCY,
           N.UNAMORT_FEE_AMT_LCL,
           N.UNAMORT_COST_AMT_CCY,
           N.UNAMORT_COST_AMT_LCL,
           N.PV_EXPECTED_CF_IA_CCY,
           N.PV_EXPECTED_CF_IA_LCL,
           N.IA_UNWINDING_INTEREST_CCY,
           N.IA_UNWINDING_INTEREST_LCL,
           N.LIMIT_AMT_CCY,
           N.CCF_RATE,
           N.CCF_AMOUNT_CCY,
           N.CCF_AMOUNT_LCL,
           N.PREPAYMENT_RATE,
           N.PREPAYMENT_AMOUNT_CCY,
           N.PREPAYMENT_AMOUNT_LCL,
           N.EAD_AMOUNT_CCY,
           N.EAD_AMOUNT_LCL,
           N.ECL_ON_BS_CCY,
           N.ECL_ON_BS_LCL,
           N.ECL_OFF_BS_CCY,
           N.ECL_OFF_BS_LCL,
           N.ECL_TOTAL_CCY,
           N.ECL_TOTAL_LCL,
           N.RESERVED_AMOUNT_2
               AS ECL_ON_BS_FINAL_CCY,
           N.RESERVED_AMOUNT_3
               AS ECL_ON_BS_FINAL_LCL,
           N.RESERVED_AMOUNT_4
               AS ECL_TOTAL_FINAL_CCY,
           N.RESERVED_AMOUNT_5
               AS ECL_TOTAL_FINAL_LCL,
           N.SPECIAL_REASON,
           N.AMORT_FEE_AMT_ILS_CCY,
           N.AMORT_FEE_AMT_ILS_LCL,
           N.UNAMORT_FEE_AMT_ILS_CCY,
           N.UNAMORT_FEE_AMT_ILS_LCL,
           A.CR_STAGE
                  STAGE_ORI,
           NVL(N.RESERVED_FLAG_1, 0)
               AS COVID_FLAG,
           NVL(N.RESERVED_FLAG_2, 0)
               AS RESTRUCTURE_FLAG,
           NVL(A.RESERVED_FLAG_1, -1)
               AS FLAG_IMP,
           N.RESERVED_AMOUNT_6
               AS NILAI_TERCATAT_ON,
           N.RESERVED_AMOUNT_7
               AS NILAI_TERCATAT_OFF,
           N.RESERVED_VARCHAR_2
               AS TRA,
           N.RESERVED_VARCHAR_5
               AS ASET_KEUANGAN,
           NVL(N.BI_CODE, ' ')
               AS BI_CODE,
           N.RESERVED_AMOUNT_8 AMORT99_FS,
           N.RESERVED_DATE_1 AS RESTRUCTURE_DATE,
            N.RESERVED_FLAG_3 AS FLAG_H,
            N.RESERVED_FLAG_6 AS LPEI_FLAG,
            N.RESERVED_VARCHAR_6 AS COMPANY_NO,
            N.RESERVED_VARCHAR_7 AS USER_CODE_1,
            N.RESERVED_VARCHAR_8 AS USER_CODE_3,
            N.RESERVED_VARCHAR_9 AS REGION_CODE,
            N.RESERVED_VARCHAR_10 AS BRANCH_NAME,
            N.RESERVED_VARCHAR_11 AS MAIN_BRANCH,
            N.RESERVED_VARCHAR_12 AS MAIN_BRANCH_DESCRIPTION
    FROM IFRS_NOMINATIVE N,
         IFRS_MASTER_ACCOUNT A
    WHERE 1 = 1
      AND N.REPORT_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE)
      --           AND N.REPORT_DATE = '30-Sep-2021'
      AND A.DOWNLOAD_DATE = N.REPORT_DATE
      AND N.MASTERID = A.MASTERID
      AND ((N.DATA_SOURCE = 'BTRD'
        AND N.ACCOUNT_STATUS = 'A'
        AND NVL(N.BI_CODE, ' ') <> '0')
        OR (N.DATA_SOURCE = 'CRD'
            AND (N.ACCOUNT_STATUS = 'A'
                OR N.outstanding_on_bs_ccy > 0))
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

/
/