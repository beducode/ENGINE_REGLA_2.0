CREATE OR REPLACE PROCEDURE SP_IFRS_NMNATIVE_CRD(V_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900'),
                                                 V_PROSESFILEKE    NUMBER) AS
  V_CURRDATE   DATE;
  V_BANYAKFILE NUMBER;
  V_FILEKE     NUMBER;
BEGIN

  IF v_DOWNLOADDATECUR = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
  ELSE
    V_CURRDATE := V_DOWNLOADDATECUR;
  END IF;

  V_FILEKE     := V_PROSESFILEKE - 1;
  V_BANYAKFILE := 10;

  DBMS_OUTPUT.ENABLE(buffer_size => NULL);
  dbms_output.put_line('REPORT_DATE' || '|' || 'SOURCE_TYPE' || '|' ||
                       'DATA_SOURCE' || '|' || 'BRANCH_CODE' || '|' ||
                       'LBU_FORM' || '|' || 'NOREK_LBU' || '|' ||
                       'ACCOUNT_NUMBER' || '|' || 'CUSTOMER_NUMBER' || '|' ||
                       'CUSTOMER_NAME' || '|' || 'GOL_DEB' || '|' ||
                       'PRODUCT_GROUP' || '|' || 'PRODUCT_TYPE' || '|' ||
                       'PRODUCT_CODE' || '|' || 'PRODUCT_DESC' || '|' ||
                       'START_DATE' || '|' || 'MATURITY_DATE' || '|' ||
                       'DELINQUENCY' || '|' || 'DAY_PAST_DUE' || '|' ||
                       'GROUP_SEGMENT' || '|' || 'SEGMENT' || '|' ||
                       'SUB_SEGMENT' || '|' || 'BUCKET_NAME' || '|' ||
                       'BI_COLLECTABILITY' || '|' || 'STAGE' || '|' ||
                       'ASSESSMENT_IMP' || '|' || 'NPL_FLAG' || '|' ||
                       'IFRS9_CLASS' || '|' || 'CURRENCY' || '|' ||
                       'EXCHANGE_RATE' || '|' ||
                       'CONTRACTUAL_INTEREST_RATE' || '|' ||
                       'OUTSTANDING_ON_BS_CCY' || '|' ||
                       'OUTSTANDING_ON_BS_LCL' || '|' ||
                       'OUTSTANDING_OFF_BS_CCY' || '|' ||
                       'OUTSTANDING_OFF_BS_LCL' || '|' ||
                       'CARRYING_AMOUNT_CCY' || '|' ||
                       'CARRYING_AMOUNT_LCL' || '|' ||
                       'INTEREST_ACCRUED_CCY' || '|' ||
                       'INTEREST_ACCRUED_LCL' || '|' || 'LIMIT_AMT_CCY' || '|' ||
                       'CCF_RATE' || '|' || 'CCF_AMOUNT_CCY' || '|' ||
                       'CCF_AMOUNT_LCL' || '|' || 'PREPAYMENT_RATE' || '|' ||
                       'PREPAYMENT_AMOUNT_CCY' || '|' ||
                       'PREPAYMENT_AMOUNT_LCL' || '|' || 'EAD_AMOUNT_CCY' || '|' ||
                       'EAD_AMOUNT_LCL' || '|' || 'ECL_ON_BS_CCY' || '|' ||
                       'ECL_ON_BS_LCL' || '|' || 'ECL_OFF_BS_CCY' || '|' ||
                       'ECL_OFF_BS_LCL' || '|' || 'ECL_TOTAL_CCY' || '|' ||
                       'ECL_TOTAL_LCL' || '|' ||
                       'ECL_ON_BS_FINAL_CCY' || '|' || 'ECL_ON_BS_FINAL_LCL'|| '|' ||
                       'ECL_TOTAL_FINAL_CCY' || '|' || 'ECL_TOTAL_FINAL_LCL'|| '|' ||
                       'SPECIAL_REASON');

  for rec in (SELECT REPORT_DATE,
                     SOURCE_TYPE,
                     DATA_SOURCE,
                     BRANCH_CODE,
                     LBU_FORM,
                     NOREK_LBU,
                     ACCOUNT_NUMBER,
                     CUSTOMER_NUMBER,
                     CUSTOMER_NAME,
                     GOL_DEB,
                     PRODUCT_GROUP,
                     PRODUCT_TYPE,
                     PRODUCT_CODE,
                     PRODUCT_DESC,
                     START_DATE,
                     MATURITY_DATE,
                     DELINQUENCY,
                     DAY_PAST_DUE,
                     GROUP_SEGMENT,
                     SEGMENT,
                     SUB_SEGMENT,
                     BUCKET_NAME,
                     BI_COLLECTABILITY,
                     STAGE,
                     ASSESSMENT_IMP,
                     NPL_FLAG,
                     IFRS9_CLASS,
                     CURRENCY,
                     EXCHANGE_RATE,
                     CONTRACTUAL_INTEREST_RATE,
                     OUTSTANDING_ON_BS_CCY,
                     OUTSTANDING_ON_BS_LCL,
                     OUTSTANDING_OFF_BS_CCY,
                     OUTSTANDING_OFF_BS_LCL,
                     CARRYING_AMOUNT_CCY,
                     CARRYING_AMOUNT_LCL,
                     A.SALDO_YADIT_CCY         INTEREST_ACCRUED_CCY,
                     A.SALDO_YADIT_LCL         INTEREST_ACCRUED_LCL,
                     LIMIT_AMT_CCY,
                     CCF_RATE,
                     CCF_AMOUNT_CCY,
                     CCF_AMOUNT_LCL,
                     PREPAYMENT_RATE,
                     PREPAYMENT_AMOUNT_CCY,
                     PREPAYMENT_AMOUNT_LCL,
                     EAD_AMOUNT_CCY,
                     EAD_AMOUNT_LCL,
                     ECL_ON_BS_CCY,
                     ECL_ON_BS_LCL,
                     ECL_OFF_BS_CCY,
                     ECL_OFF_BS_LCL,
                     ECL_TOTAL_CCY,
                     ECL_TOTAL_LCL,
                     RESERVED_AMOUNT_2 AS ECL_ON_BS_FINAL_CCY,
                     RESERVED_AMOUNT_3 AS ECL_ON_BS_FINAL_LCL,
                     RESERVED_AMOUNT_4 AS ECL_TOTAL_FINAL_CCY,
                     RESERVED_AMOUNT_5 AS ECL_TOTAL_FINAL_LCL,
                     SPECIAL_REASON
                FROM IFRS_NOMINATIVE a
               WHERE REPORT_DATE = V_CURRDATE
                 AND DATA_SOURCE = 'CRD'
                 AND (ACCOUNT_STATUS = 'A' OR a.outstanding_on_bs_ccy > 0)
                 AND EXISTS
               (SELECT 1
                        FROM (SELECT MINPK + (DIFF * V_FILEKE) + CASE
                                       WHEN (V_FILEKE + 1) > V_BANYAKFILE THEN
                                        MAXPK
                                       ELSE
                                        0
                                     END MINPK,
                                     MINPK + (DIFF * (V_FILEKE + 1)) - CASE
                                       WHEN (V_FILEKE + 1) >= V_BANYAKFILE THEN
                                        0
                                       ELSE
                                        1
                                     END MAXPK
                                FROM (SELECT min(pkid) MINPK,
                                             max(pkid) MAXPK,
                                             floor((max(pkid) - min(pkid)) /
                                                   V_BANYAKFILE) DIFF
                                        FROM IFRS_NOMINATIVE a
                                       WHERE DATA_SOURCE = 'CRD'
                                         AND REPORT_DATE = V_CURRDATE
                                         AND (ACCOUNT_STATUS = 'A' OR
                                             (ACCOUNT_STATUS <> 'A' and
                                             a.outstanding_on_bs_ccy > 0))) A) PK
                       WHERE PKID BETWEEN MINPK AND MAXPK))
  loop

    dbms_output.put_line(REC.REPORT_DATE || '|' || REC.SOURCE_TYPE || '|' ||
                         REC.DATA_SOURCE || '|' || REC.BRANCH_CODE || '|' ||
                         REC.LBU_FORM || '|' || REC.NOREK_LBU || '|' ||
                         REC.ACCOUNT_NUMBER || '|' || REC.CUSTOMER_NUMBER || '|' ||
                         REC.CUSTOMER_NAME || '|' || REC.GOL_DEB || '|' ||
                         REC.PRODUCT_GROUP || '|' || REC.PRODUCT_TYPE || '|' ||
                         REC.PRODUCT_CODE || '|' || REC.PRODUCT_DESC || '|' ||
                         REC.START_DATE || '|' || REC.MATURITY_DATE || '|' ||
                         REC.DELINQUENCY || '|' || REC.DAY_PAST_DUE || '|' ||
                         REC.GROUP_SEGMENT || '|' || REC.SEGMENT || '|' ||
                         REC.SUB_SEGMENT || '|' || REC.BUCKET_NAME || '|' ||
                         REC.BI_COLLECTABILITY || '|' || REC.STAGE || '|' ||
                         REC.ASSESSMENT_IMP || '|' || REC.NPL_FLAG || '|' ||
                         REC.IFRS9_CLASS || '|' || REC.CURRENCY || '|' ||
                         REC.EXCHANGE_RATE || '|' ||
                         REC.CONTRACTUAL_INTEREST_RATE || '|' ||
                         REC.OUTSTANDING_ON_BS_CCY || '|' ||
                         REC.OUTSTANDING_ON_BS_LCL || '|' ||
                         REC.OUTSTANDING_OFF_BS_CCY || '|' ||
                         REC.OUTSTANDING_OFF_BS_LCL || '|' ||
                         REC.CARRYING_AMOUNT_CCY || '|' ||
                         REC.CARRYING_AMOUNT_LCL || '|' ||
                         REC.INTEREST_ACCRUED_CCY || '|' ||
                         REC.INTEREST_ACCRUED_LCL || '|' ||
                         REC.LIMIT_AMT_CCY || '|' || REC.CCF_RATE || '|' ||
                         REC.CCF_AMOUNT_CCY || '|' || REC.CCF_AMOUNT_LCL || '|' ||
                         REC.PREPAYMENT_RATE || '|' ||
                         REC.PREPAYMENT_AMOUNT_CCY || '|' ||
                         REC.PREPAYMENT_AMOUNT_LCL || '|' ||
                         REC.EAD_AMOUNT_CCY || '|' || REC.EAD_AMOUNT_LCL || '|' ||
                         REC.ECL_ON_BS_CCY || '|' || REC.ECL_ON_BS_LCL || '|' ||
                         REC.ECL_OFF_BS_CCY || '|' || REC.ECL_OFF_BS_LCL || '|' ||
                         REC.ECL_TOTAL_CCY || '|' || REC.ECL_TOTAL_LCL || '|' ||
                         REC.ECL_ON_BS_FINAL_CCY || '|' || REC.ECL_ON_BS_FINAL_LCL || '|' ||
                         REC.ECL_TOTAL_FINAL_CCY || '|' || REC.ECL_TOTAL_FINAL_LCL || '|' ||
                         REC.SPECIAL_REASON);

  end loop;
END;