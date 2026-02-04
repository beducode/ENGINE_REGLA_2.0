CREATE OR REPLACE PROCEDURE SP_IFRS_NMNATIVE_ILS_EMPLLOAN(v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900')) AS
  V_CURRDATE DATE;
BEGIN

  IF v_DOWNLOADDATECUR = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
  ELSE
    V_CURRDATE := v_DOWNLOADDATECUR;
  END IF;

  DBMS_OUTPUT.ENABLE(buffer_size => NULL);
  dbms_output.put_line('REPORT_DATE' || '|' || 'SOURCE_TYPE' || '|' ||
                       'DATA_SOURCE' || '|' || 'BRANCH_CODE' || '|' ||
                       'LBU_FORM' || '|' || 'NOREK_LBU' || '|' ||
                       'FACILITY_NUMBER' || '|' || 'ACCOUNT_NUMBER' || '|' ||
                       'CUSTOMER_NUMBER' || '|' || 'CUSTOMER_NAME' || '|' ||
                       'SECTOR_ECONOMIC' || '|' || 'SECTOR_COMMODITY' || '|' ||
                       'GOL_DEB' || '|' || 'PRODUCT_GROUP' || '|' ||
                       'PRODUCT_TYPE' || '|' || 'PRODUCT_CODE' || '|' ||
                       'PRODUCT_DESC' || '|' || 'PRODUCT_CODE_GL' || '|' ||
                       'START_DATE' || '|' || 'LOAN_DUE_DATE' || '|' ||
                       'NEXT_PAYMENT_DATE' || '|' || 'DAY_PAST_DUE' || '|' ||
                       'RATING_CODE' || '|' || 'EXTERNAL_RATING' || '|' ||
                       'SWIFT_CODE' || '|' || 'IMP_RATING' || '|' ||
                       'GROUP_SEGMENT' || '|' || 'SEGMENT' || '|' ||
                       'SUB_SEGMENT' || '|' || 'BUCKET_NAME' || '|' ||
                       'BI_COLLECTABILITY' || '|' || 'STAGE' || '|' ||
                       'LIFETIME_PERIOD' || '|' || 'ASSESSMENT_IMP' || '|' ||
                       'NPL_FLAG' || '|' || 'POCI_FLAG' || '|' ||
                       'BTB_FLAG' || '|' || 'REVOLVING_FLAG' || '|' ||
                       'COMMITMENT_FLAG' || '|' || 'IFRS9_CLASS' || '|' ||
                       'CURRENCY' || '|' || 'EXCHANGE_RATE' || '|' ||
                       'MARKET_RATE' || '|' || 'INTEREST_CALCULATION_CODE' || '|' ||
                       'CONTRACTUAL_INTEREST_RATE' || '|' || 'EIR' || '|' ||
                       'OUTSTANDING_ON_BS_CCY' || '|' ||
                       'OUTSTANDING_ON_BS_LCL' || '|' ||
                       'OUTSTANDING_OFF_BS_CCY' || '|' ||
                       'OUTSTANDING_OFF_BS_LCL' || '|' ||
                       'CARRYING_AMOUNT_CCY' || '|' ||
                       'CARRYING_AMOUNT_LCL' || '|' || 'SALDO_YADIT_CCY' || '|' ||
                       'SALDO_YADIT_LCL' || '|' || 'INITIAL_FEE_CCY' || '|' ||
                       'INITIAL_FEE_LCL' || '|' || 'INITIAL_COST_CCY' || '|' ||
                       'INITIAL_COST_LCL' || '|' || 'AMORT_FEE_CCY' || '|' ||
                       'AMORT_FEE_LCL' || '|' || 'AMORT_COST_CCY' || '|' ||
                       'AMORT_COST_LCL' || '|' || 'UNAMORT_FEE_AMT_CCY' || '|' ||
                       'UNAMORT_FEE_AMT_LCL' || '|' ||
                       'UNAMORT_COST_AMT_CCY' || '|' ||
                       'UNAMORT_COST_AMT_LCL' || '|' ||
                       'PV_EXPECTED_CF_IA_CCY' || '|' ||
                       'PV_EXPECTED_CF_IA_LCL' || '|' ||
                       'IA_UNWINDING_INTEREST_CCY' || '|' ||
                       'IA_UNWINDING_INTEREST_LCL' || '|' || 'CCF_RATE' || '|' ||
                       'CCF_AMOUNT_CCY' || '|' || 'CCF_AMOUNT_LCL' || '|' ||
                       'PREPAYMENT_RATE' || '|' || 'PREPAYMENT_AMOUNT_CCY' || '|' ||
                       'PREPAYMENT_AMOUNT_LCL' || '|' || 'EAD_AMOUNT_CCY' || '|' ||
                       'EAD_AMOUNT_LCL' || '|' || 'ECL_ON_BS_CCY' || '|' ||
                       'ECL_ON_BS_LCL' || '|' || 'ECL_OFF_BS_CCY' || '|' ||
                       'ECL_OFF_BS_LCL' || '|' || 'ECL_TOTAL_CCY' || '|' ||
                       'ECL_TOTAL_LCL' || '|' ||
                       'ECL_ON_BS_FINAL_CCY' || '|' || 'ECL_ON_BS_FINAL_LCL'|| '|' ||
                       'ECL_TOTAL_FINAL_CCY' || '|' || 'ECL_TOTAL_FINAL_LCL'|| '|' ||
                       'SPECIAL_REASON' || '|' ||
                       'AMORT_FEE_AMT_ILS_CCY' || '|' ||
                       'AMORT_FEE_AMT_ILS_LCL' || '|' ||
                       'UNAMORT_FEE_AMT_ILS_CCY' || '|' ||
                       'UNAMORT_FEE_AMT_ILS_LCL');

  for rec in (SELECT REPORT_DATE,
                     SOURCE_TYPE,
                     DATA_SOURCE,
                     BRANCH_CODE,
                     LBU_FORM,
                     NOREK_LBU,
                     FACILITY_NUMBER,
                     ACCOUNT_NUMBER,
                     CUSTOMER_NUMBER,
                     CUSTOMER_NAME,
                     SECTOR_ECONOMIC,
                     SECTOR_COMMODITY,
                     GOL_DEB,
                     PRODUCT_GROUP,
                     PRODUCT_TYPE,
                     PRODUCT_CODE,
                     PRODUCT_DESC,
                     PRODUCT_CODE_GL,
                     START_DATE,
                     MATURITY_DATE AS LOAN_DUE_DATE,
                     NEXT_PAYMENT_DATE,
                     DAY_PAST_DUE,
                     RATING_CODE,
                     EXTERNAL_RATING,
                     SWIFT_CODE,
                     IMP_RATING,
                     GROUP_SEGMENT,
                     SEGMENT,
                     SUB_SEGMENT,
                     BUCKET_NAME,
                     BI_COLLECTABILITY,
                     STAGE,
                     LIFETIME_PERIOD,
                     ASSESSMENT_IMP,
                     NPL_FLAG,
                     POCI_FLAG,
                     BTB_FLAG,
                     REVOLVING_FLAG,
                     COMMITMENT_FLAG,
                     IFRS9_CLASS,
                     CURRENCY,
                     EXCHANGE_RATE,
                     MARKET_RATE,
                     INTEREST_CALCULATION_CODE,
                     CONTRACTUAL_INTEREST_RATE,
                     EIR,
                     OUTSTANDING_ON_BS_CCY,
                     OUTSTANDING_ON_BS_LCL,
                     OUTSTANDING_OFF_BS_CCY,
                     OUTSTANDING_OFF_BS_LCL,
                     CARRYING_AMOUNT_CCY,
                     CARRYING_AMOUNT_LCL,
                     SALDO_YADIT_CCY,
                     SALDO_YADIT_LCL,
                     INITIAL_FEE_CCY,
                     INITIAL_FEE_LCL,
                     INITIAL_COST_CCY,
                     INITIAL_COST_LCL,
                     AMORT_FEE_CCY,
                     AMORT_FEE_LCL,
                     AMORT_COST_CCY,
                     AMORT_COST_LCL,
                     UNAMORT_FEE_AMT_CCY,
                     UNAMORT_FEE_AMT_LCL,
                     UNAMORT_COST_AMT_CCY,
                     UNAMORT_COST_AMT_LCL,
                     PV_EXPECTED_CF_IA_CCY,
                     PV_EXPECTED_CF_IA_LCL,
                     IA_UNWINDING_INTEREST_CCY,
                     IA_UNWINDING_INTEREST_LCL,
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
                     SPECIAL_REASON,
                     AMORT_FEE_AMT_ILS_CCY,
                     AMORT_FEE_AMT_ILS_LCL,
                     UNAMORT_FEE_AMT_ILS_CCY,
                     UNAMORT_FEE_AMT_ILS_LCL
                FROM IFRS_NOMINATIVE
               WHERE DATA_SOURCE = 'ILS'
                 and REPORT_DATE = V_CURRDATE
                 and account_status = 'A'
                 and SEGMENT IN 'EMPLOYEE LOAN')
  loop

    dbms_output.put_line(REC.REPORT_DATE || '|' || REC.SOURCE_TYPE || '|' ||
                         REC.DATA_SOURCE || '|' || REC.BRANCH_CODE || '|' ||
                         REC.LBU_FORM || '|' || REC.NOREK_LBU || '|' ||
                         REC.FACILITY_NUMBER || '|' || REC.ACCOUNT_NUMBER || '|' ||
                         REC.CUSTOMER_NUMBER || '|' || REC.CUSTOMER_NAME || '|' ||
                         REC.SECTOR_ECONOMIC || '|' ||
                         REC.SECTOR_COMMODITY || '|' || REC.GOL_DEB || '|' ||
                         REC.PRODUCT_GROUP || '|' || REC.PRODUCT_TYPE || '|' ||
                         REC.PRODUCT_CODE || '|' || REC.PRODUCT_DESC || '|' ||
                         REC.PRODUCT_CODE_GL || '|' || REC.START_DATE || '|' ||
                         REC.LOAN_DUE_DATE || '|' || REC.NEXT_PAYMENT_DATE || '|' ||
                         REC.DAY_PAST_DUE || '|' || REC.RATING_CODE || '|' ||
                         REC.EXTERNAL_RATING || '|' || REC.SWIFT_CODE || '|' ||
                         REC.IMP_RATING || '|' || REC.GROUP_SEGMENT || '|' ||
                         REC.SEGMENT || '|' || REC.SUB_SEGMENT || '|' ||
                         REC.BUCKET_NAME || '|' || REC.BI_COLLECTABILITY || '|' ||
                         REC.STAGE || '|' || REC.LIFETIME_PERIOD || '|' ||
                         REC.ASSESSMENT_IMP || '|' || REC.NPL_FLAG || '|' ||
                         REC.POCI_FLAG || '|' || REC.BTB_FLAG || '|' ||
                         REC.REVOLVING_FLAG || '|' || REC.COMMITMENT_FLAG || '|' ||
                         REC.IFRS9_CLASS || '|' || REC.CURRENCY || '|' ||
                         REC.EXCHANGE_RATE || '|' || REC.MARKET_RATE || '|' ||
                         REC.INTEREST_CALCULATION_CODE || '|' ||
                         REC.CONTRACTUAL_INTEREST_RATE || '|' || REC.EIR || '|' ||
                         REC.OUTSTANDING_ON_BS_CCY || '|' ||
                         REC.OUTSTANDING_ON_BS_LCL || '|' ||
                         REC.OUTSTANDING_OFF_BS_CCY || '|' ||
                         REC.OUTSTANDING_OFF_BS_LCL || '|' ||
                         REC.CARRYING_AMOUNT_CCY || '|' ||
                         REC.CARRYING_AMOUNT_LCL || '|' ||
                         REC.SALDO_YADIT_CCY || '|' || REC.SALDO_YADIT_LCL || '|' ||
                         REC.INITIAL_FEE_CCY || '|' || REC.INITIAL_FEE_LCL || '|' ||
                         REC.INITIAL_COST_CCY || '|' ||
                         REC.INITIAL_COST_LCL || '|' || REC.AMORT_FEE_CCY || '|' ||
                         REC.AMORT_FEE_LCL || '|' || REC.AMORT_COST_CCY || '|' ||
                         REC.AMORT_COST_LCL || '|' ||
                         REC.UNAMORT_FEE_AMT_CCY || '|' ||
                         REC.UNAMORT_FEE_AMT_LCL || '|' ||
                         REC.UNAMORT_COST_AMT_CCY || '|' ||
                         REC.UNAMORT_COST_AMT_LCL || '|' ||
                         REC.PV_EXPECTED_CF_IA_CCY || '|' ||
                         REC.PV_EXPECTED_CF_IA_LCL || '|' ||
                         REC.IA_UNWINDING_INTEREST_CCY || '|' ||
                         REC.IA_UNWINDING_INTEREST_LCL || '|' ||
                         REC.CCF_RATE || '|' || REC.CCF_AMOUNT_CCY || '|' ||
                         REC.CCF_AMOUNT_LCL || '|' || REC.PREPAYMENT_RATE || '|' ||
                         REC.PREPAYMENT_AMOUNT_CCY || '|' ||
                         REC.PREPAYMENT_AMOUNT_LCL || '|' ||
                         REC.EAD_AMOUNT_CCY || '|' || REC.EAD_AMOUNT_LCL || '|' ||
                         REC.ECL_ON_BS_CCY || '|' || REC.ECL_ON_BS_LCL || '|' ||
                         REC.ECL_OFF_BS_CCY || '|' || REC.ECL_OFF_BS_LCL || '|' ||
                         REC.ECL_TOTAL_CCY || '|' || REC.ECL_TOTAL_LCL || '|' ||
                         REC.ECL_ON_BS_FINAL_CCY || '|' || REC.ECL_ON_BS_FINAL_LCL || '|' ||
                         REC.ECL_TOTAL_FINAL_CCY || '|' || REC.ECL_TOTAL_FINAL_LCL || '|' ||
                         REC.SPECIAL_REASON || '|' ||
                         REC.AMORT_FEE_AMT_ILS_CCY || '|' ||
                         REC.AMORT_FEE_AMT_ILS_LCL || '|' ||
                         REC.UNAMORT_FEE_AMT_ILS_CCY || '|' ||
                         REC.UNAMORT_FEE_AMT_ILS_LCL);
  end loop;
END;