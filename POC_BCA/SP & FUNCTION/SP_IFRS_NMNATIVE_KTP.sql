CREATE OR REPLACE PROCEDURE SP_IFRS_NMNATIVE_KTP(v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900')) AS
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
                       'ACCOUNT_NUMBER' || '|' || 'CUSTOMER_NUMBER' || '|' ||
                       'CUSTOMER_NAME' || '|' || 'GOL_DEB' || '|' ||
                       'PRODUCT_GROUP' || '|' || 'PRODUCT_TYPE' || '|' ||
                       'PRODUCT_CODE' || '|' || 'PRODUCT_DESC' || '|' ||
                       'INSTRUMENT' || '|' || 'INV_TYPE' || '|' ||
                       'SEC_NAME' || '|' || 'PORTFOLIO' || '|' ||
                       'CONTRACT_ID' || '|' || 'START_DATE' || '|' ||
                       'MATURITY_DATE' || '|' || 'SETTLE_DATE' || '|' ||
                       'RATING_CODE' || '|' || 'SWIFT_CODE' || '|' ||
                       'IMP_RATING' || '|' || 'GROUP_SEGMENT' || '|' ||
                       'SEGMENT' || '|' || 'SUB_SEGMENT' || '|' ||
                       'BUCKET_NAME' || '|' || 'BI_COLLECTABILITY' || '|' ||
                       'STAGE' || '|' || 'ASSESSMENT_IMP' || '|' ||
                       'IFRS9_CLASS' || '|' || 'CURRENCY' || '|' ||
                       'EXCHANGE_RATE' || '|' ||
                       'CONTRACTUAL_INTEREST_RATE' || '|' || 'EIR' || '|' ||
                       'COA_BAL' || '|' || 'NOTIONAL_AMOUNT_CCY' || '|' ||
                       'NOTIONAL_AMOUNT_LCL' || '|' ||
                       'INTEREST_RECEIVABLE_CCY' || '|' ||
                       'INTEREST_RECEIVABLE_LCL' || '|' ||
                       'PREMI_DISCOUNT_AMOUNT_CCY' || '|' ||
                       'PREMI_DISCOUNT_AMOUNT_LCL' || '|' ||
                       'CARRYING_AMOUNT_CCY' || '|' ||
                       'CARRYING_AMOUNT_LCL' || '|' || 'MARKET_VALUE_CCY' || '|' ||
                       'MARKET_VALUE_LCL' || '|' ||
                       'UNAMORTIZED_DISC_PREMIUM_CCY' || '|' ||
                       'UNAMORTIZED_DISC_PREMIUM_LCL' || '|' ||
                       'IA_UNWINDING_INTEREST_CCY' || '|' ||
                       'IA_UNWINDING_INTEREST_LCL' || '|' || 'CCF_RATE' || '|' ||
                       'CCF_AMOUNT_CCY' || '|' || 'CCF_AMOUNT_LCL' || '|' ||
                       'PREPAYMENT_RATE' || '|' || 'PREPAYMENT_AMOUNT_CCY' || '|' ||
                       'PREPAYMENT_AMOUNT_LCL' || '|' || 'EAD_AMOUNT_CCY' || '|' ||
                       'EAD_AMOUNT_LCL' || '|' || 'ECL_ON_BS_CCY' || '|' ||
                       'ECL_ON_BS_LCL' || '|' || 'ECL_OFF_BS_CCY' || '|' ||
                       'ECL_OFF_BS_LCL' || '|' || 'ECL_TOTAL_CCY' || '|' ||
                       'ECL_TOTAL_LCL' || '|' || 'ECL_ON_BS_FINAL_CCY' || '|' ||
                       'ECL_ON_BS_FINAL_LCL'|| '|' || 'ECL_TOTAL_FINAL_CCY' || '|' ||
                       'ECL_TOTAL_FINAL_LCL'|| '|' || 'SPECIAL_REASON');

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
                     INSTRUMENT,
                     INV_TYPE,
                     SEC_NAME,
                     PORTFOLIO,
                     CONTRACT_ID,
                     START_DATE,
                     MATURITY_DATE,
                     SETTLE_DATE,
                     RATING_CODE,
                     SWIFT_CODE,
                     IMP_RATING,
                     GROUP_SEGMENT,
                     SEGMENT,
                     SUB_SEGMENT,
                     BUCKET_NAME,
                     BI_COLLECTABILITY,
                     STAGE,
                     ASSESSMENT_IMP,
                     IFRS9_CLASS,
                     CURRENCY,
                     EXCHANGE_RATE,
                     CONTRACTUAL_INTEREST_RATE,
                     EIR,
                     COA_BAL,
                     PRINCIPAL_AMOUNT_CCY NOTIONAL_AMOUNT_CCY,
                     PRINCIPAL_AMOUNT_LCL NOTIONAL_AMOUNT_LCL,
                     INTEREST_RECEIVABLE_CCY,
                     INTEREST_RECEIVABLE_LCL,
                     PREMI_DISCOUNT_AMOUNT_CCY,
                     PREMI_DISCOUNT_AMOUNT_LCL,
                     CARRYING_AMOUNT_CCY,
                     CARRYING_AMOUNT_LCL,
                     NVL(MARKET_RATE,
                         0) MARKET_VALUE_CCY,
                     NVL(MARKET_RATE,
                         0) * NVL(EXCHANGE_RATE,
                                  1) MARKET_VALUE_LCL,
                     NVL(UNAMORT_FEE_AMT_CCY,
                         0) AS UNAMORTIZED_DISC_PREMIUM_CCY,
                     NVL(UNAMORT_FEE_AMT_LCL,
                         0) AS UNAMORTIZED_DISC_PREMIUM_LCL,
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
                     SPECIAL_REASON
                FROM IFRS_NOMINATIVE A
               WHERE DATA_SOURCE = 'KTP'
                 and account_status = 'A'
                 AND UPPER(PRODUCT_CODE) <> 'BORROWING'
                 AND REPORT_DATE = V_CURRDATE)
  loop

    dbms_output.put_line(REC.REPORT_DATE || '|' || REC.SOURCE_TYPE || '|' ||
                         REC.DATA_SOURCE || '|' || REC.BRANCH_CODE || '|' ||
                         REC.LBU_FORM || '|' || REC.NOREK_LBU || '|' ||
                         REC.ACCOUNT_NUMBER || '|' || REC.CUSTOMER_NUMBER || '|' ||
                         REC.CUSTOMER_NAME || '|' || REC.GOL_DEB || '|' ||
                         REC.PRODUCT_GROUP || '|' || REC.PRODUCT_TYPE || '|' ||
                         REC.PRODUCT_CODE || '|' || REC.PRODUCT_DESC || '|' ||
                         REC.INSTRUMENT || '|' || REC.INV_TYPE || '|' ||
                         REC.SEC_NAME || '|' || REC.PORTFOLIO || '|' ||
                         REC.CONTRACT_ID || '|' || REC.START_DATE || '|' ||
                         REC.MATURITY_DATE || '|' || REC.SETTLE_DATE || '|' ||
                         REC.RATING_CODE || '|' || REC.SWIFT_CODE || '|' ||
                         REC.IMP_RATING || '|' || REC.GROUP_SEGMENT || '|' ||
                         REC.SEGMENT || '|' || REC.SUB_SEGMENT || '|' ||
                         REC.BUCKET_NAME || '|' || REC.BI_COLLECTABILITY || '|' ||
                         REC.STAGE || '|' || REC.ASSESSMENT_IMP || '|' ||
                         REC.IFRS9_CLASS || '|' || REC.CURRENCY || '|' ||
                         REC.EXCHANGE_RATE || '|' ||
                         REC.CONTRACTUAL_INTEREST_RATE || '|' || REC.EIR || '|' ||
                         REC.COA_BAL || '|' || REC.NOTIONAL_AMOUNT_CCY || '|' ||
                         REC.NOTIONAL_AMOUNT_LCL || '|' ||
                         REC.INTEREST_RECEIVABLE_CCY || '|' ||
                         REC.INTEREST_RECEIVABLE_LCL || '|' ||
                         REC.PREMI_DISCOUNT_AMOUNT_CCY || '|' ||
                         REC.PREMI_DISCOUNT_AMOUNT_LCL || '|' ||
                         REC.CARRYING_AMOUNT_CCY || '|' ||
                         REC.CARRYING_AMOUNT_LCL || '|' ||
                         REC.MARKET_VALUE_CCY || '|' ||
                         REC.MARKET_VALUE_LCL || '|' ||
                         REC.UNAMORTIZED_DISC_PREMIUM_CCY || '|' ||
                         REC.UNAMORTIZED_DISC_PREMIUM_LCL || '|' ||
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
                         REC.SPECIAL_REASON);
  end loop;
END;