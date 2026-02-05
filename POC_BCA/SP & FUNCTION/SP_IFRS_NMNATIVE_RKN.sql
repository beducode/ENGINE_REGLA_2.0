CREATE OR REPLACE PROCEDURE SP_IFRS_NMNATIVE_RKN(v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900')) AS
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
                       'START_DATE' || '|' || 'MATURITY_DATE' || '|' ||
                       'DAY_PAST_DUE' || '|' || 'GROUP_SEGMENT' || '|' ||
                       'SEGMENT' || '|' || 'SUB_SEGMENT' || '|' ||
                       'BUCKET_NAME' || '|' || 'BI_COLLECTABILITY' || '|' ||
                       'STAGE' || '|' || 'ASSESSMENT_IMP' || '|' ||
                       'IFRS9_CLASS' || '|' || 'CURRENCY' || '|' ||
                       'EXCHANGE_RATE' || '|' ||
                       'CONTRACTUAL_INTEREST_RATE' || '|' ||
                       'OUTSTANDING_PRINCIPAL_CCY' || '|' ||
                       'OUTSTANDING_PRINCIPAL_LCL' || '|' ||
                       'EAD_AMOUNT_CCY' || '|' || 'EAD_AMOUNT_LCL' || '|' ||
                       'ECL_TOTAL_CCY' || '|' || 'ECL_TOTAL_LCL' || '|' ||
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
                     DAY_PAST_DUE,
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
                     OUTSTANDING_PRINCIPAL_CCY,
                     OUTSTANDING_PRINCIPAL_LCL,
                     EAD_AMOUNT_CCY,
                     EAD_AMOUNT_LCL,
                     ECL_TOTAL_CCY,
                     ECL_TOTAL_LCL,
                     RESERVED_AMOUNT_2 AS ECL_ON_BS_FINAL_CCY,
                     RESERVED_AMOUNT_3 AS ECL_ON_BS_FINAL_LCL,
                     RESERVED_AMOUNT_4 AS ECL_TOTAL_FINAL_CCY,
                     RESERVED_AMOUNT_5 AS ECL_TOTAL_FINAL_LCL,
                     SPECIAL_REASON
                FROM IFRS_NOMINATIVE
               WHERE REPORT_DATE = V_CURRDATE
                 AND DATA_SOURCE = 'RKN'
                 AND ACCOUNT_STATUS = 'A'
                 AND NVL(OUTSTANDING_PRINCIPAL_CCY,0) >= 0)
  loop

    dbms_output.put_line(REC.REPORT_DATE || '|' || REC.SOURCE_TYPE || '|' ||
                         REC.DATA_SOURCE || '|' || REC.BRANCH_CODE || '|' ||
                         REC.LBU_FORM || '|' || REC.NOREK_LBU || '|' ||
                         REC.ACCOUNT_NUMBER || '|' || REC.CUSTOMER_NUMBER || '|' ||
                         REC.CUSTOMER_NAME || '|' || REC.GOL_DEB || '|' ||
                         REC.PRODUCT_GROUP || '|' || REC.PRODUCT_TYPE || '|' ||
                         REC.PRODUCT_CODE || '|' || REC.PRODUCT_DESC || '|' ||
                         REC.START_DATE || '|' || REC.MATURITY_DATE || '|' ||
                         REC.DAY_PAST_DUE || '|' || REC.GROUP_SEGMENT || '|' ||
                         REC.SEGMENT || '|' || REC.SUB_SEGMENT || '|' ||
                         REC.BUCKET_NAME || '|' || REC.BI_COLLECTABILITY || '|' ||
                         REC.STAGE || '|' || REC.ASSESSMENT_IMP || '|' ||
                         REC.IFRS9_CLASS || '|' || REC.CURRENCY || '|' ||
                         REC.EXCHANGE_RATE || '|' ||
                         REC.CONTRACTUAL_INTEREST_RATE || '|' ||
                         REC.OUTSTANDING_PRINCIPAL_CCY || '|' ||
                         REC.OUTSTANDING_PRINCIPAL_LCL || '|' ||
                         REC.EAD_AMOUNT_CCY || '|' || REC.EAD_AMOUNT_LCL || '|' ||
                         REC.ECL_TOTAL_CCY || '|' || REC.ECL_TOTAL_LCL || '|' ||
                         REC.ECL_ON_BS_FINAL_CCY || '|' || REC.ECL_ON_BS_FINAL_LCL || '|' ||
                         REC.ECL_TOTAL_FINAL_CCY || '|' || REC.ECL_TOTAL_FINAL_LCL || '|' ||
                         REC.SPECIAL_REASON);
  end loop;
END;