CREATE OR REPLACE PROCEDURE SP_IFRS_RPT_NOMINATIVE_CRD (V_CURRDT DATE DEFAULT '1-JAN-1900')
IS
 V_BANYAKREC NUMBER;
 V_BANYAKFILE NUMBER;
 V_CURRDATE DATE;
begin

EXECUTE IMMEDIATE 'alter session enable parallel dml';

V_BANYAKREC := 500000;

EXECUTE IMMEDIATE 'TRUNCATE TABLE ifrs_rpt_nominative_crd';

    IF V_CURRDT = '1-JAN-1900' THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := V_CURRDT;
    END IF;

insert /*+ PARALLEL(8) */ into ifrs_rpt_nominative_crd
SELECT /*+ PARALLEL(8) */ ' ' RPT_KE,
       rownum RPT_ROWNUM,
              REPORT_DATE,
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
                 AND (ACCOUNT_STATUS = 'A' OR a.outstanding_on_bs_ccy > 0);COMMIT;

select CEIL(COUNT(1)/V_BANYAKREC) INTO V_BANYAKFILE from ifrs_rpt_nominative_crd;

FOR LOOP_COUNTER IN 1 .. V_BANYAKFILE
LOOP
   UPDATE /*+ PARALLEL(8) */ ifrs_rpt_nominative_crd SET RPT_KE = LOOP_COUNTER
   WHERE RPT_ROWNUM BETWEEN ((LOOP_COUNTER - 1)* V_BANYAKREC)+1 AND LOOP_COUNTER * V_BANYAKREC;COMMIT;
end loop;

end;