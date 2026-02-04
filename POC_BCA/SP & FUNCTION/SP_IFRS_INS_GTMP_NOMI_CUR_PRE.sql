CREATE OR REPLACE PROCEDURE SP_IFRS_INS_GTMP_NOMI_CUR_PRE
(
   v_DOWNLOADDATE    DATE DEFAULT NULL
)
AS
   v_QUERY      VARCHAR2 (32000);
   V_PREVDATE   DATE;
BEGIN
   IF v_DOWNLOADDATE IS NOT NULL
   THEN
      V_PREVDATE := LAST_DAY (ADD_MONTHS (v_DOWNLOADDATE, -1));

      EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_NOMINATIVE_CURR_PREV';

      v_QUERY :=
            'INSERT INTO GTMP_NOMINATIVE_CURR_PREV
        (
  report_date,
  masterid,
  customer_number,
  customer_name,
  account_number,
  facility_number,
  account_status,
  data_source,
  sub_segment,
  poci_flag,
  stage,
  currency,
  exchange_rate,
  writeoff_flag,
  impaired_flag,
  special_reason,
  outstanding_on_bs_ccy,
  ecl_total_ccy,
  ecl_off_bs_lcl,
  ECL_OFF_BS_CCY,
  RESERVED_AMOUNT_2,
  reserved_amount_3,
  RESERVED_AMOUNT_4,
  reserved_amount_5,
  reserved_amount_6,
  RESERVED_AMOUNT_6_CCY,
  PRODUCT_CODE,
  RESERVED_VARCHAR_2,
  RESERVED_VARCHAR_5,
  UNUSED_AMT_CCY,
  UNUSED_AMT_LCL,
  BI_CODE,
  reserved_flag_1,
  reserved_flag_2,
  OUTSTANDING_ON_BS_LCL,
  OUTSTANDING_OFF_BS_LCL,
  OUTSTANDING_OFF_BS_CCY,
  UNAMORT_FEE_AMT_CCY,
  UNAMORT_FEE_AMT_LCL,
  UNAMORT_FEE_AMT_ILS_CCY,
  UNAMORT_FEE_AMT_ILS_LCL,
  IA_UNWINDING_INTEREST_CCY,
  IA_UNWINDING_INTEREST_LCL
      )
        SELECT
            report_date,
  masterid,
  customer_number,
  customer_name,
  account_number,
  facility_number,
  account_status,
  data_source,
  sub_segment,
  poci_flag,
  stage,
  currency,
  exchange_rate,
  writeoff_flag,
  impaired_flag,
  special_reason,
  case when outstanding_on_bs_ccy < 0 then 0 else outstanding_on_bs_ccy end outstanding_on_bs_ccy,
  ecl_total_ccy,
  ecl_off_bs_lcl,
  ECL_OFF_BS_CCY,
  RESERVED_AMOUNT_2,
  reserved_amount_3,
  RESERVED_AMOUNT_4,
  reserved_amount_5,
  reserved_amount_6,
  reserved_amount_6/exchange_rate RESERVED_AMOUNT_6_CCY,
  PRODUCT_CODE,
  RESERVED_VARCHAR_2,
  RESERVED_VARCHAR_5,
  reserved_amount_7/exchange_rate UNUSED_AMT_CCY ,
  reserved_amount_7 UNUSED_AMT_LCL,
  BI_CODE,
  reserved_flag_1,
  reserved_flag_2,
  case when OUTSTANDING_ON_BS_LCL < 0 then 0 else OUTSTANDING_ON_BS_LCL end OUTSTANDING_ON_BS_LCL,
  OUTSTANDING_OFF_BS_LCL,
  OUTSTANDING_OFF_BS_CCY,
  UNAMORT_FEE_AMT_CCY,
  UNAMORT_FEE_AMT_LCL,
  UNAMORT_FEE_AMT_ILS_CCY,
  UNAMORT_FEE_AMT_ILS_LCL,
  IA_UNWINDING_INTEREST_CCY,
  IA_UNWINDING_INTEREST_LCL
        FROM IFRS_NOMINATIVE
        WHERE report_date = '''
         || TO_CHAR (v_DOWNLOADDATE, 'dd MON yyyy')
         || ''' OR
          report_date = '''
         || TO_CHAR (V_PREVDATE, 'dd MON yyyy')
         || '''
    '    ;


      EXECUTE IMMEDIATE v_QUERY;

      COMMIT;


      delete GTMP_NOMINATIVE_CURR_PREV
       where masterid in (select curr.masterid
                            from (select *
                                    from GTMP_NOMINATIVE_CURR_PREV
                                   where report_date = V_PREVDATE
                                     and account_status = 'W') PREV,
                                 (select *
                                    from GTMP_NOMINATIVE_CURR_PREV
                                   where report_date = v_DOWNLOADDATE
                                     and account_status = 'W') CURR
                           where prev.masterid = curr.masterid);

      commit;

      delete from GTMP_NOMINATIVE_CURR_PREV limit
       where data_source = 'LIMIT'
         and exists
       (select 1
                from GTMP_NOMINATIVE_CURR_PREV ils
               where data_source = 'ILS'
                 and ils.account_status = 'A'
                 and ils.facility_number = limit.account_number
                 and ils.report_date = limit.report_date);
      commit;

delete from GTMP_NOMINATIVE_CURR_PREV
where  nvl(RESERVED_VARCHAR_2,' ') = ' '
and nvl(RESERVED_VARCHAR_5,' ') = ' ';

commit;






/*
delete GTMP_NOMINATIVE_CURR_PREV del
where (del.report_date = V_PREVDATE
or del.report_date = v_DOWNLOADDATE)
and exists (select 1 from GTMP_NOMINATIVE_CURR_PREV curr , GTMP_NOMINATIVE_CURR_PREV prev
where curr.report_date = v_DOWNLOADDATE
and prev.report_date = V_PREVDATE
and curr.masterid = prev.masterid
and curr.RESERVED_AMOUNT_5 = prev.RESERVED_AMOUNT_5
and (nvl(curr.RESERVED_VARCHAR_2,' ')||nvl(curr.RESERVED_VARCHAR_5,' ')) = (nvl(prev.RESERVED_VARCHAR_2,' ')||nvl(prev.RESERVED_VARCHAR_5,' '))
and ((curr.account_status = prev.account_status)
and (curr.writeoff_flag = prev.writeoff_flag))
and del.masterid = curr.masterid
and del.masterid = prev.masterid);

Commit;
*/

---- INSERT UNTUK TRESURY
INSERT INTO GTMP_NOMINATIVE_CURR_PREV
  (report_date,
   masterid,
   customer_number,
   customer_name,
   account_number,
   facility_number,
   account_status,
   data_source,
   sub_segment,
   poci_flag,
   stage,
   currency,
   exchange_rate,
   writeoff_flag,
   impaired_flag,
   special_reason,
   outstanding_on_bs_ccy,
   ecl_total_ccy,
   ecl_off_bs_lcl,
   ECL_OFF_BS_CCY,
   RESERVED_AMOUNT_2,
   reserved_amount_3,
   RESERVED_AMOUNT_4,
   reserved_amount_5,
   reserved_amount_6,
   RESERVED_AMOUNT_6_CCY,
   PRODUCT_CODE,
   RESERVED_VARCHAR_2,
   RESERVED_VARCHAR_5,
   UNUSED_AMT_CCY,
   UNUSED_AMT_LCL,
   BI_CODE,
   reserved_flag_1,
   reserved_flag_2,
   OUTSTANDING_ON_BS_LCL,
   OUTSTANDING_OFF_BS_LCL,
   OUTSTANDING_OFF_BS_CCY,
   UNAMORT_FEE_AMT_CCY,
   UNAMORT_FEE_AMT_LCL,
   UNAMORT_FEE_AMT_ILS_CCY,
   UNAMORT_FEE_AMT_ILS_LCL,
   IA_UNWINDING_INTEREST_CCY,
   IA_UNWINDING_INTEREST_LCL)
  SELECT v_DOWNLOADDATE,
         PREV.MASTERID,
         PREV.CUSTOMER_NUMBER,
         PREV.CUSTOMER_NAME,
         PREV.ACCOUNT_NUMBER,
         PREV.FACILITY_NUMBER,
         'W' ACCOUNT_STATUS,
         PREV.DATA_SOURCE,
         PREV.SUB_SEGMENT,
         PREV.POCI_FLAG,
         PREV.STAGE,
         CURR.CURRENCY,
         CURR.EXCHANGE_RATE,
         'Y' WRITEOFF_FLAG,
         PREV.IMPAIRED_FLAG,
         PREV.SPECIAL_REASON,
         CURR.WO_AMOUNT OUTSTANDING_ON_BS_CCY,
         0 ECL_TOTAL_CCY,
         0 ECL_OFF_BS_LCL,
         0 ECL_OFF_BS_CCY,
         0 RESERVED_AMOUNT_2,
         0 RESERVED_AMOUNT_3,
         0 RESERVED_AMOUNT_4,
         0 RESERVED_AMOUNT_5,
         0 RESERVED_AMOUNT_6,
         CURR.WO_AMOUNT RESERVED_AMOUNT_6_CCY,
         PREV.PRODUCT_CODE,
         PREV.RESERVED_VARCHAR_2,
         PREV.RESERVED_VARCHAR_5,
         0 UNUSED_AMT_CCY,
         0 UNUSED_AMT_LCL,
         PREV.BI_CODE,
         PREV.RESERVED_FLAG_1,
         PREV.RESERVED_FLAG_2,
         0 OUTSTANDING_ON_BS_LCL,
         0 OUTSTANDING_OFF_BS_LCL,
         0 OUTSTANDING_OFF_BS_CCY,
         0 UNAMORT_FEE_AMT_CCY,
         0 UNAMORT_FEE_AMT_LCL,
         0 UNAMORT_FEE_AMT_ILS_CCY,
         0 UNAMORT_FEE_AMT_ILS_LCL,
         0 IA_UNWINDING_INTEREST_CCY,
         0 IA_UNWINDING_INTEREST_LCL
    FROM tblu_wo_Treasury CURR, GTMP_NOMINATIVE_CURR_PREV PREV
   WHERE CURR.DOWNLOAD_DATE = v_DOWNLOADDATE
     AND PREV.REPORT_DATE = V_PREVDATE
     AND PREV.DATA_SOURCE = 'KTP'
     AND CURR.ACCOUNT_NUMBER = PREV.ACCOUNT_NUMBER
     and not exists (select 1
            from GTMP_NOMINATIVE_CURR_PREV curr_val
           where curr_val.masterid = prev.masterid
             and curr_val.report_date = v_DOWNLOADDATE);

     COMMIT;
   END IF;
END;