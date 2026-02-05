CREATE OR REPLACE PROCEDURE USPS_LGD_ANALYSIS_RECOVERY_D
(
     V_DDATE_MAID VARCHAR2 DEFAULT ' ',
      v_Where VARCHAR2 DEFAULT ' ',
    v_pageNumber NUMBER DEFAULT 0,
   v_pageSize NUMBER DEFAULT 0,
    v_SortColumn varchar2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
 v_DATE DATE;
 v_ACCNUM VARCHAR2(50);
 v_CUSTNUM VARCHAR2(50);
 v_RULEID VARCHAR2(18);
 V_QUERY VARCHAR2(3000);

 v_pageSize2 NUMBER;
 v_SortColumn2 varchar2(4000);

BEGIN
    v_DATE := TO_DATE(SUBSTR(V_DDATE_MAID, 1, 10), 'yyyy/MM/dd');
     --v_MASTERID := TO_NUMBER(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11));
    --v_ACCNUM := TO_CHAR(SUBSTR(V_DDATE_MAID, INSTR(V_DDATE_MAID, '*', 1) + 1, LENGTH(V_DDATE_MAID)));
   --2018/09/02141*0090380544400001*00010733663
    v_RULEID :=TO_NUMBER(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11));
  --  v_ACCNUM :=TO_CHAR(SUBSTR(V_DDATE_MAID, INSTR(V_DDATE_MAID, '*', 1) + 1, LENGTH(V_DDATE_MAID)));
       V_QUERY :=
        ' SELECT '''||V_DDATE_MAID  ||'''
        || A.ACCOUNT_NUMBER || ''*'' || A.CUSTOMER_NUMBER AS DDATE_MAID,
        A.DOWNLOAD_DATE AS DOWNLOAD_DATE,
        A.SEGMENTATION_NAME AS SEGMENTATION_NAME,
        A.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        A.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,A.CUSTOMER_NAME,A.NPL_DATE , A.CLOSED_DATE ,A.Currency ,
        ROUND(A.TOTAL_LOSS_AMT,2) As TOTAL_LOSS_AMT,
        ROUND(recov_amt_bf_npv,2) as recov_amt_bf_npv,
        ROUND(RECOVERY_AMOUNT,2) AS RECOVERY_AMOUNT,ROUND(discount_rate,6) as discount_rate,
        ROUND(recov_percentage, 6) as recov_percentage,
        ROUND (1-LOSS_RATE, 6) AS  "LGD_RATE"
        FROM IFRS_LGD A  WHERE'
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '   A.DOWNLOAD_DATE = ''' || (v_DATE)  ||  ''''
            || CASE WHEN v_RULEID <> ' '
                THEN 'AND UPPER(A.RULE_ID) = ''' || UPPER(LTRIM(RTRIM(v_RULEID))) || ''' '
                ELSE ''
            END
--             || CASE WHEN v_ACCNUM <> ' '
--                THEN 'AND UPPER(A.ACCOUNT_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(v_ACCNUM))) || '%'' '
--                ELSE ''
--            END

         END
        || '
          ORDER BY A.DOWNLOAD_DATE';
--         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEST_QUERY';
--         INSERT INTO TEST_QUERY
--         SELECT V_QUERY FROM DUAL;

   COMMIT;

   OPEN Cur_out FOR V_QUERY;

    --dbms_output.put_line(v_Query);
    --select * from TEST_QUERY

END;