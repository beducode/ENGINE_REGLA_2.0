CREATE OR REPLACE PROCEDURE  USPS_LGD_HISTORY_PAYMENT_ALL
(
     V_DDATE_MAID VARCHAR2 DEFAULT ' ',
      v_Where VARCHAR2 DEFAULT ' ',
  --  v_pageNumber NUMBER DEFAULT 0,
   -- v_pageSize NUMBER DEFAULT 0,
    v_SortColumn varchar2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
 v_DATE DATE;
 v_ACCNUM VARCHAR2(50);
 v_CUSTNUM VARCHAR2(50);
 v_Flag VARCHAR(1);
 v_RULEID VARCHAR2(18);
 V_QUERY VARCHAR2(3000);
 V_QUERY2 VARCHAR2(3000);
 v_string varchar2(2000);
 V_RULE_NAME varchar2(50);


 v_pageSize2 NUMBER;
 v_SortColumn2 varchar2(4000);

BEGIN




    v_DATE := TO_DATE(SUBSTR(V_DDATE_MAID, 1, 10), 'yyyy/MM/dd');

   --2018/09/02141*0090380544400001*00010733663
    v_RULEID :=TO_NUMBER(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11));
    v_ACCNUM :=TO_CHAR(SUBSTR(V_DDATE_MAID, INSTR(V_DDATE_MAID, '*', 1,1)+1 ,INSTR(V_DDATE_MAID, '*', 1,2)- INSTR(V_DDATE_MAID, '*', 1,1)-1));

   v_CUSTNUM:=TO_CHAR(SUBSTR(V_DDATE_MAID, LENGTH(v_Date)+LENGTH(v_RULEID)+LENGTH(v_ACCNUM) + 4, LENGTH(V_DDATE_MAID)));
   --v_CUSTNUM:=TO_CHAR(SUBSTR(V_DDATE_MAID, INSTR(V_DDATE_MAID, '*', 1,2)+2, INSTR(V_DDATE_MAID, '*', 1,2)-INSTR(V_DDATE_MAID, 'A', 1,1)-1));


   v_Flag:=TO_CHAR(SUBSTR(V_DDATE_MAID, LENGTH(v_Date)+LENGTH(v_RULEID)+LENGTH(v_ACCNUM) +LENGTH(v_CUSTNUM)+ 4, LENGTH(V_DDATE_MAID)));
   v_string:=LENGTH(v_ACCNUM) +LENGTH(v_Date)+LENGTH(v_RULEID);

    SELECT LGD_RULE_NAME INTO V_RULE_NAME   FROM  IFRS_LGD_RULES_CONFIG where PKID=v_RULEID;

--'''||V_DDATE_MAID  ||''' AS DDATE_MAID
    V_QUERY :=
        ' SELECT '''|| V_RULE_NAME ||''' AS "Segmentation Name",
        A.ACCOUNT_NUMBER AS "Account Number",
        A.CUSTOMER_NUMBER AS "Customer Number",
         A.ACCOUNT_STATUS AS "Account Status", A.PAYMENT_DATE As "Payment Date",
         A.CURRENCY AS "Currency",A.RECOVERY_AMOUNT AS "Recovery Amount"
       FROM IFRS_LGD_DATA_DETAIL A  WHERE'
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '   A.PAYMENT_DATE < = ''' || (v_DATE)  ||  ''''
--            || CASE WHEN v_RULEID <> ' '
--                THEN 'AND UPPER(A.RULE_ID) LIKE ''%' || UPPER(LTRIM(RTRIM(v_RULEID))) || '%'' '
--                ELSE ''
--            END
--             || CASE WHEN v_ACCNUM <> ' '
--                THEN 'AND UPPER(A.ACCOUNT_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(v_ACCNUM))) || '%'' '
--                ELSE ''
--            END
--             || CASE WHEN v_CUSTNUM <> ' '
--                THEN 'AND UPPER(A.CUSTOMER_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(v_CUSTNUM))) || '%'' '
--                ELSE ''
--         END

        END
        || 'ORDER BY A.PAYMENT_DATE,A.ACCOUNT_NUMBER';
--         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEST_QUERY';
--         INSERT INTO TEST_QUERY
--         SELECT V_QUERY FROM DUAL;

   COMMIT;

   OPEN Cur_out FOR V_QUERY;

    --dbms_output.put_line(v_Query);
    --select * from TEST_QUERY

END;