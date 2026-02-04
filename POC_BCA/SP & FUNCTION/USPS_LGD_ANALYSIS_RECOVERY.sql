CREATE OR REPLACE PROCEDURE USPS_LGD_ANALYSIS_RECOVERY
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
    --v_CONFIGNAME VARCHAR2(50);
    v_RULEID VARCHAR2(18);
 V_QUERY VARCHAR2(3000);

 v_pageSize2 NUMBER;
 v_SortColumn2 varchar2(4000);

BEGIN
    v_DATE := TO_DATE(SUBSTR(V_DDATE_MAID, 1, 10), 'yyyy/MM/dd');
     --NVL(A.RESERVED_VARCHAR_12,'''')
    v_RULEID :=NVL(TO_CHAR(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11)),0);

  --  SELECT PKID INTO v_CONFIGID FROM IFRS_MSTR_SEGMENT_RULES_HEADER where SEGMENT =v_CONFIGNAME and SEGMENT_TYPE='LGD_SEG';
    --SELECT PKID FROM IFRS_MSTR_SEGMENT_RULES_HEADER where SEGMENT='SME' and SEGMENT_TYPE='LGD_SEG';

--    IF v_SortColumn = ' '
--    THEN
--
--        v_SortColumn2 := v_SortColumn;
--    END IF;
--
    IF v_pageSize = 0 THEN
        v_pageSize2 := 1;
    ELSE
        v_pageSize2 := v_pageSize;
    END IF;



    V_QUERY :=
        'SELECT '''||V_DDATE_MAID  ||'''  AS DDATE_MAID  ,A.DOWNLOAD_DATE ,
        A.SEGMENTATION_NAME ,
        ROUND(SUM(TOTAL_LOSS_AMT),2) AS TOTAL_LOSS_AMT,
        ROUND(SUM(RECOV_AMT_BF_NPV),2) AS RECOV_AMT_BF_NPV,
        ROUND(SUM(RECOVERY_AMOUNT),2) AS RECOVERY_AMOUNT,
        ROUND(SUM(RECOVERY_AMOUNT)/SUM(TOTAL_LOSS_AMT),6) as RECOVERY_RATE ,
        ROUND (1-SUM(RECOVERY_AMOUNT)/SUM(TOTAL_LOSS_AMT), 6) AS  LGD_RATE
        FROM IFRS_LGD A  WHERE'
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '   A.DOWNLOAD_DATE = ''' || (v_DATE)  ||  ''''
            || CASE WHEN v_RULEID <> ' '
                THEN 'AND UPPER(A.RULE_ID) = ''' || UPPER(LTRIM(RTRIM(v_RULEID))) || ''' '
                ELSE ''
            END

        END
        || '
         GROUP BY A.DOWNLOAD_DATE, A.SEGMENTATION_NAME
        ORDER BY A.DOWNLOAD_DATE, A.SEGMENTATION_NAME
        OFFSET ' || TO_CHAR(v_pageNumber) || ' ROWS
        FETCH NEXT ' || TO_CHAR(v_pageSize2) || ' ROWS ONLY';


         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEST_QUERY';
         INSERT INTO TEST_QUERY
         SELECT V_QUERY FROM DUAL;

   COMMIT;

   OPEN Cur_out FOR V_QUERY;

    --dbms_output.put_line(v_Query);
    --select * from TEST_QUERY

END;