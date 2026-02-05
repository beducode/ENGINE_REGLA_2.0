CREATE OR REPLACE PROCEDURE USPS_LGD_ANALYSIS_SUMMARY
(
    V_PERIOD in VARCHAR2  DEFAULT ' ',
    V_RULE_NAME varchar2 DEFAULT ' ',
    v_Where VARCHAR2 DEFAULT ' ',
    v_pageNumber NUMBER DEFAULT 0,
    v_pageSize NUMBER DEFAULT 0,
    v_SortColumn varchar2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS

 V_QUERY VARCHAR2(3000);
 V_DOWNLOAD_DATE_DD DATE;
 v_pageSize2 NUMBER;
 v_SortColumn2 varchar2(4000);
  v_CONFIGID NUMBER;


BEGIN

    IF v_SortColumn = ' '
    THEN

        v_SortColumn2 := v_SortColumn;
    END IF;

    IF v_pageSize = 0 THEN
        v_pageSize2 := 1;
    ELSE
        v_pageSize2 := v_pageSize;
    END IF;

     IF (V_RULE_NAME <>' ') THEN
        SELECT currdate INTO V_DOWNLOAD_DATE_DD FROM IFRS_PRC_DATE;
        SELECT  NVL(PKID,0)  INTO v_CONFIGID FROM IFRS_LGD_RULES_CONFIG where UPPER(LGD_RULE_NAME) =UPPER(V_RULE_NAME);-- and SEGMENT_TYPE='LGD_SEG';
    ELSE
        v_CONFIGID := 0;
    END IF;



    V_QUERY :=
        'SELECT
        A.PERIOD ,
        A.RULE_NAME ,
        A.RECOVERY_RATE ,
        A.LGD_EXPECTED_RECOVERY ,
        TO_CHAR(A.PERIOD,''yyyy/MM/dd'') || A.RULE_ID  || ''*''  AS DDATE_MAID
        FROM IFRS_LGD_EXPECTED_RECOVERY A  WHERE'
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '   A.PERIOD = ''' || (V_PERIOD)  ||  ''''
            || CASE WHEN V_RULE_NAME <> ' '
                THEN 'AND UPPER(A.RULE_ID) =''' || UPPER(LTRIM(RTRIM(v_CONFIGID))) || ''' '
                ELSE ''
            END

        END
        || '
        ORDER BY A.PERIOD';
--         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEST_QUERY';
--         INSERT INTO TEST_QUERY
--         SELECT V_QUERY FROM DUAL;

   COMMIT;

   OPEN Cur_out FOR V_QUERY;

    --dbms_output.put_line(v_Query);
    --select * from TEST_QUERY

END;