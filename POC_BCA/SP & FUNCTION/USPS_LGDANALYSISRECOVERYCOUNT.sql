CREATE OR REPLACE PROCEDURE  USPS_LGDANALYSISRECOVERYCOUNT
(
     V_DDATE_MAID VARCHAR2 DEFAULT ' ',
      v_Where VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
 v_DATE DATE;
    --v_CONFIGNAME VARCHAR2(50);
    v_CONFIGID VARCHAR2(18);
 V_QUERY VARCHAR2(3000);



BEGIN
    v_DATE := TO_DATE(SUBSTR(V_DDATE_MAID, 1, 10), 'yyyy/MM/dd');
    v_CONFIGID := NVL(TO_CHAR(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11)),0);



  --IF (v_CONFIGNAME <>' ') THEN

      --  SELECT  NVL(PKID,0)  INTO v_CONFIGID FROM IFRS_MSTR_SEGMENT_RULES_HEADER where SEGMENT =v_CONFIGNAME and SEGMENT_TYPE='LGD_SEG';
    --ELSE
      --  v_CONFIGID := 0;
    --END IF;


    V_QUERY :=
        'SELECT  COUNT(*) FROM
        IFRS_LGD A  WHERE'
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '   A.PERIOD = ''' || (v_DATE)  ||  ''''
            || CASE WHEN v_CONFIGID <> ' '
                THEN 'AND UPPER(A.SEGMENTATION_ID) LIKE ''%' || UPPER(LTRIM(RTRIM(v_CONFIGID))) || '%'' '
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