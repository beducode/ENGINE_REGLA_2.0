CREATE OR REPLACE PROCEDURE USPS_ECL_MOVEMENT_DETAIL
(
   V_PERIOD_FROM DATE,
   V_PERIOD_TO DATE,
   V_SEGMENTS VARCHAR2,
   V_CHANGE_REASON VARCHAR2,
    v_pageNumber NUMBER,
    v_pageSize NUMBER,
    v_sortParameter VARCHAR2,
    v_count VARCHAR2,
    V_TYPE VARCHAR2,
   Cur_out OUT SYS_REFCURSOR
)
AS
    V_QUERY VARCHAR2(4000);
    v_Select VARCHAR2(4000);
    v_sortParameter2 VARCHAR2(2000);
    v_pageSize2 NUMBER;
BEGIN

    IF v_sortParameter IS NULL THEN
        v_sortParameter2 := '"DATA_SOURCE"';
    ELSE
        v_sortParameter2 := v_sortParameter;
    END IF;

    IF(v_pageSize = 0) THEN
        v_pageSize2 := 10;
    ELSE
        v_pageSize2 := v_pageSize;
    END IF;

    IF(v_Count = 'YES') THEN
        v_Select := 'SELECT COUNT(*)';
    ELSE
     v_Select := 'SELECT * ';
     END IF;

    v_Query :=  v_Select || 'FROM(
                        SELECT
                        DATA_SOURCE,
                        SUB_SEGMENT,
                        ACCOUNT_NUMBER,
                        CUSTOMER_NAME,
                        CUSTOMER_NUMBER,'
                        ||  V_TYPE  ||',
                        STAGE
                        FROM IFRS_REPORT_ECL_MOVEMENT_DTL
                        WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                        AND '|| V_TYPE  ||' <> 0
                        AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' ||
                        CASE WHEN V_SEGMENTS = 'ALL' THEN ''
                        ELSE 'AND SUB_SEGMENT IN ('|| V_SEGMENTS ||')'
                        END ||'
                    )
                PIVOT(
                    SUM('||  V_TYPE  ||')
                    FOR (STAGE)
                    IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                 )' ||
                CASE WHEN v_Count = 'YES' THEN ''
                ELSE '
                      ORDER BY ' || v_sortParameter2 || '' || '
                      OFFSET ' || TO_CHAR(v_pageNumber) || ' ROWS
                      FETCH NEXT ' || TO_CHAR(v_pageSize2) || ' ROWS ONLY'
                END;

    OPEN Cur_out FOR V_QUERY;
END;