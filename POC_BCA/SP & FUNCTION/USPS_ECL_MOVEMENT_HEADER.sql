CREATE OR REPLACE PROCEDURE USPS_ECL_MOVEMENT_HEADER
(
   V_PERIOD_FROM DATE,
   V_PERIOD_TO DATE,
   V_SEGMENTS VARCHAR2,
   V_TYPE VARCHAR2,
   Cur_out OUT SYS_REFCURSOR
)
AS
    V_QUERY VARCHAR2(4000);
BEGIN

    V_QUERY := 'SELECT *
            FROM(
            SELECT SEQ_NO, IMP_CHANGE_REASON, ' || V_TYPE || ', STAGE
            FROM IFRS_REPORT_ECL_MOVEMENT_DTL
            WHERE SEQ_NO NOT IN (0, 99)
            AND REPORT_DATE BETWEEN '''|| TO_CHAR(V_PERIOD_FROM) ||''' AND '''|| TO_CHAR(V_PERIOD_TO) ||''' '||
            CASE WHEN V_SEGMENTS = 'ALL' THEN ''
            ELSE 'AND SUB_SEGMENT IN ('|| V_SEGMENTS ||')'
            END || 'UNION ALL
            SELECT SEQ_NO, IMP_CHANGE_REASON, ' || V_TYPE || ', STAGE
            FROM IFRS_REPORT_ECL_MOVEMENT_DTL
            WHERE SEQ_NO = 0
            AND REPORT_DATE = '''|| TO_CHAR(V_PERIOD_FROM) ||''' '||
            CASE WHEN V_SEGMENTS = 'ALL' THEN ''
            ELSE 'AND SUB_SEGMENT IN ('|| V_SEGMENTS ||')'
            END || 'UNION ALL
            SELECT SEQ_NO, IMP_CHANGE_REASON, ' || V_TYPE || ', STAGE
            FROM IFRS_REPORT_ECL_MOVEMENT_DTL
            WHERE SEQ_NO = 99
            AND REPORT_DATE = '''|| TO_CHAR(V_PERIOD_TO) ||''' '||
            CASE WHEN V_SEGMENTS = 'ALL' THEN ''
            ELSE 'AND SUB_SEGMENT IN ('|| V_SEGMENTS ||')'
            END || '
                )
            PIVOT(
                SUM(' || V_TYPE || ')
                FOR (STAGE)
                IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
            )
            ORDER BY SEQ_NO, IMP_CHANGE_REASON';


    OPEN Cur_out FOR V_QUERY;
END;