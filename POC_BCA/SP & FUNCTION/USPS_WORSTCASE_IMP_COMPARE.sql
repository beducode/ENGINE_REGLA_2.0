CREATE OR REPLACE PROCEDURE USPS_WORSTCASE_IMP_COMPARE
(
    v_downloadDateTo DATE,
    v_downloadDateFrom DATE,
    v_dataSource VARCHAR2,
    v_sortParameter VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(4000);
    v_DiffDate NUMBER DEFAULT 0;
     v_Date DATE;
BEGIN


    SELECT TO_NUMBER(MONTHS_BETWEEN (A.CURRDATE, TO_DATE(TO_CHAR(v_downloadDateFrom,'dd MON yyyy')))) INTO v_DiffDate FROM IFRS_PRC_DATE A;
    --SELECT MONTHS_BETWEEN (A.CURRDATE, TO_DATE(v_downloadDateFrom, 'dd-mm-yyyy')) INTO v_DiffDate FROM IFRS_PRC_DATE A;


    IF(v_DiffDate <= 3) THEN

        v_Query := 'SELECT
                    C.GOL_DEB "GolonganDebitur",
                    C.CUSTOMER_NUMBER "CustomerNumber",
                    C.CUSTOMER_NAME "CustomerName",
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                    TO_CHAR(C.REPORT_DATE, ''Mon YYYY'') "Period",
                    C.BI_COLLECTABILITY "BICollectability",
                    SUM(CASE WHEN C.DATA_SOURCE = ''KTP'' THEN ROUND(C.PRINCIPAL_AMOUNT_CCY, 6)
                    ELSE ROUND(C.OUTSTANDING_ON_BS_CCY, 6) END) "OutstandingOnBs",
                    SUM(ROUND(C.OUTSTANDING_OFF_BS_CCY, 6))  "OutstandingOffBs",
                    SUM(ROUND(C.OUTSTANDING_PRINCIPAL_DIFF, 6 ))  "OutstandingDiff",
                    SUM(ROUND(C.ECL_TOTAL_CCY, 6)) "ECLTotal",
                    SUM(ROUND(C.ECL_TOTAL_DIFF, 6 )) "ECLTotalDiff",
                    SUM(ROUND(C.CARRYING_AMOUNT_CCY, 6 )) "CarryingValue",
                    SUM(ROUND(X.CV_DIFF, 6 )) "CarryingValueDiff"
                    FROM IFRS_NOMINATIVE C
                    JOIN
                    (
                        SELECT
                        NVL(A.CARRYING_AMOUNT_CCY, 0) - NVL(B.CARRYING_AMOUNT_CCY, 0) AS "CV_DIFF",
                        MASTERID
                        FROM IFRS_NOMINATIVE A
                        JOIN IFRS_NOMINATIVE B
                        USING(MASTERID)
                        WHERE A.REPORT_DATE = ''' || TO_CHAR(v_downloadDateTo) || '''
                        AND B.REPORT_DATE = ''' || TO_CHAR(v_downloadDateFrom) || '''
                    ) X
                    ON C.MASTERID = X.MASTERID
                    JOIN IFRS_MASTER_BRANCH D
                    ON C.BRANCH_CODE = D.BRANCH_NUM
                    AND C.REPORT_DATE >= ''' || TO_CHAR(v_downloadDateFrom) || '''
                    AND C.REPORT_DATE <= ''' || TO_CHAR(v_downloadDateTo) || '''
                    AND C.DATA_SOURCE LIKE ''%' || v_dataSource || '%''
                    GROUP BY C.GOL_DEB, C.CUSTOMER_NUMBER,
                    C.CUSTOMER_NAME, D.BRANCH_NUM || ''_'' || D.BRANCH_NAME,
                    TO_CHAR(C.REPORT_DATE, ''Mon YYYY''), C.BI_COLLECTABILITY';

    ELSE

        v_Query := 'SELECT
                    C.GOL_DEB "GolonganDebitur",
                    C.CUSTOMER_NUMBER "CustomerNumber",
                    C.CUSTOMER_NAME "CustomerName",
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                    TO_CHAR(C.REPORT_DATE, ''Mon YYYY'') "Period",
                    C.BI_COLLECTABILITY "BICollectability",
                    SUM(CASE WHEN C.DATA_SOURCE = ''KTP'' THEN ROUND(C.PRINCIPAL_AMOUNT_CCY, 6)
                    ELSE ROUND(C.OUTSTANDING_ON_BS_CCY, 6) END) "OutstandingOnBs",
                    SUM(ROUND(C.OUTSTANDING_OFF_BS_CCY, 6))  "OutstandingOffBs",
                    SUM(ROUND(C.OUTSTANDING_PRINCIPAL_DIFF, 6 ))  "OutstandingDiff",
                    SUM(ROUND(C.ECL_TOTAL_CCY, 6)) "ECLTotal",
                    SUM(ROUND(C.ECL_TOTAL_DIFF, 6 )) "ECLTotalDiff",
                    SUM(ROUND(C.CARRYING_AMOUNT_CCY, 6 )) "CarryingValue",
                    SUM(ROUND(X.CV_DIFF, 6 )) "CarryingValueDiff"
                    FROM IFRS_NOMINATIVE_ACV C
                    JOIN
                    (
                        SELECT
                        NVL(A.CARRYING_AMOUNT_CCY, 0) - NVL(B.CARRYING_AMOUNT_CCY, 0) AS "CV_DIFF",
                        MASTERID
                        FROM IFRS_NOMINATIVE_ACV A
                        JOIN IFRS_NOMINATIVE_ACV B
                        USING(MASTERID)
                        WHERE A.REPORT_DATE = ''' || TO_CHAR(v_downloadDateTo) || '''
                        AND B.REPORT_DATE = ''' || TO_CHAR(v_downloadDateFrom) || '''
                    ) X
                    ON C.MASTERID = X.MASTERID
                    JOIN IFRS_MASTER_BRANCH D
                    ON C.BRANCH_CODE = D.BRANCH_NUM
                    AND C.REPORT_DATE >= ''' || TO_CHAR(v_downloadDateFrom) || '''
                    AND C.REPORT_DATE <= ''' || TO_CHAR(v_downloadDateTo) || '''
                    AND C.DATA_SOURCE LIKE ''%' || v_dataSource || '%''
                    GROUP BY C.GOL_DEB, C.CUSTOMER_NUMBER,
                    C.CUSTOMER_NAME, D.BRANCH_NUM || ''_'' || D.BRANCH_NAME,
                    TO_CHAR(C.REPORT_DATE, ''Mon YYYY''), C.BI_COLLECTABILITY';
    END IF;

   OPEN Cur_out FOR v_query;

END;