CREATE OR REPLACE PROCEDURE USPS_IMP_MOVEMENT
(
    v_downloadDateTo DATE,
    v_downloadDateFrom DATE,
    v_sortParameter VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(4000);
BEGIN

        v_Query := 'SELECT  /*+ index(c IDX_IFRS_NOMINATIVE_4) */
                    C.PRODUCT_GROUP "Produk",
                    C.GOL_DEB "GolonganDebitur",
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                    TO_CHAR(C.REPORT_DATE, ''Mon YYYY'') "Period",
                    SUM(CASE WHEN C.DATA_SOURCE IN (''PBMM'',''KTP'') THEN ROUND(C.PRINCIPAL_AMOUNT_CCY, 6)
                    ELSE ROUND(C.OUTSTANDING_ON_BS_CCY, 6) END) "OutstandingOnBs",
                    SUM(ROUND(C.OUTSTANDING_OFF_BS_CCY, 6))  "OutstandingOffBs",
                    SUM(ROUND(C.OUTSTANDING_PRINCIPAL_DIFF, 6))  "OutstandingDiff",
                    SUM(ROUND(C.ECL_TOTAL_CCY, 6)) "ECLTotal",
                    SUM(ROUND(C.ECL_TOTAL_DIFF, 6)) "ECLTotalDiff"
                    FROM IFRS_NOMINATIVE C
                    JOIN IFRS_MASTER_BRANCH D
                    ON C.BRANCH_CODE = D.BRANCH_NUM
                    AND C.REPORT_DATE >= ''' || TO_CHAR(v_downloadDateFrom) || '''
                    AND C.REPORT_DATE <= ''' || TO_CHAR(v_downloadDateTo) || '''
                    GROUP BY C.PRODUCT_GROUP, C.GOL_DEB,
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME, TO_CHAR(C.REPORT_DATE, ''Mon YYYY'')';

   OPEN Cur_out FOR v_query;

END;