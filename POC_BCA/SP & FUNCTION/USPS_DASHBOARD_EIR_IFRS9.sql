CREATE OR REPLACE PROCEDURE USPS_DASHBOARD_EIR_IFRS9
(
    V_PeriodYear in VARCHAR2  DEFAULT ' ',
    V_AmortMethod in VARCHAR2  DEFAULT ' ',
    V_BranchCode varchar2 DEFAULT ' ',
    V_Goldebt varchar2 DEFAULT ' ',
    v_Where VARCHAR2 DEFAULT ' ',
    v_pageNumber NUMBER DEFAULT 0,
    v_pageSize NUMBER DEFAULT 0,
    v_SortColumn varchar2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    V_QUERY VARCHAR2(3000);
    v_pageSize2 NUMBER;
    v_SortColumn2 varchar2(4000);
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

    V_QUERY :=
        'SELECT DISTINCT (A.DOWNLOAD_DATE || ''_'' || C.RESERVED_VARCHAR_2) AS PKID,
            A.DOWNLOAD_DATE,
            C.RESERVED_VARCHAR_2,
            SUM(INITIAL_FEE_CCY) AS INITIAL_FEE,
            SUM(INITIAL_COST_CCY) AS INITIAL_COST,
            SUM(UNAMORT_FEE_AMT_CCY) AS UNAMORT_FEE,
            SUM(UNAMORT_COST_AMT_CCY) AS UNAMORT_COST,
            SUM(AMORT_FEE_CCY) AS AMORT_FEE,
            SUM(AMORT_COST_CCY) AS AMORT_COST,
            SUM(AMORT_FEE_MTD_CCY) AS MTD_AMORT_FEE,
            SUM(AMORT_COST_MTD_CCY) AS MTD_AMORT_COST,
            SUM(AMORT_FEE_YTD_CCY) AS YTD_AMORT_FEE,
            SUM(AMORT_COST_YTD_CCY) AS YTD_AMORT_COST
        FROM IFRS_NOMINATIVE A
            JOIN IFRS_MASTER_PRODUCT_PARAM B ON A.PRODUCT_CODE = B.PRD_CODE
                AND A.DATA_SOURCE = B.DATA_SOURCE
            JOIN IFRS_MASTER_ACCOUNT C ON A.PRODUCT_CODE = C.PRODUCT_CODE
                AND A.DATA_SOURCE = C.DATA_SOURCE
        WHERE '
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            '  EXTRACT(YEAR FROM A.DOWNLOAD_DATE) = ''' || (V_PeriodYear)  ||  ''''
            || CASE WHEN V_AmortMethod <> ' '
                THEN ' AND UPPER(A.AMORT_TYPE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_AmortMethod))) || '%'' '
                ELSE ''
            END
            || CASE WHEN V_BranchCode <> ' '
                THEN ' AND UPPER(A.BRANCH_CODE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_BranchCode))) || '%'' '
                ELSE ''
            END
            || CASE WHEN V_Goldebt <> ' '
                THEN ' AND UPPER(C.RESERVED_VARCHAR_2) LIKE ''%' || UPPER(LTRIM(RTRIM(V_Goldebt))) || '%'' '
                ELSE ''
            END
        END
        || ' GROUP BY (A.DOWNLOAD_DATE || ''_'' || C.RESERVED_VARCHAR_2),
            A.DOWNLOAD_DATE,
            C.RESERVED_VARCHAR_2
        ORDER   BY A.DOWNLOAD_DATE,
            C.RESERVED_VARCHAR_2';

    OPEN Cur_out FOR V_QUERY;

END;