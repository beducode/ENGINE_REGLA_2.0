CREATE OR REPLACE PROCEDURE USPS_INDIVIDUAL_IMP_D
(
    v_downloadDate DATE,
    v_pageNumber NUMBER,
    v_pageSize NUMBER,
    v_sortParameter VARCHAR2,
    v_count VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(4000);
    v_Select VARCHAR2(4000);
    v_sortParameter2 VARCHAR2(2000);
    v_pageSize2 NUMBER;
BEGIN

    IF v_sortParameter IS NULL THEN
        v_sortParameter2 := '"Period"';
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
        v_Select := 'SELECT
                    TO_CHAR(C.REPORT_DATE,''Mon YYYY'') "Period",
                    C.ACCOUNT_NUMBER "AccountNumber",
                    C.CUSTOMER_NUMBER "CustomerNumber",
                    C.CUSTOMER_NAME "CustomerName",
                    C.PRODUCT_CODE || ''_'' || C.PRODUCT_DESC "ProductDescription",
                    C.SUB_SEGMENT "CustomerImpair",
                    C.RATING_CODE "RatingImp",
                    C.DAY_PAST_DUE "DayPastDue",
                    C.BI_COLLECTABILITY "BICollectability",
                    C.BTB_FLAG "BackToBackFlag",
                    C.ASSESSMENT_IMP "ImpairedFlag",
                    ROUND(C.CONTRACTUAL_INTEREST_RATE, 6) "CIR",
                    ROUND(C.EIR, 6) "EIR",
                    C.CURRENCY "Currency",
                    CASE WHEN C.DATA_SOURCE IN (''PBMM'',''KTP'') THEN ROUND(C.PRINCIPAL_AMOUNT_CCY, 6)
                    ELSE  ROUND(C.OUTSTANDING_ON_BS_CCY, 6) END "OutstandingOnBs",
                    ROUND(C.OUTSTANDING_OFF_BS_CCY, 6)  "OutstandingOffBs",
                    ROUND(C.CARRYING_AMOUNT_CCY, 6) "CarryingAmount",
                    ROUND(C.ECL_TOTAL_CCY, 6) "IndividualImpairment",
                    ROUND(C.IA_UNWINDING_INTEREST_CCY, 6) "UnwindingInterest"';
    END IF;

        v_Query :=  v_Select || '
                    FROM IFRS_NOMINATIVE C
                    WHERE UPPER(C.ASSESSMENT_IMP) = ''I''
                    AND C.REPORT_DATE = ''' || TO_CHAR(v_downloadDate) || '''' ||
                    CASE WHEN v_Count = 'YES' THEN ''
                    ELSE '
                          ORDER BY ' || v_sortParameter2 || '' || '
                          OFFSET ' || TO_CHAR(v_pageNumber) || ' ROWS
                          FETCH NEXT ' || TO_CHAR(v_pageSize2) || ' ROWS ONLY'
                    END;

   OPEN Cur_out FOR v_Query;

END;