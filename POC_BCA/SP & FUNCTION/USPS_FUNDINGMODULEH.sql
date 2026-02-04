CREATE OR REPLACE PROCEDURE  USPS_FUNDINGMODULEH
(
    v_DOWNLOAD_DATE date DEFAULT '01-JAN-1900',
    v_PRD_CODE varchar2 DEFAULT ' ',
    v_ACCOUNT_NUMBER varchar2 DEFAULT ' ',
    V_BRANCH_CODE varchar2 DEFAULT ' ',
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
BEGIN

    IF v_SortColumn = ' '
    THEN
        v_SortColumn2 := 'MASTERID';
    ELSE
        v_SortColumn2 := v_SortColumn;
    END IF;

    IF v_pageSize = 0 THEN
        v_pageSize2 := 1;
    ELSE
        v_pageSize2 := v_pageSize;
    END IF;

    IF (V_DOWNLOAD_DATE ='01-JAN-1900') THEN
        SELECT currdate INTO V_DOWNLOAD_DATE_DD FROM IFRS_PRC_DATE;
    ELSE
        V_DOWNLOAD_DATE_DD := V_DOWNLOAD_DATE;
    END IF;

    V_QUERY :=
        'SELECT A.AMORT_TYPE,
            TO_CHAR(A.download_date, ''yyyy/MM/dd'') || A.MASTERID  || ''*'' || A.ACCOUNT_NUMBER AS DDATE_MAID,
            A.DOWNLOAD_DATE,
            A.PRODUCT_ENTITY,
            A.DATA_SOURCE,
            C.BRANCH_NAME,
            A.ACCOUNT_NUMBER,
            A.ACCOUNT_STATUS,
            A.CUSTOMER_NUMBER,
            A.CUSTOMER_NAME,
            A.LOAN_START_DATE,
            A.LOAN_DUE_DATE,
            B.PRD_DESC,
            A.CURRENCY,
            A.EXCHANGE_RATE,
            A.INTEREST_RATE,
            NVL(A.INITIAL_OUTSTANDING,0) AS INITIAL_OUTSTANDING,
            NVL(A.TENOR,0) AS TENOR,
             NVL(A.INITIAL_UNAMORT_ORG_FEE,0)      AS INITIAL_UNAMORT_ORG_FEE,
           NVL(A.INITIAL_UNAMORT_TXN_COST,0)     AS INITIAL_UNAMORT_TXN_COST
        FROM IFRS_LI_MASTER_ACCOUNT A
            JOIN IFRS_MASTER_PRODUCT_PARAM B ON A.PRODUCT_CODE = B.PRD_CODE AND B.DATA_SOURCE = ''FUNDING''
            LEFT JOIN IFRS_MASTER_BRANCH C ON A.BRANCH_CODE = C.BRANCH_NUM
        WHERE A.DATA_SOURCE IN (''FUNDING'') '
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            'AND A.DOWNLOAD_DATE = ''' || TO_CHAR (V_DOWNLOAD_DATE_DD, 'dd-MON-yyyy')  ||  ''' '
            || CASE WHEN v_PRD_CODE <> ' '
            THEN 'AND UPPER(A.PRODUCT_CODE) LIKE ''%' || UPPER(LTRIM(RTRIM(v_PRD_CODE))) || '%'' '
            ELSE ''
        END
         || CASE WHEN V_BRANCH_CODE <> ' '
                THEN 'AND UPPER(A.BRANCH_CODE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_BRANCH_CODE))) || '%'' '
                ELSE ''
            END
            || CASE WHEN v_ACCOUNT_NUMBER <> ' '
                THEN 'AND UPPER(A.ACCOUNT_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(v_ACCOUNT_NUMBER))) || '%'' '
                ELSE ''
            END
        END
        ||  '

        ORDER BY ' || v_SortColumn2 || ' ' || '
        OFFSET ' || TO_CHAR(v_pageNumber) || ' ROWS
        FETCH NEXT ' || TO_CHAR(v_pageSize2) || ' ROWS ONLY';

   OPEN Cur_out FOR V_QUERY;

END;