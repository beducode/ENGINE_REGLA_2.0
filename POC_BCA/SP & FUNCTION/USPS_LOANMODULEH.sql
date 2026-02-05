CREATE OR REPLACE PROCEDURE USPS_LOANMODULEH
(
    V_DOWNLOAD_DATE date DEFAULT '01-JAN-1900',
    V_DATA_SOURCE varchar2 DEFAULT ' ',
    V_FACILITY_NUMBER varchar2 DEFAULT ' ',
    V_CUSTOMER_NUMBER varchar2 DEFAULT ' ',
    V_CUSTOMER_NAME VARCHAR2 DEFAULT ' ',
    V_ACCOUNT_NUMBER varchar2 DEFAULT ' ',
     V_BRANCH_CODE varchar2 DEFAULT ' ',
    V_SEGMENT varchar2 DEFAULT ' ',
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
        'SELECT A.ACCOUNT_NUMBER,A.AMORT_TYPE,
        TO_CHAR(A.download_date, ''yyyy/MM/dd'') || A.MASTERID  || ''*'' || A.ACCOUNT_NUMBER AS DDATE_MAID,
            A.DOWNLOAD_DATE,
            A.PRODUCT_ENTITY,
            A.DATA_SOURCE,
            A.SEGMENT,
            A.CUSTOMER_NUMBER,
            A.CUSTOMER_NAME,
            A.FACILITY_NUMBER,
            NVL(A.ACCOUNT_STATUS, '''') || '' - '' || TC.DESCRIPTION AS ACCOUNT_STATUS,
            A.LOAN_START_DATE,
            A.LOAN_DUE_DATE,
            B.BRANCH_NAME,
            A.BRANCH_CODE,
            P.PRD_DESC AS PRODUCT_NAME,
            A.CURRENCY,
            A.EXCHANGE_RATE,
            A.IFRS9_CLASS,
            A.EIR,
            NVL(A.INTEREST_RATE,0)      AS INTEREST_RATE,
            NVL(A.PLAFOND,0)            AS PLAFOND,
            NVL(A.OUTSTANDING,0)        AS OUTSTANDING,
            NVL(A.FAIR_VALUE_AMOUNT,0)  AS FAIR_VALUE_AMOUNT,
            NVL(A.EAD_AMOUNT,0)         AS EAD_AMOUNT,
            NVL(A.ECL_AMOUNT,0)         AS ECL_AMOUNT,
            A.CR_STAGE,
            A.TENOR,
            A.BI_COLLECTABILITY,
             NVL(A.RESERVED_VARCHAR_12,'''') as RESERVED_VARCHAR_12,
             A.IMPAIRED_FLAG,
             NVL(A.STAFF_LOAN_FLAG,0) as STAFF_LOAN_FLAG,
             NVL(A.RESERVED_VARCHAR_3,'''') as RESERVED_VARCHAR_3,
             NVL(A.RESERVED_VARCHAR_2,'''') || '' - '' || TC1.DESCRIPTION  as RESERVED_VARCHAR_2
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
        LEFT JOIN IFRS_MASTER_PRODUCT_PARAM P
            ON (A.PRODUCT_CODE=P.PRD_CODE  AND A.DATA_SOURCE = P.DATA_SOURCE)
        LEFT JOIN IFRS_MASTER_BRANCH B ON A.BRANCH_CODE = B.BRANCH_NUM AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        LEFT JOIN (SELECT * FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''B95'')TC ON TC.VALUE1 = A.ACCOUNT_STATUS
        LEFT JOIN (SELECT * FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''B202'')TC1 ON TC1.VALUE1 = A.RESERVED_VARCHAR_2
        WHERE A.DATA_SOURCE IN (SELECT VALUE1 FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''B98'') '
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
            ' AND A.DOWNLOAD_DATE = ''' || TO_CHAR (V_DOWNLOAD_DATE_DD, 'dd-MON-yyyy')  ||  ''''
            || CASE WHEN V_DATA_SOURCE = 'LIMIT'
                    THEN 'AND UPPER(A.DATA_SOURCE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_DATA_SOURCE))) || '%''
                          AND A.SUB_SEGMENT NOT LIKE ''%BTRD'' '
                    WHEN V_DATA_SOURCE <> ' '
                    THEN 'AND UPPER(A.DATA_SOURCE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_DATA_SOURCE))) || '%'' '
                    ELSE ''
            END
            || CASE WHEN V_FACILITY_NUMBER <> ' '
                THEN 'AND UPPER(A.FACILITY_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_FACILITY_NUMBER))) || '%'' '
                ELSE ''
            END
            || CASE WHEN V_CUSTOMER_NUMBER <> ' '
                THEN 'AND UPPER(A.CUSTOMER_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_CUSTOMER_NUMBER))) || '%'''
                ELSE ''
            END
            || CASE WHEN V_CUSTOMER_NAME <> ' '
                THEN 'AND UPPER(A.CUSTOMER_NAME) LIKE ''%' || UPPER(LTRIM(RTRIM(V_CUSTOMER_NAME))) || '%'''
                ELSE ''
            END
            || CASE WHEN V_BRANCH_CODE <> ' '
                THEN 'AND UPPER(A.BRANCH_CODE) LIKE ''%' || SUBSTR(UPPER(LTRIM(RTRIM(V_BRANCH_CODE))),(LTRIM(INSTR(V_BRANCH_CODE, '_')) - 4), 4) || ''''
                ELSE ''
            END
            || CASE WHEN V_ACCOUNT_NUMBER <> ' '
                THEN 'AND UPPER(A.ACCOUNT_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_ACCOUNT_NUMBER))) || '%'''
                ELSE ''
            END
            || CASE WHEN V_SEGMENT <> ' '
                THEN 'AND UPPER(A.SEGMENT) LIKE ''%' || UPPER(LTRIM(RTRIM(V_SEGMENT))) || '%'''
                ELSE ''
            END
        END
        || '
        ORDER BY ' || v_SortColumn2 || ' ' || '
        OFFSET ' || TO_CHAR(v_pageNumber) || ' ROWS
        FETCH NEXT ' || TO_CHAR(v_pageSize2) || ' ROWS ONLY';

   OPEN Cur_out FOR V_QUERY;

    --dbms_output.put_line(v_Query);

END;