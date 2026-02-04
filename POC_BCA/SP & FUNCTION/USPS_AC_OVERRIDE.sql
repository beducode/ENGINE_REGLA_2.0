CREATE OR REPLACE PROCEDURE USPS_AC_OVERRIDE
(
    V_LEVEL number,
    V_VALUE varchar2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    V_QUERY VARCHAR2(3000);
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

    V_QUERY :=
        'SELECT ima.PKID,
            ima.MASTER_ACCOUNT_CODE,
            ima.ACCOUNT_NUMBER,
            ima.CUSTOMER_NUMBER,
            ima.CUSTOMER_NAME,
            imp.PRD_DESC AS PRODUCT_DESC,
            ima.IFRS9_CLASS
        FROM IFRS_MASTER_ACCOUNT ima
            JOIN IFRS_MASTER_PRODUCT_PARAM imp
                ON ima.PRODUCT_CODE = imp.PRD_CODE
        WHERE DOWNLOAD_DATE = ''' || TO_CHAR (V_CURRDATE, 'dd-MON-yyyy')  ||  '''
            AND ACCOUNT_STATUS = ''A''
            AND MASTER_ACCOUNT_CODE NOT IN (SELECT MASTER_ACCOUNT_CODE FROM IFRS_AC_OVERRIDE)'
            ||
                CASE  WHEN V_LEVEL = 1
                    THEN ' AND ima.MASTER_ACCOUNT_CODE LIKE ''%' || LTRIM(RTRIM(V_VALUE)) || '%'' '
                WHEN V_LEVEL = 2
                    THEN ' AND ima.PRODUCT_CODE = ''' || LTRIM(RTRIM(V_VALUE)) || ''' '
                WHEN V_LEVEL = 3
                    THEN ' AND ima.ACCOUNT_NUMBER = ''' || LTRIM(RTRIM(V_VALUE)) || ''' '
                    ELSE ''
                END
        || ' ORDER BY PKID';

    --DBMS_OUTPUT.PUT_LINE (v_QUERY);

    OPEN Cur_out FOR V_QUERY;

END;