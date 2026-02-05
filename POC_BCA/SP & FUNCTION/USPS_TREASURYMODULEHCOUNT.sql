CREATE OR REPLACE PROCEDURE  USPS_TREASURYMODULEHCOUNT
(
    V_DOWNLOAD_DATE date DEFAULT '01-JAN-1900',
    V_DATA_SOURCE varchar2 DEFAULT ' ',
    V_FACILITY_NUMBER varchar2 DEFAULT ' ',
    V_CUSTOMER_NUMBER varchar2 DEFAULT ' ',
    V_CUSTOMER_NAME VARCHAR2 DEFAULT ' ',
    V_ACCOUNT_NUMBER varchar2 DEFAULT ' ',
    V_BRANCH_CODE varchar2 DEFAULT ' ',
    v_Where VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    V_QUERY VARCHAR2(3000);
    v_DOWNLOAD_DATE_DD DATE;
BEGIN

    IF (V_DOWNLOAD_DATE ='01-JAN-1900') THEN
        SELECT currdate INTO V_DOWNLOAD_DATE_DD FROM IFRS_PRC_DATE;
    ELSE
        V_DOWNLOAD_DATE_DD := V_DOWNLOAD_DATE;
    END IF;

    V_QUERY :=
        '
        SELECT COUNT(*) FROM IFRS_MASTER_ACCOUNT
        WHERE DATA_SOURCE IN (SELECT VALUE1 FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''B97'') '
        ||
        CASE WHEN v_Where <> '' THEN
            v_Where
        ELSE
            ' AND DOWNLOAD_DATE = '''|| TO_CHAR (V_DOWNLOAD_DATE_DD, 'dd-MON-yyyy')  ||  '''' ||
                CASE WHEN V_DATA_SOURCE <> ' '
                    THEN 'AND UPPER(DATA_SOURCE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_DATA_SOURCE))) || '%'' '
                    ELSE ''
                END ||
                /*
                CASE WHEN V_PRODUCT_TYPE <> ' '
                    THEN 'AND PRODUCT_TYPE LIKE ''%' || LTRIM(RTRIM(V_PRODUCT_TYPE)) || '%'' '
                    ELSE ''
                END ||
                CASE WHEN V_PRODUCT_CODE <> ' '
                    THEN 'AND PRODUCT_CODE LIKE ''%' || LTRIM(RTRIM(V_PRODUCT_CODE)) || '%'' '
                    ELSE ''
                END ||
                CASE WHEN V_PRODUCT_GROUP <> ' '
                    THEN 'AND PRODUCT_GROUP LIKE ''%' || LTRIM(RTRIM(V_PRODUCT_GROUP)) || '%'' '
                    ELSE ''
                END ||
                */
                CASE WHEN V_FACILITY_NUMBER <> ' '
                    THEN 'AND UPPER(FACILITY_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_FACILITY_NUMBER))) || '%'' '
                    ELSE ''
                END ||
                CASE WHEN V_CUSTOMER_NUMBER <> ' '
                    THEN 'AND UPPER(CUSTOMER_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_CUSTOMER_NUMBER))) || '%'''
                    ELSE ''
                END ||
                CASE WHEN V_CUSTOMER_NAME <> ' '
                    THEN 'AND UPPER(CUSTOMER_NAME) LIKE ''%' || UPPER(LTRIM(RTRIM(V_CUSTOMER_NAME))) || '%'''
                    ELSE ''
                END
                || CASE WHEN V_BRANCH_CODE <> ' '
                THEN 'AND UPPER(A.BRANCH_CODE) LIKE ''%' || UPPER(LTRIM(RTRIM(V_BRANCH_CODE))) || '%'''
                ELSE ''
            END ||
                CASE WHEN V_ACCOUNT_NUMBER <> ' '
                    THEN 'AND UPPER(ACCOUNT_NUMBER) LIKE ''%' || UPPER(LTRIM(RTRIM(V_ACCOUNT_NUMBER))) || '%'''
                    ELSE ''
                END
        END;

    OPEN Cur_out FOR v_Query;

END;