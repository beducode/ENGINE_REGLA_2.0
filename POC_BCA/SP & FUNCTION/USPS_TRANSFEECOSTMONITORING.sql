CREATE OR REPLACE PROCEDURE USPS_TRANSFEECOSTMONITORING
(
    v_DOWNLOAD_DATE_From date DEFAULT '01-JAN-1900',
    v_DOWNLOAD_DATE_To date DEFAULT '01-JAN-1900',
    v_DATASOURCE varchar2 default ' ',
    v_ACCTNO VARCHAR2 default ' ',
    v_METHOD varchar2 default ' ',
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

    IF v_SortColumn = ' ' THEN
        v_SortColumn2 := 'MASTERID';
    ELSE
        v_SortColumn2 := v_SortColumn;
    END IF;

    IF v_pageSize = 0 THEN
        v_pageSize2 := 1;
    ELSE
        v_pageSize2 := v_pageSize;
    END IF;

    V_QUERY := '
        SELECT DATASOURCE
            , DOWNLOAD_DATE
            , ACCTNO
            , PRD_CODE
            , BRCODE
            , TRX_CODE
            , CCY
            , FLAG_REVERSE
            , AMOUNT
            , METHOD
            , SRCPROCESS
            , case when STATUS in (''ACT'', ''PNL'') then ''Y'' else ''N'' end "Process Flag"
            , STATUS
        FROM IFRS_ACCT_COST_FEE
        WHERE  '
        || CASE WHEN v_Where <> ' ' THEN
            v_Where
        ELSE
        ' DOWNLOAD_DATE BETWEEN ''' || TO_CHAR (v_DOWNLOAD_DATE_From, 'dd-MON-yyyy')  || ''' AND ''' || TO_CHAR (v_DOWNLOAD_DATE_To, 'dd-MON-yyyy') || ''''
            || CASE WHEN v_DATASOURCE <> ' '
                THEN ' AND DATASOURCE = ''' || v_DATASOURCE || ''' '
                ELSE ' '

            END
             || CASE WHEN v_METHOD <> ' '
                THEN ' AND METHOD = ''' || v_METHOD || ''' '
                ELSE ' '

            END
            || CASE WHEN v_ACCTNO <> ' '
                THEN 'AND UPPER(ACCTNO) LIKE ''%' || UPPER(LTRIM(RTRIM(v_ACCTNO))) || '%'''
                ELSE ' '
            END
         END
         || '
        ORDER BY ' || v_SortColumn2 || ' ' || '';

    OPEN Cur_out FOR V_QUERY;

END;