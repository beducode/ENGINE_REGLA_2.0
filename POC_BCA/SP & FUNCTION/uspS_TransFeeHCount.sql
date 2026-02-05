CREATE OR REPLACE PROCEDURE uspS_TransFeeHCount
(
  v_DOWNLOAD_DATE_From date DEFAULT '01-JAN-1900',
    v_DOWNLOAD_DATE_To date DEFAULT '01-JAN-1900',
    v_DATASOURCE varchar2 default ' ',
      v_ACCTNO VARCHAR2 default ' ',
    v_METHOD varchar2 default ' ',
   v_Where VARCHAR2 DEFAULT ' ',
     Cur_out OUT SYS_REFCURSOR
)
AS


    V_QUERY VARCHAR2(2000);
     --  v_pageSize2 NUMBER;
   -- v_SortColumn2 varchar2(4000);

BEGIN


      V_QUERY := '
        SELECT COUNT(*)
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
         END ;


            --AND ISNULL(SRC_INPUT, '''') like ''%' + @Source_Input + '%''';

            OPEN Cur_out FOR V_QUERY;

END;