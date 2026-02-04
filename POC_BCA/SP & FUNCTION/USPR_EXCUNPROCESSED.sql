CREATE OR REPLACE PROCEDURE USPR_EXCUNPROCESSED
(
 v_EXCEPTION_CODE varchar2 DEFAULT ' ',
 v_DOWNLOAD_DATE DATE DEFAULT '01/01/2000',
 Cur_out OUT SYS_REFCURSOR
)
as
 v_query varchar2(3000);
BEGIN

    open Cur_out for select
        DOWNLOAD_DATE "Download Date",
        DATA_SOURCE "Data Source",
        PRODUCT_GROUP    "Product Group"    ,
        EXCEPTION_ID "ID Exception",
        b.EXCEPTION_DESC "Description",
        MASTER_ACCOUNT_ID "Master Account ID",
        ACCOUNT_NUMBER "Account Number",
        TABLE_NAME "Table Name",
        FIELD_NAME "Field Name",
        VALUE "Value",
        CUSTOMER_NAME "Customer Name"
    FROM IFRS_EXCEPTION_ACCOUNT a
    JOIN IFRS_MASTER_EXCEPTION b
        ON A.EXCEPTION_ID = b.PKID
    WHERE  DOWNLOAD_DATE = v_DOWNLOAD_DATE --TO_DATE( v_DOWNLOAD_DATE, 'MM/DD/YYYY HH:MI:SS AM')
        AND
         B.EXCEPTION_CODE LIKE trim(v_EXCEPTION_CODE || '%')
    order by a.DOWNLOAD_DATE, a.CREATEDDATE;

END;