CREATE OR REPLACE PROCEDURE USPS_GETDATAMAPPINGTABLE
(
    v_UploadID number default 0,
    v_Columns VARCHAR2 default ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(5000);
BEGIN

    v_Query := 'SELECT '
        || v_Columns
        || ' FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = ' || to_char(v_UploadID)
        || ' ORDER BY UPLOADID';

      OPEN Cur_out FOR V_QUERY;

END;