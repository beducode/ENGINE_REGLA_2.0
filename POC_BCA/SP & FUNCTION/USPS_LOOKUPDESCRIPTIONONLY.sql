CREATE OR REPLACE PROCEDURE USPS_LOOKUPDESCRIPTIONONLY
(
    v_value VARCHAR2,
    v_text VARCHAR2,
    v_filter VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2 (2000);
BEGIN

    v_Query :=
        'SELECT CAST(VALUE1 AS VARCHAR2(100)) AS "' || v_value || '",
            DESCRIPTION AS "' || v_text || '"
        FROM TBLM_COMMONCODEDETAIL
        WHERE ' || v_filter || '
        ORDER BY SEQUENCE';

    OPEN Cur_out FOR v_query;

END;