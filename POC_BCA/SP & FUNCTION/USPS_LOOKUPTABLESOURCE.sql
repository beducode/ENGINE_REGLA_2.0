CREATE OR REPLACE PROCEDURE  USPS_LOOKUPTABLESOURCE
(
    v_value IN VARCHAR2 DEFAULT NULL,
    v_text IN VARCHAR2 DEFAULT NULL,
    v_filter IN VARCHAR2 DEFAULT NULL,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_query   VARCHAR2 (2000);
BEGIN

    v_query :=
            'SELECT Value1 AS "' || v_value || '" ,
                Value1 AS "' || v_text || '"
            FROM TBLM_COMMONCODEDETAIL
            WHERE ' || v_filter || '
            ORDER BY Sequence ' ;

    OPEN Cur_out FOR v_query;

END;