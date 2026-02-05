CREATE OR REPLACE PROCEDURE  USPS_LOOKUPCOLUMNNAME
(
    v_value IN VARCHAR2 DEFAULT NULL,
    v_text IN VARCHAR2 DEFAULT NULL,
    v_filter IN VARCHAR2 DEFAULT NULL,
    v_blank IN NUMBER DEFAULT 0,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_query   VARCHAR2 (2000);
BEGIN
    IF ( v_blank=1 ) THEN
        v_query :=
            'SELECT *
            FROM (
                SELECT '' '' AS "' || v_value
                    || '" , ''ALL'' AS "'
                    || v_text
                    || '", -1 AS "Sequence" FROM DUAL
                UNION
                    SELECT CAST(Value1 AS VARCHAR2(100)) AS "' || v_value
                || '",
                CASE WHEN Description <> '' '' THEN
                    (CAST(Value1 AS VARCHAR2(100)) || '' - '' || Description)
                ELSE
                    CAST(Value1 AS VARCHAR2(100))
                END AS "'
                || v_text
                || '"
                , Sequence
                FROM tblM_CommonCodeDetail
                WHERE '
                || v_filter
                || ' )
            ORDER BY "Sequence"' ;
    ELSE
        v_query :=
            'SELECT CAST(Value1 AS VARCHAR2(100)) AS "' || v_value
            || '" ,
            CASE WHEN Description <> '' '' THEN
                (CAST(Value1 AS VARCHAR2(100)) || '' - '' || Description)
            ELSE
                CAST(Value1 AS VARCHAR2(100))
            END AS "'
            || v_text
            || '"
            FROM tblM_CommonCodeDetail
            WHERE '
            || v_filter
            || ' ORDER BY Sequence ' ;
    END IF;

   --DBMS_OUTPUT.PUT_LINE (v_QUERY);

   OPEN Cur_out FOR v_query;

END;