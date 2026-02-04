CREATE OR REPLACE PROCEDURE  USPS_R_PD_TERM_STRUCTURE
(
    v_MODEL_ID NUMBER,
    v_MODEL_SEQ NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_query VARCHAR2(4000);
    v_columnAlias VARCHAR2(4000);
    v_status NUMBER(10);
BEGIN
    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR
    WHERE PKID = v_MODEL_ID;

    IF v_status = 1 THEN
        SELECT LISTAGG('' || FL_YEAR || ' AS "MPD Y' || FL_YEAR || ' (%)"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM R_PD_TERM_STRUCTURE
            WHERE MODEL_ID = v_MODEL_ID
            AND MODEL_SEQ = v_MODEL_SEQ
        );

        v_query := 'SELECT *
                    FROM
                    (
                    SELECT B.BUCKET_ID, B.BUCKET_NAME, FL_YEAR, ROUND(OVERRIDE_PD * 100, 6) PD
                        FROM R_PD_TERM_STRUCTURE A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_ID = B.BUCKET_ID
                        AND A.MODEL_ID = ' || TO_CHAR(v_MODEL_ID) ||'
                        AND A.MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
                    ) A
                    PIVOT
                    (
                        SUM(PD)
                        FOR FL_YEAR IN (' || v_columnAlias ||')
                    )
                    ORDER BY BUCKET_ID';
    ELSE
        SELECT LISTAGG('' || FL_YEAR || ' AS "MPD Y' || FL_YEAR || ' (%)"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM R_PD_TERM_STRUCTURE_PEN
            WHERE MODEL_ID = v_MODEL_ID
            AND MODEL_SEQ = v_MODEL_SEQ
        );

        v_query := 'SELECT *
                    FROM
                    (
                    SELECT B.BUCKET_ID, B.BUCKET_NAME, FL_YEAR, ROUND(OVERRIDE_PD * 100, 6) PD
                        FROM R_PD_TERM_STRUCTURE_PEN A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_ID = B.BUCKET_ID
                        AND A.MODEL_ID = ' || TO_CHAR(v_MODEL_ID) ||'
                        AND A.MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
                    ) A
                    PIVOT
                    (
                        SUM(PD)
                        FOR FL_YEAR IN (' || v_columnAlias ||')
                    )
                    ORDER BY BUCKET_ID';
    END IF;

    OPEN Cur_out FOR v_query;
END;