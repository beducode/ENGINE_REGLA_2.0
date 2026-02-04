CREATE OR REPLACE PROCEDURE USPS_R_LGD_TERM_STRUCTURE
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
        SELECT LISTAGG('' || FL_YEAR || ' AS "LGD Y' || FL_YEAR || ' (%)"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM R_LGD_TERM_STRUCTURE
            WHERE MODEL_ID = v_MODEL_ID
            AND MODEL_SEQ = v_MODEL_SEQ
        );
        --Perbaikan Nilai LGD berdasarkan persentase pada screen FORWARD LOOKING MODEL SELECTION - 8 Oktober 2025
        v_query := 'SELECT *
                    FROM
                    (
                    SELECT FL_YEAR, ROUND(OVERRIDE_LGD, 6)*100 LGD
                        FROM R_LGD_TERM_STRUCTURE
                        WHERE MODEL_ID = ' || TO_CHAR(v_MODEL_ID) ||'
                        AND MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
                    ) A
                    PIVOT
                    (
                        SUM(LGD)
                        FOR FL_YEAR IN (' || v_columnAlias ||')
                    )';
    ELSE
        SELECT LISTAGG('' || FL_YEAR || ' AS "LGD Y' || FL_YEAR || ' (%)"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM R_LGD_TERM_STRUCTURE_PEN
            WHERE MODEL_ID = v_MODEL_ID
            AND MODEL_SEQ = v_MODEL_SEQ
        );
        --Perbaikan Nilai LGD berdasarkan persentase pada screen FORWARD LOOKING MODEL SELECTION - 8 Oktober 2025
        v_query := 'SELECT *
                    FROM
                    (
                    SELECT FL_YEAR, ROUND(OVERRIDE_LGD, 6)*100 LGD
                        FROM R_LGD_TERM_STRUCTURE_PEN
                        WHERE MODEL_ID = ' || TO_CHAR(v_MODEL_ID) ||'
                        AND MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
                    ) A
                    PIVOT
                    (
                        SUM(LGD)
                        FOR FL_YEAR IN (' || v_columnAlias ||')
                    )';
    END IF;

    OPEN Cur_out FOR v_query;
END;