CREATE OR REPLACE PROCEDURE  USPS_R_FL_YEAR_FORECAST (
    v_MODEL_ID    NUMBER,
    v_MODEL_SEQ   NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
BEGIN
    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR
    WHERE PKID = v_MODEL_ID;

    IF v_status = 1 THEN
        OPEN Cur_out FOR
        SELECT FL_Year,
            'YEAR ' || FL_YEAR FL_Year_Desc
        FROM R_FITTED_ODR
        WHERE MODEL_ID = v_MODEL_ID
        AND MODEL_SEQ = v_MODEL_SEQ
        ORDER BY FL_YEAR;
    ELSE
        OPEN Cur_out FOR
        SELECT FL_Year,
            'YEAR ' || FL_YEAR FL_Year_Desc
        FROM R_FITTED_ODR_PEN
        WHERE MODEL_ID = v_MODEL_ID
        AND MODEL_SEQ = v_MODEL_SEQ
        ORDER BY FL_YEAR;
    END IF;

END;