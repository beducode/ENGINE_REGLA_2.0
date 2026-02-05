CREATE OR REPLACE PROCEDURE USPS_R_MODEL_COEFF_PIVOT (
    v_MODEL_ID    NUMBER,
    v_MODEL_SEQ   NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
    v_tableName VARCHAR2(30);
    v_ColumnName VARCHAR2(4000);
BEGIN
    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR
    WHERE PKID = v_MODEL_ID;

    IF v_status = 1 THEN
        v_tableName := 'R_MULT_LINEAR_REGR_COEF';

        SELECT LISTAGG('''' || COEFF_NAME || ''' AS "' || COEFF_NAME || '"', ',') WITHIN GROUP (ORDER BY COEFF_NAME)
        INTO v_ColumnName
        FROM R_MULT_LINEAR_REGR_COEF
        WHERE MODEL_ID = v_MODEL_ID
        AND MODEL_SEQ = v_MODEL_SEQ;
    ELSE
        v_tableName := 'R_MULT_LINEAR_REGR_COEF_PEN';

        SELECT LISTAGG('''' || COEFF_NAME || ''' AS "' || COEFF_NAME || '"', ',') WITHIN GROUP (ORDER BY COEFF_NAME)
        INTO v_ColumnName
        FROM R_MULT_LINEAR_REGR_COEF_PEN
        WHERE MODEL_ID = v_MODEL_ID
        AND MODEL_SEQ = v_MODEL_SEQ;
    END IF;

    OPEN Cur_out FOR
    'SELECT ''Coeffiecients'' AS " ", B.* FROM
    (
        SELECT COEFF_NAME, ROUND(ESTIMATE_VAL,28) ESTIMATE_VAL
        FROM ' || v_tableName || '
        WHERE MODEL_ID = ' || TO_CHAR(v_MODEL_ID) || '
        AND MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
    ) A
    PIVOT
    (
        SUM(ESTIMATE_VAL)
        FOR COEFF_NAME IN (' || v_ColumnName || ')
    ) B';

END;