CREATE OR REPLACE PROCEDURE USPS_ME_DERIVED_FORCST_PIVOT (
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
        v_tableName := ' ';

        SELECT LISTAGG('''' || COEFF_NAME || ''' AS "' || COEFF_NAME || '"', ',') WITHIN GROUP (ORDER BY COEFF_NAME)
        INTO v_ColumnName
        FROM
        (
            SELECT DISTINCT B2.COEFF_NAME
            FROM IFRS_ME_DERIVED_FORCST_VAR A2
            JOIN R_MULT_LINEAR_REGR_COEF B2
            ON A2.MODEL_ID = B2.MODEL_ID
            AND UPPER(A2.ME_CODE) = UPPER(B2.COEFF_NAME)
            AND A2.MODEL_ID = v_MODEL_ID
            AND B2.MODEL_SEQ = v_MODEL_SEQ
            JOIN IFRS_FL_MODEL_VAR C2
            ON A2.MODEL_ID = C2.PKID
            AND A2.ME_PERIOD >= ADD_MONTHS(C2.IN_SAMPLE_END_DATE, 1)
            AND EXTRACT(MONTH FROM A2.ME_PERIOD) = 12
        ) A;
    ELSE
        v_tableName := '_PEN';

        SELECT LISTAGG('''' || COEFF_NAME || ''' AS "' || COEFF_NAME || '"', ',') WITHIN GROUP (ORDER BY COEFF_NAME)
        INTO v_ColumnName
        FROM
        (
            SELECT DISTINCT B2.COEFF_NAME
            FROM IFRS_ME_DERIVED_FORCST_VAR_PEN A2
            JOIN R_MULT_LINEAR_REGR_COEF_PEN B2
            ON A2.MODEL_ID = B2.MODEL_ID
            AND UPPER(A2.ME_CODE) = UPPER(B2.COEFF_NAME)
            AND A2.MODEL_ID = v_MODEL_ID
            AND B2.MODEL_SEQ = v_MODEL_SEQ
            JOIN IFRS_FL_MODEL_VAR C2
            ON A2.MODEL_ID = C2.PKID
            AND A2.ME_PERIOD >= ADD_MONTHS(C2.IN_SAMPLE_END_DATE, 1)
            AND EXTRACT(MONTH FROM A2.ME_PERIOD) = 12
        ) A;
    END IF;

    OPEN Cur_out FOR
    'SELECT * FROM
    (
        SELECT UPPER(A2.ME_CODE) ME_CODE,
            ''YEAR '' || ROW_NUMBER() OVER (PARTITION BY A2.ME_CODE ORDER BY A2.ME_PERIOD) " ",
            ROUND(A2.ME_VALUE,28) ME_VALUE
        FROM IFRS_ME_DERIVED_FORCST_VAR' || v_tableName || ' A2
        JOIN R_MULT_LINEAR_REGR_COEF' || v_tableName || ' B2
            ON A2.MODEL_ID = B2.MODEL_ID
            AND UPPER(A2.ME_CODE) = UPPER(B2.COEFF_NAME)
            AND A2.MODEL_ID = ' || TO_CHAR(v_MODEL_ID) || '
            AND B2.MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
            JOIN IFRS_FL_MODEL_VAR C2
            ON A2.MODEL_ID = C2.PKID
            AND A2.ME_PERIOD >= ADD_MONTHS(C2.IN_SAMPLE_END_DATE, 1)
            AND EXTRACT(MONTH FROM A2.ME_PERIOD) = 12
    ) A
    PIVOT
    (
        SUM(ME_VALUE)
        FOR ME_CODE IN (' || v_ColumnName || ')
    ) B
    ORDER BY " "';


END;