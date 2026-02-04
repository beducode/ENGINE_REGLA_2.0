CREATE OR REPLACE PROCEDURE USPS_R_FITTED_ODR
(
    v_MODEL_ID NUMBER,
    v_MODEL_SEQ    number,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
    v_Table_Name VARCHAR2(30);
    v_Dependent_Var_Type VARCHAR2(30);
BEGIN

    SELECT STATUS, DEPENDENT_VAR_TYPE
    INTO v_status, v_Dependent_Var_Type
    FROM IFRS_FL_MODEL_VAR
    WHERE PKID = v_MODEL_ID;

    IF v_status = 1 THEN
        v_Table_Name := 'R_FITTED_ODR';
    ELSE
        v_Table_Name := 'R_FITTED_ODR_PEN';
    END IF;

    OPEN Cur_out FOR
    'SELECT ''YEAR '' || FL_YEAR "FL Year",
        ROUND(FITTED_ODR * 100,6) ' || CASE WHEN v_Dependent_Var_Type = 'PD' THEN '"Fitted ODR (%)"' ELSE '"Fitted LGD (%)"' END || '
    FROM ' || v_Table_Name || '
    WHERE MODEL_ID = ' || TO_CHAR(v_MODEL_ID)  || '
    AND MODEL_SEQ = ' || TO_CHAR(v_MODEL_SEQ) || '
    ORDER BY FL_YEAR';
END;