CREATE OR REPLACE PROCEDURE  USPS_R_MODEL_COEFF (
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
        SELECT A.COEFF_NAME CoefficientsName,
            ROUND(A.ESTIMATE_VAL,28) Coefficients,
            ROUND(A.STD_ERROR,28) StandardError,
            ROUND(A.T_VALUE,28) TStat,
            ROUND(A.PR_GT_T_VAL,28) PValue,
            ROUND(C.VIFVALUE,28) VIFValue,
            A.SIG_FLAG SignificantFlag
        FROM R_MULT_LINEAR_REGR_COEF A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID
        AND A.MODEL_SEQ = v_MODEL_SEQ
        AND A.MODEL_ID = v_MODEL_ID
        LEFT JOIN R_LINREG_VIF_DTL C
        ON A.MODEL_ID = C.MODEL_ID
        AND A.MODEL_SEQ = C.MODEL_SEQ
        AND A.COEFF_NAME = C.VARNAME
        ORDER BY A.COEFF_NAME;
    ELSE
        OPEN Cur_out FOR
        SELECT A.COEFF_NAME CoefficientsName,
            ROUND(A.ESTIMATE_VAL,28) Coefficients,
            ROUND(A.STD_ERROR,28) StandardError,
            ROUND(A.T_VALUE,28) TStat,
            ROUND(A.PR_GT_T_VAL,28) PValue,
            ROUND(C.VIFVALUE,28) VIFValue,
            A.SIG_FLAG SignificantFlag
        FROM R_MULT_LINEAR_REGR_COEF_PEN A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID
        AND A.MODEL_SEQ = v_MODEL_SEQ
        AND A.MODEL_ID = v_MODEL_ID
        LEFT JOIN R_LINREG_VIF_DTL_PEN C
        ON A.MODEL_ID = C.MODEL_ID
        AND A.MODEL_SEQ = C.MODEL_SEQ
        AND A.COEFF_NAME = C.VARNAME
        ORDER BY A.COEFF_NAME;
    END IF;

END;