CREATE OR REPLACE PROCEDURE  USPS_R_REGRESSION_RESULT_DTL
(
    v_MODEL_ID    number,
    v_MODEL_SEQ    number,
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
        SELECT D.FL_MODEL_NAME AS "FLModelFL",
            A.MODEL_SEQ AS "ModelSequence",
            E.DESCRIPTION AS "RegressionType",
            C.MODEL_FORMULA AS "RegressionModel",
            A.ADJ_R_SQUARED AS "AdjustedRSquare",
            (DF_TOTAL + 1) AS "Observations",
            D.SIGNIFICANT_P_VALUE AS "SignificantAlpha",
            A.DF_REGR AS "DFRegression",
            A.DF_RESD AS "DFResidual",
            A.DF_TOTAL AS "DFTotal",
            ROUND(A.SUM_OF_SQR_REGR,6) AS "SumRegression",
            ROUND(A.SUM_OF_SQR_RESD,6) AS "SumResidual",
            ROUND(A.SUM_OF_SQR_TOTAL,6) AS "SumTotal",
            ROUND(A.MEAN_SUM_OF_SQR_REGR,6) AS "MeanRegression",
            ROUND(A.MEAN_SUM_OF_SQR_RESD,6) AS "MeanResidual",
            ROUND(A.F_STATISTIC,6) AS "FStatistic",
            ROUND(A.P_VALUE,6) AS "PValue"
        FROM R_MULT_LINEAR_REGR_RESULT A
        JOIN R_ME_MODEL_SCENARIO C
        ON A.MODEL_ID = C.MODEL_ID and A.MODEL_SEQ = C.MODEL_SEQ AND A.MODEL_ID = v_MODEL_ID AND A.MODEL_SEQ = v_MODEL_SEQ
        JOIN IFRS_FL_MODEL_VAR D
        ON A.MODEL_ID = D.PKID
        JOIN TBLM_COMMONCODEDETAIL E
        ON D.REGRESSION_TYPE = E.VALUE1 AND COMMONCODE = 'B139';
    ELSE
        OPEN Cur_out FOR
        SELECT D.FL_MODEL_NAME AS "FLModelFL",
            A.MODEL_SEQ AS "ModelSequence",
            E.DESCRIPTION AS "RegressionType",
            C.MODEL_FORMULA AS "RegressionModel",
            A.ADJ_R_SQUARED AS "AdjustedRSquare",
            (DF_TOTAL + 1) AS "Observations",
            D.SIGNIFICANT_P_VALUE AS "SignificantAlpha",
            A.DF_REGR AS "DFRegression",
            A.DF_RESD AS "DFResidual",
            A.DF_TOTAL AS "DFTotal",
            ROUND(A.SUM_OF_SQR_REGR,6) AS "SumRegression",
            ROUND(A.SUM_OF_SQR_RESD,6) AS "SumResidual",
            ROUND(A.SUM_OF_SQR_TOTAL,6) AS "SumTotal",
            ROUND(A.MEAN_SUM_OF_SQR_REGR,6) AS "MeanRegression",
            ROUND(A.MEAN_SUM_OF_SQR_RESD,6) AS "MeanResidual",
            ROUND(A.F_STATISTIC,6) AS "FStatistic",
            ROUND(A.P_VALUE,6) AS "PValue"
        FROM R_MULT_LINEAR_REGR_RESULT_PEN A
        JOIN R_ME_MODEL_SCENARIO_PEN C
        ON A.MODEL_ID = C.MODEL_ID and A.MODEL_SEQ = C.MODEL_SEQ AND A.MODEL_ID = v_MODEL_ID AND A.MODEL_SEQ = v_MODEL_SEQ
        JOIN IFRS_FL_MODEL_VAR D
        ON A.MODEL_ID = D.PKID
        JOIN TBLM_COMMONCODEDETAIL E
        ON D.REGRESSION_TYPE = E.VALUE1 AND COMMONCODE = 'B139';
    END IF;
END;