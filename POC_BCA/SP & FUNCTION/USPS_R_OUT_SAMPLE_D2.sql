CREATE OR REPLACE PROCEDURE  USPS_R_OUT_SAMPLE_D2 (
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
        SELECT DISTINCT B.FL_MODEL_NAME FLModelName,
        A.MODEL_SEQ ModelSeq,
        A.PERIOD Period,
        ROUND(A.ACTUAL_PD_LOG,6) ObservedLogValue,
        ROUND(A.FITTED_PD_LOG,6) FittedLogValue,
        ROUND(A.PDLOG_SQRD_ERR,6) SquaredLogDiff,
        ROUND(A.ACTUAL_PD_RATE,6) ObservedValue,
        ROUND(A.FITTED_PD_RATE,6) FittedValue,
        ROUND(A.PDRATE_SQRD_ERR,6) SquaredDiff,
        ROUND(A.PDLOG_MAPE,6) AbsoluteLogDiff,
        ROUND(A.PDRATE_MAPE,6) AbsoluteDiff
        FROM R_BACKTEST_OUT_SAMPLE_DTL A
        JOIN IFRS_FL_MODEL_VAR B
            ON A.MODEL_ID = v_MODEL_ID
            AND A.MODEL_ID = B.PKID
            AND A.MODEL_SEQ = v_MODEL_SEQ;
    ELSE
        OPEN Cur_out FOR
        SELECT DISTINCT B.FL_MODEL_NAME FLModelName,
        A.MODEL_SEQ ModelSeq,
        A.PERIOD Period,
        ROUND(A.ACTUAL_PD_LOG,6) ObservedLogValue,
        ROUND(A.FITTED_PD_LOG,6) FittedLogValue,
        ROUND(A.PDLOG_SQRD_ERR,6) SquaredLogDiff,
        ROUND(A.ACTUAL_PD_RATE,6) ObservedValue,
        ROUND(A.FITTED_PD_RATE,6) FittedValue,
        ROUND(A.PDRATE_SQRD_ERR,6) SquaredDiff,
        ROUND(A.PDLOG_MAPE,6) AbsoluteLogDiff,
        ROUND(A.PDRATE_MAPE,6) AbsoluteDiff
        FROM R_BACKTEST_OUT_SAMPLE_DTL_PEN A
        JOIN IFRS_FL_MODEL_VAR B
            ON A.MODEL_ID = v_MODEL_ID
            AND A.MODEL_ID = B.PKID
            AND A.MODEL_SEQ = v_MODEL_SEQ;
    END IF;
END;