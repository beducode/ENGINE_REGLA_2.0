CREATE OR REPLACE PROCEDURE  USPS_R_IN_SAMPLE_D1 (
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
        ROUND(A.COR_PD_LOG,6) CorrelationLog,
        ROUND(A.COR_PD_RATE,6) Correlation,
        ROUND(A.MSE_PD_LOG,6) MSELog,
        ROUND(A.MSE_PD_RATE,6) MSE,
        ROUND(A.PDLOG_MAPE,6) MAPELog,
        ROUND(A.PDRATE_MAPE,6) MAPE
        FROM R_BACKTEST_IN_SAMPLE A
        JOIN IFRS_FL_MODEL_VAR B
            ON A.MODEL_ID = v_MODEL_ID
            AND A.MODEL_ID = B.PKID
            AND A.MODEL_SEQ = v_MODEL_SEQ;
    ELSE
        OPEN Cur_out FOR
        SELECT DISTINCT B.FL_MODEL_NAME FLModelName,
        A.MODEL_SEQ ModelSeq,
        ROUND(A.COR_PD_LOG,6) CorrelationLog,
        ROUND(A.COR_PD_RATE,6) Correlation,
        ROUND(A.MSE_PD_LOG,6) MSELog,
        ROUND(A.MSE_PD_RATE,6) MSE,
        ROUND(A.PDLOG_MAPE,6) MAPELog,
        ROUND(A.PDRATE_MAPE,6) MAPE
        FROM R_BACKTEST_IN_SAMPLE_PEN A
        JOIN IFRS_FL_MODEL_VAR B
            ON A.MODEL_ID = v_MODEL_ID
            AND A.MODEL_ID = B.PKID
            AND A.MODEL_SEQ = v_MODEL_SEQ;
    END IF;
END;