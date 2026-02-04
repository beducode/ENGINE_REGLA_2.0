CREATE OR REPLACE PROCEDURE USPS_R_ADF_TEST_DTL (
    v_MODEL_ID    NUMBER,
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
        SELECT MODEL_TYPE "ModelType",
            DIFFERENCING_LAG "DifferencingLag",
            ADF_STATISTIC "ADFStatistic",
            PVALUE "PValue",
            LAGORDER "LagOrder",
            NULLHYPOTHESIS "NullHypothesis",
            ALTHYPOTHESIS "AlterHypothesis",
            FINALRESULT "FinalResult"
        FROM R_ADF_TEST_DTL
        WHERE MODEL_ID = v_MODEL_ID
        ORDER BY PKID;
    ELSE
        OPEN Cur_out FOR
        SELECT MODEL_TYPE "ModelType",
            DIFFERENCING_LAG "DifferencingLag",
            ADF_STATISTIC "ADFStatistic",
            PVALUE "PValue",
            LAGORDER "LagOrder",
            NULLHYPOTHESIS "NullHypothesis",
            ALTHYPOTHESIS "AlterHypothesis",
            FINALRESULT "FinalResult"
        FROM R_ADF_TEST_DTL_PEN
        WHERE MODEL_ID = v_MODEL_ID
        ORDER BY PKID;
    END IF;
END;