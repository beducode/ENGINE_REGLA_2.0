CREATE OR REPLACE PROCEDURE  USPS_R_OLS_ASSUMPTIONS (
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
        SELECT A.TEST_NAME AS "TestName",
        A.TEST_DESCR    AS "TestDescription",
        ROUND(A.STATS_VALUE,6)    AS "StatisticalValue",
        ROUND(A.P_VALUE,6)    AS "PValue",
        A.NULL_HYPOTHESIS AS "NullHypothesis",
        A.ALTER_HYPOTHESIS    AS "AlterHypothesis",
        A.FINAL_RESULT    AS "TestResult"
        FROM R_LINREG_STATS A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID
        AND A.MODEL_ID = v_MODEL_ID
        AND A.MODEL_SEQ = v_MODEL_SEQ;
    ELSE
        OPEN Cur_out FOR
        SELECT A.TEST_NAME AS "TestName",
        A.TEST_DESCR    AS "TestDescription",
        ROUND(A.STATS_VALUE,6)    AS "StatisticalValue",
        ROUND(A.P_VALUE,6)    AS "PValue",
        A.NULL_HYPOTHESIS AS "NullHypothesis",
        A.ALTER_HYPOTHESIS    AS "AlterHypothesis",
        A.FINAL_RESULT    AS "TestResult"
        FROM R_LINREG_STATS_PEN A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID
        AND A.MODEL_ID = v_MODEL_ID
        AND A.MODEL_SEQ = v_MODEL_SEQ;
    END IF;
END;