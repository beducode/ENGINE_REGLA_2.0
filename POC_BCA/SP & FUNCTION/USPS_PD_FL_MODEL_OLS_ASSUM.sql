CREATE OR REPLACE PROCEDURE  USPS_PD_FL_MODEL_OLS_ASSUM (
    v_pd_rule_id  number,
    v_model_id    number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
	OPEN Cur_out FOR

    SELECT DISTINCT A.TEST_NAME AS "Test Name", A.TEST_DESCR	AS "Test Description",
    A.STATS_VALUE	AS "Statistical Value", A.P_VALUE	AS "P-Value",
    A.NULL_HYPOTHESIS	AS "Null Hypothesis", A.ALTER_HYPOTHESIS	AS "Alter Hypothesis",
    A.FINAL_RESULT	AS "Test Result"
	FROM R_LINREG_STATS_PEN A
	JOIN IFRS_FL_MODEL_VAR D
	ON A.MODEL_ID = D.PKID
    WHERE A.MODEL_ID = v_model_id
    AND D.DEPENDENT_VAR_VALUE = v_pd_rule_id
    AND A.MODEL_SEQ = 469;

END;