CREATE OR REPLACE PROCEDURE  USPS_R_PD_LGD_TERM_H
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
        SELECT B.FL_MODEL_NAME AS "FLModelName",
            A.MODEL_SEQ AS "ModelSeq",
            PD.PD_RULE_NAME AS "DependantVariablePD",
            LGD.LGD_RULE_NAME AS "DependantVariableLGD",
            B.DEPENDENT_VAR_TYPE AS "DependantVariableType",
            B.IN_SAMPLE_START_DATE || ' to ' || B.IN_SAMPLE_END_DATE AS "InSamplePeriod",
            B.OUT_SAMPLE_START_DATE || ' to ' || B.OUT_SAMPLE_END_DATE AS "OutSamplePeriod",
            B.INTERCEPT_FLAG AS "IgnoreIntercept"
        FROM R_MULT_LINEAR_REGR_RESULT A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID AND B.PKID = v_MODEL_ID AND A.MODEL_SEQ = v_MODEL_SEQ
        LEFT JOIN IFRS_PD_RULES_CONFIG PD
            ON B.DEPENDENT_VAR_VALUE = PD.PKID
        LEFT JOIN IFRS_LGD_RULES_CONFIG LGD
            ON B.DEPENDENT_VAR_VALUE = LGD.PKID;
    ELSE
        OPEN Cur_out FOR
        SELECT B.FL_MODEL_NAME AS "FLModelName",
            A.MODEL_SEQ AS "ModelSeq",
            PD.PD_RULE_NAME AS "DependantVariablePD",
            LGD.LGD_RULE_NAME AS "DependantVariableLGD",
            B.DEPENDENT_VAR_TYPE AS "DependantVariableType",
            B.IN_SAMPLE_START_DATE || ' to ' || B.IN_SAMPLE_END_DATE AS "InSamplePeriod",
            B.OUT_SAMPLE_START_DATE || ' to ' || B.OUT_SAMPLE_END_DATE AS "OutSamplePeriod",
            B.INTERCEPT_FLAG AS "IgnoreIntercept"
        FROM R_MULT_LINEAR_REGR_RESULT_PEN A
        JOIN IFRS_FL_MODEL_VAR B
        ON A.MODEL_ID = B.PKID AND B.PKID = v_MODEL_ID AND A.MODEL_SEQ = v_MODEL_SEQ
        LEFT JOIN IFRS_PD_RULES_CONFIG PD
            ON B.DEPENDENT_VAR_VALUE = PD.PKID
        LEFT JOIN IFRS_LGD_RULES_CONFIG LGD
            ON B.DEPENDENT_VAR_VALUE = LGD.PKID;
    END IF;
END;