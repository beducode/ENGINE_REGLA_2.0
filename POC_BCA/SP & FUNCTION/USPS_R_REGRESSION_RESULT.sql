CREATE OR REPLACE PROCEDURE USPS_R_REGRESSION_RESULT
(
    v_Model_ID NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
BEGIN

    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR
    WHERE PKID = v_Model_ID;

    IF v_status = 1 THEN
        OPEN Cur_out FOR
        SELECT DISTINCT A.MODEL_SEQ ModelSequence,
            ROUND(A.MULT_R_SQUARED, 6) MultiRSquared,
            ROUND(A.ADJ_R_SQUARED, 6) AdjustedRSquared,
            ROUND(A.P_VALUE, 6) PValue,
            ROUND(A.RESIDUAL_MEDIAN, 6) ResidualNormalPValue,
            B.MODEL_FORMULA "RegressionModel",
            B.PASSED_FLAG "PassedValue",
            ROUND(E.FITTED_ODR * 100,6) "FittedODR",
            F.SIG_FLAG "SignificantFlag",
            G.FINAL_RESULT "TestResult",
            NVL(C.SEQ_SELECTED,0) SEQ_SELECTED,
            D.MSE_PD_RATE
        FROM R_MULT_LINEAR_REGR_RESULT A
        JOIN R_ME_MODEL_SCENARIO B
        ON A.MODEL_ID = B.MODEL_ID
        AND A.MODEL_ID = v_Model_ID
        AND A.MODEL_SEQ = B.MODEL_SEQ
        JOIN R_BACKTEST_IN_SAMPLE D
        ON A.MODEL_ID = D.MODEL_ID
        AND A.MODEL_SEQ = D.MODEL_SEQ
        LEFT JOIN (SELECT PKID AS MODEL_ID, SELECTED_MODEL_SEQ AS MODEL_SEQ, DECODE(SELECTED_MODEL_SEQ, 0,0,1) AS SEQ_SELECTED  FROM IFRS_FL_MODEL_VAR) C
        ON A.MODEL_ID = C.MODEL_ID
        AND A.MODEL_SEQ = C.MODEL_SEQ
        LEFT JOIN R_FITTED_ODR E
        ON A.MODEL_ID =  E.MODEL_ID
        AND A.MODEL_SEQ = E.MODEL_SEQ
        AND E.FL_YEAR = 1
        LEFT JOIN
        (
            SELECT MODEL_ID, MODEL_SEQ, COUNT(SIG_FLAG) SIG_FLAG
            FROM R_MULT_LINEAR_REGR_COEF
            WHERE SIG_FLAG = 'Y'
            GROUP BY MODEL_ID, MODEL_SEQ
        ) F
        ON A.MODEL_ID = F.MODEL_ID
        AND A.MODEL_SEQ = F.MODEL_SEQ
        LEFT JOIN
        (
            SELECT MODEL_ID, MODEL_SEQ, COUNT(*) FINAL_RESULT
            FROM
            (
                SELECT MODEL_ID, MODEL_SEQ
                FROM R_LINREG_STATS
                WHERE FINAL_RESULT = 'Failed to Reject Null Hypothesis'
                UNION ALL
                SELECT A3.MODEL_ID, A3.MODEL_SEQ
                FROM
                (
                    SELECT MODEL_ID, MODEL_SEQ, COUNT(*) AS MEV_COUNT
                    FROM R_LINREG_VIF_DTL
                    GROUP BY MODEL_ID, MODEL_SEQ
                ) A3
                JOIN
                (
                    SELECT MODEL_ID, MODEL_SEQ, COUNT(*) AS MEV_COUNT
                    FROM R_LINREG_VIF_DTL
                    WHERE VIFVALUE <= 14
                    GROUP BY MODEL_ID, MODEL_SEQ
                ) B3
                ON A3.MODEL_ID = B3.MODEL_ID
                AND A3.MODEL_SEQ = B3.MODEL_SEQ
                AND A3.MEV_COUNT = B3.MEV_COUNT
            ) A2
            GROUP BY MODEL_ID, MODEL_SEQ
        ) G
        ON A.MODEL_ID = G.MODEL_ID
        AND A.MODEL_SEQ = G.MODEL_SEQ
        ORDER BY NVL(C.SEQ_SELECTED,0) DESC, B.PASSED_FLAG DESC, D.MSE_PD_RATE;
    ELSE
        OPEN Cur_out FOR
        SELECT DISTINCT A.MODEL_SEQ ModelSequence,
            ROUND(A.MULT_R_SQUARED, 6) MultiRSquared,
            ROUND(A.ADJ_R_SQUARED, 6) AdjustedRSquared,
            ROUND(A.P_VALUE, 6) PValue,
            ROUND(A.RESIDUAL_MEDIAN, 6) ResidualNormalPValue,
            B.MODEL_FORMULA "RegressionModel",
            B.PASSED_FLAG "PassedValue",
            ROUND(E.FITTED_ODR * 100,6) "FittedODR",
            F.SIG_FLAG "SignificantFlag",
            G.FINAL_RESULT "TestResult",
            NVL(C.SEQ_SELECTED,0) SEQ_SELECTED,
            D.MSE_PD_RATE
        FROM R_MULT_LINEAR_REGR_RESULT_PEN A
        JOIN R_ME_MODEL_SCENARIO_PEN B
        ON A.MODEL_ID = B.MODEL_ID
        AND A.MODEL_ID = v_Model_ID
        AND A.MODEL_SEQ = B.MODEL_SEQ
        JOIN R_BACKTEST_IN_SAMPLE_PEN D
        ON A.MODEL_ID = D.MODEL_ID
        AND A.MODEL_SEQ = D.MODEL_SEQ
        LEFT JOIN (SELECT PKID AS MODEL_ID, SELECTED_MODEL_SEQ AS MODEL_SEQ, DECODE(SELECTED_MODEL_SEQ, 0,0,1) AS SEQ_SELECTED  FROM IFRS_FL_MODEL_VAR) C
        ON A.MODEL_ID = C.MODEL_ID
        AND A.MODEL_SEQ = C.MODEL_SEQ
        LEFT JOIN R_FITTED_ODR_PEN E
        ON A.MODEL_ID =  E.MODEL_ID
        AND A.MODEL_SEQ = E.MODEL_SEQ
        AND E.FL_YEAR = 1
        LEFT JOIN
        (
            SELECT MODEL_ID, MODEL_SEQ, COUNT(SIG_FLAG) SIG_FLAG
            FROM R_MULT_LINEAR_REGR_COEF_PEN
            WHERE SIG_FLAG = 'Y'
            GROUP BY MODEL_ID, MODEL_SEQ
        ) F
        ON A.MODEL_ID = F.MODEL_ID
        AND A.MODEL_SEQ = F.MODEL_SEQ
        LEFT JOIN
        (
            SELECT MODEL_ID, MODEL_SEQ, COUNT(*) FINAL_RESULT
            FROM
            (
                SELECT MODEL_ID, MODEL_SEQ
                FROM R_LINREG_STATS_PEN
                WHERE FINAL_RESULT = 'Failed to Reject Null Hypothesis'
                UNION ALL
                SELECT A3.MODEL_ID, A3.MODEL_SEQ
                FROM
                (
                    SELECT MODEL_ID, MODEL_SEQ, COUNT(*) AS MEV_COUNT
                    FROM R_LINREG_VIF_DTL_PEN
                    GROUP BY MODEL_ID, MODEL_SEQ
                ) A3
                JOIN
                (
                    SELECT MODEL_ID, MODEL_SEQ, COUNT(*) AS MEV_COUNT
                    FROM R_LINREG_VIF_DTL_PEN
                    WHERE VIFVALUE <= 14
                    GROUP BY MODEL_ID, MODEL_SEQ
                ) B3
                ON A3.MODEL_ID = B3.MODEL_ID
                AND A3.MODEL_SEQ = B3.MODEL_SEQ
                AND A3.MEV_COUNT = B3.MEV_COUNT
            ) A2
            GROUP BY MODEL_ID, MODEL_SEQ
        ) G
        ON A.MODEL_ID = G.MODEL_ID
        AND A.MODEL_SEQ = G.MODEL_SEQ
        ORDER BY NVL(C.SEQ_SELECTED,0) DESC, B.PASSED_FLAG DESC, D.MSE_PD_RATE;

    END IF;

END;