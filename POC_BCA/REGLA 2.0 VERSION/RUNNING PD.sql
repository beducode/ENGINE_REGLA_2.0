SET SERVEROUTPUT ON;

DECLARE
    V_RUNID         VARCHAR2(50) := 'S_0101_1111';
    V_DOWNLOAD_DATE DATE         := NULL;
    V_SYSCODE       VARCHAR2(10) := '0';
    V_PRC           VARCHAR2(10) := 'S';
BEGIN
    DBMS_OUTPUT.PUT_LINE('START PROCESS : ' || V_RUNID);

    ----------------------------------------------------------------
    -- 1
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_GENERATE_RULE_SEGMENT_DEV');
    IFRS9_BCA.SP_IFRS_GENERATE_RULE_SEGMENT_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 2
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_RULE_DATA_PD_DEV');
    IFRS9_BCA.SP_IFRS_RULE_DATA_PD_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 2.1
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_INSERT_GTMP_FROM_IMA_M_DEV');
    IFRS9_BCA.SP_IFRS_INSERT_GTMP_FROM_IMA_M_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 3
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_TM_SCENARIO_DATA_DEV');
    IFRS9_BCA.SP_IFRS_PD_TM_SCENARIO_DATA_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 4
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_ENR_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_ENR_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 5
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_FLOWRATE_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_FLOWRATE_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 6
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_FLOW_TO_LOSS_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_FLOW_TO_LOSS_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 7
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_TTC_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_TTC_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 8
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_ODR_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_ODR_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 9
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIG_LOGIT_ODR_DEV');
    IFRS9_BCA.SP_IFRS_PD_MIG_LOGIT_ODR_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 10
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_RULE_DATA_SEGMENT_DEV');
    IFRS9_BCA.SP_IFRS_RULE_DATA_SEGMENT_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 11
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 12
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_CORRELATION_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_CORRELATION_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 13
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_Z_SCORE_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_Z_SCORE_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 14
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_PIT_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_PIT_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 15
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_CUMULATIVE_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_CUMULATIVE_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 16
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_VAS_MARGINAL_DEV');
    IFRS9_BCA.SP_IFRS_PD_VAS_MARGINAL_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    DBMS_OUTPUT.PUT_LINE('ALL PROCESS COMPLETED SUCCESSFULLY');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END;