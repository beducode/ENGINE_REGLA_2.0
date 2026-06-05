DECLARE
    V_RUNID         VARCHAR2(50) := 'P_0101_1111';
    V_DOWNLOAD_DATE DATE         := NULL;
    V_SYSCODE       VARCHAR2(10) := NULL;
    V_PRC           VARCHAR2(10) := 'P';
BEGIN
    DBMS_OUTPUT.PUT_LINE('START PROCESS : ' || V_RUNID);

    -- ----------------------------------------------------------------
    -- 1
    -- ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_RULES_CONFIG_DEV');
    PSAK413.SP_IFRS_LGD_RULES_CONFIG_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    -- ----------------------------------------------------------------
    -- -- 2
    -- ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_WORKOUT_RULES_CONFIG_DEV');
    PSAK413.SP_IFRS_LGD_WORKOUT_RULES_CONFIG_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    -- ----------------------------------------------------------------
    -- -- 3
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_RULES_CONFIG_DEV');
     PSAK413.SP_IFRS_PD_RULES_CONFIG_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 4
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LIFETIME_RULES_CONFIG_DEV');
     PSAK413.SP_IFRS_LIFETIME_RULES_CONFIG_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 5
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_DEFAULT_RULE_DEV');
     PSAK413.SP_IFRS_DEFAULT_RULE_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 6
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_RULE_DATA_DEV');
     PSAK413.SP_IFRS_LGD_RULE_DATA_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 7
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_DATA_DEV');
     PSAK413.SP_IFRS_LGD_DATA_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 8
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_REC_DATA_DEV');
     PSAK413.SP_IFRS_LGD_REC_DATA_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 9
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_WORKOUT_DETAIL_DEV');
     PSAK413.SP_IFRS_LGD_WORKOUT_DETAIL_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    -- ----------------------------------------------------------------
    -- -- 10
    -- ----------------------------------------------------------------
     DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_WORKOUT_HEADER_DEV');
     PSAK413.SP_IFRS_LGD_WORKOUT_HEADER_DEV(
         P_RUNID         => V_RUNID,
         P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
         P_SYSCODE       => V_SYSCODE,
         P_PRC           => V_PRC
     );

    ----------------------------------------------------------------
    -- 11
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_UR_REC_DETAIL_DEV');
    PSAK413.SP_IFRS_LGD_UR_REC_DETAIL_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 12
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_UR_DETAIL_DEV');
    PSAK413.SP_IFRS_LGD_UR_DETAIL_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 13
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LGD_UR_HEADER_DEV');
    PSAK413.SP_IFRS_LGD_UR_HEADER_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 14
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_RULE_DATA_DEV');
    PSAK413.SP_IFRS_PD_RULE_DATA_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 15
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_TM_SCENARIO_DATA_DEV');
    PSAK413.SP_IFRS_PD_TM_SCENARIO_DATA_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 16
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MIGRATION_DETAIL_DEV');
    PSAK413.SP_IFRS_PD_MIGRATION_DETAIL_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 17
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_ENR_DEV');
    PSAK413.SP_IFRS_PD_MAA_ENR_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 18
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_ENR_SUM_DEV');
    PSAK413.SP_IFRS_PD_MAA_ENR_SUM_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 19
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_FLOWRATE_DEV');
    PSAK413.SP_IFRS_PD_MAA_FLOWRATE_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 20
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_FLOWRATE_SUM_DEV');
    PSAK413.SP_IFRS_PD_MAA_FLOWRATE_SUM_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 21
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_MMULT_MONTHLY_DEV');
    PSAK413.SP_IFRS_PD_MAA_MMULT_MONTHLY_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 22
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_ODR_DEV');
    PSAK413.SP_IFRS_PD_MAA_ODR_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 23
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_FLOWRATE_SMOOTH_DEV');
    PSAK413.SP_IFRS_PD_MAA_FLOWRATE_SMOOTH_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 24
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_MAA_MMULT_DEV');
    PSAK413.SP_IFRS_PD_MAA_MMULT_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 25
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_PD_TERM_STRUCTURE_DEV');
    PSAK413.SP_IFRS_PD_TERM_STRUCTURE_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 26
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LIFETIME_RULE_DATA_DEV');
    PSAK413.SP_IFRS_LIFETIME_RULE_DATA_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 27
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LIFETIME_DATA_DEV');
    PSAK413.SP_IFRS_LIFETIME_DATA_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 28
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LIFETIME_DETAIL_DEV');
    PSAK413.SP_IFRS_LIFETIME_DETAIL_DEV(
        P_RUNID         => V_RUNID,
        P_DOWNLOAD_DATE => V_DOWNLOAD_DATE,
        P_SYSCODE       => V_SYSCODE,
        P_PRC           => V_PRC
    );

    ----------------------------------------------------------------
    -- 29
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_LIFETIME_HEADER_DEV');
    PSAK413.SP_IFRS_LIFETIME_HEADER_DEV(
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