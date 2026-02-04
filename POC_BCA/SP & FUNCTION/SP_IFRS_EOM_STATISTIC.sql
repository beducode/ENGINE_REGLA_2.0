CREATE OR REPLACE PROCEDURE SP_IFRS_EOM_STATISTIC
(p_activity_name IN VARCHAR2,
                                                  p_begin_start_time IN VARCHAR2,
                                                  p_information IN VARCHAR2,
                                                  p_msg OUT VARCHAR2)
AS
    v_curr_activity      VARCHAR2(500);
    v_next_activity      VARCHAR2(500);
    v_end_time           DATE := SYSDATE;
    v_time_diff          INTERVAL DAY TO SECOND;
    v_is_view            NUMBER;
    v_count_false_report NUMBER;
    v_is_done_report     NUMBER;
    v_curr_order         NUMBER;
    v_order_data         NUMBER;
BEGIN
    -- 1. SET VARIABLE & GET p_activity_name INTO v_curr_activity
    v_curr_activity := p_activity_name;

    -- 2. SET VARIABLE & GET NEXT_ACTIVITY INTO v_next_activity
    --    AND SET VARIABLE & GET IS_VIEW INTO v_is_view
    SELECT "ORDER", NEXT_ACTIVITY, IS_VIEW
    INTO v_curr_order, v_next_activity, v_is_view
    FROM ACTUAL_JOB_STATUS
    WHERE ACTIVITY_NAME = v_curr_activity;

    -- 3. SET VARIABLE & GET ORDER INTO v_source_data
    SELECT "ORDER"
    INTO v_order_data
    FROM ACTUAL_JOB_STATUS
    WHERE ACTIVITY_NAME LIKE '%' || p_begin_start_time || '%';

    -- 4. SET VARIABLE & GET DIFFERENCE INTO v_time_diff
    SELECT v_end_time - START_TIME_ACT
    INTO v_time_diff
    FROM ACTUAL_JOB_STATUS
    WHERE ACTIVITY_NAME = v_curr_activity;

    -- 5. UPDATE END_TIME_ACT, DATE_PROCESS, IS_DONE, AND INFORMATION
    UPDATE ACTUAL_JOB_STATUS
    SET END_TIME_ACT = v_end_time,
        DATE_PROCESS = TRUNC(SYSDATE),
        IS_DONE      = 1,
        INFORMATION  = p_information
    WHERE ACTIVITY_NAME = v_curr_activity;
    COMMIT;

    -- 6. IF SOURCE DATA THEN UPDATE START_TIME_ACTUAL
    --    ELSE UPDATE DURATION_ACT
    IF v_curr_order < v_order_data THEN
        UPDATE ACTUAL_JOB_STATUS
        SET START_TIME_ACT = END_TIME_ACT - NUMTODSINTERVAL(DURATION_ACT, 'MINUTE')
        WHERE ACTIVITY_NAME = v_curr_activity
          AND "ORDER" < v_order_data;
    ELSE
        UPDATE ACTUAL_JOB_STATUS
        SET DURATION_ACT = EXTRACT(DAY FROM v_time_diff) * 24 * 60 +
                           EXTRACT(HOUR FROM v_time_diff) * 60 +
                           EXTRACT(MINUTE FROM v_time_diff) +
                           CEIL(EXTRACT(SECOND FROM v_time_diff) / 60)
        WHERE ACTIVITY_NAME = v_curr_activity
          AND "ORDER" >= v_order_data;
    END IF;
    COMMIT;

    -- 7. IF IS_VIEW = 0, THEN UPDATE THE NEXT_ACTIVITY
    --    ELSE, UPDATE NEXT_ACTIVITY TO FALSE WHERE IS_VIEW = 0
    IF v_is_view = 0 THEN
        -- 7.1. SET VARIABLE & GET COUNT FALSE INTO v_count_false_report
        SELECT COUNT(*)
        INTO v_count_false_report
        FROM ACTUAL_JOB_STATUS
        WHERE NEXT_ACTIVITY = v_next_activity
          AND IS_VIEW = 0
          AND IS_DONE = 0;

        -- 7.2. UPDATE REPORTING IF ALL REPORT IS DONE
        IF v_count_false_report = 0 THEN
            UPDATE ACTUAL_JOB_STATUS
            SET END_TIME_ACT = v_end_time,
                DURATION_ACT = EXTRACT(DAY FROM v_time_diff) * 24 * 60 +
                               EXTRACT(HOUR FROM v_time_diff) * 60 +
                               EXTRACT(MINUTE FROM v_time_diff) +
                               CEIL(EXTRACT(SECOND FROM v_time_diff) / 60),
                DATE_PROCESS = TRUNC(SYSDATE),
                IS_DONE      = 1,
                INFORMATION  = p_information
            WHERE ACTIVITY_NAME = v_next_activity;
        END IF;

        -- 7.3. SET VARIABLE AND GET v_next_activity INTO v_curr_activity
        v_curr_activity := v_next_activity;
    ELSE
        UPDATE ACTUAL_JOB_STATUS
        SET START_TIME_ACT = v_end_time,
            END_TIME_ACT   = v_end_time + NUMTODSINTERVAL(DURATION_NORMAL, 'MINUTE'),
            DURATION_ACT   = DURATION_NORMAL,
            IS_DONE        = 0,
            INFORMATION    = ' '
        WHERE NEXT_ACTIVITY = v_next_activity
          AND IS_VIEW = 0;
    END IF;
    COMMIT;

    -- 8. UPDATE ALL PROCESS_DATE INTO MAX(PROCESS_DATE) WHERE IS_DONE IS FALSE
    UPDATE ACTUAL_JOB_STATUS
    SET DATE_PROCESS = (SELECT MAX(DATE_PROCESS) FROM ACTUAL_JOB_STATUS)
    WHERE IS_DONE = 0;
    COMMIT;

    -- 9. LOOP SET TIME UNTIL END REGLA
    LOOP
        -- 9.1 SET VARIABLE & GET THE SUCCESSOR INTO v_curr_activity
        SELECT DISTINCT NEXT_ACTIVITY
        INTO v_curr_activity
        FROM ACTUAL_JOB_STATUS
        WHERE ACTIVITY_NAME = v_curr_activity;

        -- 9.2 EXIT WHEN v_curr_activity IS END REGLA
        EXIT WHEN v_curr_activity = 'END REGLA';

        -- 9.3 GET THE LAST END_TIME_ACT
        SELECT MAX(END_TIME_ACT)
        INTO v_end_time
        FROM ACTUAL_JOB_STATUS
        WHERE NEXT_ACTIVITY IN v_curr_activity
          AND IS_VIEW = 1;

        -- 9.4 UPDATE TIME
        UPDATE ACTUAL_JOB_STATUS
        SET START_TIME_ACT = v_end_time,
            END_TIME_ACT   = v_end_time + NUMTODSINTERVAL(DURATION_NORMAL, 'MINUTE'),
            DURATION_ACT   = DURATION_NORMAL,
            IS_DONE        = 0,
            INFORMATION    = ' '
        WHERE ACTIVITY_NAME = v_curr_activity;
        COMMIT;
    END LOOP;

    -- 10. IF CURRENT ACTIVITY 'END REGLA' THEN UPDATE ACTIVITY 'END REGLA'
    IF v_curr_activity = 'END REGLA' THEN
        -- 10.1 SET VARIABLE & GET THE IS_DONE INTO v_is_done_report WHERE IS_DONE = 0
        SELECT COUNT(*)
        INTO v_is_done_report
        FROM ACTUAL_JOB_STATUS
        WHERE NEXT_ACTIVITY = v_curr_activity
          AND IS_DONE = 0;

        -- 10.2. UPDATE START_TIME_ACT AND END_TIME_ACT
        UPDATE ACTUAL_JOB_STATUS
        SET START_TIME_ACT = (SELECT MIN(START_TIME_ACT)
                              FROM ACTUAL_JOB_STATUS
                              WHERE ACTIVITY_NAME <> v_curr_activity
                                AND IS_VIEW = 1),
            END_TIME_ACT   = (SELECT MAX(END_TIME_ACT)
                              FROM ACTUAL_JOB_STATUS
                              WHERE ACTIVITY_NAME <> v_curr_activity
                                AND IS_VIEW = 1)
        WHERE ACTIVITY_NAME = v_curr_activity;
        COMMIT;

        -- 10.3. SET VARIABLE & GET DIFFERENCE INTO v_time_diff
        SELECT END_TIME_ACT - START_TIME_ACT
        INTO v_time_diff
        FROM ACTUAL_JOB_STATUS
        WHERE ACTIVITY_NAME = v_curr_activity;

        -- 10.4. UPDATE END_TIME_ACT, DURATION_ACT, AND IS_DONE
        UPDATE ACTUAL_JOB_STATUS
        SET DURATION_ACT = EXTRACT(DAY FROM v_time_diff) * 24 * 60 +
                           EXTRACT(HOUR FROM v_time_diff) * 60 +
                           EXTRACT(MINUTE FROM v_time_diff) +
                           CEIL(EXTRACT(SECOND FROM v_time_diff) / 60)
        WHERE ACTIVITY_NAME = v_curr_activity;
        COMMIT;

        -- 10.5. IF v_is_done_report = 0, THEN UPDATE IS_DONE TO 1
        --      ELSE, UPDATE UPDATE IS_DONE TO 0
        IF v_is_done_report = 0 THEN
            UPDATE ACTUAL_JOB_STATUS
            SET DATE_PROCESS = TRUNC(SYSDATE),
                IS_DONE      = 1,
                INFORMATION  = p_information
            WHERE ACTIVITY_NAME = v_curr_activity;
        ELSE
            UPDATE ACTUAL_JOB_STATUS
            SET IS_DONE     = 0,
                INFORMATION = ' '
            WHERE ACTIVITY_NAME = v_curr_activity;
        END IF;
        COMMIT;
    END IF;

    p_msg := 'Success!';
    DBMS_OUTPUT.PUT_LINE(p_msg);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        p_msg := 'Error! There is a problem in SP_IFRS_EOM_STATISTIC.';
        DBMS_OUTPUT.PUT_LINE(p_msg);
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
END ;