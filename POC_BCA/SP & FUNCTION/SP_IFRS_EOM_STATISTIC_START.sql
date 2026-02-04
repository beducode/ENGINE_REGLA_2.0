CREATE OR REPLACE PROCEDURE SP_IFRS_EOM_STATISTIC_START (p_msg OUT VARCHAR2)
AS
    v_last_day   DATE := LAST_DAY (TRUNC (SYSDATE));
BEGIN
    -- 1. INSERT INTO ARCHIVE TABLE
    INSERT INTO ACTUAL_JOB_STATUS_ACV
        (SELECT SEQ_ACTUAL_JOB_STATUS_ACV.NEXTVAL,
                NO,
                ACTIVITY_NAME,
                PIC,
                P_OR_S,
                DATE_PERIODE,
                DATE_PROCESS,
                START_TIME_NORMAL,
                END_TIME_NORMAL,
                DURATION_NORMAL,
                START_TIME_ACT,
                END_TIME_ACT,
                DURATION_ACT,
                INFORMATION,
                IS_DONE,
                SET_LABA_RUGI_SEMENTARA,
                "ORDER",
                IS_VIEW,
                NEXT_ACTIVITY
           FROM ACTUAL_JOB_STATUS);

    COMMIT;

    -- 2. UPDATE DATE INTO THIS MONTH AND SET IS_DONE TO FALSE
    UPDATE ACTUAL_JOB_STATUS
       SET DATE_PERIODE = v_last_day,
           DATE_PROCESS =
               CASE
                   WHEN ACTIVITY_NAME = 'KURS 17.00 (H)' THEN v_last_day
                   ELSE (v_last_day + INTERVAL '1' DAY)
               END,
           START_TIME_NORMAL =
               TO_TIMESTAMP (
                      TO_CHAR (
                          CASE
                              WHEN ACTIVITY_NAME = 'KURS 17.00 (H)'
                              THEN
                                  v_last_day
                              ELSE
                                  (v_last_day + INTERVAL '1' DAY)
                          END,
                          'yyyyMMdd')
                   || ' '
                   || TO_CHAR (START_TIME_NORMAL, 'HH24Miss'),
                   'yyyyMMdd HH24Miss'),
           END_TIME_NORMAL =
               TO_TIMESTAMP (
                      TO_CHAR (
                          CASE
                              WHEN ACTIVITY_NAME = 'KURS 17.00 (H)'
                              THEN
                                  v_last_day
                              ELSE
                                  (v_last_day + INTERVAL '1' DAY)
                          END,
                          'yyyyMMdd')
                   || ' '
                   || TO_CHAR (END_TIME_NORMAL, 'HH24Miss'),
                   'yyyyMMdd HH24Miss'),
           IS_DONE = 0,
           INFORMATION = ' '
     WHERE 1 = 1;

    COMMIT;

    -- 3. COPY TIMESTAMP NORMAL INTO TIMESTAMP ACT
    UPDATE ACTUAL_JOB_STATUS
       SET START_TIME_ACT = START_TIME_NORMAL,
           END_TIME_ACT = END_TIME_NORMAL,
           DURATION_ACT = DURATION_NORMAL
     WHERE 1 = 1;

    COMMIT;

    p_msg := 'Success!';
    DBMS_OUTPUT.PUT_LINE (p_msg);
EXCEPTION
    WHEN OTHERS
    THEN
        ROLLBACK;
        p_msg := 'Error! There is a problem in SP_IFRS_EOM_STATISTIC_START.';
        DBMS_OUTPUT.PUT_LINE (p_msg);
        DBMS_OUTPUT.PUT_LINE (SQLERRM);
END;