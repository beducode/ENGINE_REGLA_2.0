DECLARE
    V_RUNID         VARCHAR2(50) := 'S_0101_1111'
BEGIN
    DBMS_OUTPUT.PUT_LINE('START PROCESS : ' || V_RUNID);

    ----------------------------------------------------------------
    -- 1
    ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('RUNNING: SP_IFRS_BUCKET_DETAIL');
    PSAK413.SP_IFRS_BUCKET_DETAIL();


    DBMS_OUTPUT.PUT_LINE('ALL PROCESS COMPLETED SUCCESSFULLY');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END;