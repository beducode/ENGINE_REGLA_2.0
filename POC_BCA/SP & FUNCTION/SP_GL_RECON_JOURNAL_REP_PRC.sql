CREATE OR REPLACE PROCEDURE     SP_GL_RECON_JOURNAL_REP_PRC(P_MID NUMBER)
IS

    CURSOR C_HEADER
    IS
    SELECT
        RPAD('STATUS', 14)||'|'||
        RPAD('PERIOD', 11)||'|'||
        RPAD('DATE', 11)||'|'||
        RPAD('JOURNAL NAME', 56)||'|'||
        RPAD('JOURNAL DESCRIPTION', 100)||'|'||
        RPAD('JOURNAL ID', 13)||'|'||
        RPAD('JOURNAL SEQUENCE', 16)||'|'||
        RPAD('BRANCH', 6)||'|'||
        RPAD('ACCOUNT', 7)||'|'||
        RPAD('BI', 2)||'|'||
        RPAD('PRODUCT', 7)||'|'||
        RPAD('RCC', 3)||'|'||
        RPAD('FUTURE 1', 8)||'|'||
        RPAD('FUTURE 2', 8)||'|'||
        RPAD('CURRENCY', 8)||'|'||
        LPAD('ORIGINAL CURRENCY DEBIT', 35)||'|'||
        LPAD('ORIGINAL CURRENCY CREDIT', 35)||'|'||
        LPAD('EQUIVALENT RUPIAH DEBIT', 35)||'|'||
        LPAD('EQUIVALENT RUPIAH CREDIT', 35)||'|'||
        RPAD('SLID', 50)||'|'||
        RPAD('SLAC', 50) AS HEADER_DATA
    FROM DUAL;


    CURSOR C_DETIL
    IS
    SELECT
        RPAD(GJLR.RECON_STATUS, 14)||'|'||
        RPAD(GJLR.PERIOD_NAME, 11)||'|'||
        RPAD(TO_CHAR(GJLR.EFFECTIVE_DATE, 'DD-MON-YYYY'), 11)||'|'||
        RPAD(NVL(GJLR.JE_NAME, ' '), 56)||'|'||
        RPAD(NVL(GJLR.DESCRIPTION, ' '), 100)||'|'||
        RPAD(NVL(GAC.SEGMENT1||GJLR.JE_SOURCE, ' '), 13)||'|'||
        RPAD(NVL(GJLR.JE_CATEGORY, ' '), 16)||'|'||
        RPAD(GAC.SEGMENT1, 6)||'|'||
        RPAD(GAC.SEGMENT2, 7)||'|'||
        RPAD(GAC.SEGMENT3, 2)||'|'||
        RPAD(GAC.SEGMENT4, 7)||'|'||
        RPAD(GAC.SEGMENT5, 3)||'|'||
        RPAD(GAC.SEGMENT6, 8)||'|'||
        RPAD(GAC.SEGMENT7, 8)||'|'||
        RPAD(GJLR.CURRENCY_CODE, 8)||'|'||
        LPAD(TO_CHAR(NVL(GJLR.ENTERED_DR,0), 'FM999,999,999,999,999,999,999,990.00', 'NLS_NUMERIC_CHARACTERS=".,"'), 35)||'|'||
        LPAD(TO_CHAR(NVL(GJLR.ENTERED_CR,0), 'FM999,999,999,999,999,999,999,990.00', 'NLS_NUMERIC_CHARACTERS=".,"'), 35)||'|'||
        LPAD(TO_CHAR(NVL(GJLR.ACCOUNTED_DR,0), 'FM999,999,999,999,999,999,999,990.00', 'NLS_NUMERIC_CHARACTERS=".,"'), 35)||'|'||
        LPAD(TO_CHAR(NVL(GJLR.ACCOUNTED_CR,0), 'FM999,999,999,999,999,999,999,990.00', 'NLS_NUMERIC_CHARACTERS=".,"'), 35)||'|'||
        RPAD(NVL(GJLR.SLID, ' '), 50)||'|'||
        RPAD(NVL(GJLR.SLAC, ' '), 50)||
        CHR(13)||CHR(10) AS DETIL
    FROM GL_JE_LINES_RECONCILE GJLR,
        GL_ACCOUNT_COMBINATIONS GAC
    WHERE 1=1
    AND GJLR.CCID = GAC.ID
    --and gjlr.period_name = '28-Nov-2019'
--    AND ROWNUM < 1000
    ;

    P_DIRECTORY VARCHAR2(150) :='GLM_DIR';
    P_FILENAME VARCHAR2(200) := 'gl_reconcile_journal'||TO_CHAR(SYSDATE, 'ddmmrrrr')||'.txt';
    V_FILE SYS.UTL_FILE.FILE_TYPE;
    L_PROCESS_ID NUMBER;
    L_MSG VARCHAR2(2000);
    X_ERRORMSG VARCHAR2(2000);
    CRLF VARCHAR2(10) := CHR(13)||CHR(13)||CHR(10);

BEGIN

    -- UPDATE MONITORING
    UPDATE PROCESS_MONITORING
    SET STATUS = 'Processing',
        LAST_UPDATED_BY = 'SYSTEM',
        LAST_UPDATED = SYSDATE
    WHERE ID = P_MID;

    COMMIT;

    L_PROCESS_ID :=  GL_INT_PROCESS_SEQ.NEXTVAL;
    --DBMS_OUTPUT.PUT_LINE(L_PROCESS_ID);

    V_FILE := SYS.UTL_FILE.FOPEN(P_DIRECTORY, P_FILENAME, 'wb');

    FOR C_HD
    IN C_HEADER
    LOOP
        SYS.UTL_FILE.PUT_RAW(V_FILE, SYS.UTL_RAW.CAST_TO_RAW(C_HD.HEADER_DATA));
        SYS.UTL_FILE.PUT_RAW(V_FILE, SYS.UTL_RAW.CAST_TO_RAW(CRLF));

        FOR C_DATA
        IN C_DETIL
        LOOP
            SYS.UTL_FILE.PUT_RAW(V_FILE, SYS.UTL_RAW.CAST_TO_RAW(C_DATA.DETIL));

        END LOOP;

    END LOOP;

    SYS.UTL_FILE.FCLOSE(V_FILE);

    -- UPDATE MONITORING
    UPDATE PROCESS_MONITORING
    SET STATUS = 'Success',
        FILENAME = P_FILENAME,
        LAST_UPDATED_BY = 'SYSTEM',
        LAST_UPDATED = SYSDATE
    WHERE ID = P_MID;

    COMMIT;


    L_MSG := 'End GL Account Combination Report.';

   --DBMS_OUTPUT.PUT_LINE(L_MSG);
    WRITE_LOG(P_PROCESS_ID      => L_PROCESS_ID,
              P_ERROR_CODE      => NULL,
              P_MSG             => L_MSG,
              P_PROCESS_DATE    => SYSDATE,
              P_ERROR_MSG       => X_ERRORMSG,
              P_ID_DATA         => P_MID);

EXCEPTION
WHEN OTHERS THEN

    L_MSG := SQLERRM;

    --DBMS_OUTPUT.PUT_LINE(L_MSG);
    WRITE_LOG(P_PROCESS_ID      => L_PROCESS_ID,
              P_ERROR_CODE      => NULL,
              P_MSG             => L_MSG,
              P_PROCESS_DATE    => SYSDATE,
              P_ERROR_MSG       => X_ERRORMSG,
              P_ID_DATA         => P_MID);

    UPDATE PROCESS_MONITORING
    SET STATUS = 'Error',
       LAST_UPDATED_BY = 'SYSTEM',
       LAST_UPDATED = SYSDATE,
       EXCEPTION = L_MSG
    WHERE ID = P_MID;

    COMMIT;

END;