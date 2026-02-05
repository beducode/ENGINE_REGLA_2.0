CREATE OR REPLACE PROCEDURE SP_IFRS_GL_OUTBOUND_ALL_R
IS
    V_EFFDT   VARCHAR2 (5 BYTE);
    V_COUNT   NUMBER;
BEGIN
    SELECT CURRDATE - TO_DATE ('1900-01-01', 'YYYY-MM-DD') + 1
      INTO V_EFFDT
      FROM IFRS_PRC_DATE;

    SELECT COUNT (1)
      INTO V_COUNT
      FROM IFRS_GL_OUTBOUND_ALL
     WHERE EFFDT = V_EFFDT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GL_OUTBOUND_ALL_R';

    IF (V_COUNT > 0)
    THEN
        INSERT /*+ append */
               INTO IFRS_GL_OUTBOUND_ALL_R
            (SELECT VLJ,
                    CORP,
                    CBG,
                    JRNL_ID,
                    EFFDT,
                    JRNL_BRANCH,
                    COA,
                    SEQ,
                    FILLER,
                    CCY,
                    SLID,
                    SLAC,
                    SOURCES,
                    DESCRIPTION,
                    CY,
                    CP,
                    (CASE
                         WHEN DC = 'C ' THEN 'D '
                         WHEN DC = 'D ' THEN 'C '
                     END)    DC,
                    (CASE
                         WHEN NVL (RP_SIGN, ' ') = ' ' THEN '-'
                         WHEN NVL (RP_SIGN, ' ') = '-' THEN ' '
                     END)    RP_SIGN,
                    AMT_1,
                    (CASE
                         WHEN NVL (VA_SIGN, ' ') = ' ' THEN '-'
                         WHEN NVL (VA_SIGN, ' ') = '-' THEN ' '
                     END)    VA_SIGN,
                    AMT_3
              FROM IFRS_GL_OUTBOUND_ALL)
            ORDER BY
                JRNL_ID,
                CBG,
                COA,
                SEQ,
                CCY,
                DC;

        COMMIT;
    END IF;
END;