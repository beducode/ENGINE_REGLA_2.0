CREATE OR REPLACE PROCEDURE USPS_TFMODULEJOURNALIMPAIR
(
    V_DDATE_MAID VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_DATE DATE;
    v_MASTERID NUMBER(18);
BEGIN

    v_DATE := TO_DATE(SUBSTR(V_DDATE_MAID, 1, 10), 'yyyy/MM/dd');
    v_MASTERID := TO_NUMBER(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11));

    OPEN Cur_out FOR
        SELECT DOWNLOAD_DATE AS "Download Date",
           ACCOUNT_NUMBER AS "Account Number",
           GL_ACCOUNT AS "GL Account",
           JOURNAL_TYPE AS "Journal Type",
           JOURNAL_DESC AS "Journal Description",
           CURRENCY AS "Currency",
           TXN_TYPE AS "DB/CR Flag",
           AMOUNT AS "Original Amount",
           REVERSAL_FLAG AS "Reversal Flag"
        FROM IFRS_IMP_JOURNAL_DATA
        WHERE MASTERID = v_MASTERID
            AND DOWNLOAD_DATE <= v_DATE
        ORDER BY  DOWNLOAD_DATE DESC,
            REVERSAL_FLAG DESC,
            AMOUNT DESC,
            CURRENCY ASC,
            JOURNAL_TYPE DESC,
            TXN_TYPE DESC;

END;