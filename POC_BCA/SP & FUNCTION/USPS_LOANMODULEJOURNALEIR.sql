CREATE OR REPLACE PROCEDURE USPS_LOANMODULEJOURNALEIR
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
            ACCTNO "Account Number",
            FLAG_CF "Flag CF",
            GLNO AS "GL Account",
            BRANCH AS "Branch Code",
            CASE WHEN JOURNALCODE = 'AMORT' THEN
                JOURNALCODE
            ELSE
                JOURNALCODE2
            END AS "Journal Type",
            JOURNAL_DESC as "Description",
            CCY as "Currency",
            DRCR AS "DB/CR Flag",
            N_AMOUNT AS "Original Amount",
            REVERSE as "Reversal Flag"
        FROM IFRS_ACCT_JOURNAL_DATA
        WHERE MASTERID = v_MASTERID
            AND DOWNLOAD_DATE <= v_DATE
        ORDER BY DOWNLOAD_DATE DESC,
            "Journal Type",
            N_AMOUNT DESC,
            CCY ASC,
            JOURNALCODE DESC,
            REVERSE DESC,
            DRCR DESC,
            NOREF ASC;

END;