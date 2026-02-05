CREATE OR REPLACE PROCEDURE USPS_LOANMODULEINITIALFEE
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
        SELECT
            DOWNLOAD_DATE   AS "Download Date",
            ACCTNO          AS "Account Number",
            IACF.TRX_CODE   AS "Transaction Code",
            TRANSACTION_DESC    AS "Description",
            IACF.CCY            AS "Currency",
            CASE WHEN FLAG_REVERSE = 'Y' THEN
                ABS (AMOUNT) * - 1
            ELSE
                ABS (AMOUNT)
            END AS "Fee Amount"
        FROM IFRS_ACCT_COST_FEE IACF
            JOIN IFRS_MASTER_TRANSACTION_PARAM ITP ON IACF.TRX_CODE = ITP.TRX_CODE
        WHERE FLAG_CF = 'F'
            AND STATUS IN ('PRO', 'PNL', 'ACT')
            AND SRCPROCESS = 'TRAN_DAILY'
            AND MASTERID = v_MASTERID
            AND DOWNLOAD_DATE <= v_DATE;

END;