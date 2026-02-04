CREATE OR REPLACE PROCEDURE USPS_LOANMODULEIMPAIR
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
        SELECT DOWNLOAD_DATE AS "Period",
            ACCOUNT_NUMBER AS "Account Number",
            CASE WHEN IMPAIRED_FLAG = 'I' THEN 'Individual'
                 WHEN IMPAIRED_FLAG = 'C' THEN 'Collective'
                 WHEN IMPAIRED_FLAG = 'W' THEN 'Worst Case'
                 ELSE '' END AS "Impaired Flag",
            CURRENCY AS "Currency",
            BI_COLLECTABILITY AS "BI Collectability",
            CR_STAGE AS "Stage",
            DAY_PAST_DUE AS "DPD",
            EIR AS "EIR (%)",
            EAD_AMOUNT AS "EAD Amount",
            ECL_AMOUNT AS "ECL Amount",
            CASE WHEN IMPAIRED_FLAG = 'I' THEN NVL(IA_UNWINDING_AMOUNT, 0)
                 WHEN IMPAIRED_FLAG = 'C' THEN CA_UNWINDING_AMOUNT
                 WHEN IMPAIRED_FLAG = 'W' THEN 0
                 ELSE 0 END AS "Unwinding Interest",
            COALESCE(BEGINNING_BALANCE, 0) AS "Beginning Balance",
            COALESCE(CHARGE_AMOUNT, 0) AS "Charge",
            COALESCE(WRITEBACK_AMOUNT, 0) AS "Write Back",
            COALESCE(ENDING_BALANCE, 0) AS "Ending Balance"
        FROM IFRS_MASTER_ACCOUNT_MONTHLY A
        WHERE --IMPAIRED_FLAG = 'C' AND
            --IS_IMPAIRED = 1 AND
            MASTERID = v_MASTERID
            AND DOWNLOAD_DATE BETWEEN CASE WHEN LAST_DAY(v_DATE) = v_DATE THEN ADD_MONTHS(v_DATE, -12)
                                      ELSE ADD_MONTHS(v_DATE, -13) END
                                      AND v_DATE
        ORDER BY DOWNLOAD_DATE DESC;

END;