CREATE OR REPLACE PROCEDURE  USPS_LOANMODULEEVENTCHANGES
(
    V_DDATE_MAID VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_MASTERID NUMBER(18);
    v_ACCNUM VARCHAR(50);
BEGIN

    v_MASTERID := TO_NUMBER(SUBSTR(V_DDATE_MAID, 11, INSTR(V_DDATE_MAID, '*', 1) - 11));
    v_ACCNUM := TO_CHAR(SUBSTR(V_DDATE_MAID, INSTR(V_DDATE_MAID, '*', 1) + 1, LENGTH(V_DDATE_MAID)));

    OPEN Cur_out FOR
        SELECT
          TO_CHAR(EFFECTIVE_DATE, 'yyyy/MM/dd') || V_MASTERID || '*' || v_ACCNUM as DDATE_MAID,
        ACCOUNT_NUMBER As "Account Number",
            EFFECTIVE_DATE As "Effective Date",
            EVENT_ID as "Event Id",
            REMARKS as "Remarks",
            BEFORE_VALUE as "Before value",
            AFTER_VALUE as "After Value"
        FROM IFRS_EVENT_CHANGES
        WHERE MASTERID = v_MASTERID
            UNION ALL
        SELECT
          TO_CHAR(EFFECTIVE_DATE, 'yyyy/MM/dd') || V_MASTERID || '*' || v_ACCNUM as DDATE_MAID,
         ACCOUNT_NUMBER as "Account Number",
            EFFECTIVE_DATE as "Effective Date",
            EVENT_ID as "Event Id",
            REMARKS as "Remarks",
            BEFORE_VALUE as "Before Value",
            AFTER_VALUE as "After Value"
        FROM IFRS_LBM_EVENT_CHANGES
        WHERE MASTERID = v_MASTERID
        ORDER BY "Effective Date" ASC;

END;