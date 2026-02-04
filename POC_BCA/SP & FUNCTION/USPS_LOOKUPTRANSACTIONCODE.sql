CREATE OR REPLACE PROCEDURE USPS_LOOKUPTRANSACTIONCODE
    (    v_blank IN NUMBER DEFAULT 0,
    Cur_out OUT SYS_REFCURSOR)
AS
BEGIN

     IF ( v_blank=1 ) THEN

        OPEN Cur_out FOR

        SELECT 'ALL' AS TRX_CODE, 'All Transaction Code' AS "Transaction Code"
        FROM DUAL

        UNION

        SELECT DISTINCT TRX_CODE,
            TRX_CODE || ' - ' || MAX(TRANSACTION_DESC) AS "Transaction Code"
        FROM IFRS_MASTER_TRANSACTION_PARAM
        GROUP BY TRX_CODE;

    ELSE
        OPEN Cur_out FOR
        SELECT DISTINCT TRX_CODE,
            TRX_CODE || ' - ' || MAX(TRANSACTION_DESC) AS "Transaction Code"
        FROM IFRS_MASTER_TRANSACTION_PARAM
        GROUP BY TRX_CODE;

    END IF;

END;