CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_MOVE_TO_FINMART
AS
BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ECL_MOVE_TO_FINMART';

    INSERT INTO IFRS_ECL_MOVE_TO_FINMART
        SELECT REPORT_DATE,
               MASTERID,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               ACCOUNT_NUMBER,
               DATA_SOURCE,
               SUB_SEGMENT,
               PRODUCT_CODE,
               SEQ_NO,
               IMP_CHANGE_REASON,
               ECL_ON_BS,
               ECL_OFF_BS,
               ECL_TOTAL,
               CARRY_AMOUNT,
               CURRENCY,
               EXCHANGE_RATE,
               STAGE,
               TRA,
               ASET_KEUANGAN,
               WRITEOFF_FLAG,
               SPECIAL_REASON,
               CREATEDBY,
               CREATEDDATE,
               CREATEDHOST,
               UPDATEDBY,
               UPDATEDDATE,
               UPDATEDHOST,
               UNUSED_AMT
          FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
         WHERE 1 = 1 --AND REPORT_DATE = '31-Aug-2021'
                     AND REPORT_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE);

    -- AND REPORT_DATE='30-Sep-2021';

    COMMIT;
END;