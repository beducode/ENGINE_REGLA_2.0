CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_DETAILS_3
 AS
    V_CURRDATE DATE;
BEGIN

     SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

     --GET  PREPAYMENT DETAILS
     MERGE INTO IFRS_PPR_DETAILS_3 D
     USING (
             SELECT
                A.REPORT_DATE,
                A.MASTERID,
                A.ACCOUNT_NUMBER,
                A.CURRENCY,
                A.OUTSTANDING,
                A.PREPAYMENT,
                A.SCHEDULE,
                A.ACTUAL,
                A.RATE_AMOUNT,
                A.SMM,
                A.PREPAYMENT_SEGMENT,
                A.SEGMENTATION_ID,
                A.REVOLVING_FLAG,
                A.PRODUCT_CODE,
                A.INCREMENTS,
                A.COMPONENT_TYPE,
                A.DATA_SOURCE,
                CASE WHEN A.INCREMENTS = 1 THEN 12
                     WHEN A.INCREMENTS = 3 THEN 4
                     WHEN A.INCREMENTS = 6 THEN 2
                END DURATION
            FROM IFRS_PPR_PROCESS A
            WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                AND COMPONENT_TYPE IN (0,2)
        ) S ON (D.REPORT_DATE=S.REPORT_DATE
                AND D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        MASTERID            =   S.MASTERID,
        CURRENCY            =   S.CURRENCY,
        OUTSTANDING         =   S.OUTSTANDING,
        PREPAYMENT          =   S.PREPAYMENT,
        SCHEDULE            =   S.SCHEDULE,
        ACTUAL              =   S.ACTUAL,
        RATE_AMOUNT         =   S.RATE_AMOUNT,
        SMM                 =   S.SMM,
        PREPAYMENT_SEGMENT  =   S.PREPAYMENT_SEGMENT,
        SEGMENTATION_ID     =   S.SEGMENTATION_ID,
        INCREMENTS          =   S.INCREMENTS,
        DURATION            =   S.DURATION,
        COMPONENT_TYPE      =   S.COMPONENT_TYPE,
        DATA_SOURCE         =   S.DATA_SOURCE--,
        ---CUT_OFF_DATE        =   S.CUT_OFF_DATE
    WHEN NOT MATCHED THEN
    INSERT (REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            PREPAYMENT_SEGMENT,
            SEGMENTATION_ID,
            INCREMENTS,
            DURATION,
            COMPONENT_TYPE,
            DATA_SOURCE--,
            --CUT_OFF_DATE
            )
    VALUES (S.REPORT_DATE,
            S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CURRENCY,
            S.OUTSTANDING,
            S.PREPAYMENT,
            S.SCHEDULE,
            S.ACTUAL,
            S.RATE_AMOUNT,
            S.SMM,
            S.PREPAYMENT_SEGMENT,
            S.SEGMENTATION_ID,
            S.INCREMENTS,
            S.DURATION,
            S.COMPONENT_TYPE,
            S.DATA_SOURCE--,
            --S.CUT_OFF_DATE
            );
    COMMIT;

END;