CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_TRANSACTION_DAILY
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

    /*
    5. SELECT * FROM IFRS_TRANSACTION_DAILY WHERE TRX_CODE = 'PP';
    */

    MERGE INTO IFRS_PPR_TRANSACTION_DAILY D
    USING (
             SELECT
                DOWNLOAD_DATE,
                EFFECTIVE_DATE,
                MATURITY_DATE,
                MASTERID,
                FEE_COST_ID,
                ACCOUNT_NUMBER,
                FACILITY_NUMBER,
                CUSTOMER_NUMBER,
                BRANCH_CODE,
                DATA_SOURCE,
                PRD_TYPE,
                PRD_CODE,
                TRX_CODE,
                CCY,
                EVENT_CODE,
                TRX_REFERENCE_NUMBER,
                ORG_CCY_AMT,
                EQV_LCY_AMT,
                DEBET_CREDIT_FLAG,
                TRX_SOURCE,
                INTERNAL_NO,
                REVOLVING_FLAG,
                TRX_LEVEL,
                CREATED_DATE
             FROM IFRS_TRANSACTION_DAILY
             WHERE TRX_CODE = 'PP'
                AND EFFECTIVE_DATE = LAST_DAY(V_CURRDATE)
          ) S ON (D.DOWNLOAD_DATE=S.DOWNLOAD_DATE
                  AND D.EFFECTIVE_DATE = S.EFFECTIVE_DATE
                  AND D.MASTERID = S.MASTERID
                  AND D.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        MATURITY_DATE           =   S.MATURITY_DATE,
        FEE_COST_ID             =   S.FEE_COST_ID,
        FACILITY_NUMBER         =   S.FACILITY_NUMBER,
        CUSTOMER_NUMBER         =   S.CUSTOMER_NUMBER,
        BRANCH_CODE             =   S.BRANCH_CODE,
        DATA_SOURCE             =   S.DATA_SOURCE,
        PRD_TYPE                =   S.PRD_TYPE,
        PRD_CODE                =   S.PRD_CODE,
        TRX_CODE                =   S.TRX_CODE,
        CCY                     =   S.CCY,
        EVENT_CODE              =   S.EVENT_CODE,
        TRX_REFERENCE_NUMBER    =   S.TRX_REFERENCE_NUMBER,
        ORG_CCY_AMT             =   S.ORG_CCY_AMT,
        EQV_LCY_AMT             =   S.EQV_LCY_AMT,
        DEBET_CREDIT_FLAG       =   S.DEBET_CREDIT_FLAG,
        TRX_SOURCE              =   S.TRX_SOURCE,
        INTERNAL_NO             =   S.INTERNAL_NO,
        REVOLVING_FLAG          =   S.REVOLVING_FLAG,
        TRX_LEVEL               =   S.TRX_LEVEL,
        CREATED_DATE            =   S.CREATED_DATE,
        UPDATEDDATE             =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            EFFECTIVE_DATE,
            MATURITY_DATE,
            MASTERID,
            FEE_COST_ID,
            ACCOUNT_NUMBER,
            FACILITY_NUMBER,
            CUSTOMER_NUMBER,
            BRANCH_CODE,
            DATA_SOURCE,
            PRD_TYPE,
            PRD_CODE,
            TRX_CODE,
            CCY,
            EVENT_CODE,
            TRX_REFERENCE_NUMBER,
            ORG_CCY_AMT,
            EQV_LCY_AMT,
            IDR_ORG_CCY_AMT,
            IDR_EQV_LCY_AMT,
            DEBET_CREDIT_FLAG,
            TRX_SOURCE,
            INTERNAL_NO,
            REVOLVING_FLAG,
            TRX_LEVEL,
            CREATED_DATE)
    VALUES (S.DOWNLOAD_DATE,
            S.EFFECTIVE_DATE,
            S.MATURITY_DATE,
            S.MASTERID,
            S.FEE_COST_ID,
            S.ACCOUNT_NUMBER,
            S.FACILITY_NUMBER,
            S.CUSTOMER_NUMBER,
            S.BRANCH_CODE,
            S.DATA_SOURCE,
            S.PRD_TYPE,
            S.PRD_CODE,
            S.TRX_CODE,
            S.CCY,
            S.EVENT_CODE,
            S.TRX_REFERENCE_NUMBER,
            S.ORG_CCY_AMT,
            S.EQV_LCY_AMT,
            0,
            0,
            S.DEBET_CREDIT_FLAG,
            S.TRX_SOURCE,
            S.INTERNAL_NO,
            S.REVOLVING_FLAG,
            S.TRX_LEVEL,
            S.CREATED_DATE);
     COMMIT;

 END;