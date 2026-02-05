CREATE OR REPLACE PROCEDURE SP_IFRS_TRANS_UPLOAD
    AS

    V_CURRDATE DATE;
    V_PREVDATE DATE;

    BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    SELECT PREVDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;


DELETE IFRS_TRANS_DAILY_UPLOAD
WHERE DOWNLOAD_DATE >= V_CURRDATE;COMMIT;

INSERT INTO IFRS_TRANS_DAILY_UPLOAD
SELECT * FROM TBLU_IMA_TRANSACTION WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

DELETE IFRS_TRANSACTION_DAILY
 WHERE     DOWNLOAD_DATE = V_CURRDATE
       AND ACCOUNT_NUMBER IN (SELECT ACCOUNT_NUMBER
                                FROM IFRS_TRANS_DAILY_UPLOAD WHERE DOWNLOAD_DATE = V_CURRDATE);

COMMIT;


INSERT INTO IFRS_TRANSACTION_DAILY (
                                    DOWNLOAD_DATE    ,
                                    MASTERID         ,
                                    ACCOUNT_NUMBER   ,
                                    TRX_CODE         ,
                                    CCY              ,
                                    ORG_CCY_AMT      ,
                                    DEBET_CREDIT_FLAG,
                                    CREATED_DATE
)
   SELECT V_CURRDATE,
          0,
          ACCOUNT_NUMBER,
          TRX_CODE,
          CURRENCY_CODE,
          ORG_CCY_AMT,
          DEBET_CREDIT_FLAG,
          SYSDATE
     FROM IFRS_TRANS_DAILY_UPLOAD
     WHERE DOWNLOAD_DATE = V_CURRDATE;

COMMIT;

/*
INSERT INTO IFRS_TRANSACTION_DAILY
(
DOWNLOAD_DATE         ,
EFFECTIVE_DATE        ,
MATURITY_DATE         ,
MASTERID              ,
FEE_COST_ID           ,
ACCOUNT_NUMBER        ,
FACILITY_NUMBER       ,
CUSTOMER_NUMBER       ,
BRANCH_CODE           ,
DATA_SOURCE           ,
PRD_TYPE              ,
PRD_CODE              ,
TRX_CODE              ,
CCY                   ,
EVENT_CODE            ,
TRX_REFERENCE_NUMBER  ,
ORG_CCY_AMT           ,
EQV_LCY_AMT           ,
DEBET_CREDIT_FLAG     ,
TRX_SOURCE            ,
INTERNAL_NO           ,
REVOLVING_FLAG        ,
TRX_LEVEL             ,
CREATED_DATE
)
SELECT
V_CURRDATE            ,
EFFECTIVE_DATE        ,
MATURITY_DATE         ,
0                     ,
FEE_COST_ID           ,
ACCOUNT_NUMBER        ,
FACILITY_NUMBER       ,
CUSTOMER_NUMBER       ,
BRANCH_CODE           ,
DATA_SOURCE           ,
PRD_TYPE              ,
PRD_CODE              ,
TRX_CODE              ,
CCY                   ,
EVENT_CODE            ,
TRX_REFERENCE_NUMBER  ,
ORG_CCY_AMT           ,
EQV_LCY_AMT           ,
DEBET_CREDIT_FLAG     ,
TRX_SOURCE            ,
INTERNAL_NO           ,
REVOLVING_FLAG        ,
TRX_LEVEL             ,
CREATED_DATE
FROM IFRS_TRANSACTION_DAILY
WHERE DOWNLOAD_DATE = V_PREVDATE;
COMMIT;
*/

END;