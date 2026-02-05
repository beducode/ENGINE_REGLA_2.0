CREATE OR REPLACE PROCEDURE     SP_INITIAL_UPDATE_ILS_PROD
AS
    V_CURRDATE DATE;
    V_PREVDATE DATE;
    V_DATE     DATE;
    V_SPNAME   VARCHAR2(100);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_UPDATE_LOG';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.GTMP_IFRS_MASTER_ACCOUNT';

    /*
    DELETE  FROM IFRS.IFRS_STATISTIC
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND PRC_NAME = 'INIT';
    COMMIT;
    */
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS.IFRS_PRC_DATE;

    SELECT PREVDATE INTO V_PREVDATE FROM IFRS.IFRS_PRC_DATE;

    delete IFRS.IFRS_STATISTIC where DOWNLOAD_DATE=V_CURRDATE and SP_NAME='SP_INITIAL_UPDATE_ILS' AND COUNTER=0;

    insert into IFRS.IFRS_STATISTIC (DOWNLOAD_DATE, SP_NAME, START_DATE, END_DATE, COUNTER, SESSIONID, ISCOMPLETE, PRC_NAME,
                                PRC_PROCESS_TIME, SESSION_PROCESS_TIME, REMARK, PKID)
    values (V_CURRDATE, 'SP_INITIAL_UPDATE_ILS', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, '', 'N', 'AMT', '', '', '', '');

    V_DATE :=
            TO_DATE('26-' || TO_CHAR(V_CURRDATE, 'MON-YYYY'), 'DD-MON-YYYY');

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            '');

    COMMIT;

    --V_SPNAME := 'SP_IFRS_IMA_UPLOAD';
    --SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'INIT','Y');
    /*
    V_SPNAME := 'SP_IFRS_MASTERID_PROD';
    SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'INIT','Y');

    V_SPNAME := 'SP_IFRS_PAYMENT_SETTING_PROD';
    SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'INIT','Y');
    */

    --SP_IFRS_IMA_UPLOAD;
    IFRS.SP_IFRS_TRANS_UPLOAD;
    IFRS.SP_IFRS_MASTER_PAYSET_UPLOAD;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT IMA HOLIDAY');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --IF HOLIDAY
    -----------------------------------------------------------------------------------------------------------------
    -- MASTER_ACCOUNT

    IF (FN_HOLIDAY(V_CURRDATE) = 1)
    THEN
        DELETE IFRS.IFRS_MASTER_ACCOUNT
        WHERE DOWNLOAD_DATE = V_CURRDATE
          AND DATA_SOURCE IN ('ILS', 'LIMIT');

        COMMIT;

        DELETE IFRS.IFRS_MASTER_EXCHANGE_RATE
        WHERE DOWNLOAD_DATE = V_CURRDATE;

        COMMIT;

        INSERT INTO IFRS.IFRS_MASTER_ACCOUNT (DOWNLOAD_DATE,
                                         MASTERID,
                                         MASTER_ACCOUNT_CODE,
                                         DATA_SOURCE,
                                         GLOBAL_CUSTOMER_NUMBER,
                                         CUSTOMER_NUMBER,
                                         CUSTOMER_NAME,
                                         FACILITY_NUMBER,
                                         ACCOUNT_NUMBER,
                                         PREVIOUS_ACCOUNT_NUMBER,
                                         ACCOUNT_STATUS,
                                         INTEREST_RATE,
                                         MARKET_RATE,
                                         PRODUCT_GROUP,
                                         PRODUCT_TYPE,
                                         PRODUCT_CODE,
                                         PRODUCT_ENTITY,
                                         GL_CONSTNAME,
                                         BRANCH_CODE,
                                         BRANCH_CODE_OPEN,
                                         CURRENCY,
                                         EXCHANGE_RATE,
                                         INITIAL_OUTSTANDING,
                                         OUTSTANDING,
                                         OUTSTANDING_IDC,
                                         OUTSTANDING_JF,
                                         OUTSTANDING_BANK,
                                         OUTSTANDING_PASTDUE,
                                         PLAFOND,
                                         PLAFOND_CASH,
                                         INTEREST_ACCRUED,
                                         INSTALLMENT_AMOUNT,
                                         UNUSED_AMOUNT,
                                         DOWN_PAYMENT_AMOUNT,
                                         JF_FLAG,
                                         LOAN_START_DATE,
                                         LOAN_DUE_DATE,
                                         LOAN_START_AMORTIZATION,
                                         LOAN_END_AMORTIZATION,
                                         INSTALLMENT_GRACE_PERIOD,
                                         NEXT_PAYMENT_DATE,
                                         NEXT_INT_PAYMENT_DATE,
                                         LAST_PAYMENT_DATE,
                                         FIRST_INSTALLMENT_DATE,
                                         TENOR,
                                         REMAINING_TENOR,
                                         PAYMENT_CODE,
                                         PAYMENT_TERM,
                                         INTEREST_CALCULATION_CODE,
                                         INTEREST_PAYMENT_TERM,
                                         RESTRUCTURE_DATE,
                                         RESTRUCTURE_FLAG,
                                         POCI_FLAG,
                                         STAFF_LOAN_FLAG,
                                         BELOW_MARKET_FLAG,
                                         BTB_FLAG,
                                         COMMITTED_FLAG,
                                         REVOLVING_FLAG,
                                         IAS_CLASS,
                                         IFRS9_CLASS,
                                         BM_RESULT,
                                         SPPI_RESULT,
                                         AMORT_TYPE,
                                         EIR_STATUS,
                                         ECF_STATUS,
                                         EIR,
                                         EIR_AMOUNT,
                                         FAIR_VALUE_AMOUNT,
                                         INITIAL_UNAMORT_TXN_COST,
                                         INITIAL_UNAMORT_ORG_FEE,
                                         UNAMORT_COST_AMT,
                                         UNAMORT_FEE_AMT,
                                         DAILY_AMORT_AMT,
                                         UNAMORT_AMT_TOTAL_JF,
                                         UNAMORT_FEE_AMT_JF,
                                         UNAMORT_COST_AMT_JF,
                                         ORIGINAL_COLLECTABILITY,
                                         BI_COLLECTABILITY,
                                         DAY_PAST_DUE,
                                         DPD_START_DATE,
                                         DPD_ZERO_COUNTER,
                                         NPL_DATE,
                                         NPL_FLAG,
                                         DEFAULT_DATE,
                                         DEFAULT_FLAG,
                                         WRITEOFF_FLAG,
                                         WRITEOFF_DATE,
                                         OUTSTANDING_WO,
                                         IMPAIRED_FLAG,
                                         IS_IMPAIRED,
                                         GROUP_SEGMENT,
                                         SEGMENT,
                                         SUB_SEGMENT,
                                         CR_STAGE,
                                         LIFETIME,
                                         EAD_RULE_ID,
                                         EAD_SEGMENT,
                                         EAD_AMOUNT,
                                         LGD_RULE_ID,
                                         LGD_SEGMENT,
                                         PD_RULE_ID,
                                         PD_SEGMENT,
                                         BUCKET_GROUP,
                                         BUCKET_ID,
                                         ECL_12_AMOUNT,
                                         ECL_LIFETIME_AMOUNT,
                                         ECL_AMOUNT,
                                         CA_UNWINDING_AMOUNT,
                                         IA_UNWINDING_AMOUNT,
                                         IA_UNWINDING_SUM_AMOUNT,
                                         BEGINNING_BALANCE,
                                         ENDING_BALANCE,
                                         WRITEBACK_AMOUNT,
                                         CHARGE_AMOUNT,
                                         RESERVED_VARCHAR_1,
                                         RESERVED_VARCHAR_2,
                                         RESERVED_VARCHAR_3,
                                         RESERVED_VARCHAR_4,
                                         RESERVED_VARCHAR_5,
                                         RESERVED_VARCHAR_6,
                                         RESERVED_VARCHAR_7,
                                         RESERVED_VARCHAR_8,
                                         RESERVED_VARCHAR_9,
                                         RESERVED_VARCHAR_10,
                                         RESERVED_VARCHAR_11,
                                         RESERVED_VARCHAR_12,
                                         RESERVED_VARCHAR_13,
                                         RESERVED_VARCHAR_14,
                                         RESERVED_VARCHAR_15,
                                         RESERVED_VARCHAR_16,
                                         RESERVED_VARCHAR_17,
                                         RESERVED_VARCHAR_18,
                                         RESERVED_VARCHAR_19,
                                         RESERVED_VARCHAR_20,
                                         RESERVED_VARCHAR_21,
                                         RESERVED_VARCHAR_22,
                                         RESERVED_VARCHAR_23,
                                         RESERVED_VARCHAR_24,
                                         RESERVED_VARCHAR_25,
                                         RESERVED_VARCHAR_26,
                                         RESERVED_VARCHAR_27,
                                         RESERVED_VARCHAR_28,
                                         RESERVED_VARCHAR_29,
                                         RESERVED_VARCHAR_30,
                                         RESERVED_AMOUNT_1,
                                         RESERVED_AMOUNT_2,
                                         RESERVED_AMOUNT_3,
                                         RESERVED_AMOUNT_4,
                                         RESERVED_AMOUNT_5,
                                         RESERVED_AMOUNT_6,
                                         RESERVED_AMOUNT_7,
                                         RESERVED_AMOUNT_8,
                                         RESERVED_AMOUNT_9,
                                         RESERVED_AMOUNT_10,
                                         RESERVED_AMOUNT_11,
                                         RESERVED_AMOUNT_12,
                                         RESERVED_AMOUNT_13,
                                         RESERVED_AMOUNT_14,
                                         RESERVED_AMOUNT_15,
                                         RESERVED_AMOUNT_16,
                                         RESERVED_AMOUNT_17,
                                         RESERVED_AMOUNT_18,
                                         RESERVED_AMOUNT_19,
                                         RESERVED_AMOUNT_20,
                                         RESERVED_RATE_1,
                                         RESERVED_RATE_2,
                                         RESERVED_RATE_3,
                                         RESERVED_RATE_4,
                                         RESERVED_RATE_5,
                                         RESERVED_RATE_6,
                                         RESERVED_RATE_7,
                                         RESERVED_RATE_8,
                                         RESERVED_RATE_9,
                                         RESERVED_RATE_10,
                                         RESERVED_FLAG_1,
                                         RESERVED_FLAG_2,
                                         RESERVED_FLAG_3,
                                         RESERVED_FLAG_4,
                                         RESERVED_FLAG_5,
                                         RESERVED_FLAG_6,
                                         RESERVED_FLAG_7,
                                         RESERVED_FLAG_8,
                                         RESERVED_FLAG_9,
                                         RESERVED_FLAG_10,
                                         RESERVED_DATE_1,
                                         RESERVED_DATE_2,
                                         RESERVED_DATE_3,
                                         RESERVED_DATE_4,
                                         RESERVED_DATE_5,
                                         RESERVED_DATE_6,
                                         RESERVED_DATE_7,
                                         RESERVED_DATE_8,
                                         RESERVED_DATE_9,
                                         RESERVED_DATE_10,
                                         UNAMORT_BENEFIT,
                                         CREATEDBY,
                                         CREATEDDATE,
                                         CREATEDHOST)
        SELECT V_CURRDATE,
               MASTERID,
               MASTER_ACCOUNT_CODE,
               DATA_SOURCE,
               GLOBAL_CUSTOMER_NUMBER,
               CUSTOMER_NUMBER,
               CUSTOMER_NAME,
               FACILITY_NUMBER,
               ACCOUNT_NUMBER,
               PREVIOUS_ACCOUNT_NUMBER,
               ACCOUNT_STATUS,
               INTEREST_RATE,
               MARKET_RATE,
               PRODUCT_GROUP,
               PRODUCT_TYPE,
               PRODUCT_CODE,
               PRODUCT_ENTITY,
               GL_CONSTNAME,
               BRANCH_CODE,
               BRANCH_CODE_OPEN,
               CURRENCY,
               EXCHANGE_RATE,
               INITIAL_OUTSTANDING,
               OUTSTANDING,
               OUTSTANDING_IDC,
               OUTSTANDING_JF,
               OUTSTANDING_BANK,
               OUTSTANDING_PASTDUE,
               PLAFOND,
               PLAFOND_CASH,
               INTEREST_ACCRUED,
               INSTALLMENT_AMOUNT,
               UNUSED_AMOUNT,
               DOWN_PAYMENT_AMOUNT,
               JF_FLAG,
               LOAN_START_DATE,
               LOAN_DUE_DATE,
               LOAN_START_AMORTIZATION,
               LOAN_END_AMORTIZATION,
               INSTALLMENT_GRACE_PERIOD,
               NEXT_PAYMENT_DATE,
               NEXT_INT_PAYMENT_DATE,
               LAST_PAYMENT_DATE,
               FIRST_INSTALLMENT_DATE,
               TENOR,
               REMAINING_TENOR,
               PAYMENT_CODE,
               PAYMENT_TERM,
               INTEREST_CALCULATION_CODE,
               INTEREST_PAYMENT_TERM,
               RESTRUCTURE_DATE,
               RESTRUCTURE_FLAG,
               POCI_FLAG,
               STAFF_LOAN_FLAG,
               BELOW_MARKET_FLAG,
               BTB_FLAG,
               COMMITTED_FLAG,
               REVOLVING_FLAG,
               IAS_CLASS,
               IFRS9_CLASS,
               BM_RESULT,
               SPPI_RESULT,
               AMORT_TYPE,
               EIR_STATUS,
               ECF_STATUS,
               EIR,
               EIR_AMOUNT,
               FAIR_VALUE_AMOUNT,
               INITIAL_UNAMORT_TXN_COST,
               INITIAL_UNAMORT_ORG_FEE,
               UNAMORT_COST_AMT,
               UNAMORT_FEE_AMT,
               DAILY_AMORT_AMT,
               UNAMORT_AMT_TOTAL_JF,
               UNAMORT_FEE_AMT_JF,
               UNAMORT_COST_AMT_JF,
               ORIGINAL_COLLECTABILITY,
               BI_COLLECTABILITY,
               DAY_PAST_DUE,
               DPD_START_DATE,
               DPD_ZERO_COUNTER,
               NPL_DATE,
               NPL_FLAG,
               DEFAULT_DATE,
               DEFAULT_FLAG,
               WRITEOFF_FLAG,
               WRITEOFF_DATE,
               OUTSTANDING_WO,
               IMPAIRED_FLAG,
               IS_IMPAIRED,
               GROUP_SEGMENT,
               SEGMENT,
               SUB_SEGMENT,
               CR_STAGE,
               LIFETIME,
               EAD_RULE_ID,
               EAD_SEGMENT,
               EAD_AMOUNT,
               LGD_RULE_ID,
               LGD_SEGMENT,
               PD_RULE_ID,
               PD_SEGMENT,
               BUCKET_GROUP,
               BUCKET_ID,
               ECL_12_AMOUNT,
               ECL_LIFETIME_AMOUNT,
               ECL_AMOUNT,
               CA_UNWINDING_AMOUNT,
               IA_UNWINDING_AMOUNT,
               IA_UNWINDING_SUM_AMOUNT,
               BEGINNING_BALANCE,
               ENDING_BALANCE,
               WRITEBACK_AMOUNT,
               CHARGE_AMOUNT,
               RESERVED_VARCHAR_1,
               RESERVED_VARCHAR_2,
               RESERVED_VARCHAR_3,
               RESERVED_VARCHAR_4,
               RESERVED_VARCHAR_5,
               RESERVED_VARCHAR_6,
               RESERVED_VARCHAR_7,
               RESERVED_VARCHAR_8,
               RESERVED_VARCHAR_9,
               RESERVED_VARCHAR_10,
               RESERVED_VARCHAR_11,
               RESERVED_VARCHAR_12,
               RESERVED_VARCHAR_13,
               RESERVED_VARCHAR_14,
               RESERVED_VARCHAR_15,
               RESERVED_VARCHAR_16,
               RESERVED_VARCHAR_17,
               RESERVED_VARCHAR_18,
               RESERVED_VARCHAR_19,
               RESERVED_VARCHAR_20,
               RESERVED_VARCHAR_21,
               RESERVED_VARCHAR_22,
               RESERVED_VARCHAR_23,
               RESERVED_VARCHAR_24,
               RESERVED_VARCHAR_25,
               RESERVED_VARCHAR_26,
               RESERVED_VARCHAR_27,
               RESERVED_VARCHAR_28,
               RESERVED_VARCHAR_29,
               RESERVED_VARCHAR_30,
               RESERVED_AMOUNT_1,
               RESERVED_AMOUNT_2,
               RESERVED_AMOUNT_3,
               RESERVED_AMOUNT_4,
               RESERVED_AMOUNT_5,
               RESERVED_AMOUNT_6,
               RESERVED_AMOUNT_7,
               RESERVED_AMOUNT_8,
               RESERVED_AMOUNT_9,
               RESERVED_AMOUNT_10,
               RESERVED_AMOUNT_11,
               RESERVED_AMOUNT_12,
               RESERVED_AMOUNT_13,
               RESERVED_AMOUNT_14,
               RESERVED_AMOUNT_15,
               RESERVED_AMOUNT_16,
               RESERVED_AMOUNT_17,
               RESERVED_AMOUNT_18,
               RESERVED_AMOUNT_19,
               RESERVED_AMOUNT_20,
               RESERVED_RATE_1,
               RESERVED_RATE_2,
               RESERVED_RATE_3,
               RESERVED_RATE_4,
               RESERVED_RATE_5,
               RESERVED_RATE_6,
               RESERVED_RATE_7,
               RESERVED_RATE_8,
               RESERVED_RATE_9,
               RESERVED_RATE_10,
               RESERVED_FLAG_1,
               RESERVED_FLAG_2,
               RESERVED_FLAG_3,
               RESERVED_FLAG_4,
               RESERVED_FLAG_5,
               RESERVED_FLAG_6,
               RESERVED_FLAG_7,
               RESERVED_FLAG_8,
               RESERVED_FLAG_9,
               RESERVED_FLAG_10,
               RESERVED_DATE_1,
               RESERVED_DATE_2,
               RESERVED_DATE_3,
               RESERVED_DATE_4,
               RESERVED_DATE_5,
               RESERVED_DATE_6,
               RESERVED_DATE_7,
               RESERVED_DATE_8,
               RESERVED_DATE_9,
               RESERVED_DATE_10,
               UNAMORT_BENEFIT,
               CREATEDBY,
               CREATEDDATE,
               CREATEDHOST
        FROM IFRS.IFRS_MASTER_ACCOUNT
        WHERE DOWNLOAD_DATE = V_PREVDATE
          AND DATA_SOURCE IN ('ILS', 'LIMIT', 'PBMM');

        COMMIT;

        /*
        INSERT INTO IFRS.IFRS_MASTER_CUSTOMER_RATING
        SELECT
            0                      ,
            V_CURRDATE             ,
            GLOBAL_CUSTOMER_NUMBER ,
            CUSTOMER_NUMBER        ,
            RATING_SOURCE_1        ,
            RATING_AGENCY_1        ,
            RATING_TYPE_1          ,
            RATING_CODE_1          ,
            RATING_SOURCE_2        ,
            RATING_AGENCY_2        ,
            RATING_TYPE_2          ,
            RATING_CODE_2          ,
            RATING_SOURCE_3        ,
            RATING_AGENCY_3        ,
            RATING_TYPE_3          ,
            RATING_CODE_3          ,
            RATING_SOURCE_4        ,
            RATING_AGENCY_4        ,
            RATING_TYPE_4          ,
            RATING_CODE_4          ,
            RATING_SOURCE_5        ,
            RATING_AGENCY_5        ,
            RATING_TYPE_5          ,
            RATING_CODE_5          ,
            GOL_DEB                ,
            BI_COLLECTABILITY      ,
            BANK_IND               ,
            RESERVED_VARCHAR_1     ,
            RESERVED_VARCHAR_2     ,
            RESERVED_VARCHAR_3     ,
            RESERVED_VARCHAR_4     ,
            RESERVED_VARCHAR_5     ,
            RESERVED_VARCHAR_6     ,
            RESERVED_VARCHAR_7     ,
            RESERVED_VARCHAR_8     ,
            RESERVED_VARCHAR_9     ,
            RESERVED_VARCHAR_10    ,
            RESERVED_AMOUNT_1      ,
            RESERVED_AMOUNT_2      ,
            RESERVED_AMOUNT_3      ,
            RESERVED_AMOUNT_4      ,
            RESERVED_AMOUNT_5      ,
            RESERVED_RATE_1        ,
            RESERVED_RATE_2        ,
            RESERVED_RATE_3        ,
            RESERVED_RATE_4        ,
            RESERVED_RATE_5        ,
            RESERVED_FLAG_1        ,
            RESERVED_FLAG_2        ,
            RESERVED_FLAG_3        ,
            RESERVED_FLAG_4        ,
            RESERVED_FLAG_5        ,
            RESERVED_DATE_1        ,
            RESERVED_DATE_2        ,
            RESERVED_DATE_3        ,
            RESERVED_DATE_4        ,
            RESERVED_DATE_5        ,
            CREATEDBY              ,
            CREATEDDATE            ,
            CREATEDHOST            ,
            UPDATEDBY              ,
            UPDATEDDATE            ,
            UPDATEDHOST
        FROM IFRS.IFRS_MASTER_CUSTOMER_RATING WHERE DOWNLOAD_DATE = V_PREVDATE; COMMIT;
        */
        INSERT INTO IFRS.IFRS_MASTEr_EXCHANGE_RATE
        SELECT 0,
               V_CURRDATE,
               CURRENCY,
               CURRENCY_DESC,
               RATE_AMOUNT,
               MAINTAIN_DATE,
               CREATEDBY,
               SYSDATE,
               CREATEDHOST,
               UPDATEDBY,
               UPDATEDDATE,
               UPDATEDHOST
        FROM IFRS.IFRS_MASTER_EXCHANGE_RATE
        WHERE DOWNLOAD_DATE = V_PREVDATE;

        COMMIT;

        MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
        USING IFRS.IFRS_IMA_AMORT_CURR X
        ON (A.DOWNLOAD_DATE = V_CURRDATE
            AND A.MASTERID = X.MASTERID
            AND A.ACCOUNT_NUMBER = X.ACCOUNT_NUMBER
            AND X.AMORT_TYPE = 'EIR')
        WHEN MATCHED
            THEN
            UPDATE
            SET --UNAMORT_AMT_TOTAL = b.UNAMORT_AMT_TOTAL,
                A.UNAMORT_FEE_AMT         = X.UNAMORT_FEE_AMT,
                A.UNAMORT_COST_AMT        = X.UNAMORT_COST_AMT,
                A.FAIR_VALUE_AMOUNT       = X.FAIR_VALUE_AMOUNT,
                A.LOAN_START_AMORTIZATION = X.LOAN_START_AMORTIZATION,
                A.LOAN_END_AMORTIZATION   = X.LOAN_END_AMORTIZATION,
                A.AMORT_TYPE              = X.AMORT_TYPE;

        COMMIT;

        UPDATE IFRS.IFRS_MASTER_ACCOUNT
        SET FAIR_VALUE_AMOUNT =
                    COALESCE(OUTSTANDING, 0)
                    + COALESCE(OUTSTANDING_IDC, 0)
                    + +COALESCE(UNAMORT_FEE_AMT, 0)
                    + COALESCE(UNAMORT_COST_AMT, 0)
                    + COALESCE(UNAMORT_BENEFIT, 0)
        WHERE DOWNLOAD_DATE = V_CURRDATE
          AND DATA_SOURCE = 'ILS';

        COMMIT;
    END IF;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT IMA HOLIDAY');

    --TRANSACTION_DAILY
    /*
        IF (FN_HOLIDAY(V_CURRDATE) = 1) THEN

        DELETE IFRS.IFRS_TRANSACTION_DAILY WHERE DOWNLOAD_DATE = V_CURRDATE;
        COMMIT;

        INSERT INTO IFRS.IFRS_TRANSACTION_DAILY (DOWNLOAD_DATE,EFFECTIVE_DATE,MATURITY_DATE,MASTERID,FEE_COST_ID,ACCOUNT_NUMBER,FACILITY_NUMBER,CUSTOMER_NUMBER,BRANCH_CODE,
                                        DATA_SOURCE,PRD_TYPE,PRD_CODE,TRX_CODE,CCY,EVENT_CODE,TRX_REFERENCE_NUMBER,ORG_CCY_AMT,EQV_LCY_AMT,DEBET_CREDIT_FLAG,
                                        TRX_SOURCE,INTERNAL_NO,REVOLVING_FLAG,TRX_LEVEL,CREATED_DATE)
       SELECT V_CURRDATE,
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
         FROM IFRS.IFRS_TRANSACTION_DAILY
        WHERE DOWNLOAD_DATE = V_PREVDATE;
    END IF;
    COMMIT;

    --MASTER_PAYMENT_SETTING
        IF (FN_HOLIDAY(V_CURRDATE) = 1) THEN

        DELETE IFRS.IFRS_MASTER_PAYMENT_SETTING WHERE DOWNLOAD_DATE = V_CURRDATE;
        COMMIT;

        INSERT INTO IFRS.IFRS_MASTER_PAYMENT_SETTING
        (
        DOWNLOAD_DATE,MASTERID,ACCOUNT_NUMBER,COMPONENT_TYPE,COMPONENT_STATUS,FREQUENCY,INCREMENTS,AMOUNT,TIMES_ORG,TIMES_USED,DATE_START,DATE_END,PMT_DATE,IS_INSERTED)
        SELECT
        V_CURRDATE,MASTERID,ACCOUNT_NUMBER,COMPONENT_TYPE,COMPONENT_STATUS,FREQUENCY,INCREMENTS,AMOUNT,TIMES_ORG,TIMES_USED,DATE_START,DATE_END,PMT_DATE,IS_INSERTED
        FROM IFRS.IFRS_MASTER_PAYMENT_SETTING
        WHERE DOWNLOAD_DATE = V_PREVDATE;

    END IF;
    COMMIT;
    */
    DELETE IFRS.IFRS_MASTER_ACCOUNT
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND CREATEDBY = 'DKP';

    COMMIT;

    INSERT INTO IFRS.IFRS_MASTER_ACCOUNT A (MASTERID,
                                       MASTER_ACCOUNT_CODE,
                                       CREATEDBY,
                                       CREATEDHOST,
                                       CREATEDDATE,
                                       PKID,
                                       DOWNLOAD_DATE,
                                       DATA_SOURCE,
                                       ACCOUNT_NUMBER,
                                       PRODUCT_CODE,
                                       BRANCH_CODE,
                                       CUSTOMER_NUMBER,
                                       CUSTOMER_NAME,
                                       RESERVED_VARCHAR_15,
                                       CURRENCY,
                                       OUTSTANDING,
                                       LOAN_START_DATE,
                                       LOAN_DUE_DATE,
                                       INTEREST_RATE,
                                       INTEREST_ACCRUED,
                                       RESERVED_VARCHAR_23,
                                       RESERVED_FLAG_1,
                                       IFRS9_CLASS,
                                       DAY_PAST_DUE,
                                       CR_STAGE,
                                       GROUP_SEGMENT,
                                       SEGMENT,
                                       SUB_SEGMENT,
                                       EAD_SEGMENT,
                                       EAD_RULE_ID,
                                       LGD_RULE_ID,
                                       LGD_SEGMENT,
                                       PD_RULE_ID,
                                       PD_SEGMENT,
                                       BUCKET_GROUP,
                                       RESERVED_VARCHAR_5,
                                       SEGMENT_RULE_ID,
                                       REVOLVING_FLAG,
                                       ACCOUNT_STATUS,
                                       IMPAIRED_FLAG,
                                       RESERVED_VARCHAR_2,
                                       IS_IMPAIRED,
                                       BI_COLLECTABILITY)
    SELECT 0,
           ACCOUNT_NUMBER || DATA_SOURCE,
           'DKP',
           'ADMIN',
           SYSDATE,
           0,
           DOWNLOAD_DATE,
           DATA_SOURCE,
           ACCOUNT_NUMBER,
           PRODUCT_CODE,
           BRANCH_CODE,
           0,
           CUSTOMER_NAME,
           ISS_BANK_SWIFT_CODE,
           CCY,
           AMOUNT,
           START_DATE,
           MATURITY_DATE,
           INTEREST_RATE,
           INTEREST_ACCRUED,
           BI_CODE,
           BS_FLAG,
           'AMORT',
           0,
           1,
           'BANK_BTRD',
           'BANK_BTRD',
           'BANK BTRD - EXTERNAL IDR',
           '',
           '',
           '',
           '',
           1,
           '',
           'IR11_1',
           'BANK_BTRD',
           438,
           0,
           'A',
           'C',
           GOL_DEB,
           '1',
           BI_COLLECTABILITY
    FROM IFRS.TBLU_IMA_TRADE
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    IFRS.SP_IFRS_MASTERID_PROD;

    UPDATE IFRS.IFRS_MASTER_ACCOUNT
    SET BRANCH_CODE =
            CASE
                WHEN LENGTH(BRANCH_CODE) > 4 THEN SUBSTR(BRANCH_CODE, 4, 4)
                ELSE BRANCH_CODE
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE IN ('ILS',
                          'CRD',
                          'LIMIT',
                          'PBMM');

    COMMIT;

    UPDATE IFRS.IFRS_TRANSACTION_DAILY
    SET BRANCH_CODE =
            CASE
                WHEN LENGTH(BRANCH_CODE) > 4 THEN SUBSTR(BRANCH_CODE, 4, 4)
                ELSE BRANCH_CODE
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    UPDATE IFRS.IFRS_MASTER_BRANCH
    SET BRANCH_NUM     =
            CASE
                WHEN LENGTH(BRANCH_NUM) > 4 THEN SUBSTR(BRANCH_NUM, 4, 4)
                ELSE BRANCH_NUM
                END,
        MAIN_BRANCH_CD =
            CASE
                WHEN LENGTH(MAIN_BRANCH_CD) > 4
                    THEN
                    SUBSTR(MAIN_BRANCH_CD, 4, 4)
                ELSE
                    MAIN_BRANCH_CD
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT *
           FROM IFRS.IFRS_STG_BTRD_IMA
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DATA_SOURCE = 'BTRD')
    WHEN MATCHED
        THEN
        UPDATE
        SET A.PRODUCT_CODE = B.PRODUCT_TYPE
        WHERE A.PRODUCT_CODE IS NULL;

    COMMIT;

    MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT DATA_SOURCE,
                           PRD_CODE,
                           PRD_TYPE,
                           PRD_GROUP,
                           RESERVED_VARCHAR_1
           FROM IFRS.IFRS_MASTER_PRODUCT_PARAM) B
    ON (TRIM(A.PRODUCT_CODE) = TRIM(B.PRD_CODE)
        AND A.DATA_SOURCE = B.DATA_SOURCE
        AND A.DOWNLOAD_DATE = V_CURRDATE)
    WHEN MATCHED
        THEN
        UPDATE
        SET RESERVED_VARCHAR_27 = B.RESERVED_VARCHAR_1,
            PRODUCT_GROUP       = B.PRD_GROUP,
            PRODUCT_TYPE        = B.PRD_TYPE;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --TRANSACTION_DAILY
    -----------------------------------------------------------------------------------------------------------------

    MERGE INTO IFRS.IFRS_TRANSACTION_DAILY A
    USING (SELECT DOWNLOAD_DATE,
                  MASTERID,
                  LOAN_DUE_DATE,
                  FACILITY_NUMBER,
                  CUSTOMER_NUMBER,
                  BRANCH_CODE,
                  DATA_SOURCE,
                  PRODUCT_TYPE,
                  PRODUCT_CODE,
                  REVOLVING_FLAG
           FROM IFRS.IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.MASTERID = B.MASTERID)
    WHEN MATCHED
        THEN
        UPDATE
        SET A.MATURITY_DATE   = B.LOAN_DUE_DATE,
            A.FACILITY_NUMBER = B.FACILITY_NUMBER,
            A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER,
            A.BRANCH_CODE     = B.BRANCH_CODE,
            A.DATA_SOURCE     = B.DATA_SOURCE,
            A.PRD_TYPE        = B.PRODUCT_TYPE,
            A.PRD_CODE        = B.PRODUCT_CODE,
            A.REVOLVING_FLAG  = B.REVOLVING_FLAG;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --GTMP_MASTER_ACCOUNT
    -----------------------------------------------------------------------------------------------------------------
    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT INTO TMP');

    COMMIT;

    INSERT INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SELECT *
    FROM IFRS.IFRS_MASTER_ACCOUNT
    WHERE DOWNLOAD_DATE IN (V_CURRDATE, V_PREVDATE);

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT INTO TMP');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --RESET IFRS.GTMP_MASTER_ACCOUNT
    -----------------------------------------------------------------------------------------------------------------
    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET MARKET_RATE     = NULL,
        WRITEOFF_FLAG   = NULL,
        RESERVED_FLAG_6 = NULL
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE IN ('PBMM', 'ILS');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --MARKET_RATE
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE MARKET_RATE');

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT D.*
           FROM IFRS.IFRS_MASTER_MARKETRATE_PARAM D
                    JOIN
                (SELECT MAX(EFF_DATE)    EFF_DATE,
                        PRD_CODE,
                        MAX(CREATEDDATE) CREATEDDATE
                 FROM IFRS.IFRS_MASTER_MARKETRATE_PARAM
                 WHERE EFF_DATE <= V_CURRDATE
                 GROUP BY PRD_CODE) F
                ON D.EFF_DATE = F.EFF_DATE
                    AND D.PRD_CODE = F.PRD_CODE
                    AND D.CREATEDDATE = F.CREATEDDATE
           WHERE D.EFF_DATE <= V_CURRDATE) B
    ON (A.DOWNLOAD_DATE >= B.EFF_DATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND (A.PRODUCT_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
        AND (A.CURRENCY = B.CCY OR B.CCY = 'ALL'))
    WHEN MATCHED
        THEN
        UPDATE SET A.MARKET_RATE = B.MKT_RATE;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END
                                ',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE MARKET_RATE');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --WRITEOFF_FLAG
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE WRITEOFF_FLAG');

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT B.DOWNLOAD_DATE, B.ACCOUNT_NUMBER, B.ACCOUNT_STATUS
           FROM IFRS.IFRS_MASTER_ACCOUNT B
           WHERE DOWNLOAD_DATE = V_CURRDATE
           GROUP BY B.ACCOUNT_NUMBER, B.ACCOUNT_STATUS, DOWNLOAD_DATE) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DATA_SOURCE IN ('PBMM', 'ILS'))
    WHEN MATCHED
        THEN
        UPDATE
        SET WRITEOFF_FLAG =
                CASE
                    WHEN WRITEOFF_DATE IS NOT NULL AND A.DATA_SOURCE = 'PBMM'
                        THEN 1
                    WHEN WRITEOFF_DATE IS NOT NULL AND A.DATA_SOURCE = 'ILS' AND A.Outstanding > 0
                        THEN 1
                    ELSE 0 END
        WHERE A.DATA_SOURCE IN ('PBMM', 'ILS');
    /*
    UPDATE SET
       WRITEOFF_FLAG =
          CASE WHEN WRITEOFF_DATE IS NOT NULL THEN 1 ELSE 0 END
            WHERE A.DATA_SOURCE IN ('PBMM', 'ILS');

     */
    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE WRITEOFF_FLAG');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --IS_IMPAIRED
    -----------------------------------------------------------------------------------------------------------------
    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT DATA_SOURCE, PRD_CODE, IS_IMPAIRED
           FROM IFRS.IFRS_MASTER_PRODUCT_PARAM) B
    ON (A.DATA_SOURCE = B.DATA_SOURCE
        AND A.PRODUCT_CODE = B.PRD_CODE
        AND B.DATA_SOURCE <> 'LIMIT')
    WHEN MATCHED
        THEN
        UPDATE
        SET A.IS_IMPAIRED = B.IS_IMPAIRED
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET IS_IMPAIRED = 1
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'LIMIT';

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --GET COMMITED_FLAG
    -----------------------------------------------------------------------------------------------------------------

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT ACCOUNT_NUMBER, COMMITTED_FLAG
           FROM IFRS.IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_dATE = V_CURRDATE
             AND DATA_SOURCE = 'LIMIT') B
    ON (A.FACILITY_NUMBER = B.ACCOUNT_NUMBER
        AND A.DOWNLOAD_dATE = V_CURRDATE
        AND A.DATA_SOURCE IN ('ILS', 'BTRD', 'PBMM'))
    WHEN MATCHED
        THEN
        UPDATE SET A.COMMITTED_FLAG = B.COMMITTED_FLAG;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --GET ACCOUNT_STATUS
    -----------------------------------------------------------------------------------------------------------------

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET ACCOUNT_STATUS = 'A'
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE IN ('PBMM', 'KTP');

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET ACCOUNT_STATUS = 'W'
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'ILS'
      AND BI_COLLECTABILITY = 'C'
      AND OUTSTANDING > 0;

    COMMIT;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --GET SECTOR COMMODITY
    -----------------------------------------------------------------------------------------------------------------
    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT *
           FROM IFRS.IFRS_STG_INDUSTRY_DIMENSION
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.RESERVED_VARCHAR_10 = B.SEKON_CD
        AND A.DOWNLOAD_DATE = V_CURRDATE)
    WHEN MATCHED
        THEN
        UPDATE SET A.RESERVED_VARCHAR_11 = B.COMMODITY_CD;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --GET PREVIOUS RESERVED_DATE_6,7,8,3, IMPAIRED_FLAG, EIR
    -----------------------------------------------------------------------------------------------------------------

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DOWNLOAD_DATE,
                  MASTERID,
                  ACCOUNT_NUMBER,
                  RESERVED_DATE_6,
                  RESERVED_DATE_7,
                  RESERVED_DATE_8,
                  IMPAIRED_FLAG,
                  EIR,
                  RESERVED_DATE_3
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_PREVDATE
             AND DATA_SOURCE IN ('CRD', 'ILS')) B
    ON (A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DATA_SOURCE IN ('CRD', 'ILS'))
    WHEN MATCHED
        THEN
        UPDATE
        SET A.RESERVED_DATE_6 =
                CASE
                    WHEN A.RESERVED_DATE_6 IS NULL THEN B.RESERVED_DATE_6
                    ELSE NULL
                    END,
            A.RESERVED_DATE_7 =
                CASE
                    WHEN A.RESERVED_DATE_7 IS NULL THEN B.RESERVED_DATE_7
                    ELSE NULL
                    END,
            A.RESERVED_DATE_8 =
                CASE
                    WHEN A.RESERVED_DATE_8 IS NULL THEN B.RESERVED_DATE_8
                    ELSE NULL
                    END,
            A.RESERVED_DATE_3 =
                CASE
                    WHEN A.RESERVED_DATE_3 IS NULL THEN B.RESERVED_DATE_3
                    ELSE A.RESERVED_DATE_3
                    END,
            A.IMPAIRED_FLAG   = B.IMPAIRED_FLAG,
            A.EIR             = B.EIR;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --BTB_FLAG , OUTSTANDING_WO, DEFAULT_DATE , EARLY_PAYMENT_FLAG(RESERVED_FLAG_1), POCI_FLAG, DAY_PAST_DUE, NPL_FLAG,
    --TENOR, BRANCH_CODE_OPEN, STAFF_LOAN_FLAG, RESERVED_DATE_6 - FIRST TIME DPD 30
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE ANOTHER');

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESERVED_DATE_3 =
            CASE
                WHEN RESERVED_DATE_3 IS NULL
                    THEN
                    CASE
                        WHEN RATING_CODE >= 5 THEN DOWNLOAD_DATE
                        ELSE RESERVED_DATE_3
                        END
                ELSE
                    RESERVED_DATE_3
                END,
        RESERVED_DATE_7 =
            CASE
                WHEN RESERVED_DATE_7 IS NULL
                    THEN
                    CASE
                        WHEN RATING_CODE >= 2 THEN DOWNLOAD_DATE
                        ELSE RESERVED_DATE_7
                        END
                ELSE
                    RESERVED_DATE_7
                END,
        RESERVED_DATE_6 =
            CASE
                WHEN RESERVED_DATE_6 IS NULL
                    THEN
                    CASE
                        WHEN RATING_CODE >= 3 THEN DOWNLOAD_DATE
                        ELSE RESERVED_DATE_6
                        END
                ELSE
                    RESERVED_DATE_6
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'CRD';

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET DAY_PAST_DUE          =
            CASE
                WHEN DPD_START_DATE IS NULL
                    OR (PRODUCT_CODE IN ('310',
                                         '311',
                                         '313',
                                         '316',
                                         '312',
                                         '314')
                        AND DATA_SOURCE = 'ILS'
                        AND BI_COLLECTABILITY < '3'
                        AND DPD_START_DATE BETWEEN V_DATE
                            AND LAST_DAY(V_CURRDATE))
                    THEN
                    0
                ELSE
                    DOWNLOAD_DATE - DPD_START_DATE
                END,
        ORIGINAL_DAY_PAST_DUE =
            CASE
                WHEN DPD_START_DATE IS NULL
                    OR (PRODUCT_CODE IN ('310',
                                         '311',
                                         '313',
                                         '316',
                                         '312',
                                         '314')
                        AND DATA_SOURCE = 'ILS'
                        AND BI_COLLECTABILITY < '3'
                        AND DPD_START_DATE BETWEEN V_DATE
                            AND LAST_DAY(V_CURRDATE))
                    THEN
                    0
                ELSE
                    DOWNLOAD_DATE - DPD_START_DATE
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    -- 600083179 - REGLA - Perubahan Pembacaan BTB Flag utk Secured Personal Loan
    -- Perubahan BTB_FLAG sebelumnya membaca USER_CODE_ILS_1 = 8,9 menjadi 7,8,9

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET BTB_FLAG          =
            CASE WHEN RESERVED_VARCHAR_4 IN ('7', '8', '9') THEN 1 ELSE 0 END,
        OUTSTANDING_WO    =
            CASE WHEN WRITEOFF_FLAG = 1 THEN OUTSTANDING ELSE 0 END,
        DEFAULT_DATE      =
            CASE
                WHEN RESERVED_DATE_3 = NPL_DATE THEN RESERVED_DATE_3
                ELSE NPL_DATE
                END,
        RESERVED_FLAG_1   =
            CASE
                WHEN OUTSTANDING = 0 OR RESERVED_VARCHAR_1 = 'PREPAYMENT'
                    THEN
                    1
                ELSE
                    0
                END,
        POCI_FLAG         =
            CASE
                WHEN LOAN_START_DATE = DOWNLOAD_DATE
                         AND BI_COLLECTABILITY >= 3
                         AND DAY_PAST_DUE > 180
                    OR (ACCOUNT_NUMBER IN
                        (SELECT ACCOUNT_NUMBER
                         FROM IFRS.TBLU_POCI
                         WHERE DOWNLOAD_DATE = V_CURRDATE))
                    THEN
                    1
                ELSE
                    0
                END, --FIFI
        NEXT_PAYMENT_DATE =
            CASE
                WHEN NEXT_INT_PAYMENT_DATE < NEXT_PAYMENT_DATE
                    THEN
                    NEXT_INT_PAYMENT_DATE
                ELSE
                    CASE
                        WHEN EXTRACT(YEAR FROM NEXT_PAYMENT_DATE) = '2999'
                            THEN
                            LOAN_DUE_DATE
                        ELSE
                            NEXT_PAYMENT_DATE
                        END
                END,
        LAST_PAYMENT_DATE =
            CASE
                WHEN LAST_PAYMENT_DATE IS NULL THEN RESERVED_DATE_9
                ELSE LAST_PAYMENT_DATE
                END,
        NPL_FLAG          =
            CASE WHEN BI_COLLECTABILITY IN ('3', '4', '5') THEN 1 ELSE 0 END,
        TENOR             = CASE WHEN TENOR = 0 THEN 1 ELSE TENOR END,
        BRANCH_CODE_OPEN  = BRANCH_CODE,
        STAFF_LOAN_FLAG   =
            CASE
                WHEN PRODUCT_CODE IN (SELECT PRD_CODE
                                      FROM IFRS.IFRS_MASTER_PRODUCT_PARAM
                                      WHERE STAFF_LOAN_IND = 1)
                    THEN
                    1
                ELSE
                    0
                END, --FIFI
        RESERVED_DATE_6   =
            CASE
                WHEN RESERVED_DATE_6 IS NULL
                    THEN
                    CASE
                        WHEN DAY_PAST_DUE >= 30 THEN DOWNLOAD_DATE
                        ELSE NULL
                        END
                ELSE
                    RESERVED_DATE_6
                END,
        RESERVED_DATE_7   =
            CASE
                WHEN RESERVED_DATE_7 IS NULL
                    THEN
                    CASE
                        WHEN BI_COLLECTABILITY IN ('2',
                                                   '3',
                                                   '4',
                                                   '5',
                                                   'C')
                            THEN
                            DOWNLOAD_DATE
                        ELSE
                            NULL
                        END
                ELSE
                    RESERVED_DATE_7
                END,
        RESERVED_DATE_8   =
            CASE
                WHEN RESERVED_DATE_8 IS NULL
                    THEN
                    CASE
                        WHEN ACCOUNT_STATUS = 'C' THEN DOWNLOAD_DATE
                        ELSE NULL
                        END
                ELSE
                    RESERVED_DATE_8
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'ILS';

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE ANOTHER');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --CKPN_FLAG - RESERVED_FLAG_6
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE CKPN_FLAG');

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESERVED_FLAG_6 =
            CASE
                WHEN RESERVED_VARCHAR_9 LIKE '%H%'
                    THEN
                    1
                 ELSE
--                     CASE
--                         WHEN RESERVED_VARCHAR_2 IN ('S', 'K')
--                                  AND DAY_PAST_DUE >= 365
--                             OR (PRODUCT_CODE IN ('300',
--                                                  '301',
--                                                  '302',
--                                                  '303',
--                                                  '305',
--                                                  '306',
--                                                  '330',
--                                                  '610',
--                                                  '322')
--                                 AND DAY_PAST_DUE >= 365)
--                             OR (PRODUCT_CODE IN ('310',
--                                                  '311',
--                                                  '313',
--                                                  '316',
--                                                  '312',
--                                                  '314')
--                                 AND DAY_PAST_DUE >= 210)
--                             OR (PRODUCT_CODE IN ('320', '321', '230')
--                                 AND BRANCH_CODE = '0960'
--                                 AND DAY_PAST_DUE >= 210)
--                             OR (RESERVED_VARCHAR_2 IN ('I', 'O')
--                                 AND RATING_CODE = 9)
--                             THEN
--                             1
--                             ELSE
                        0
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE IN ('CRD', 'ILS', 'PBMM');

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE CKPN_FLAG');

    COMMIT;

    ----------------------------------------------------------------------------------------------------------------
    --PRODUCT_ENTITY DAN BRANCH_CODE
    -----------------------------------------------------------------------------------------------------------------
    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'PRODUCT_ENTITY - BRANCH_CODE');

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET PRODUCT_ENTITY =
            CASE
                WHEN PRODUCT_ENTITY IS NULL THEN 'C'
                ELSE PRODUCT_ENTITY
                END,
        BRANCH_CODE    =
            CASE WHEN DATA_SOURCE IN ('PBMM', 'KTP') THEN '0998' END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'PRODUCT_ENTITY - BRANCH_CODE');

    COMMIT;


    /*
    -----------------------------------------------------------------------------------------------------------------
    --INTEREST_PAYMENT_TERM
    -----------------------------------------------------------------------------------------------------------------
        MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
        USING (SELECT B.DOWNLOAD_DATE, B.ACCOUNT_NUMBER,B.FREQUENCY
                FROM IFRS.IFRS_MASTER_PAYMENT_SETTING B WHERE B.DOWNLOAD_DATE = V_CURRDATE GROUP BY B.ACCOUNT_NUMBER,B.FREQUENCY,B.DOWNLOAD_DATE
              ) B
        ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE = 'ILS')
        WHEN MATCHED THEN
        UPDATE
        SET A.INTEREST_PAYMENT_TERM = LTRIM(RTRIM(B.FREQUENCY));
    COMMIT;*/

    -----------------------------------------------------------------------------------------------------------------
    --EXCHANGE_RATE
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE EXCHANGE_RATE');

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT DOWNLOAD_DATE, CURRENCY, MAX(RATE_AMOUNT) RA
           FROM IFRS.IFRS_MASTER_EXCHANGE_RATE
           WHERE DOWNLOAD_DATE = V_CURRDATE
           GROUP BY DOWNLOAD_DATE, CURRENCY) B
    ON (A.CURRENCY = B.CURRENCY
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
    WHEN MATCHED
        THEN
        UPDATE SET A.EXCHANGE_RATE = B.RA;

    COMMIT;

    /*
    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET EXCHANGE_RATE = CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE EXCHANGE_RATE END
    WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'ILS';
    COMMIT;
    */

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE EXCHANGE_RATE');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --REVOLVING_FLAG , AMORT_TYPE
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE REVOLVING_FLAG');

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT PRD_CODE, DATA_SOURCE, REPAY_TYPE_VALUE
           FROM IFRS.IFRS_MASTER_PRODUCT_PARAM) B
    ON (A.PRODUCT_CODE = B.PRD_CODE AND A.DATA_SOURCE = B.DATA_SOURCE)
    WHEN MATCHED
        THEN
        UPDATE
        SET A.REVOLVING_FLAG =
                CASE WHEN B.REPAY_TYPE_VALUE = 'REV' THEN 1 ELSE 0 END
        WHERE DOWNLOAD_DATE = V_CURRDATE
          AND A.DATA_SOURCE IN ('ILS',
                                'BTRD',
                                'KTP',
                                'PBMM');

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET REVOLVING_FLAG =
            CASE
                WHEN DATA_SOURCE = 'BTRD'
                    OR (DATA_SOURCE = 'ILS'
                        AND PRODUCT_CODE IN ('BGL',
                                             'BSL',
                                             'BPC',
                                             'BGP',
                                             'BGB'))
                    THEN
                    0
                ELSE
                    REVOLVING_FLAG
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET REVOLVING_FLAG =
            CASE WHEN PRODUCT_CODE LIKE 'B%' THEN 0 ELSE REVOLVING_FLAG END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET AMORT_TYPE = CASE WHEN REVOLVING_FLAG = 1 THEN 'SL' ELSE 'EIR' END
    WHERE DOWNLOAD_dATE = V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE REVOLVING_FLAG');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    -- PLAFOND
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE PLAFOND');

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DOWNLOAD_DATE, INITIAL_OUTSTANDING, ACCOUNT_NUMBER
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.FACILITY_NUMBER = B.ACCOUNT_NUMBER
        AND A.DATA_SOURCE IN ('PBMM', 'ILS')
        AND A.DOWNLOAD_DATE = V_CURRDATE)
    WHEN MATCHED
        THEN
        UPDATE
        SET A.PLAFOND = B.INITIAL_OUTSTANDING
        WHERE A.DATA_SOURCE IN ('PBMM', 'ILS');

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET PLAFOND =
            CASE
                WHEN FACILITY_NUMBER IS NULL THEN OUTSTANDING
                ELSE PLAFOND
                END
    where DATA_SOURCE!='CRD'
    ;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE PLAFOND');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    -- PRODUCT_CODE - BTRD
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE PRODUCT_CODE BTRD');

    COMMIT;


    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET PRODUCT_CODE =
            CASE
                WHEN PRODUCT_CODE = 'SAC'
                    AND SUBSTR(ACCOUNT_NUMBER, 1, 1) = 'L'
                    THEN
                    'SAC-L'
                WHEN PRODUCT_CODE = 'SAC'
                    AND SUBSTR(ACCOUNT_NUMBER, 1, 1) = 'E'
                    THEN
                    'SAC-E'
                WHEN PRODUCT_CODE = 'LAC'
                    AND SUBSTR(ACCOUNT_NUMBER, 1, 1) = 'L'
                    THEN
                    'LAC-L'
                WHEN PRODUCT_CODE = 'LAC'
                    AND SUBSTR(ACCOUNT_NUMBER, 1, 1) = 'E'
                    THEN
                    'LAC-E'
                ELSE
                    PRODUCT_CODE
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'BTRD';

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET PRODUCT_CODE =
            CASE
                WHEN PRODUCT_CODE IS NULL THEN PRODUCT_TYPE
                ELSE PRODUCT_CODE
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'BTRD';

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE PRODUCT_CODE BTRD');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    -- RESERVED_VARCHAR_1 AND RESERVED_VARCHAR_12
    -----------------------------------------------------------------------------------------------------------------

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESERVED_VARCHAR_1  =
            CASE
                WHEN TRIM(RESERVED_VARCHAR_1) IS NULL THEN 'X'
                ELSE RESERVED_VARCHAR_1
                END,
        RESERVED_VARCHAR_12 =
            CASE
                WHEN TRIM(RESERVED_VARCHAR_12) IS NULL THEN 'X'
                ELSE RESERVED_VARCHAR_12
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'KTP';

    COMMIT;


    -----------------------------------------------------------------------------------------------------------------
    --UPDATE MASTER_PAYMENT_SETTING
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE MASTER_PAYMENT');

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT MISSING PAYMENT SETTING');

    COMMIT;

    IFRS.SP_IFRS_PAYMENT_SETTING_PROD;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'INSERT MISSING PAYMENT SETTING');

    COMMIT;

    UPDATE IFRS.IFRS_MASTER_PAYMENT_SETTING
    SET DATE_END =
            CASE
                WHEN FREQUENCY = 'M'
                    THEN
                    CASE
                        WHEN FN_ISDATE(
                                     (EXTRACT(DAY FROM DATE_START)
                                         || '-'
                                         || TO_CHAR(
                                              ADD_MONTHS(
                                                      DATE_START,
                                                      (TIMES_ORG * INCREMENTS) - 1),
                                              'MM-YYYY')),
                                     'DD-MM-YYYY') = 1
                            THEN
                            TO_DATE(
                                    (EXTRACT(DAY FROM DATE_START)
                                        || '-'
                                        || TO_CHAR(
                                             ADD_MONTHS(DATE_START,
                                                        (TIMES_ORG * INCREMENTS) - 1),
                                             'MM-YYYY')),
                                    'DD-MM-YYYY')
                        ELSE
                            ADD_MONTHS(DATE_START,
                                       (TIMES_ORG * INCREMENTS) - 1)
                        END
                ELSE
                    ADD_MONTHS(DATE_START, (TIMES_ORG * INCREMENTS) - 1)
                END,
        PMT_DATE =
            CASE
                WHEN FREQUENCY = 'M'
                    THEN
                    EXTRACT(DAY FROM DATE_START)
                ELSE
                    EXTRACT(
                            DAY FROM ADD_MONTHS(DATE_START,
                                                (TIMES_ORG * INCREMENTS) - 1))
                END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.TMP_MAX_PAYSET';

    INSERT INTO IFRS.TMP_MAX_PAYSET
    SELECT MASTERID, MAX(DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
          FROM IFRS.IFRS_MASTER_PAYMENT_SETTING
          WHERE MASTERID IN
                (SELECT A3.MASTERID
                 FROM IFRS.IFRS_TRANSACTION_DAILY A3
                          LEFT JOIN IFRS.IFRS_MASTER_PAYMENT_SETTING B3
                                    ON A3.DOWNLOAD_DATE = B3.DOWNLOAD_DATE
                                        AND A3.MASTERID = B3.MASTERID
                 WHERE A3.DOWNLOAD_DATE = V_CURRDATE
                   AND B3.MASTERID IS NULL)
          GROUP BY MASTERID;

    COMMIT;

    INSERT INTO IFRS.IFRS_MASTER_PAYMENT_SETTING (DOWNLOAD_DATE,
                                             MASTERID,
                                             ACCOUNT_NUMBER,
                                             COMPONENT_TYPE,
                                             COMPONENT_STATUS,
                                             FREQUENCY,
                                             INCREMENTS,
                                             AMOUNT,
                                             TIMES_ORG,
                                             TIMES_USED,
                                             DATE_START,
                                             DATE_END,
                                             PMT_DATE,
                                             IS_INSERTED)
    SELECT V_CURRDATE DOWNLOAD_DATE,
           A.MASTERID,
           ACCOUNT_NUMBER,
           COMPONENT_TYPE,
           COMPONENT_STATUS,
           FREQUENCY,
           INCREMENTS,
           AMOUNT,
           TIMES_ORG,
           TIMES_USED,
           DATE_START,
           DATE_END,
           PMT_DATE,
           IS_INSERTED
    FROM IFRS.IFRS_MASTER_PAYMENT_SETTING A
             JOIN
         ( IFRS.TMP_MAX_PAYSET) B
         ON A.DOWNLOAD_DATE = B.MAX_DOWNLOAD_DATE
             AND A.MASTERID = B.MASTERID;

    COMMIT;

    DELETE IFRS.IFRS_MASTER_PAYMENT_SETTING
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND MASTERID = 0;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE MASTER_PAYMENT');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE CC
    -----------------------------------------------------------------------------------------------------------------
    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET INTEREST_RATE = INTEREST_RATE * 100
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND INTEREST_RATE < 1
      AND DATA_SOURCE = 'CRD';

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE LIMIT
    -----------------------------------------------------------------------------------------------------------------

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE LIMIT');

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESERVED_FLAG_1 = NULL
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'LIMIT';

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESERVED_FLAG_1 = '0'
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND DATA_SOURCE = 'LIMIT'
      AND ACCOUNT_NUMBER NOT IN
          (SELECT DISTINCT NVL(FACILITY_NUMBER, 0)
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_dATE = V_CURRDATE
             AND DATA_SOURCE IN ('PBMM', 'ILS')
             AND ACCOUNT_STATUS <> 'C')
      AND PRODUCT_CODE LIKE 'K%'
      AND PRODUCT_CODE <> 'KFX'
      AND RESERVED_AMOUNT_14 <> 0
      AND RESERVED_VARCHAR_18 <> 'A';

    COMMIT;

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT SUB_SEGMENT, AVG(NVL(EIR, INTEREST_RATE)) IR
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE
           GROUP BY SUB_SEGMENT) B
    ON (A.SUB_SEGMENT = B.SUB_SEGMENT
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DATA_SOURCE = 'LIMIT')
    WHEN MATCHED
        THEN
        UPDATE SET A.INTEREST_RATE = B.IR;

    COMMIT;

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET CR_STAGE = 1
    WHERE DATA_SOURCE = 'LIMIT'
      AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE LIMIT');

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE BI_COLLECTABILITY FOR BTRD AND KTP BY CUSTNO
    -----------------------------------------------------------------------------------------------------------------

    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
    USING (SELECT DISTINCT DOWNLOAD_DATE,
                           CUSTOMER_NUMBER,
                           MAX(BI_COLLECTABILITY) BI_COLLECTABILITY
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE
             AND DATA_SOURCE = 'ILS'
             AND ACCOUNT_STATUS = 'A'
             AND RESERVED_VARCHAR_2 IN ('M', 'L')
             AND PRODUCT_CODE NOT LIKE '3%'
             AND PRODUCT_CODE <> '610'
             AND PRODUCT_CODE NOT LIKE 'B%'
           GROUP BY DOWNLOAD_DATE, CUSTOMER_NUMBER) B
    ON (A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.DATA_SOURCE IN ('BTRD', 'KTP', 'PBMM'))
    WHEN MATCHED
        THEN
        UPDATE SET A.BI_COLLECTABILITY = B.BI_COLLECTABILITY;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE RESTRUCTURE_FLAG - COVID
    -----------------------------------------------------------------------------------------------------------------

    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET RESTRUCTURE_FLAG =
            CASE WHEN RESERVED_FLAG_4 = 1 THEN 1 ELSE RESTRUCTURE_FLAG END;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE KOLEK DAN GOLDEB BOND TIPHONE BASED ON PERMINTAAN ARK --> RAL CHANGED : 20210831
    -----------------------------------------------------------------------------------------------------------------
    UPDATE IFRS.GTMP_IFRS_MASTER_ACCOUNT
    SET BI_COLLECTABILITY  = '5',
        RESERVED_VARCHAR_2 = 'L'
    WHERE 1 = 1
      AND CUSTOMER_NUMBER = '00023156064'
      AND ACCOUNT_NUMBER = '94118'
      AND DATA_SOURCE = 'KTP';

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --LAST_PAYMENT_DATE
    -----------------------------------------------------------------------------------------------------------------
    --SP_UPDATE_LAST_PAYMENT_DATE;
    -----------------------------------------------------------------------------------------------------------------
    --UPDATE WORSTCASE FLAG
    -----------------------------------------------------------------------------------------------------------------
    /*--FIFI
    MERGE INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT A
          USING
          (
              SELECT MAX(DOWNLOAD_DATE), CUSTOMER_NUMBER
                FROM IFRS.TBLU_WORSTCASE_LIST
                GROUP BY CUSTOMER_NUMBER
                HAVING MAX(DOWNLOAD_DATE) <= V_CURRDATE
          ) B
          ON (A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
      WHEN MATCHED THEN UPDATE SET
          A.IMPAIRED_FLAG = 'W';
    COMMIT;

    INSERT  INTO IFRS.IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE WORSTCASE'); COMMIT;

    */
    -----------------------------------------------------------------------------------------------------------------
    --UPDATE IFRS.IFRS_MASTER_ACCOUNT
    -----------------------------------------------------------------------------------------------------------------
    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'START',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE IMA');

    COMMIT;

    MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
    USING (SELECT *
           FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
           WHERE DOWNLOAD_DATE = V_CURRDATE) B
    ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
    WHEN MATCHED
        THEN
        UPDATE
        SET A.MARKET_RATE           = B.MARKET_RATE,
            A.WRITEOFF_FLAG         = B.WRITEOFF_FLAG,
            A.RESERVED_FLAG_6       = B.RESERVED_FLAG_6,
            A.BTB_FLAG              = B.BTB_FLAG,
            A.OUTSTANDING_WO        = B.OUTSTANDING_WO,
            A.DEFAULT_DATE          = B.DEFAULT_DATE,
            A.RESERVED_FLAG_1       = B.RESERVED_FLAG_1,
            A.POCI_FLAG             = B.POCI_FLAG,
            --                                                  A.NEXT_PAYMENT_DATE   = B.NEXT_PAYMENT_DATE,
            A.DAY_PAST_DUE          = B.DAY_PAST_DUE,
            A.NPL_FLAG              = B.NPL_FLAG,
            A.TENOR                 = B.TENOR,
            A.STAFF_LOAN_FLAG       = B.STAFF_LOAN_FLAG,
            A.EXCHANGE_RATE         = B.EXCHANGE_RATE,
            A.REVOLVING_FLAG        = B.REVOLVING_FLAG,
            A.PLAFOND               = B.PLAFOND,
            A.RESERVED_DATE_6       = B.RESERVED_DATE_6,
            A.LAST_PAYMENT_DATE     = B.LAST_PAYMENT_DATE,
            A.RESERVED_DATE_7       = B.RESERVED_DATE_7,
            A.RESERVED_DATE_8       = B.RESERVED_DATE_8,
            A.SEGMENT_RULE_ID       = B.SEGMENT_RULE_ID,
            A.GROUP_SEGMENT         = B.GROUP_SEGMENT,
            A.SEGMENT               = B.SEGMENT,
            A.SUB_SEGMENT           = B.SUB_SEGMENT,
            A.PREPAYMENT_RULE_ID    = B.PREPAYMENT_RULE_ID,
            A.PREPAYMENT_SEGMENT    = B.PREPAYMENT_SEGMENT,
            A.IMPAIRED_FLAG         = B.IMPAIRED_FLAG,
            A.ACCOUNT_STATUS        = B.ACCOUNT_STATUS,
            A.RATING_CODE           = B.RATING_CODE,
            A.RESERVED_VARCHAR_22   = B.RESERVED_VARCHAR_22, --RATING_CODE TURUNAN
            A.PRODUCT_CODE          = B.PRODUCT_CODE,
            A.RESERVED_VARCHAR_1    = B.RESERVED_VARCHAR_1,
            A.IS_IMPAIRED           = B.IS_IMPAIRED,
            A.COMMITTED_FLAG        = B.COMMITTED_FLAG,
            A.CR_STAGE              = B.CR_STAGE,
            A.INTEREST_RATE         = B.INTEREST_RATE,
            A.RESERVED_DATE_3       = B.RESERVED_DATE_3,
            A.BI_COLLECTABILITY     = B.BI_COLLECTABILITY,
            A.ORIGINAL_DAY_PAST_DUE = B.ORIGINAL_DAY_PAST_DUE,
            A.RESERVED_VARCHAR_11   = B.RESERVED_VARCHAR_11,
            A.RESTRUCTURE_FLAG      = B.RESTRUCTURE_FLAG,
            A.RESERVED_VARCHAR_2    = B.RESERVED_VARCHAR_2;

    COMMIT;

    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            'UPDATE IMA');

    COMMIT;


    INSERT INTO IFRS.IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                 DTM,
                                 OPS,
                                 PROCNAME,
                                 REMARK)
    VALUES (V_CURRDATE,
            SYSTIMESTAMP,
            'END',
            'SP_INITIAL_UPDATE_ILS_PROD',
            '');

    COMMIT;

    delete IFRS.IFRS_STATISTIC where DOWNLOAD_DATE=V_CURRDATE and SP_NAME='SP_INITIAL_UPDATE_ILS' AND COUNTER=0;

    COMMIT;
END;