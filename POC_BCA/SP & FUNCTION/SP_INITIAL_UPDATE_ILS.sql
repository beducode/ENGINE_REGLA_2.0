CREATE OR REPLACE PROCEDURE SP_INITIAL_UPDATE_ILS
AS
V_CURRDATE DATE;
V_PREVDATE DATE;
V_SPNAME VARCHAR2(100);

    BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_UPDATE_LOG';

            EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';
            /*
            DELETE  FROM IFRS_STATISTIC
            WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND PRC_NAME = 'INIT';
            COMMIT;
            */
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS_DATE_DAY1;
            SELECT PREVDATE INTO V_PREVDATE FROM IFRS_DATE_DAY1;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,''); COMMIT;

            SP_IFRS_MASTERID;

                UPDATE IFRS_MASTER_ACCOUNT_MONTHLY
                SET BRANCH_CODE = CASE WHEN LENGTH(BRANCH_CODE) > 4 THEN SUBSTR(BRANCH_CODE,4,4) ELSE BRANCH_CODE END
                WHERE DOWNLOAD_DATE = V_CURRDATE
                AND DATA_SOURCE IN ('ILS','CRD','LIMIT');COMMIT;


                MERGE INTO IFRS_MASTER_ACCOUNT_MONTHLY A
                USING(SELECT DISTINCT DATA_SOURCE, PRD_CODE,PRD_TYPE,PRD_GROUP,RESERVED_VARCHAR_1 FROM IFRS_MASTER_PRODUCT_PARAM)B
                ON (TRIM(A.PRODUCT_CODE) = TRIM(B.PRD_CODE) AND A.DATA_SOURCE = B.DATA_SOURCE AND A.DOWNLOAD_DATE = V_CURRDATE)
                WHEN MATCHED THEN UPDATE
                SET RESERVED_VARCHAR_27 = B.RESERVED_VARCHAR_1,
                PRODUCT_GROUP = B.PRD_GROUP,
                PRODUCT_TYPE = B.PRD_TYPE;COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --TRANSACTION_DAILY
            -----------------------------------------------------------------------------------------------------------------

            MERGE INTO IFRS_TRANSACTION_DAILY A
            USING (SELECT DOWNLOAD_DATE,MASTERID,LOAN_DUE_DATE, FACILITY_NUMBER, CUSTOMER_NUMBER, BRANCH_CODE, DATA_SOURCE, PRODUCT_TYPE, PRODUCT_CODE, REVOLVING_FLAG
            FROM IFRS_MASTER_ACCOUNT_MONTHLY WHERE DOWNLOAD_DATE = V_CURRDATE) B
            ON (A.DOWNLOAD_DATE = V_CURRDATE AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
            WHEN MATCHED THEN UPDATE
            SET     A.MATURITY_DATE = B.LOAN_DUE_DATE,
                    A.FACILITY_NUMBER = B.FACILITY_NUMBER,
                    A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER,
                    A.BRANCH_CODE = B.BRANCH_CODE,
                    A.DATA_SOURCE = B.DATA_SOURCE,
                    A.PRD_TYPE = B.PRODUCT_TYPE,
                    A.PRD_CODE = B.PRODUCT_CODE,
                    A.REVOLVING_FLAG = B.REVOLVING_FLAG;
            COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --GTMP_MASTER_ACCOUNT
            -----------------------------------------------------------------------------------------------------------------
            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'INSERT INTO TMP'); COMMIT;

            INSERT INTO GTMP_IFRS_MASTER_ACCOUNT SELECT * FROM IFRS_MASTER_ACCOUNT_MONTHLY WHERE DOWNLOAD_DATE IN (V_CURRDATE,V_PREVDATE); COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'INSERT INTO TMP'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --RESET GTMP_MASTER_ACCOUNT
            -----------------------------------------------------------------------------------------------------------------
            UPDATE GTMP_IFRS_MASTER_ACCOUNT
                SET
                MARKET_RATE         = NULL,
                WRITEOFF_FLAG       = NULL,
                RESERVED_FLAG_6     = NULL
                WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'ILS';
            COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --MARKET_RATE
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE MARKET_RATE'); COMMIT;

        MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
             USING (
             SELECT D.* FROM IFRS_MASTER_MARKETRATE_PARAM D
             JOIN (SELECT MAX(EFF_DATE)EFF_DATE, PRD_CODE, MAX(CREATEDDATE)CREATEDDATE FROM IFRS_MASTER_MARKETRATE_PARAM
                    WHERE EFF_DATE <= V_CURRDATE
             GROUP BY PRD_CODE) F
             ON D.EFF_DATE = F.EFF_DATE AND D.PRD_CODE = F.PRD_CODE AND D.CREATEDDATE = F.CREATEDDATE
             WHERE D.EFF_DATE <= V_CURRDATE
             ) B
                ON (    A.DOWNLOAD_DATE >= B.EFF_DATE
                    AND A.DOWNLOAD_DATE = V_CURRDATE
                    AND (A.PRODUCT_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
                    AND (A.CURRENCY = B.CCY OR B.CCY = 'ALL'))
        WHEN MATCHED
        THEN
           UPDATE SET A.MARKET_RATE = B.MKT_RATE;

        COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END
            ' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE MARKET_RATE'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --WRITEOFF_FLAG
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE WRITEOFF_FLAG'); COMMIT;

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT B.DOWNLOAD_DATE, B.ACCOUNT_NUMBER,B.ACCOUNT_STATUS
                    FROM IFRS_MASTER_ACCOUNT_MONTHLY B WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY B.ACCOUNT_NUMBER,B.ACCOUNT_STATUS,DOWNLOAD_DATE
                  ) B
            ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE = 'ILS')
            WHEN MATCHED THEN
            UPDATE
            SET WRITEOFF_FLAG =     CASE    WHEN WRITEOFF_DATE IS NOT NULL
                                            THEN 1
                                            ELSE 0
                                    END
                                    WHERE A.DATA_SOURCE = 'ILS';

            COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE WRITEOFF_FLAG'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --IS_IMPAIRED
            -----------------------------------------------------------------------------------------------------------------
            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT DISTINCT DATA_SOURCE, PRD_CODE, IS_IMPAIRED FROM IFRS_MASTER_PRODUCT_PARAM) B
            ON (A.DATA_SOURCE = B.DATA_SOURCE AND A.PRODUCT_CODE = B.PRD_CODE AND B.DATA_SOURCE <> 'LIMIT')
            WHEN MATCHED THEN UPDATE
                SET A.IS_IMPAIRED = B.IS_IMPAIRED
                WHERE A.DOWNLOAD_DATE = V_CURRDATE;
                COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT SET IS_IMPAIRED = 1 WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'LIMIT';COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --GET COMMITED_FLAG
            -----------------------------------------------------------------------------------------------------------------

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT ACCOUNT_NUMBER,COMMITTED_FLAG FROM IFRS_MASTER_ACCOUNT_MONTHLY WHERE DOWNLOAD_dATE = V_CURRDATE AND DATA_SOURCE = 'LIMIT')B
            ON (A.FACILITY_NUMBER = B.ACCOUNT_NUMBER AND A.DOWNLOAD_dATE = V_CURRDATE AND A.DATA_SOURCE IN ('ILS','BTRD'))
            WHEN MATCHED THEN UPDATE
            SET A.COMMITTED_FLAG = B.COMMITTED_FLAG;COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --GET ACCOUNT_STATUS
            -----------------------------------------------------------------------------------------------------------------

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
               SET ACCOUNT_STATUS = 'A'
             WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'KTP';

             UPDATE GTMP_IFRS_MASTER_ACCOUNT
                SET ACCOUNT_STATUS = 'W'
                WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'ILS' AND BI_COLLECTABILITY = 'C';COMMIT;

            COMMIT;
            -----------------------------------------------------------------------------------------------------------------
            --GET PREVIOUS RESERVED_DATE_6,7,8,3, IMPAIRED_FLAG, EIR
            -----------------------------------------------------------------------------------------------------------------

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (
                    SELECT DOWNLOAD_DATE, ACCOUNT_NUMBER, RESERVED_DATE_6,RESERVED_DATE_7,RESERVED_DATE_8, IMPAIRED_FLAG, EIR, RESERVED_DATE_3
                      FROM GTMP_IFRS_MASTER_ACCOUNT
                        WHERE DOWNLOAD_DATE = V_PREVDATE AND DATA_SOURCE = 'ILS'
                  ) B
            ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
            WHEN MATCHED THEN UPDATE
            SET A.RESERVED_DATE_6 = CASE WHEN A.RESERVED_DATE_6 IS NULL THEN B.RESERVED_DATE_6 ELSE NULL END,
                A.RESERVED_DATE_7 = CASE WHEN A.RESERVED_DATE_7 IS NULL THEN B.RESERVED_DATE_7 ELSE NULL END,
                A.RESERVED_DATE_8 = CASE WHEN A.RESERVED_DATE_8 IS NULL THEN B.RESERVED_DATE_8 ELSE NULL END,
                A.IMPAIRED_FLAG = B.IMPAIRED_FLAG,
                A.EIR = B.EIR,
                A.RESERVED_DATE_3 = B.RESERVED_DATE_3
            WHERE A.DOWNLOAD_DATE = V_CURRDATE
            AND A.DATA_SOURCE = 'ILS';

            COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --BTB_FLAG , OUTSTANDING_WO, DEFAULT_DATE , EARLY_PAYMENT_FLAG(RESERVED_FLAG_1), POCI_FLAG, DAY_PAST_DUE, NPL_FLAG,
            --TENOR, BRANCH_CODE_OPEN, STAFF_LOAN_FLAG, RESERVED_DATE_6 - FIRST TIME DPD 30
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE ANOTHER'); COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET BTB_FLAG           = CASE WHEN RESERVED_VARCHAR_4 IN ('8', '9') THEN 1 ELSE 0 END,
                OUTSTANDING_WO     = CASE WHEN WRITEOFF_FLAG = 1 THEN OUTSTANDING ELSE 0 END,
                DEFAULT_DATE       = CASE WHEN RESERVED_DATE_3 = NPL_DATE THEN RESERVED_DATE_3 ELSE NPL_DATE END,
                RESERVED_FLAG_1    = CASE WHEN OUTSTANDING = 0 OR RESERVED_VARCHAR_1 = 'PREPAYMENT' THEN 1 ELSE 0 END,
                POCI_FLAG          = CASE WHEN LOAN_START_DATE = DOWNLOAD_DATE AND BI_COLLECTABILITY >= 3 AND DAY_PAST_DUE > 180
                                       OR (ACCOUNT_NUMBER IN (SELECT ACCOUNT_NUMBER FROM TBLU_POCI WHERE DOWNLOAD_DATE = V_CURRDATE)) THEN 1 ELSE 0 END, --FIFI
                NEXT_PAYMENT_DATE  = CASE WHEN NEXT_INT_PAYMENT_DATE < NEXT_PAYMENT_DATE THEN NEXT_INT_PAYMENT_DATE
                                       ELSE
                                          CASE WHEN EXTRACT (YEAR FROM NEXT_PAYMENT_DATE) = '2999'
                                              THEN LOAN_DUE_DATE
                                              ELSE NEXT_PAYMENT_DATE
                                          END
                                    END,
                LAST_PAYMENT_DATE  = CASE WHEN LAST_PAYMENT_DATE IS NULL THEN RESERVED_DATE_9 ELSE LAST_PAYMENT_DATE END,
                DAY_PAST_DUE       = CASE WHEN DPD_START_DATE IS NULL THEN 0 ELSE (DOWNLOAD_DATE - DPD_START_DATE) + 1 END,
                NPL_FLAG           = CASE WHEN BI_COLLECTABILITY IN ('3', '4', '5') THEN 1 ELSE 0 END,
                TENOR              = CASE WHEN TENOR = 0 THEN 1 ELSE TENOR END,
                BRANCH_CODE_OPEN   = BRANCH_CODE,
                STAFF_LOAN_FLAG    = CASE WHEN PRODUCT_CODE IN (SELECT PRD_CODE FROM IFRS_MASTER_PRODUCT_PARAM WHERE STAFF_LOAN_IND = 1) THEN 1 ELSE 0 END, --FIFI
                RESERVED_DATE_6    = CASE WHEN RESERVED_DATE_6 IS NULL THEN
                                      CASE WHEN DAY_PAST_DUE >= 30 THEN DOWNLOAD_DATE ELSE NULL END
                                    ELSE RESERVED_DATE_6 END,
                RESERVED_DATE_7    = CASE WHEN RESERVED_DATE_7 IS null THEN
                                      CASE WHEN BI_COLLECTABILITY IN( '2','3','4','5','C') THEN DOWNLOAD_DATE ELSE NULL END
                                    ELSE RESERVED_DATE_7 END,
                RESERVED_DATE_8    = CASE WHEN RESERVED_DATE_8 IS null THEN
                                        CASE WHEN ACCOUNT_STATUS = 'C' THEN DOWNLOAD_DATE ELSE NULL END
                                    ELSE RESERVED_DATE_8 END
            WHERE DOWNLOAD_DATE  = V_CURRDATE
                AND DATA_SOURCE = 'ILS';

        COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE ANOTHER'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --CKPN_FLAG - RESERVED_FLAG_6
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE CKPN_FLAG'); COMMIT;

        UPDATE GTMP_IFRS_MASTER_ACCOUNT
           SET RESERVED_FLAG_6 =
                  CASE
                     WHEN RESERVED_VARCHAR_9 LIKE '%H%'
                     THEN
                        1
                     ELSE
                        CASE
                           WHEN    RESERVED_VARCHAR_2 IN ('S', 'K') AND DAY_PAST_DUE >= 365
                                OR (    PRODUCT_CODE IN ('300',
                                                         '301',
                                                         '302',
                                                         '303',
                                                         '305',
                                                         '306',
                                                         '330',
                                                         '610',
                                                         '322')
                                    AND DAY_PAST_DUE >= 365)
                                OR (    PRODUCT_CODE IN ('310',
                                                         '311',
                                                         '313',
                                                         '316',
                                                         '312',
                                                         '314')
                                    AND DAY_PAST_DUE >= 210)
                                OR (    PRODUCT_CODE IN ('320', '321', '230')
                                    AND BRANCH_CODE = '0960'
                                    AND DAY_PAST_DUE >= 210)
                                OR (RESERVED_VARCHAR_2 IN ('I', 'O') AND DAY_PAST_DUE >= 210)
                           THEN
                              1
                           ELSE
                              0
                        END
                  END
                  WHERE DOWNLOAD_DATE = V_CURRDATE
                  AND DATA_SOURCE IN ('CRD','ILS');
                  COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE CKPN_FLAG'); COMMIT;

            ----------------------------------------------------------------------------------------------------------------
            --PRODUCT_ENTITY DAN BRANCH_CODE
            -----------------------------------------------------------------------------------------------------------------
            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'PRODUCT_ENTITY - BRANCH_CODE'); COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET PRODUCT_ENTITY = CASE WHEN PRODUCT_ENTITY IS NULL THEN 'C' ELSE PRODUCT_ENTITY END,
                BRANCH_CODE = CASE WHEN DATA_SOURCE = 'KTP' THEN '0998'END
                WHERE DOWNLOAD_DATE = V_CURRDATE;
            COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'PRODUCT_ENTITY - BRANCH_CODE'); COMMIT;


            /*
            -----------------------------------------------------------------------------------------------------------------
            --INTEREST_PAYMENT_TERM
            -----------------------------------------------------------------------------------------------------------------
                MERGE INTO IFRS_MASTER_ACCOUNT_MONTHLY A
                USING (SELECT B.DOWNLOAD_DATE, B.ACCOUNT_NUMBER,B.FREQUENCY
                        FROM IFRS_MASTER_PAYMENT_SETTING B WHERE B.DOWNLOAD_DATE = V_CURRDATE GROUP BY B.ACCOUNT_NUMBER,B.FREQUENCY,B.DOWNLOAD_DATE
                      ) B
                ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE = 'ILS')
                WHEN MATCHED THEN
                UPDATE
                SET A.INTEREST_PAYMENT_TERM = LTRIM(RTRIM(B.FREQUENCY));
            COMMIT;*/

            -----------------------------------------------------------------------------------------------------------------
            --EXCHANGE_RATE
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE EXCHANGE_RATE'); COMMIT;

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT DISTINCT DOWNLOAD_DATE, CURRENCY,MAX(RATE_AMOUNT) RA
                    FROM IFRS_MASTER_EXCHANGE_RATE WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY DOWNLOAD_DATE, CURRENCY
                  ) B
            ON (A.CURRENCY = B.CURRENCY AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
            WHEN MATCHED THEN
            UPDATE
            SET A.EXCHANGE_RATE = B.RA;
            COMMIT;

            /*
            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET EXCHANGE_RATE = CASE WHEN CURRENCY = 'IDR' THEN 1 ELSE EXCHANGE_RATE END
            WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'ILS';
            COMMIT;
            */

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE EXCHANGE_RATE'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            --REVOLVING_FLAG , AMORT_TYPE
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE REVOLVING_FLAG'); COMMIT;

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT DISTINCT PRD_CODE, DATA_SOURCE, REPAY_TYPE_VALUE FROM IFRS_MASTER_PRODUCT_PARAM) B
            ON (A.PRODUCT_CODE = B.PRD_CODE AND A.DATA_SOURCE = B.DATA_SOURCE)
            WHEN MATCHED THEN
            UPDATE SET A.REVOLVING_FLAG = CASE WHEN B.REPAY_TYPE_VALUE = 'REV' THEN 1 ELSE 0 END
            WHERE DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE IN ('ILS','BTRD','KTP');

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET REVOLVING_FLAG = CASE WHEN DATA_SOURCE = 'BTRD' OR (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('BGL','BSL','BPC','BGP','BGB')) THEN 0 ELSE REVOLVING_FLAG END
            WHERE DOWNLOAD_DATE = V_CURRDATE;
            COMMIT;

            COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET REVOLVING_FLAG = CASE WHEN PRODUCT_CODE LIKE 'B%' THEN 0 ELSE REVOLVING_FLAG END
                WHERE DOWNLOAD_DATE = V_CURRDATE;

            COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET AMORT_TYPE = CASE WHEN REVOLVING_FLAG = 1 THEN 'SL' ELSE 'EIR' END WHERE DOWNLOAD_dATE = V_CURRDATE; COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE REVOLVING_FLAG'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            -- PLAFOND
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE PLAFOND'); COMMIT;

            MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
            USING (SELECT DOWNLOAD_DATE,INITIAL_OUTSTANDING,ACCOUNT_NUMBER FROM GTMP_IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = V_CURRDATE) B
            ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.FACILITY_NUMBER = B.ACCOUNT_NUMBER AND A.DATA_SOURCE = 'ILS' AND A.DOWNLOAD_DATE = V_CURRDATE)
            WHEN MATCHED THEN UPDATE SET
            A.PLAFOND = B.INITIAL_OUTSTANDING
            WHERE A.DATA_SOURCE = 'ILS';

            COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET PLAFOND = CASE  WHEN FACILITY_NUMBER IS NULL
                                THEN OUTSTANDING
                                ELSE PLAFOND
                          END;

            COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE PLAFOND'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            -- PRODUCT_CODE - BTRD
            -----------------------------------------------------------------------------------------------------------------

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE PRODUCT_CODE BTRD'); COMMIT;


            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET PRODUCT_CODE = CASE WHEN PRODUCT_CODE = 'SAC' AND SUBSTR(ACCOUNT_NUMBER,1,1) = 'L' THEN 'SAC-L'
                                    WHEN PRODUCT_CODE = 'SAC' AND SUBSTR(ACCOUNT_NUMBER,1,1) = 'E' THEN 'SAC-E'
                                    WHEN PRODUCT_CODE = 'LAC' AND SUBSTR(ACCOUNT_NUMBER,1,1) = 'L' THEN 'LAC-L'
                                    WHEN PRODUCT_CODE = 'LAC' AND SUBSTR(ACCOUNT_NUMBER,1,1) = 'E' THEN 'LAC-E'
                                    ELSE PRODUCT_CODE
                               END
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND DATA_SOURCE = 'BTRD';
            COMMIT;

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET PRODUCT_CODE = CASE WHEN PRODUCT_CODE IS NULL THEN PRODUCT_TYPE ELSE PRODUCT_CODE END
            WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'BTRD';
            COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
                    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE PRODUCT_CODE BTRD'); COMMIT;

            -----------------------------------------------------------------------------------------------------------------
            -- RESERVED_VARCHAR_1 AND RESERVED_VARCHAR_12
            -----------------------------------------------------------------------------------------------------------------

            UPDATE GTMP_IFRS_MASTER_ACCOUNT
                SET RESERVED_VARCHAR_1 = CASE   WHEN TRIM(RESERVED_VARCHAR_1) IS NULL THEN 'X'
                                                ELSE RESERVED_VARCHAR_1 END,
                    RESERVED_VARCHAR_12 = CASE  WHEN TRIM(RESERVED_VARCHAR_12) IS NULL THEN 'X'
                                                ELSE RESERVED_VARCHAR_12 END
                WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'KTP';
            COMMIT;


            -----------------------------------------------------------------------------------------------------------------
            --UPDATE CC
            -----------------------------------------------------------------------------------------------------------------
            UPDATE GTMP_IFRS_MASTER_ACCOUNT
            SET INTEREST_RATE = INTEREST_RATE*100
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND DATA_SOURCE = 'CRD';
            COMMIT;


            -----------------------------------------------------------------------------------------------------------------
            --UPDATE IFRS_MASTER_ACCOUNT_MONTHLY
            -----------------------------------------------------------------------------------------------------------------
            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE IMA'); COMMIT;

            MERGE INTO IFRS_MASTER_ACCOUNT_MONTHLY A
                 USING (SELECT *
                          FROM GTMP_IFRS_MASTER_ACCOUNT
                         WHERE DOWNLOAD_DATE = V_CURRDATE)B
                    ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
            WHEN MATCHED
            THEN
               UPDATE SET A.MARKET_RATE         = B.MARKET_RATE,
                          A.WRITEOFF_FLAG       = B.WRITEOFF_FLAG,
                          A.RESERVED_FLAG_6     = B.RESERVED_FLAG_6,
                          A.BTB_FLAG            = B.BTB_FLAG,
                          A.OUTSTANDING_WO      = B.OUTSTANDING_WO,
                          A.DEFAULT_DATE        = B.DEFAULT_DATE,
                          A.RESERVED_FLAG_1     = B.RESERVED_FLAG_1,
                          A.POCI_FLAG           = B.POCI_FLAG,
                          A.NEXT_PAYMENT_DATE   = B.NEXT_PAYMENT_DATE,
                          A.DAY_PAST_DUE        = B.DAY_PAST_DUE,
                          A.NPL_FLAG            = B.NPL_FLAG,
                          A.TENOR               = B.TENOR,
                          A.STAFF_LOAN_FLAG     = B.STAFF_LOAN_FLAG,
                          A.EXCHANGE_RATE       = B.EXCHANGE_RATE,
                          A.REVOLVING_FLAG      = B.REVOLVING_FLAG,
                          A.PLAFOND             = B.PLAFOND,
                          A.RESERVED_DATE_6     = B.RESERVED_DATE_6,
                          A.LAST_PAYMENT_DATE   = B.LAST_PAYMENT_DATE,
                          A.RESERVED_DATE_7     = B.RESERVED_DATE_7,
                          A.RESERVED_DATE_8     = B.RESERVED_DATE_8,
                          A.SEGMENT_RULE_ID     = B.SEGMENT_RULE_ID,
                          A.GROUP_SEGMENT       = B.GROUP_SEGMENT,
                          A.SEGMENT             = B.SEGMENT,
                          A.SUB_SEGMENT         = B.SUB_SEGMENT,
                          A.PREPAYMENT_RULE_ID  = B.PREPAYMENT_RULE_ID,
                          A.PREPAYMENT_SEGMENT  = B.PREPAYMENT_SEGMENT,
                          A.IMPAIRED_FLAG       = B.IMPAIRED_FLAG,
                          A.ACCOUNT_STATUS      = B.ACCOUNT_STATUS,
                          A.RATING_CODE         = B.RATING_CODE,
                          A.RESERVED_VARCHAR_22 = B.RESERVED_VARCHAR_22, --RATING_CODE TURUNAN
                          A.PRODUCT_CODE        = B.PRODUCT_CODE,
                          A.RESERVED_VARCHAR_1  = B.RESERVED_VARCHAR_1,
                          A.IS_IMPAIRED         = B.IS_IMPAIRED,
                          A.COMMITTED_FLAG      = B.COMMITTED_FLAG,
                          A.CR_STAGE            = B.CR_STAGE,
                          A.INTEREST_RATE       = B.INTEREST_RATE
                          ;
            COMMIT;

            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,'UPDATE IMA'); COMMIT;


            INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
            VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_INITIAL_UPDATE_ILS_PROD' ,''); COMMIT;
    END;