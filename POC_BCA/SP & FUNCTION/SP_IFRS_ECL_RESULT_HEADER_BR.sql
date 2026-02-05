CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_RESULT_HEADER_BR (
   v_DOWNLOADDATE        DATE DEFAULT ('1-JAN-1900'),
   v_DOWNLOADDATEPREV    DATE DEFAULT ('1-JAN-1900'))
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
   V_ECLID      NUMBER;
BEGIN
   SELECT PKID
     INTO V_ECLID
     FROM IFRS_ECL_MODEL_HEADER
    WHERE ACTIVE_FLAG = 1;

   IF V_DOWNLOADDATE = '1-JAN-1900'
   THEN
      SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
   ELSE
      V_CURRDATE := v_DOWNLOADDATE;
   END IF;

   IF v_DOWNLOADDATEPREV = '1-JAN-1900'
   THEN
      SELECT CURRDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;
   ELSE
      V_PREVDATE := v_DOWNLOADDATEPREV;
   END IF;

   EXECUTE IMMEDIATE 'truncate table IFRS_ECL_RESULT_HEADER_BR_PRE';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ECL_RESULT_HEADER_BR2_PRE';

   INSERT INTO IFRS_ECL_RESULT_HEADER_BR_PRE
      SELECT * FROM IFRS_ECL_RESULT_HEADER_BR;

   INSERT INTO IFRS_ECL_RESULT_HEADER_BR2_PRE
      SELECT * FROM IFRS_ECL_RESULT_HEADER_BR2;

   COMMIT;

   EXECUTE IMMEDIATE 'truncate table IFRS_ECL_RESULT_BR';

   EXECUTE IMMEDIATE 'truncate table IFRS_ECL_RESULT_HEADER_BR';

   EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ECL_RESULT_HEADER_BR2';

   INSERT INTO IFRS_ECL_RESULT_BR (DOWNLOAD_DATE,
                                   ECL_MODEL_ID,
                                   MASTERID,
                                   PF_SEGMENT_ID,
                                   GROUP_SEGMENT,
                                   SEGMENT,
                                   SUB_SEGMENT,
                                   ACCOUNT_NUMBER,
                                   CUSTOMER_NUMBER,
                                   CUSTOMER_NAME,
                                   DATA_SOURCE,
                                   PRODUCT_GROUP,
                                   PRODUCT_TYPE,
                                   PRODUCT_CODE,
                                   CURRENCY,
                                   EXCHANGE_RATE,
                                   EIR,
                                   INTEREST_RATE,
                                   OUTSTANDING,
                                   FAIR_VALUE_AMOUNT,
                                   INTEREST_ACCRUED,
                                   UNUSED_AMOUNT,
                                   BI_COLLECTABILITY,
                                   DAY_PAST_DUE,
                                   BUCKET_GROUP,
                                   BUCKET_ID,
                                   CR_STAGE,
                                   IMPAIRED_FLAG,
                                   LIFETIME_PERIOD,
                                   PREPAYMENT_AMOUNT,
                                   CCF_AMOUNT,
                                   ECL_AMOUNT,
                                   SPECIAL_REASON,
                                   CREATEDBY,
                                   CREATEDDATE,
                                   CREATEDHOST,
                                   EAD_AMOUNT,
                                   IFRS9_CLASS,
                                   RESERVED_VARCHAR_4,
                                   ECL_AMOUNT_OFF_BS,
                                   ECL_AMOUNT_ON_BS,
                                   ECL_AMOUNT_ON_BS_FINAL,
                                   ECL_AMOUNT_FINAL)
      SELECT A.DOWNLOAD_DATE,
             B.ECL_MODEL_ID,
             B.MASTERID,
             A.SEGMENT_RULE_ID AS PF_SEGMENT_ID,
             A.GROUP_SEGMENT,
             A.SEGMENT,
             A.SUB_SEGMENT,
             A.ACCOUNT_NUMBER,
             A.CUSTOMER_NUMBER,
             A.CUSTOMER_NAME,
             A.DATA_SOURCE,
             A.PRODUCT_GROUP,
             A.PRODUCT_TYPE,
             A.PRODUCT_CODE,
             A.CURRENCY,
             A.EXCHANGE_RATE,
             A.EIR,
             A.INTEREST_RATE,
             NVL (A.OUTSTANDING, 0) AS OUTSTANDING,
             NVL (B.FAIR_VALUE_AMOUNT, 0) AS FAIR_VALUE_AMOUNT,
             NVL (A.INTEREST_ACCRUED, 0) AS INTEREST_ACCRUED,
             B.UNUSED_AMOUNT,
             A.BI_COLLECTABILITY,
             A.DAY_PAST_DUE,
             A.BUCKET_GROUP,
             B.BUCKET_ID,
             B.CR_STAGE,
             'C' AS IMPAIRED_FLAG,
             B.LIFETIME_PERIOD,
             B.PREPAYMENT_AMOUNT,
             B.CCF_AMOUNT,
             CASE
                WHEN NVL (B.EAD_AMOUNT, 0) > 0
                     AND NVL (c.ECL_AMOUNT, 0) > NVL (B.EAD_AMOUNT, 0)
                THEN
                   NVL (B.EAD_AMOUNT, 0)
                ELSE
                   NVL (c.ECL_AMOUNT, 0)
             END
                AS ECL_AMOUNT,
             '' AS SPECIAL_REASON,
             'ADMIN',
             SYSDATE,
             'HOST',
             B.EAD_AMOUNT,
             A.IFRS9_CLASS,
             A.RESERVED_VARCHAR_4,
             C.ECL_AMOUNT_OFF_BS,
             C.ECL_AMOUNT_ON_BS,
             C.ECL_AMOUNT_ON_BS_FINAL,
             C.ECL_AMOUNT_FINAL
        FROM IFRS_MASTER_ACCOUNT A
             JOIN (  SELECT DOWNLOAD_DATE,
                            ECL_MODEL_ID,
                            MASTERID,
                            BUCKET_ID,
                            CR_STAGE,
                            LIFETIME_PERIOD,
                            MAX (FAIR_VALUE_AMOUNT) FAIR_VALUE_AMOUNT,
                            UNUSED_AMOUNT,
                            SUM (
                               CASE
                                  WHEN COUNTER_PAYSCHD = 1
                                  THEN
                                     NVL (EAD_AMOUNT, 0)
                                  ELSE
                                     0
                               END)
                               AS EAD_AMOUNT,
                            SUM (NVL (PREPAYMENT_AMOUNT, 0))
                               AS PREPAYMENT_AMOUNT,
                            SUM (NVL (CCF_AMOUNT, 0)) AS CCF_AMOUNT,
                            SUM (NVL (ECL_AMOUNT, 0)) AS ECL_AMOUNT
                       FROM IFRS_ECL_RESULT_DETAIL_CALC
                      WHERE DOWNLOAD_DATE = V_CURRDATE
                            AND ECL_MODEL_ID = V_ECLID
                   GROUP BY DOWNLOAD_DATE,
                            ECL_MODEL_ID,
                            MASTERID,
                            BUCKET_ID,
                            CR_STAGE,
                            LIFETIME_PERIOD,
                            UNUSED_AMOUNT) B
                ON A.MASTERID = B.MASTERID
             JOIN IFRS_ECL_RESULT_DETAIL C
                ON     A.MASTERID = C.MASTERID
                   AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
                   AND A.DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;


   INSERT INTO IFRS_ECL_RESULT_HEADER_BR (DOWNLOAD_DATE,
                                          ECL_MODEL_ID,
                                          PF_SEGMENT_ID,
                                          GROUP_SEGMENT,
                                          SEGMENT,
                                          SUB_SEGMENT,
                                          DATA_SOURCE,
                                          PRODUCT_GROUP,
                                          PRODUCT_TYPE,
                                          PRODUCT_CODE,
                                          BUCKET_GROUP,
                                          BUCKET_ID,
                                          CR_STAGE,
                                          OUTSTANDING,
                                          PREPAYMENT_AMOUNT,
                                          CCF_AMOUNT,
                                          ECL_AMOUNT,
                                          BRANCH_CODE,
                                          EAD_AMOUNT,
                                          FAIR_VALUE_AMOUNT,
                                          CREATEDBY,
                                          CREATEDHOST,
                                          CREATEDDATE,
                                          IFRS9_CLASS,
                                          RESERVED_VARCHAR_4,
                                          EXCHANGE_RATE,
                                          CURRENCY,
                                          ECL_ON_BS,
                                          ECL_OFF_BS,
                                          ECL_AMOUNT_FINAL)
        SELECT A.DOWNLOAD_DATE,
               A.ECL_MODEL_ID,
               A.PF_SEGMENT_ID,
               A.GROUP_SEGMENT,
               A.SEGMENT,
               A.SUB_SEGMENT,
               A.DATA_SOURCE,
               A.PRODUCT_GROUP,
               A.PRODUCT_TYPE,
               A.PRODUCT_CODE,
               A.BUCKET_GROUP,
               A.BUCKET_ID,
               A.CR_STAGE,
               SUM (OUTSTANDING * NVL (EXCHANGE_RATE, 1)) AS OUTSTANDING,
               SUM (PREPAYMENT_AMOUNT * NVL (EXCHANGE_RATE, 1))
                  AS PREPAYMENT_AMOUNT,
               SUM (CCF_AMOUNT * NVL (EXCHANGE_RATE, 1)) AS CCF_AMOUNT,
               SUM (ECL_AMOUNT * NVL (EXCHANGE_RATE, 1)) AS ECL_AMOUNT,
               B.BRANCH_CODE,
               SUM (A.EAD_AMOUNT),
               SUM (A.FAIR_VALUE_AMOUNT),
               'ADMIN',
               'LOCALHOST',
               SYSDATE,
               IFRS9_CLASS,
               RESERVED_VARCHAR_4,
               EXCHANGE_RATE,
               CURRENCY,
               SUM (ECL_AMOUNT_ON_BS_FINAL) * EXCHANGE_RATE,
               SUM (ECL_AMOUNT_OFF_BS) * EXCHANGE_RATE,
               SUM (ECL_AMOUNT_FINAL) * EXCHANGE_RATE
          FROM    IFRS_ECL_RESULT_BR A
               JOIN
                  (SELECT MASTERID, BRANCH_CODE
                     FROM IFRS_MASTER_ACCOUNT
                    WHERE DOWNLOAD_DATE = V_CURRDATE) B
               ON A.MASTERID = B.MASTERID
         WHERE DOWNLOAD_DATE = V_CURRDATE AND ECL_MODEL_ID = V_ECLID
      GROUP BY A.DOWNLOAD_DATE,
               A.ECL_MODEL_ID,
               A.PF_SEGMENT_ID,
               A.GROUP_SEGMENT,
               A.SEGMENT,
               A.SUB_SEGMENT,
               A.DATA_SOURCE,
               A.PRODUCT_GROUP,
               A.PRODUCT_TYPE,
               A.PRODUCT_CODE,
               A.BUCKET_GROUP,
               A.BUCKET_ID,
               A.CR_STAGE,
               B.BRANCH_CODE,
               IFRS9_CLASS,
               RESERVED_VARCHAR_4,
               EXCHANGE_RATE,
               CURRENCY;

   COMMIT;


   EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_NOMI_STAGE';

   INSERT INTO TEMP_NOMI_STAGE
      SELECT MASTERID, STAGE
        FROM IFRS_NOMINATIVE
       WHERE REPORT_DATE = V_CURRDATE;

   MERGE INTO IFRS_ECL_RESULT_BR A
        USING TEMP_NOMI_STAGE B
           ON (A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED
   THEN
      UPDATE SET A.CR_STAGE = B.STAGE;

   --   MERGE INTO IFRS_ECL_RESULT_BR A
   --     USING (SELECT MASTERID, STAGE FROM IFRS_NOMINATIVE WHERE REPORT_DATE=V_CURRDATE) B
   --     ON (A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE)
   --     WHEN MATCHED THEN UPDATE
   --     SET A.CR_STAGE = B.STAGE;

   COMMIT;


   INSERT INTO IFRS_ECL_RESULT_HEADER_BR2 (DOWNLOAD_DATE,
                                           ECL_MODEL_ID,
                                           PF_SEGMENT_ID,
                                           GROUP_SEGMENT,
                                           SEGMENT,
                                           SUB_SEGMENT,
                                           DATA_SOURCE,
                                           PRODUCT_GROUP,
                                           PRODUCT_TYPE,
                                           PRODUCT_CODE,
                                           BUCKET_GROUP,
                                           BUCKET_ID,
                                           CR_STAGE,
                                           OUTSTANDING,
                                           PREPAYMENT_AMOUNT,
                                           CCF_AMOUNT,
                                           ECL_AMOUNT,
                                           BRANCH_CODE,
                                           EAD_AMOUNT,
                                           FAIR_VALUE_AMOUNT,
                                           CREATEDBY,
                                           CREATEDHOST,
                                           CREATEDDATE,
                                           IFRS9_CLASS,
                                           RESERVED_VARCHAR_4,
                                           EXCHANGE_RATE,
                                           CURRENCY,
                                           ECL_ON_BS,
                                           ECL_OFF_BS,
                                           ECL_AMOUNT_FINAL)
        SELECT A.DOWNLOAD_DATE,
               A.ECL_MODEL_ID,
               A.PF_SEGMENT_ID,
               A.GROUP_SEGMENT,
               A.SEGMENT,
               A.SUB_SEGMENT,
               A.DATA_SOURCE,
               A.PRODUCT_GROUP,
               A.PRODUCT_TYPE,
               A.PRODUCT_CODE,
               A.BUCKET_GROUP,
               A.BUCKET_ID,
               A.CR_STAGE,
               SUM (OUTSTANDING * NVL (EXCHANGE_RATE, 1)) AS OUTSTANDING,
               SUM (PREPAYMENT_AMOUNT * NVL (EXCHANGE_RATE, 1))
                  AS PREPAYMENT_AMOUNT,
               SUM (CCF_AMOUNT * NVL (EXCHANGE_RATE, 1)) AS CCF_AMOUNT,
               SUM (ECL_AMOUNT * NVL (EXCHANGE_RATE, 1)) AS ECL_AMOUNT,
               B.BRANCH_CODE,
               SUM (A.EAD_AMOUNT),
               SUM (A.FAIR_VALUE_AMOUNT),
               'ADMIN',
               'LOCALHOST',
               SYSDATE,
               IFRS9_CLASS,
               RESERVED_VARCHAR_4,
               EXCHANGE_RATE,
               CURRENCY,
               SUM (ECL_AMOUNT_ON_BS_FINAL) * EXCHANGE_RATE,
               SUM (ECL_AMOUNT_OFF_BS) * EXCHANGE_RATE,
               SUM (ECL_AMOUNT_FINAL) * EXCHANGE_RATE
          FROM    IFRS_ECL_RESULT_BR A
               JOIN
                  (SELECT MASTERID, BRANCH_CODE
                     FROM IFRS_MASTER_ACCOUNT
                    WHERE DOWNLOAD_DATE = V_CURRDATE) B
               ON A.MASTERID = B.MASTERID
         WHERE DOWNLOAD_DATE = V_CURRDATE AND ECL_MODEL_ID = V_ECLID
      GROUP BY A.DOWNLOAD_DATE,
               A.ECL_MODEL_ID,
               A.PF_SEGMENT_ID,
               A.GROUP_SEGMENT,
               A.SEGMENT,
               A.SUB_SEGMENT,
               A.DATA_SOURCE,
               A.PRODUCT_GROUP,
               A.PRODUCT_TYPE,
               A.PRODUCT_CODE,
               A.BUCKET_GROUP,
               A.BUCKET_ID,
               A.CR_STAGE,
               B.BRANCH_CODE,
               IFRS9_CLASS,
               RESERVED_VARCHAR_4,
               EXCHANGE_RATE,
               CURRENCY;

   COMMIT;
END;