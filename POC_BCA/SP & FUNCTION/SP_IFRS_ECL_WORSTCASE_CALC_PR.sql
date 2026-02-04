CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_WORSTCASE_CALC_PR (
    v_ECLID          NUMBER DEFAULT (0),
    v_DOWNLOADDATE   DATE DEFAULT ('1-JAN-1900'))
AS
    V_CURRDATE   DATE;
BEGIN
    IF v_DOWNLOADDATE = '1-JAN-1900'
    THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := v_DOWNLOADDATE;
    END IF;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

    INSERT INTO GTMP_IFRS_MASTER_ACCOUNT (PKID,
                                          DOWNLOAD_DATE,
                                          MASTERID,
                                          MASTER_ACCOUNT_CODE,
                                          CUSTOMER_NUMBER,
                                          ACCOUNT_NUMBER,
                                          OUTSTANDING,
                                          IMPAIRED_FLAG,
                                          RESERVED_RATE_1)
        SELECT A.PKID,
               A.DOWNLOAD_DATE,
               A.MASTERID,
               ' '     MASTER_ACCOUNT_CODE,
               A.CUSTOMER_NUMBER,
               A.ACCOUNT_NUMBER,
               OUTSTANDING,
               'W'     AS IMPAIRED_FLAG,
               NVL (B.PERCENTAGE, 0) / 100
          FROM IFRS_ECL_RESULT_DETAIL_PR  A
               JOIN TBLU_WORSTCASE_LIST B
                   ON     A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                      AND A.DOWNLOAD_DATE = V_CURRDATE
                      AND A.ECL_MODEL_ID = v_ECLID
                      AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
                      AND (A.OUTSTANDING + A.UNUSED_AMOUNT) > 0
                      AND (   NVL (A."SEGMENT", ' ') LIKE ('%COMMERCIAL%')
                           OR NVL (A."SEGMENT", ' ') LIKE ('%CORPORATE%')
                           OR NVL (A."SEGMENT", ' ') LIKE
                                  ('%KOMERSIAL_BTRD%')
                           OR NVL (A."SEGMENT", ' ') LIKE
                                  ('%KORPORASI_BTRD%')
                           OR NVL (A."SEGMENT", ' ') LIKE ('%PBMM%'));

    COMMIT;

    DELETE IFRS_ECL_RESULT_DETAIL_CALC_PR
     WHERE     DOWNLOAD_DATE = V_CURRDATE
           AND ECL_MODEL_ID = v_ECLID
           AND MASTERID IN (SELECT MASTERID FROM GTMP_IFRS_MASTER_ACCOUNT)
           AND COUNTER_PAYSCHD > 1;

    COMMIT;


    MERGE INTO IFRS_ECL_RESULT_DETAIL_CALC_PR A
         USING GTMP_IFRS_MASTER_ACCOUNT B
            ON (    A.DOWNLOAD_DATE = V_CURRDATE
                AND A.ECL_MODEL_ID = v_ECLID
                AND A.MASTERID = B.MASTERID)
    WHEN MATCHED
    THEN
        UPDATE SET
            A.PD_RATE = 1,
            A.LGD_RATE = B.RESERVED_RATE_1,
            A.DISCOUNT_RATE = 1,
            A.ECL_AMOUNT =
                  CASE
                      WHEN NVL (A.EAD_AMOUNT, 0) = 0
                      THEN
                          NVL (B.OUTSTANDING, 0)
                      ELSE
                          NVL (A.EAD_AMOUNT, 0)
                  END
                * B.RESERVED_RATE_1;

    COMMIT;

    MERGE INTO IFRS_ECL_RESULT_DETAIL_PR A
         USING (SELECT A2.MASTERID, B2.ECL_AMOUNT, A2.IMPAIRED_FLAG
                  FROM GTMP_IFRS_MASTER_ACCOUNT  A2
                       JOIN IFRS_ECL_RESULT_DETAIL_CALC_PR B2
                           ON     B2.DOWNLOAD_DATE = V_CURRDATE
                              AND B2.ECL_MODEL_ID = v_ECLID
                              AND A2.MASTERID = B2.MASTERID) B
            ON (    A.DOWNLOAD_DATE = V_CURRDATE
                AND A.ECL_MODEL_ID = v_ECLID
                AND A.MASTERID = B.MASTERID)
    WHEN MATCHED
    THEN
        UPDATE SET
            A.ECL_AMOUNT = B.ECL_AMOUNT,
            A.IMPAIRED_FLAG = B.IMPAIRED_FLAG,
            A.SPECIAL_REASON = 'WORSTCASE';

    COMMIT;
END;