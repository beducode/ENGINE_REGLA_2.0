CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_FIRST_NPL_DATE(V_CURRDATE DATE)
AS
V_PREVDATE DATE;
BEGIN
    DELETE IFRS_LGD_FIRST_NPL_DATE
    WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    V_PREVDATE := ADD_MONTHS(V_CURRDATE,-1);

    INSERT INTO IFRS_LGD_FIRST_NPL_DATE
    (
        PKID,
        DOWNLOAD_DATE,
        MASTERID,
        MASTER_ACCOUNT_CODE,
        DATA_SOURCE,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        FACILITY_NUMBER,
        ACCOUNT_NUMBER,
        ACCOUNT_STATUS,
        BI_COLLECTABILITY,
        RATING_CODE,
        GROUP_SEGMENT,
        SEGMENT,
        SUB_SEGMENT,
        SEGMENT_RULE_ID,
        INTEREST_RATE,
        PRODUCT_CODE,
        CURRENCY,
        EXCHANGE_RATE,
        FIRST_NPL_DATE,
        FIRST_NPL_OS
    )
    SELECT
        PKID,
        DOWNLOAD_DATE,
        MASTERID,
        MASTER_ACCOUNT_CODE,
        DATA_SOURCE,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        FACILITY_NUMBER,
        ACCOUNT_NUMBER,
        ACCOUNT_STATUS,
        BI_COLLECTABILITY,
        RATING_CODE,
        GROUP_SEGMENT,
        SEGMENT,
        SUB_SEGMENT,
        SEGMENT_RULE_ID,
        INTEREST_RATE,
        PRODUCT_CODE,
        CURRENCY,
        EXCHANGE_RATE,
        DOWNLOAD_DATE FIRST_NPL_DATE,
        OUTSTANDING FIRST_NPL_OS
    FROM IFRS_MASTER_ACCOUNT_MONTHLY
    WHERE DOWNLOAD_DATE = V_CURRDATE
    AND
    (
        (BI_COLLECTABILITY IN ('3','4','5','C')
        AND DATA_SOURCE = 'ILS'
        AND PRODUCT_CODE NOT LIKE 'B%'
--      AND OUTSTANDING > 0
        )
    OR
        (RATING_CODE >= '5'
        AND DATA_SOURCE = 'CRD'
--      AND OUTSTANDING > 0
        )
    )
    AND MASTERID NOT IN
    (SELECT MASTERID FROM IFRS_LGD_FIRST_NPL_DATE);
    COMMIT;

--------------------------------------------------------------------------------------------------------------------------------
-- TAMBAHAN UNTUK UPDATE RESERVED_DATE_6 , 7 ,8
--------------------------------------------------------------------------------------------------------------------------------
    MERGE INTO IFRS_MASTER_ACCOUNT_MONTHLY A
    USING (
            SELECT DOWNLOAD_DATE, MASTERID, ACCOUNT_NUMBER, RESERVED_DATE_6,RESERVED_DATE_7,RESERVED_DATE_8, IMPAIRED_FLAG, EIR, RESERVED_DATE_3
              FROM IFRS_MASTER_ACCOUNT_MONTHLY
                WHERE DOWNLOAD_DATE = V_PREVDATE AND DATA_SOURCE IN ('CRD','ILS')
          ) B
    ON (A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE IN ('CRD','ILS'))
    WHEN MATCHED THEN UPDATE
    SET A.RESERVED_DATE_6 = CASE WHEN B.RESERVED_DATE_6 IS NULL THEN NULL ELSE B.RESERVED_DATE_6 END,
        A.RESERVED_DATE_7 = CASE WHEN B.RESERVED_DATE_7 IS NULL THEN NULL ELSE B.RESERVED_DATE_7 END,
        A.RESERVED_DATE_8 = CASE WHEN B.RESERVED_DATE_8 IS NULL THEN NULL ELSE B.RESERVED_DATE_8 END;
        COMMIT;

    UPDATE IFRS_MASTER_ACCOUNT_MONTHLY
    SET RESERVED_DATE_3 = NULL,
    RESERVED_AMOUNT_8 = NULL,
    RESERVED_DATE_8 = CASE WHEN RESERVED_DATE_8 IS NULL
                           THEN CASE WHEN ACCOUNT_STATUS = 'C' AND DATA_SOURCE = 'ILS'
                                     THEN DOWNLOAD_DATE
                                     ELSE NULL
                                END
                           ELSE RESERVED_DATE_8
                      END,    -- ADD WILLY PERHITUNGAN LIFETIME

    RESERVED_DATE_7 = CASE WHEN DATA_SOURCE = 'CRD'
                           THEN CASE WHEN RESERVED_DATE_7 IS NULL
                                     THEN CASE WHEN RATING_CODE >= 2
                                               THEN DOWNLOAD_DATE ELSE RESERVED_DATE_7
                                          END
                                     ELSE RESERVED_DATE_7
                                END

                           WHEN DATA_SOURCE = 'ILS'
                           THEN CASE WHEN RESERVED_DATE_7 IS NULL
                                     THEN CASE WHEN BI_COLLECTABILITY IN( '2','3','4','5','C')
                                               THEN DOWNLOAD_DATE ELSE NULL
                                          END
                                     ELSE RESERVED_DATE_7
                                END
                      END,    -- ADD WILLY PERHITUNGAN LIFETIME

    RESERVED_DATE_6 = CASE WHEN DATA_SOURCE = 'CRD'
                           THEN CASE WHEN RESERVED_DATE_6 IS NULL
                                     THEN CASE WHEN RATING_CODE >= 3
                                               THEN DOWNLOAD_DATE
                                               ELSE RESERVED_DATE_6
                                          END
                                     ELSE RESERVED_DATE_6
                                END

                           WHEN DATA_SOURCE = 'ILS'
                           THEN CASE WHEN RESERVED_DATE_6 IS NULL
                                     THEN CASE WHEN DAY_PAST_DUE >= 30
                                               THEN DOWNLOAD_DATE ELSE NULL
                                          END
                                     ELSE RESERVED_DATE_6
                                END
                      END    -- ADD WILLY PERHITUNGAN LIFETIME
    WHERE DOWNLOAD_DATE = V_CURRDATE
    AND DATA_SOURCE IN ('ILS','CRD');
    COMMIT;

    MERGE INTO IFRS_MASTER_ACCOUNT_MONTHLY A
    USING IFRS_LGD_FIRST_NPL_DATE B
    ON (A.DOWNLOAD_DATE = V_CURRDATE
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.RESERVED_DATE_3 = CASE WHEN V_CURRDATE >= LAST_DAY(B.FIRST_NPL_DATE) THEN B.FIRST_NPL_DATE ELSE NULL END,
        A.RESERVED_AMOUNT_8 = CASE WHEN V_CURRDATE >= LAST_DAY(B.FIRST_NPL_DATE) THEN B.FIRST_NPL_OS ELSE NULL END;
    COMMIT;
END;