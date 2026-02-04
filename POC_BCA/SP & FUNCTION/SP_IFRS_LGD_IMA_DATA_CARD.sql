CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_IMA_DATA_CARD(V_EFF_DATE DATE)
AS
    v_MIN_DATE DATE;
BEGIN
    DELETE TMP_LGD_IMA
    WHERE DATA_SOURCE = 'CRD';
    COMMIT;

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    DBMS_STATS.UNLOCK_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA');
    DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA',DEGREE=>2);

    v_MIN_DATE := '31 JAN 2008';

    WHILE v_MIN_DATE <= V_EFF_DATE LOOP
        SP_IFRS_INSERT_GTMP_FROM_IMA_M(v_MIN_DATE, 'CRD');

        INSERT /*+ PARALLEL(8) */ INTO TMP_LGD_IMA
        (
            PROCESS_DATE,
            DOWNLOAD_DATE,
            MASTERID,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            FACILITY_NUMBER,
            ACCOUNT_NUMBER,
            OUTSTANDING,
            CURRENCY,
            ACCOUNT_STATUS,
            BI_COLLECTABILITY,
            PRODUCT_CODE,
            FIRST_NPL_DATE,
            FIRST_NPL_OS,
            INTEREST_RATE,
            EIR,
            GROUP_SEGMENT,
            SEGMENT,
            SUB_SEGMENT,
            LGD_RULE_ID,
            LGD_SEGMENT,
            SEGMENT_RULE_ID,
            DATA_SOURCE,
            RESERVED_VARCHAR_1,
            RESERVED_VARCHAR_2,
            RESERVED_DATE_1
        )
        SELECT
            V_EFF_DATE PROCESS_DATE,
            A.DOWNLOAD_DATE,
            A.MASTERID,
            A.CUSTOMER_NUMBER,
            A.CUSTOMER_NAME,
            A.FACILITY_NUMBER,
            A.ACCOUNT_NUMBER,
            A.OUTSTANDING,
            A.CURRENCY,
            A.ACCOUNT_STATUS,
            A.BI_COLLECTABILITY,
            A.PRODUCT_CODE,
            A.RESERVED_DATE_3,
            A.RESERVED_AMOUNT_8,
            A.INTEREST_RATE,
            A.EIR,
            A.GROUP_SEGMENT,
            A.SEGMENT,
            A.SUB_SEGMENT,
            A.LGD_RULE_ID,
            A.LGD_SEGMENT,
            A.SEGMENT_RULE_ID,
            A.DATA_SOURCE,
            A.RESERVED_VARCHAR_9,
            A.RESERVED_VARCHAR_2,
            A.WRITEOFF_DATE
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_LGD_FIRST_NPL_DATE B
        ON A.DOWNLOAD_DATE >= B.FIRST_NPL_DATE
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
--        WHERE data_source = 'CRD'
--        and rating_code >= '5'
        ;
        COMMIT;

        update /*+ PARALLEL(8) */ ifrs_prc_date_k
        set currdate = v_min_date;
        commit;

        v_MIN_DATE := ADD_MONTHS(v_MIN_DATE, 1);
    END LOOP;

    DBMS_STATS.UNLOCK_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA');
    DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA',DEGREE=>2);

    MERGE /*+ PARALLEL(8) */ INTO TMP_LGD_IMA A
    USING
    (
        SELECT A2.ACCOUNT_NUMBER,
            A2.INTEREST_RATE,
            A2.FIRST_NPL_DATE
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE DATA_SOURCE = 'CRD'
            GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MIN_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.INTEREST_RATE = CASE WHEN B.FIRST_NPL_DATE >= '1 JAN 2019' THEN
                            ROUND(B.INTEREST_RATE,2) / 100
                          ELSE
                            ROUND(B.INTEREST_RATE,2)
                          END;
    COMMIT;

END;