CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_UPDATE_BUCKET(v_ECLID NUMBER DEFAULT (0), v_DOWNLOADDATE DATE DEFAULT NULL)
AS
BEGIN
    /*****************************************************
    01 INITIAL UPDATE
    ******************************************************/
    SP_IFRS_INSERT_GTMP_FROM_IMA(v_DOWNLOADDATE);

    UPDATE GTMP_IFRS_MASTER_ACCOUNT
    SET BUCKET_GROUP = NULL,
        BUCKET_ID = NULL
    WHERE CREATEDBY <> 'DKP';

    COMMIT;

    /*****************************************************
    02 UPDATE BUCKET ID and BUCKET GROUP for DPD and DLQ
    ******************************************************/
    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_ECL_MODEL_CONFIG B
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
        JOIN VW_IFRS_BUCKET C
        ON B.BUCKET_GROUP = C.BUCKET_GROUP
        AND C.OPTION_GROUPING IN ('DPD','DLQ')
        AND CASE WHEN C.OPTION_GROUPING = 'DPD' THEN A.DAY_PAST_DUE ELSE TO_NUMBER(NVL(A.RATING_CODE,1)) END BETWEEN C.RANGE_START AND C.RANGE_END
        WHERE B.ECL_MODEL_ID = v_ECLID
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;

    COMMIT;

    /******************************************************************************
    03 UPDATE BUCKET ID and BUCKET GROUP for BUCKET GROUPING other than DPD and DLQ
    *******************************************************************************/
    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_ECL_MODEL_CONFIG B
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
        JOIN VW_IFRS_BUCKET C
        ON B.BUCKET_GROUP = C.BUCKET_GROUP
        AND C.OPTION_GROUPING IN ('IR','ER','PEF','SNP','SNPFI','PEFFI')
        AND NVL(UPPER(A.RATING_CODE), 'X') = C.BUCKET_NAME
        WHERE B.ECL_MODEL_ID = v_ECLID
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;

    COMMIT;

    /***********************************************************************************
    04 UPDATE BUCKET ID and BUCKET GROUP for UNRATED PLACEMENT and NOSTRO
    ************************************************************************************/
    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, 12 BUCKET_ID , 'BR9_1' BUCKET_GROUP
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_ECL_MODEL_CONFIG B
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
        WHERE B.ECL_MODEL_ID = v_ECLID
        AND A.SEGMENT IN ('PLACEMENT', 'NOSTRO', 'BANK_BTRD')
        AND A.RATING_CODE = 'UNK'
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;

    COMMIT;

    /***********************************************************************************
    05 UPDATE BUCKET ID and BUCKET GROUP for KTP PLACEMENT INTERNAL
    ************************************************************************************/

    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP , A.RATING_CODE
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN VW_IFRS_BUCKET C
        ON C.BUCKET_GROUP = 'BR9_1'
        AND A.RATING_CODE LIKE '%BR%'
        AND C.OPTION_GROUPING IN ('IR','ER','PEF','SNP','SNPFI','PEFFI')
        AND NVL(A.RATING_CODE, 'X') = C.BUCKET_NAME
        WHERE GROUP_SEGMENT IN ('PLACEMENT','NOSTRO')
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;

    COMMIT;

    /***********************************************************************************
    06 UPDATE BUCKET ID and BUCKET GROUP for MUTFUND
    ************************************************************************************/
    UPDATE GTMP_IFRS_MASTER_ACCOUNT
    SET BUCKET_ID = '12',
        BUCKET_GROUP = 'BR9_1'
    WHERE RESERVED_VARCHAR_26 = 'MUTFUND';
    COMMIT;

    /***********************************************************************************
    08 UPDATE BUCKET ID and BUCKET GROUP for KTP BOND RATING EXTERNAL INTERNAL CR 20231004
    ************************************************************************************/
    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, 12 BUCKET_ID, 'IR11_1' BUCKET_GROUP
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_ECL_MODEL_CONFIG B
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
        WHERE B.ECL_MODEL_ID = v_ECLID
        AND A.SEGMENT IN ('BOND_CORPORATE')
        AND A.SUB_SEGMENT = 'BOND_CORPORATE - INTERNAL'
        AND A.RATING_CODE = 'UNK'
        AND A.DATA_SOURCE = 'KTP'
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;

    COMMIT;

    /***********************************************************************************
    09 UPDATE BUCKET ID and BUCKET GROUP for UNRATED INDIVIDUAL
    ************************************************************************************/
    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT IMA
    USING
    (
        SELECT
            A.MASTERID, E.BUCKET_GROUP, E.MAX_BUCKET_ID BUCKET_ID
        FROM GTMP_IFRS_MASTER_ACCOUNT A
        JOIN IFRS_ECL_MODEL_CONFIG B
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
        JOIN
        (
            SELECT CUSTOMER_NUMBER, MAX(PKID) MAX_OVERRIDEID
            FROM IFRS_IA_OVERRIDEH
            WHERE EFFECTIVE_DATE <= v_DOWNLOADDATE
            GROUP BY CUSTOMER_NUMBER
        ) C
        ON A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
        JOIN TBLT_PAYMENTEXPECTEDH D
        ON A.ACCOUNT_NUMBER = D.ACCOUNT_NUMBER
        AND C.MAX_OVERRIDEID = D.OVERRIDEID
        JOIN VW_IFRS_MAX_BUCKET E
        ON B.BUCKET_GROUP = E.BUCKET_GROUP
        WHERE B.ECL_MODEL_ID = v_ECLID
        AND A.BUCKET_GROUP IS NULL
    ) TMP
    ON (IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
        UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;
    COMMIT;

    /***********************************************************************************
    07 UPDATE BUCKET ID and BUCKET GROUP for BTRD CODE 0 1
    ************************************************************************************/

    UPDATE GTMP_IFRS_MASTER_ACCOUNT
    SET BUCKET_ID = NULL,
        BUCKET_GROUP = NULL
    WHERE DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('0','1');
    COMMIT;


    /***********************************************************************************
    10 Final UPDATE BUCKET ID and BUCKET GROUP to IMA
    ************************************************************************************/
    MERGE INTO IFRS_MASTER_ACCOUNT IMA
    USING GTMP_IFRS_MASTER_ACCOUNT TMP
    ON (IMA.DOWNLOAD_DATE = v_DOWNLOADDATE AND IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP;
    COMMIT;
END;