CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_PARTIAL_RUN_LIST(v_ECLID NUMBER DEFAULT (0), V_CURRDATE DATE DEFAULT('1-JAN-1900'))
AS
    V_ECL_RUN_DATETIME DATE;
    V_RUN_SEQUENCE NUMBER(10);
    V_RUN_STATUS NUMBER(10);
    V_EXISTS NUMBER(10);
    V_LAST_UPLOADID NUMBER(10);
    V_LAST_UPLOADID_2 NUMBER(10);
    V_HEADERID NUMBER(18);
    V_HAS_HEADERID NUMBER(1);
    V_REASON_COUNT NUMBER(10);
BEGIN
    SELECT COUNT(*)
    INTO V_RUN_SEQUENCE
    FROM IFRS_PARTIAL_RUN_HEADER
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    IF V_RUN_SEQUENCE > 0 THEN
        SELECT RUN_STATUS
        INTO V_RUN_STATUS
        FROM IFRS_PARTIAL_RUN_HEADER
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND RUN_SEQUENCE = V_RUN_SEQUENCE;

        IF V_RUN_STATUS = 1 THEN
            DELETE IFRS_PARTIAL_RUN_DETAIL
            WHERE HEADERID = (SELECT PKID FROM IFRS_PARTIAL_RUN_HEADER
                                WHERE DOWNLOAD_DATE = V_CURRDATE
                                AND RUN_SEQUENCE = V_RUN_SEQUENCE);

            DELETE IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;

            COMMIT;

            V_RUN_SEQUENCE := V_RUN_SEQUENCE - 1;
        END IF;
    END IF;


    SELECT NVL(MAX_DATE, MIN_DATE)
    INTO V_ECL_RUN_DATETIME
    FROM
    (
        SELECT MIN(START_DATE) MIN_DATE
        FROM IFRS_STATISTIC
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND SP_NAME LIKE 'SP_IFRS_ECL_MODEL_CONFIG%'
    ) A
    LEFT JOIN
    (
        SELECT MAX(NVL(END_DATE,START_DATE)) MAX_DATE
        FROM IFRS_STATISTIC
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND SP_NAME LIKE 'SP_IFRS_NOMINATIVE_TO_IMA_PR%'
    ) B
    ON 1 = 1;

    V_RUN_SEQUENCE := V_RUN_SEQUENCE + 1;
    V_HAS_HEADERID := 0;
    V_REASON_COUNT := 0;

    /*=============================================================================================
        1. Check Worstcase List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 7
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        INSERT INTO IFRS_PARTIAL_RUN_HEADER
        (
            DOWNLOAD_DATE
            ,RUN_SEQUENCE
            ,REASON
        )
        SELECT V_CURRDATE
            ,V_RUN_SEQUENCE
            ,'Change in Worstcase'
        FROM DUAL;

        SELECT PKID, 1, 1
        INTO V_HEADERID, V_HAS_HEADERID, V_REASON_COUNT
        FROM IFRS_PARTIAL_RUN_HEADER
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND RUN_SEQUENCE = V_RUN_SEQUENCE;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 7
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 7
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(COLUMN_2,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Worstcase - Added' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID_2
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(A.COLUMN_2,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Worstcase - Modified percentage from ' || TRIM(NVL(B.COLUMN_3,'-')) || ' to ' || TRIM(NVL(A.COLUMN_3,'-')) AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
        AND NVL(A.COLUMN_3,'-') != NVL(B.COLUMN_3,'-');

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(COLUMN_2,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Worstcase - Deleted' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID_2
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID
        );

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM IFRS_PARTIAL_RUN_DETAIL
        WHERE HEADERID = V_HEADERID;

        IF V_EXISTS = 0 THEN
            DELETE IFRS_PARTIAL_RUN_HEADER
            WHERE PKID = V_HEADERID;

            V_REASON_COUNT := 0;
        END IF;

        COMMIT;
    END IF;


    /*=============================================================================================
        2. Check Individual List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 30
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_6 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in Individual'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',Individual'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        V_REASON_COUNT := V_REASON_COUNT + 1;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 30
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_6 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 30
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_6 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(COLUMN_1,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Individual - Added' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID
        AND TRIM(NVL(COLUMN_1,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_1,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID_2
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(A.COLUMN_1,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Individual - Modified' ||
                CASE WHEN NVL(A.COLUMN_2,'-') != NVL(B.COLUMN_2,'-') THEN ' discount_rate_trs from ' || TRIM(NVL(B.COLUMN_2,'-')) || ' to ' || TRIM(NVL(A.COLUMN_2,'-')) END ||
                CASE WHEN NVL(A.COLUMN_3,'-') != NVL(B.COLUMN_3,'-') THEN ' discount_rate_trf from ' || TRIM(NVL(B.COLUMN_3,'-')) || ' to ' || TRIM(NVL(A.COLUMN_3,'-')) END ||
                CASE WHEN NVL(A.COLUMN_4,'-') != NVL(B.COLUMN_4,'-') THEN ' expected_period from ' || TRIM(NVL(B.COLUMN_4,'-')) || ' to ' || TRIM(NVL(A.COLUMN_4,'-')) END ||
                CASE WHEN NVL(A.COLUMN_2,'-') != NVL(B.COLUMN_5,'-') THEN ' expected_cf_percent from ' || TRIM(NVL(B.COLUMN_5,'-')) || ' to ' || TRIM(NVL(A.COLUMN_5,'-')) END
            AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_1,'-') = NVL(B.COLUMN_1,'-')
        AND
        (
            NVL(A.COLUMN_2,'-') != NVL(B.COLUMN_2,'-')
            OR NVL(A.COLUMN_3,'-') != NVL(B.COLUMN_3,'-')
            OR NVL(A.COLUMN_4,'-') != NVL(B.COLUMN_4,'-')
            OR NVL(A.COLUMN_5,'-') != NVL(B.COLUMN_5,'-')
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,TRIM(NVL(COLUMN_1,'-')) AS CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Individual - Deleted' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID_2
        AND TRIM(NVL(COLUMN_1,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_1,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID
        );

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM IFRS_PARTIAL_RUN_DETAIL
        WHERE HEADERID = V_HEADERID
        AND REASON LIKE 'Individual%';

        IF V_EXISTS = 0 THEN
            IF V_REASON_COUNT = 1 THEN
                DELETE IFRS_PARTIAL_RUN_HEADER
                WHERE PKID = V_HEADERID;
            ELSE
                UPDATE IFRS_PARTIAL_RUN_HEADER
                SET REASON = REPLACE(REASON,',Individual','')
                WHERE PKID = V_HEADERID;
            END IF;
            V_REASON_COUNT := V_REASON_COUNT - 1;
        END IF;

        COMMIT;
    END IF;


    /*=============================================================================================
        3. Check Adjustment Covid List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 22
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in Adjustment Covid'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',Adjustment Covid'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        V_REASON_COUNT := V_REASON_COUNT + 1;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 22
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 22
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE TBLU_ADJUSTMENT_COVID_HIST';

        INSERT INTO TBLU_ADJUSTMENT_COVID_HIST
        (
            DOWNLOAD_DATE,
            NO_REKENING,
            ACCOUNT_NUMBER,
            PERSEN_PENCADANGAN,
            UPLOADID,
            UPLOADBY,
            UPLOADDATE,
            UPLOADHOST
        )
        SELECT DISTINCT
            TO_DATE(A.COLUMN_1,'YYYYMMDD') DOWNLOAD_DATE,
            A.COLUMN_2 AS NO_REKENING,
            B.ACCOUNT_NUMBER,
            CASE WHEN TO_NUMBER(A.COLUMN_4) < 1 THEN TO_NUMBER(A.COLUMN_4) * 100 ELSE TO_NUMBER(A.COLUMN_4) END AS PERSEN_PENCADANGAN,
            A.UPLOADID,
            C.CREATEDBY UPLOADBY,
            C.CREATEDDATE UPLOADDATE,
            C.CREATEDHOST UPLOADHOST
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN IFRS_MASTER_ACCOUNT B
        ON B.DOWNLOAD_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE)
        AND A.COLUMN_2 = SUBSTR(B.ACCOUNT_NUMBER,2,10)
        JOIN TBLT_UPLOAD_POOL C
        ON A.UPLOADID = C.PKID
        WHERE A.UPLOADID = V_LAST_UPLOADID_2;
        COMMIT;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,B.ACCOUNT_NUMBER
            ,'Adjustment Covid - Added' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_ADJUSTMENT_COVID B
        ON A.UPLOADID = B.UPLOADID
        AND TRIM(NVL(A.COLUMN_2,'-')) = B.NO_REKENING
        AND A.UPLOADID = V_LAST_UPLOADID
        AND TRIM(NVL(A.COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID_2
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,C.ACCOUNT_NUMBER
            ,'Adjustment Covid - Modified persen pencadangan from ' || TRIM(NVL(B.COLUMN_4,'-')) || ' to ' || TRIM(NVL(A.COLUMN_4,'-')) AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
        AND NVL(A.COLUMN_4,'-') != NVL(B.COLUMN_4,'-')
        JOIN TBLU_ADJUSTMENT_COVID C
        ON A.UPLOADID = C.UPLOADID
        AND TRIM(NVL(A.COLUMN_2,'-')) = C.NO_REKENING;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,B.ACCOUNT_NUMBER
            ,'Adjustment Covid - Deleted' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_ADJUSTMENT_COVID_HIST B
        ON A.UPLOADID = B.UPLOADID
        AND TRIM(NVL(A.COLUMN_2,'-')) = B.NO_REKENING
        AND A.UPLOADID = V_LAST_UPLOADID_2
        AND TRIM(NVL(A.COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID
        );

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM IFRS_PARTIAL_RUN_DETAIL
        WHERE HEADERID = V_HEADERID
        AND REASON LIKE 'Adjustment Covid%';

        IF V_EXISTS = 0 THEN
            IF V_REASON_COUNT = 1 THEN
                DELETE IFRS_PARTIAL_RUN_HEADER
                WHERE PKID = V_HEADERID;
            ELSE
                UPDATE IFRS_PARTIAL_RUN_HEADER
                SET REASON = REPLACE(REASON,',Adjustment Covid','')
                WHERE PKID = V_HEADERID;
            END IF;
            V_REASON_COUNT := V_REASON_COUNT - 1;
        END IF;

        COMMIT;
    END IF;


    /*=============================================================================================
        4. Check Disaster Loan List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 20
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in Disaster Loan'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',Disaster Loan'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        V_REASON_COUNT := V_REASON_COUNT + 1;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 20
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 20
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'Disaster Loan - Added' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID_2
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(A.COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'Disaster Loan - Modified ecl_amount from ' || TRIM(NVL(B.COLUMN_3,'-')) || ' to ' || TRIM(NVL(A.COLUMN_3,'-')) AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
        AND NVL(A.COLUMN_3,'-') != NVL(B.COLUMN_3,'-');

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'Disaster Loan - Deleted' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID_2
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID
        );

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM IFRS_PARTIAL_RUN_DETAIL
        WHERE HEADERID = V_HEADERID
        AND REASON LIKE 'Disaster Loan%';

        IF V_EXISTS = 0 THEN
            IF V_REASON_COUNT = 1 THEN
                DELETE IFRS_PARTIAL_RUN_HEADER
                WHERE PKID = V_HEADERID;
            ELSE
                UPDATE IFRS_PARTIAL_RUN_HEADER
                SET REASON = REPLACE(REASON,',Disaster Loan','')
                WHERE PKID = V_HEADERID;
            END IF;
            V_REASON_COUNT := V_REASON_COUNT - 1;
        END IF;

        COMMIT;
    END IF;


    /*=============================================================================================
        5. Check SBLC List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 13
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in SBLC'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',SBLC'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        V_REASON_COUNT := V_REASON_COUNT + 1;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 13
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 13
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'SBLC - Added' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID_2
        );

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(A.COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'SBLC - Modified rating from ' || TRIM(NVL(B.COLUMN_3,'-')) || ' to ' || TRIM(NVL(A.COLUMN_3,'-')) AS REASON
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
        AND NVL(A.COLUMN_3,'-') != NVL(B.COLUMN_3,'-');

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            ,'-' AS CUSTOMER_NUMBER
            ,TRIM(NVL(COLUMN_2,'-')) AS ACCOUNT_NUMBER
            ,'SBLC - Deleted' AS REASON
        FROM TBLU_DOC_TEMP_DETAIL
        WHERE UPLOADID = V_LAST_UPLOADID_2
        AND TRIM(NVL(COLUMN_2,'-')) NOT IN
        (
            SELECT TRIM(NVL(COLUMN_2,'-'))
            FROM TBLU_DOC_TEMP_DETAIL
            WHERE UPLOADID = V_LAST_UPLOADID
        );

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM IFRS_PARTIAL_RUN_DETAIL
        WHERE HEADERID = V_HEADERID
        AND REASON LIKE 'SBLC%';

        IF V_EXISTS = 0 THEN
            IF V_REASON_COUNT = 1 THEN
                DELETE IFRS_PARTIAL_RUN_HEADER
                WHERE PKID = V_HEADERID;
            ELSE
                UPDATE IFRS_PARTIAL_RUN_HEADER
                SET REASON = REPLACE(REASON,',SBLC','')
                WHERE PKID = V_HEADERID;
            END IF;
            V_REASON_COUNT := V_REASON_COUNT - 1;
        END IF;

        COMMIT;
    END IF;



    /*=============================================================================================
        6. Check Rating Bank List
    =============================================================================================*/
    SELECT COUNT(A.PKID)
    INTO V_EXISTS
    FROM TBLT_UPLOAD_POOL A
    JOIN TBLU_DOC_TEMP_DETAIL B
    ON A.PKID = B.UPLOADID
    AND A.MAPPINGID = 9
    AND A.STATUS = 'APPROVED'
    AND A.CREATEDDATE > V_ECL_RUN_DATETIME
    AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in Bank Multiplier'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',Bank Multiplier'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        V_REASON_COUNT := V_REASON_COUNT + 1;

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 9
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD');

        SELECT MAX(A.PKID)
        INTO V_LAST_UPLOADID_2
        FROM TBLT_UPLOAD_POOL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.PKID = B.UPLOADID
        AND A.MAPPINGID = 9
        AND A.STATUS = 'APPROVED'
        AND B.COLUMN_1 = TO_CHAR(V_CURRDATE,'YYYYMMDD')
        AND A.PKID < V_LAST_UPLOADID
        AND A.CREATEDDATE < V_ECL_RUN_DATETIME;

        SELECT COUNT(*)
        INTO V_EXISTS
        FROM TBLU_DOC_TEMP_DETAIL A
        JOIN TBLU_DOC_TEMP_DETAIL B
        ON A.UPLOADID = V_LAST_UPLOADID
        AND B.UPLOADID = V_LAST_UPLOADID_2
        AND NVL(A.COLUMN_1,'-') = NVL(B.COLUMN_1,'-')
        AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
        AND NVL(A.COLUMN_5,'-') = NVL(B.COLUMN_5,'-')
        AND NVL(A.COLUMN_9,'-') != NVL(B.COLUMN_9,'-');

        IF V_EXISTS > 0 THEN
            INSERT INTO GTMP_IFRS_MASTER_ACCOUNT
            SELECT * FROM IFRS_MASTER_ACCOUNT
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND DATA_SOURCE IN ('BTRD','KTP');
            COMMIT;

            INSERT INTO IFRS_PARTIAL_RUN_DETAIL
            (
                HEADERID
                ,CUSTOMER_NUMBER
                ,ACCOUNT_NUMBER
                ,REASON
            )
            SELECT DISTINCT V_HEADERID
                ,'-' AS CUSTOMER_NUMBER
                ,C.ACCOUNT_NUMBER
                ,'Bank Multiplier - Modified from ' || TRIM(NVL(B.COLUMN_9,'-')) || ' to ' || TRIM(NVL(A.COLUMN_9,'-')) AS REASON
            FROM TBLU_DOC_TEMP_DETAIL A
            JOIN TBLU_DOC_TEMP_DETAIL B
            ON A.UPLOADID = V_LAST_UPLOADID
            AND B.UPLOADID = V_LAST_UPLOADID_2
            AND NVL(A.COLUMN_1,'-') = NVL(B.COLUMN_1,'-')
            AND NVL(A.COLUMN_2,'-') = NVL(B.COLUMN_2,'-')
            AND NVL(A.COLUMN_5,'-') = NVL(B.COLUMN_5,'-')
            AND NVL(A.COLUMN_9,'-') != NVL(B.COLUMN_9,'-')
            JOIN
            (
                SELECT A2.ACCOUNT_NUMBER, B2.SWIFT_CODE, B2.RATING_SOURCE RATING_SOURCE_ORI
                FROM GTMP_IFRS_MASTER_ACCOUNT A2
                JOIN (
                    SELECT DISTINCT (SUBSTR(SWIFT_CODE,1,8))SWIFT_CODE, RATING_SOURCE,
                    CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK' ELSE RATING_FINAL END AS RATING_FINAL
                    FROM TBLU_RATING_BANK
                    WHERE UPLOADID = V_LAST_UPLOADID
                    AND RATING_SOURCE IN ('PEFINDO','PEF')
                ) B2
                ON B2.SWIFT_CODE = SUBSTR(A2.RESERVED_VARCHAR_15,1,8)
                AND ((DATA_SOURCE ='BTRD' AND RESERVED_VARCHAR_23 IN ('2','3')) OR (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('4','5') AND RESERVED_FLAG_10 = 1)OR DATA_SOURCE IN ('ILS','LIMIT'))
                AND A2.CURRENCY = 'IDR'
                AND RATING_FINAL IS NOT NULL
                UNION ALL
                SELECT A2.ACCOUNT_NUMBER, B2.SWIFT_CODE, B2.RATING_SOURCE_ORI
                FROM GTMP_IFRS_MASTER_ACCOUNT A2
                JOIN (
                    SELECT DISTINCT (SUBSTR(A3.SWIFT_CODE,1,8))SWIFT_CODE, A3.RATING_SOURCE_ORI, COALESCE(B3.STANDARD_AND_POORS, C3.STANDARD_AND_POORS, A3.RATING_FINAL) AS RATING_FINAL
                    FROM (
                        SELECT SWIFT_CODE,
                        CASE WHEN RATING_SOURCE IN ('PEFINDO','PEF','FRS_IND') THEN 'PEFINDO' ELSE 'STANDARD_AND_POORS' END AS RATING_SOURCE,
                        RATING_SOURCE RATING_SOURCE_ORI,
                        CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK' ELSE RATING_FINAL END AS RATING_FINAL
                        FROM TBLU_RATING_BANK
                        WHERE UPLOADID = V_LAST_UPLOADID
                    ) A3
                    LEFT JOIN  IFRS_MASTER_RATING_BANK B3
                    ON A3.RATING_FINAL = B3.STANDARD_AND_POORS
                    LEFT JOIN IFRS_MASTER_RATING_BANK C3
                    ON A3.RATING_FINAL = C3.MOODYS
                    WHERE A3.RATING_SOURCE ='STANDARD_AND_POORS'
                ) B2
                ON B2.SWIFT_CODE = SUBSTR(A2.RESERVED_VARCHAR_15,1,8)
                AND ((DATA_SOURCE ='BTRD' AND RESERVED_VARCHAR_23 IN ('2','3')) OR (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('4','5') AND RESERVED_FLAG_10 = 1)OR DATA_SOURCE IN ('ILS','LIMIT'))
                AND A2.CURRENCY <> 'IDR'
                AND RATING_FINAL IS NOT NULL
                UNION ALL
                SELECT A2.ACCOUNT_NUMBER, B2.SWIFT_CODE, B2.RATING_SOURCE RATING_SOURCE_ORI
                FROM GTMP_IFRS_MASTER_ACCOUNT A2
                JOIN (
                    SELECT DISTINCT (SUBSTR(SWIFT_CODE,1,8))SWIFT_CODE, RATING_SOURCE,
                    CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK' ELSE RATING_FINAL END AS RATING_FINAL
                    FROM TBLU_RATING_BANK
                    WHERE UPLOADID = V_LAST_UPLOADID
                    AND RATING_SOURCE IN ('PEFINDO','PEF')
                ) B2
                ON B2.SWIFT_CODE = SUBSTR(A2.RESERVED_VARCHAR_1,1,8)
                AND ((A2.DATA_SOURCE = 'KTP' AND PRODUCT_CODE LIKE 'PLACEMENT%') OR (A2.DATA_SOURCE = 'RKN'))
                AND A2.CURRENCY = 'IDR'
                AND RATING_FINAL IS NOT NULL
                UNION ALL
                SELECT A2.ACCOUNT_NUMBER, B2.SWIFT_CODE, B2.RATING_SOURCE_ORI
                FROM GTMP_IFRS_MASTER_ACCOUNT A2
                JOIN (
                    SELECT DISTINCT (SUBSTR(A3.SWIFT_CODE,1,8))SWIFT_CODE, A3.RATING_SOURCE_ORI, COALESCE(B3.STANDARD_AND_POORS, C3.STANDARD_AND_POORS, A3.RATING_FINAL) AS RATING_FINAL
                    FROM (
                        SELECT SWIFT_CODE,
                        CASE WHEN RATING_SOURCE IN ('PEFINDO','PEF','FRS_IND') THEN 'PEFINDO' ELSE 'STANDARD_AND_POORS' END AS RATING_SOURCE,
                        RATING_SOURCE RATING_SOURCE_ORI,
                        CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK' ELSE RATING_FINAL END AS RATING_FINAL
                        FROM TBLU_RATING_BANK
                        WHERE UPLOADID = V_LAST_UPLOADID
                    ) A3
                    LEFT JOIN  IFRS_MASTER_RATING_BANK B3
                    ON A3.RATING_FINAL = B3.STANDARD_AND_POORS
                    LEFT JOIN IFRS_MASTER_RATING_BANK C3
                    ON A3.RATING_FINAL = C3.MOODYS
                    WHERE A3.RATING_SOURCE ='STANDARD_AND_POORS'
                ) B2
                ON B2.SWIFT_CODE = SUBSTR(A2.RESERVED_VARCHAR_1,1,8)
                AND ((A2.DATA_SOURCE = 'KTP' AND PRODUCT_CODE LIKE 'PLACEMENT%') OR (A2.DATA_SOURCE = 'RKN'))
                AND A2.CURRENCY <> 'IDR'
                AND RATING_FINAL IS NOT NULL
            ) C
            ON SUBSTR(NVL(A.COLUMN_2,'-'),1,8) = C.SWIFT_CODE
            AND NVL(A.COLUMN_5,'-') = C.RATING_SOURCE_ORI;

            SELECT COUNT(*)
            INTO V_EXISTS
            FROM IFRS_PARTIAL_RUN_DETAIL
            WHERE HEADERID = V_HEADERID
            AND REASON LIKE 'Bank Multiplier%';

            IF V_EXISTS = 0 THEN
                IF V_REASON_COUNT = 1 THEN
                    DELETE IFRS_PARTIAL_RUN_HEADER
                    WHERE PKID = V_HEADERID;
                ELSE
                    UPDATE IFRS_PARTIAL_RUN_HEADER
                    SET REASON = REPLACE(REASON,',Bank Multiplier','')
                    WHERE PKID = V_HEADERID;
                END IF;
                V_REASON_COUNT := V_REASON_COUNT - 1;
            END IF;

            COMMIT;
        END IF;
    END IF;



    /*=============================================================================================
        7. Check Stage Override
    =============================================================================================*/
    SELECT COUNT(*)
    INTO V_EXISTS
    FROM IFRS_STAGE_OVERRIDE_H
    WHERE NVL(UPDATEDDATE, CREATEDDATE) > V_ECL_RUN_DATETIME;

    IF V_EXISTS > 0 THEN
        IF V_HAS_HEADERID = 0 THEN
            INSERT INTO IFRS_PARTIAL_RUN_HEADER
            (
                DOWNLOAD_DATE
                ,RUN_SEQUENCE
                ,REASON
            )
            SELECT V_CURRDATE
                ,V_RUN_SEQUENCE
                ,'Change in Stage Override'
            FROM DUAL;

            SELECT PKID, 1
            INTO V_HEADERID, V_HAS_HEADERID
            FROM IFRS_PARTIAL_RUN_HEADER
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        ELSE
            UPDATE IFRS_PARTIAL_RUN_HEADER
            SET REASON = REASON || ',Stage Override'
            WHERE DOWNLOAD_DATE = V_CURRDATE
            AND RUN_SEQUENCE = V_RUN_SEQUENCE;
        END IF;

        INSERT INTO IFRS_PARTIAL_RUN_DETAIL
        (
            HEADERID
            ,CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,REASON
        )
        SELECT V_HEADERID
            , CUSTOMER_NUMBER
            ,'-' AS ACCOUNT_NUMBER
            ,'Stage Override - Modified' AS REASON
        FROM IFRS_STAGE_OVERRIDE_H
        WHERE NVL(UPDATEDDATE, CREATEDDATE) > V_ECL_RUN_DATETIME;
        COMMIT;
    END IF;


END;