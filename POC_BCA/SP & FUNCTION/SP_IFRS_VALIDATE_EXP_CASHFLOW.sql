CREATE OR REPLACE PROCEDURE SP_IFRS_VALIDATE_EXP_CASHFLOW (v_UploadId IN number)
AS
    v_Message varchar2(100);
    v_Count number(10);
    v_ColName varchar2(30);
    v_ColName2 varchar2(30);
    v_ColDesc varchar2(50);
    v_query varchar2(4000);
    v_querycount varchar2(4000);
BEGIN
    --Validate customer_number
    v_ColDesc := 'CUSTOMER_NUMBER';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_ColName2 := v_ColName;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

    EXECUTE IMMEDIATE 'INSERT INTO GTMP_IFRS_MASTER_ACCOUNT
                        (
                            PKID,
                            DOWNLOAD_DATE,
                            MASTERID,
                            MASTER_ACCOUNT_CODE,
                            DATA_SOURCE,
                            GLOBAL_CUSTOMER_NUMBER,
                            CUSTOMER_NUMBER,
                            ACCOUNT_NUMBER,
                            CURRENCY
                        )
                        SELECT
                            PKID,
                            DOWNLOAD_DATE,
                            MASTERID,
                            MASTER_ACCOUNT_CODE,
                            DATA_SOURCE,
                            GLOBAL_CUSTOMER_NUMBER,
                            CUSTOMER_NUMBER,
                            ACCOUNT_NUMBER,
                            CURRENCY
                        FROM IFRS_MASTER_ACCOUNT
                        WHERE DOWNLOAD_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE)
                        AND CUSTOMER_NUMBER IN (SELECT DISTINCT ' || v_ColName || ' FROM GTMP_TBLU_DOC_TEMP_DETAIL)';

    COMMIT;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
					WHERE ' || v_ColName || ' NOT IN (SELECT CUSTOMER_NUMBER FROM GTMP_IFRS_MASTER_ACCOUNT)';

    v_Message := 'Customer not exist in master account.';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE ' || v_ColName || ' NOT IN (SELECT CUSTOMER_NUMBER FROM GTMP_IFRS_MASTER_ACCOUNT)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;


    v_ColDesc := 'EXPECTED_PERIOD';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
					WHERE TO_DATE(' || v_ColName || ', ''YYYYMMDD'') < (SELECT LAST_DAY(CURRDATE) FROM IFRS_PRC_DATE)';

    v_Message := 'Expected period must be greater or equal to system date.';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE TO_DATE(' || v_ColName || ', ''YYYYMMDD'') < (SELECT CURRDATE FROM IFRS_PRC_DATE)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    v_ColDesc := 'DISCOUNT_RATE_TRS';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    v_Message := 'Discount_Rate_Trs must between 0 and 100';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    v_ColDesc := 'DISCOUNT_RATE_TRF';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    v_Message := 'Discount_Rate_Trf must between 0 and 100';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    v_ColDesc := 'EXPECTED_CF_PERCENT';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    v_Message := 'Expected CF Percent must between 0 and 100';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 100';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    --Validate total EXPECTED_CF_PERCENT per customer must be less than 100
    v_querycount:= 'SELECT COUNT(*) FROM
                    (SELECT ' || v_ColName2 || ' FROM GTMP_TBLU_DOC_TEMP_DETAIL
                     GROUP BY ' || v_ColName2 || ' HAVING SUM(NVL(' || v_ColName || ',0)) > 100)';

    v_Message := 'Sum Expected CF Percent Per Customer must be less than 100';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE ' || v_ColName2 || ' IN
                 (SELECT ' || v_ColName2 || ' FROM GTMP_TBLU_DOC_TEMP_DETAIL
                  GROUP BY ' || v_ColName2 || ' HAVING SUM(NVL(' || v_ColName || ',0)) > 100)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

END;