CREATE OR REPLACE PROCEDURE SP_IFRS_BI_OVERRIDE (v_UploadId IN number)
AS
    v_Message varchar2(100);
    v_Count number(10);
    v_ColName varchar2(30);
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

    v_ColDesc := 'BI_COLLECTABILITY';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) <= 0 OR NVL(' || v_ColName || ',0) > 5';

    v_Message := 'BI Collectability must between 1 and 5';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) <= 0 OR NVL(' || v_ColName || ',0) > 5';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

END;