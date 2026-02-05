CREATE OR REPLACE PROCEDURE SP_IFRS_VALIDATE_FL_VAR (v_UploadId IN number)
AS
    v_Message varchar2(100);
    v_Col number(10) := 1;
    v_ColName varchar2(50);
    v_Count number(10);
    v_Max number(10);
    v_query varchar2(4000);
    v_querycount varchar2(4000);
    v_me_timeseries varchar2(10);
    v_timeseries number(5);
    v_start_Period varchar2(8);
    v_second_Period varchar2(8);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLU_DOC_TEMP_DETAIL';

    --Transpose the Macro Economic column base data to row base for the validation
    SELECT
    COUNT(1) INTO v_Max
    FROM TBLU_DOC_TEMP_HEADER
    WHERE UPLOADID = v_UploadId;

    v_Col := 2;

    -- START LOOP
    WHILE (v_Col <= v_Max)
    LOOP
        SELECT COLUMN_NAME
        INTO v_ColName
        FROM
        (
            SELECT COLUMN_NAME,
                ROW_NUMBER() OVER (ORDER BY PKID) COLUMN_NUMBER
            FROM TBLU_DOC_TEMP_HEADER
            WHERE UPLOADID = v_UploadId
        ) A
        WHERE COLUMN_NUMBER = v_Col;

        EXECUTE IMMEDIATE 'INSERT INTO GTMP_TBLU_DOC_TEMP_DETAIL
                            (
                                NO_URUT,
                                UPLOADID,
                                COLUMN_1,
                                COLUMN_2,
                                COLUMN_3
                            )
                            SELECT RANK() OVER (ORDER BY ROWID ASC) AS NO_URUT,
                                UPLOADID,
                                COLUMN_1,
                                ''' || v_ColName || ''' ME_CODE,
                                COLUMN_' || TO_CHAR(v_Col) || ' ME_VAL
                            FROM TBLU_DOC_TEMP_DETAIL
                            WHERE UPLOADID = ' || TO_CHAR(v_UploadId);

        COMMIT;

        v_Col := v_Col + 1;
    END LOOP;

    --Validate ME Code
    v_querycount := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                     WHERE UPPER(COLUMN_2) NOT IN (SELECT UPPER(ME_CODE) FROM TBLM_MACRO_ECONOMIC)';
    v_Message := 'Macro Economic code should be registered before uploading the detail data.';
    v_query := 'SELECT DISTINCT UPLOADID,NULL,COLUMN_2,'''',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL WHERE UPPER(COLUMN_2) NOT IN (SELECT UPPER(ME_CODE) FROM TBLM_MACRO_ECONOMIC)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    --Validate ME Period : Check Missing Period
    SELECT MONTHS_BETWEEN(MAX(TO_DATE(CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || '01' ELSE COLUMN_1 END, 'YYYYMMDD')),
                          MIN(TO_DATE(CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || '01' ELSE COLUMN_1 END, 'YYYYMMDD'))) + 1,
                          MIN(CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || '01' ELSE COLUMN_1 END)
    INTO v_Max, v_start_Period
    FROM GTMP_TBLU_DOC_TEMP_DETAIL;

    SELECT MIN(CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || '01' ELSE COLUMN_1 END)
    INTO v_second_Period
    FROM GTMP_TBLU_DOC_TEMP_DETAIL
    WHERE NO_URUT = 2;

    SELECT CASE WHEN MONTHS_BETWEEN (TO_DATE(v_second_period, 'YYYYMMDD'), TO_DATE(v_start_Period, 'YYYYMMDD')) = 3 THEN
       'quarterly'
    ELSE
       'monthly'
    END INTO v_me_timeseries
    FROM DUAL;

    IF LOWER(v_me_timeseries) = 'quarterly' THEN
        v_Max := v_Max/3;
        v_timeseries := 3;
    ELSE
        v_timeseries := 1;
    END IF;

    v_querycount := 'SELECT COUNT(*) FROM
                    (SELECT ' || to_char(v_UploadId) || ' UPLOADID,''PERIOD'' COLUMN_NAME,to_char(add_months(to_date(''' || v_start_Period || ''',''YYYYMMDD''), (Rownum - 1) * ' || TO_CHAR(v_timeseries) || '),''YYYYMMDD'') COLUMN_VALUE,''' || v_Message || ''' ERRORMESSAGE
                     FROM DUAL
                     Connect By Rownum <= ' || TO_CHAR(v_Max) || '
                    ) A WHERE COLUMN_VALUE NOT IN
                    (SELECT CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || ''01'' ELSE COLUMN_1 END FROM GTMP_TBLU_DOC_TEMP_DETAIL)';
    v_Message := 'Period is missing';
    v_query := 'SELECT * FROM
                (SELECT ' || to_char(v_UploadId) || ' UPLOADID,''PERIOD'' COLUMN_NAME,to_char(add_months(to_date(''' || v_start_Period || ''',''YYYYMMDD''), (Rownum - 1) * ' || TO_CHAR(v_timeseries) || '),''YYYYMMDD'') COLUMN_VALUE,''' || v_Message || ''' ERRORMESSAGE
                 FROM DUAL
                 Connect By Rownum <= ' || TO_CHAR(v_Max) || '
                ) A WHERE COLUMN_VALUE NOT IN
                (SELECT CASE WHEN LENGTH(COLUMN_1) = 6 THEN COLUMN_1 || ''01'' ELSE COLUMN_1 END FROM GTMP_TBLU_DOC_TEMP_DETAIL)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;


    --Validate ME Period : Check Duplicate Period
    v_querycount := 'SELECT COUNT(*) FROM
                     (SELECT COLUMN_1 FROM GTMP_TBLU_DOC_TEMP_DETAIL
                      WHERE COLUMN_2 = (SELECT MAX(COLUMN_2) FROM GTMP_TBLU_DOC_TEMP_DETAIL) --take only one me_code for period comparison
                     GROUP BY COLUMN_1 HAVING COUNT(COLUMN_1) > 1) A';
    v_Message := 'Duplicate Period';
    v_query := 'SELECT DISTINCT UPLOADID,NO_URUT,''PERIOD'',COLUMN_1,''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL WHERE COLUMN_1 IN
               (SELECT COLUMN_1 FROM GTMP_TBLU_DOC_TEMP_DETAIL
                WHERE COLUMN_2 = (SELECT MAX(COLUMN_2) FROM GTMP_TBLU_DOC_TEMP_DETAIL)
                GROUP BY COLUMN_1 HAVING COUNT(COLUMN_1) > 1)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

END;