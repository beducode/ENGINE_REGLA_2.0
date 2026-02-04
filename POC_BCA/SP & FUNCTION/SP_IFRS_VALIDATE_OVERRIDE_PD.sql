CREATE OR REPLACE PROCEDURE SP_IFRS_VALIDATE_OVERRIDE_PD (v_UploadId IN number)
AS
    v_Message varchar2(100);
    v_Count number(10);
    v_ColName varchar2(30);
    v_ColDesc varchar2(50);
    v_query varchar2(4000);
    v_querycount varchar2(4000);
    v_ColName_PDRuleName varchar2(250);
    v_ColName_FL_YEAR varchar2(30);
    v_ColName_BucketID varchar2(30);
BEGIN
    --Validate Segment
    v_ColDesc := 'PD_RULE_NAME';

    SELECT
        COLUMN_ALIAS, COLUMN_ALIAS
    INTO v_ColName, v_ColName_PDRuleName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
					WHERE ' || v_ColName || ' NOT IN (SELECT PD_RULE_NAME FROM IFRS_PD_RULES_CONFIG)';

    v_Message := 'PD Rule Name not exist in PD Configuration.';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE ' || v_ColName || ' NOT IN (SELECT PD_RULE_NAME FROM IFRS_PD_RULES_CONFIG)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    --Validate FL_YEAR
    v_ColDesc := 'FL_YEAR';

    SELECT
        COLUMN_ALIAS,COLUMN_ALIAS
    INTO v_ColName,v_ColName_FL_YEAR
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 1 OR NVL(' || v_ColName || ',0) > 10';

    v_Message := 'FL_YEAR must between 1 and 10';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 1 OR NVL(' || v_ColName || ',0) > 10';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;


    v_ColDesc := 'BUCKET_ID';

    SELECT
        COLUMN_ALIAS
    INTO v_ColName_BucketID
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    --Validate missing FL_YEAR
    v_querycount:= 'SELECT COUNT(*) FROM
                    (
                        SELECT DISTINCT ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_BucketID || ' BUCKET_ID, NVL(' || v_ColName || ',0) FL_YEAR
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.PD_RULE_NAME, A2.BUCKET_ID, C2.FL_YEAR FROM
                        (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_BucketID || ' BUCKET_ID
                            FROM GTMP_TBLU_DOC_TEMP_DETAIL
                        ) A2
                        JOIN IFRS_PD_RULES_CONFIG B2
                        ON A2.PD_RULE_NAME = B2.PD_RULE_NAME
                        CROSS JOIN
                        (
                            SELECT ROWNUM FL_YEAR
                            FROM   DUAL
                            CONNECT BY ROWNUM <= (SELECT MAX(EXPECTED_LIFE) FROM IFRS_PD_RULES_CONFIG)
                        ) C2
                        WHERE C2.FL_YEAR BETWEEN 1 AND B2.EXPECTED_LIFE
                    ) B
                    ON A.PD_RULE_NAME = B.PD_RULE_NAME
                    AND A.BUCKET_ID = B.BUCKET_ID
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.FL_YEAR IS NULL';

    v_Message := 'FL_YEAR is missing';

    v_query   := 'SELECT UPLOADID,NULL NO_URUT, ''PD_RULE_NAME,BUCKET_ID,FL_YEAR'' COLUMN_NAME, B.PD_RULE_NAME || '','' || TO_CHAR(B.BUCKET_ID) || '','' || TO_CHAR(B.FL_YEAR) COLUMN_VALUE,''' || v_Message || ''' FROM
                    (
                    SELECT DISTINCT ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_BucketID || ' BUCKET_ID, NVL(' || v_ColName || ',0) FL_YEAR
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.PD_RULE_NAME, A2.BUCKET_ID, C2.FL_YEAR FROM
                        (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_BucketID || ' BUCKET_ID
                            FROM GTMP_TBLU_DOC_TEMP_DETAIL
                        ) A2
                        JOIN IFRS_PD_RULES_CONFIG B2
                        ON A2.PD_RULE_NAME = B2.PD_RULE_NAME
                        CROSS JOIN
                        (
                            SELECT ROWNUM FL_YEAR
                            FROM   DUAL
                            CONNECT BY ROWNUM <= (SELECT MAX(EXPECTED_LIFE) FROM IFRS_PD_RULES_CONFIG)
                        ) C2
                        WHERE C2.FL_YEAR BETWEEN 1 AND B2.EXPECTED_LIFE
                    ) B
                    ON A.PD_RULE_NAME = B.PD_RULE_NAME
                    AND A.BUCKET_ID = B.BUCKET_ID
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.FL_YEAR IS NULL';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

    --Validate missing Bucket_ID
    v_querycount:= 'SELECT COUNT(*) FROM
                    (
                        SELECT DISTINCT ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_FL_YEAR || ' FL_YEAR, NVL(' || v_ColName_BucketID || ',0) BUCKET_ID
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.PD_RULE_NAME, A2.FL_YEAR, B2.BUCKET_ID
                        FROM
                        (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_FL_YEAR || ' FL_YEAR
                            FROM GTMP_TBLU_DOC_TEMP_DETAIL
                        ) A2
                        JOIN
                        (
                            SELECT DISTINCT A3.PD_RULE_NAME, B3.BUCKET_ID
                            FROM IFRS_PD_RULES_CONFIG A3
                            JOIN IFRS_BUCKET_DETAIL B3
                            ON A3.BUCKET_GROUP = B3.BUCKET_GROUP
                            JOIN VW_IFRS_MAX_BUCKET C3
                            ON B3.BUCKET_GROUP = C3.BUCKET_GROUP
                            AND B3.BUCKET_ID <= C3.MAX_BUCKET_ID
                        ) B2
                        ON A2.PD_RULE_NAME = B2.PD_RULE_NAME
                    ) B
                    ON A.PD_RULE_NAME = B.PD_RULE_NAME
                    AND A.BUCKET_ID = B.BUCKET_ID
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.BUCKET_ID IS NULL';

    v_Message := 'BUCKET_ID is missing';

    v_query   := 'SELECT UPLOADID,NULL NO_URUT, ''PD_RULE_NAME,FL_YEAR,BUCKET_ID'' COLUMN_NAME, B.PD_RULE_NAME || '','' || TO_CHAR(B.FL_YEAR) || '','' || TO_CHAR(B.BUCKET_ID) COLUMN_VALUE,''' || v_Message || ''' FROM
                    (
                        SELECT DISTINCT ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_FL_YEAR || ' FL_YEAR, NVL(' || v_ColName_BucketID || ',0) BUCKET_ID
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.PD_RULE_NAME, A2.FL_YEAR, B2.BUCKET_ID
                        FROM
                        (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_PDRuleName || ' PD_RULE_NAME,' || v_ColName_FL_YEAR || ' FL_YEAR
                            FROM GTMP_TBLU_DOC_TEMP_DETAIL
                        ) A2
                        JOIN
                        (
                            SELECT DISTINCT A3.PD_RULE_NAME, B3.BUCKET_ID
                            FROM IFRS_PD_RULES_CONFIG A3
                            JOIN IFRS_BUCKET_DETAIL B3
                            ON A3.BUCKET_GROUP = B3.BUCKET_GROUP
                            JOIN VW_IFRS_MAX_BUCKET C3
                            ON B3.BUCKET_GROUP = C3.BUCKET_GROUP
                            AND B3.BUCKET_ID <= C3.MAX_BUCKET_ID
                        ) B2
                        ON A2.PD_RULE_NAME = B2.PD_RULE_NAME
                    ) B
                    ON A.PD_RULE_NAME = B.PD_RULE_NAME
                    AND A.BUCKET_ID = B.BUCKET_ID
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.BUCKET_ID IS NULL';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;




    v_ColDesc := 'MPD_OVERRIDE';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 1';

    v_Message := 'MPD_OVERRIDE must between 0 and 1';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 1';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

END;