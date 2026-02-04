CREATE OR REPLACE PROCEDURE SP_IFRS_VALIDATE_OVERRIDE_LGD (v_UploadId IN number)
AS
    v_Message varchar2(100);
    v_Count number(10);
    v_ColName varchar2(30);
    v_ColDesc varchar2(50);
    v_query varchar2(4000);
    v_querycount varchar2(4000);
    v_ColName_LGDRuleName varchar2(250);
BEGIN
    --Validate LGD rule name
    v_ColDesc := 'LGD_RULE_NAME';

    SELECT
        COLUMN_ALIAS,COLUMN_ALIAS
    INTO v_ColName,v_ColName_LGDRuleName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
					WHERE ' || v_ColName || ' NOT IN (SELECT LGD_RULE_NAME FROM IFRS_LGD_RULES_CONFIG)';

    v_Message := 'LGD_RULE_NAME not exist in LGD Configuration.';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE ' || v_ColName || ' NOT IN (SELECT LGD_RULE_NAME FROM IFRS_LGD_RULES_CONFIG)';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;



    --Validate FL_YEAR
    v_ColDesc := 'FL_YEAR';

    SELECT
        COLUMN_ALIAS
    INTO v_ColName
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

    --Validate missing FL_YEAR
    v_querycount:= 'SELECT COUNT(*) FROM
                    (
                        SELECT DISTINCT ' || v_ColName_LGDRuleName || ' LGD_RULE_NAME, NVL(' || v_ColName || ',0) FL_YEAR
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.LGD_RULE_NAME, B2.FL_YEAR FROM
                        (
                            SELECT UPLOADID, LGD_RULE_NAME, MAX(FL_YEAR) MAX_FL_YEAR FROM
                            (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_LGDRuleName || ' LGD_RULE_NAME, NVL(' || v_ColName || ',0) FL_YEAR
                            from GTMP_TBLU_DOC_TEMP_DETAIL
                            )
                            GROUP BY UPLOADID, LGD_RULE_NAME
                        ) A2
                        CROSS JOIN
                        (
                            SELECT ROWNUM FL_YEAR
                            FROM   DUAL
                            CONNECT BY ROWNUM <= (SELECT MAX(NVL(' || v_ColName || ',0)) FROM GTMP_TBLU_DOC_TEMP_DETAIL)
                        ) B2
                        WHERE B2.FL_YEAR BETWEEN 1 AND A2.MAX_FL_YEAR
                    ) B
                    ON A.LGD_RULE_NAME = B.LGD_RULE_NAME
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.FL_YEAR IS NULL';

    v_Message := 'FL_YEAR is missing';

    v_query   := 'SELECT UPLOADID,NULL NO_URUT, ''LGD_RULE_NAME,FL_YEAR'' COLUMN_NAME, B.LGD_RULE_NAME || '','' || TO_CHAR(B.FL_YEAR) COLUMN_VALUE,''' || v_Message || ''' FROM
                    (
                    SELECT DISTINCT ' || v_ColName_LGDRuleName || ' LGD_RULE_NAME, NVL(' || v_ColName || ',0) FL_YEAR
                        FROM GTMP_TBLU_DOC_TEMP_DETAIL
                    ) A
                    RIGHT JOIN
                    (
                        SELECT A2.UPLOADID, A2.LGD_RULE_NAME, B2.FL_YEAR FROM
                        (
                            SELECT UPLOADID, LGD_RULE_NAME, MAX(FL_YEAR) MAX_FL_YEAR FROM
                            (
                            SELECT DISTINCT UPLOADID, ' || v_ColName_LGDRuleName || ' LGD_RULE_NAME, NVL(' || v_ColName || ',0) FL_YEAR
                            from GTMP_TBLU_DOC_TEMP_DETAIL
                            )
                            GROUP BY UPLOADID, LGD_RULE_NAME
                        ) A2
                        CROSS JOIN
                        (
                            SELECT ROWNUM FL_YEAR
                            FROM   DUAL
                            CONNECT BY ROWNUM <= (SELECT MAX(NVL(' || v_ColName || ',0)) FROM GTMP_TBLU_DOC_TEMP_DETAIL)
                        ) B2
                        WHERE B2.FL_YEAR BETWEEN 1 AND A2.MAX_FL_YEAR
                    ) B
                    ON A.LGD_RULE_NAME = B.LGD_RULE_NAME
                    AND A.FL_YEAR = B.FL_YEAR
                    WHERE A.FL_YEAR IS NULL';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;


    v_ColDesc := 'LGD_OVERRIDE';

    SELECT
        COLUMN_ALIAS INTO v_ColName
    FROM GTMP_SOURCE_HEADER
    WHERE COLUMN_SOURCE = v_ColDesc;

    v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                    WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 1';

    v_Message := 'LGD_OVERRIDE must between 0 and 1';

    v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                  WHERE NVL(' || v_ColName || ',0) < 0 OR NVL(' || v_ColName || ',0) > 1';

    EXECUTE IMMEDIATE  v_querycount INTO v_Count;

    IF (v_Count > 0)
    THEN
        EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
        COMMIT;
    END IF;

END;