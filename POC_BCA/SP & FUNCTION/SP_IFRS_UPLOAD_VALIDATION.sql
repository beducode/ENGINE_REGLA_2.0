CREATE OR REPLACE PROCEDURE SP_IFRS_UPLOAD_VALIDATION (v_UploadId IN number)
AS
    v_Result number(10);
    v_Message varchar2(100);
    v_Max number(10);
    v_Col number(10) := 1;
    v_ColType varchar2(50);
    v_ColName varchar2(50);
    v_ColDesc varchar2(50);
    v_ColMaxLen number(10);
    v_ColNullStatus varchar2(3);
    v_Count number(10);
    v_query varchar2(4000);
    v_querycount varchar2(4000);
    v_queryCountNull varchar2(4000);
    v_queryCheckNull varchar2(4000);
    v_countNull number(10);
    v_keyHistory varchar2(100);
    v_queryCountKey varchar2(4000);
    v_queryKey varchar2(4000);
    v_ColNumber varchar2(3);
    v_colPK varchar(4000);
    v_queryPK varchar(4000);
    v_queryCountPK varchar(4000);
    v_ColPKDesc varchar(4000);
    v_flagApproval number(1);
    v_CreatedBy varchar2(36);
    v_CreatedDate date;
    v_CreatedHost varchar2(30);
    v_mappingName varchar2(250);
    v_mappingType varchar2(50);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLU_DOC_TEMP_EXCEPTION';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_DESTINATION';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLU_DOC_TEMP_DETAIL';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_SOURCE_HEADER';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_PK';


    SELECT
        MAPPINGNAME
    INTO v_mappingName
    FROM TBLM_MAPPINGRULEHEADER_NEW
    WHERE PKID = (SELECT
        MAPPINGID
    FROM TBLT_UPLOAD_POOL
    WHERE PKID = v_UploadId);

    IF v_mappingName NOT IN ('FORWARD_LOOKING_VAR', 'FORWARD_LOOKING_VAR_FORECAST') THEN
        INSERT INTO GTMP_DESTINATION
        SELECT
            ROW_NUMBER() OVER (ORDER BY COLUMN_ID) NO_URUT,
            UPPER(COLUMN_NAME) COLUMN_DESTINATION,
            DATA_TYPE,
            CASE WHEN DATA_TYPE = 'NUMBER' THEN
                NVL(DATA_PRECISION,50)
            ELSE
                NVL(DATA_LENGTH, 0)
            END AS MAX_LENGTH,
            NULLABLE
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = (SELECT
            TABLEDESTINATION
        FROM TBLM_MAPPINGRULEHEADER_NEW
        WHERE PKID = (SELECT
            MAPPINGID
        FROM TBLT_UPLOAD_POOL
        WHERE PKID = v_UploadId))
        AND COLUMN_NAME NOT IN ('PKID','UPLOADID', 'UPLOADBY', 'UPLOADDATE', 'UPLOADHOST', 'APPROVEDBY', 'APPROVEDDATE', 'APPROVEDHOST');

        COMMIT;

        INSERT INTO GTMP_SOURCE_HEADER(NO_URUT, COLUMN_SOURCE, COLUMN_ALIAS)
        SELECT
            ROW_NUMBER() OVER (ORDER BY PKID) NO_URUT,
            UPPER(COLUMN_NAME) COLUMN_SOURCE,
            'COLUMN_' || TO_CHAR (ROW_NUMBER() OVER (ORDER BY PKID)) COLUMN_ALIAS
        FROM TBLU_DOC_TEMP_HEADER
        WHERE UPLOADID = v_UploadId;

        COMMIT;

        INSERT INTO GTMP_PK
        (
            ORDINAL_POSITION,
            COLUMN_PK,
            COLUMN_DETAIL
        )
        SELECT
            COLUMN_POSITION,
            UPPER(COLUMN_NAME),
            'COLUMN_' || TO_CHAR (C.NO_URUT)
        FROM USER_IND_COLUMNS A
        JOIN USER_INDEXES B
            ON A.INDEX_NAME = B.INDEX_NAME
            AND B.UNIQUENESS = 'UNIQUE' --In order to apply constraints in the upload table, index name must be unique
            AND A.COLUMN_NAME != 'PKID'
        LEFT JOIN GTMP_SOURCE_HEADER C
            ON A.COLUMN_NAME = C.COLUMN_SOURCE
        WHERE A.TABLE_NAME = (SELECT TABLEDESTINATION
                              FROM TBLM_MAPPINGRULEHEADER_NEW
                              WHERE PKID = (SELECT MAPPINGID
                                            FROM TBLT_UPLOAD_POOL
                                            WHERE PKID = v_UploadId
                                            )
                            )
        ORDER BY COLUMN_POSITION ASC;

        COMMIT;

        SELECT COUNT(*)
        INTO v_Count
        FROM GTMP_PK;

        IF (v_Count = 0) THEN
            INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ERRORMESSAGE)
            SELECT
                v_UploadId,
                'Unique index must be created in the target table'
            FROM dual;
            COMMIT;
        ELSE
            INSERT INTO GTMP_TBLU_DOC_TEMP_DETAIL
            SELECT
                RANK() OVER (ORDER BY ROWID ASC) AS NO_URUT,
                A.*
            FROM TBLU_DOC_TEMP_DETAIL A
            WHERE UPLOADID = v_UploadId;

            COMMIT;

            SELECT COUNT(*) INTO v_Count
            FROM (SELECT DISTINCT
                CASE
                    WHEN COLUMN_SOURCE IS NULL OR
                        COLUMN_DESTINATION IS NULL THEN 'NOT MATCH'
                    ELSE 'MATCH'
                END CHECK_HEADER
            FROM GTMP_DESTINATION A
            FULL OUTER JOIN GTMP_SOURCE_HEADER B
                ON A.COLUMN_DESTINATION = B.COLUMN_SOURCE
                AND A.NO_URUT = B.NO_URUT) H
            WHERE CHECK_HEADER = 'NOT MATCH';

            --CHECK HEADER NAME BETWEEN SOURCE AND DESTINATION
            IF (v_Count > 0)
            THEN
                v_Result := 0;
                v_Message := 'Column name or number of uploaded columns does not match.';
                INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ERRORMESSAGE)
                SELECT v_UploadId,v_Message FROM dual;
                COMMIT;
            ELSE
                ---CHECK KEY HISTORY MUST BE ONE IF THE MAPPING TYPE IS DEL_INS
                SELECT
                    MAPPINGTYPE, KEYHISTORY
                INTO v_mappingType, v_keyHistory
                FROM TBLM_MAPPINGRULEHEADER_NEW
                WHERE PKID = (SELECT
                    MAPPINGID
                FROM TBLT_UPLOAD_POOL
                WHERE PKID = v_UploadId);

                IF v_mappingType = 'DEL_INS' THEN
                    SELECT
                    NO_URUT INTO v_ColNumber
                    FROM GTMP_SOURCE_HEADER
                    WHERE COLUMN_SOURCE = v_keyHistory;

                    v_queryCountKey := 'SELECT COUNT(1) FROM (SELECT DISTINCT  COLUMN_' || v_ColNumber || ' FROM GTMP_TBLU_DOC_TEMP_DETAIL)A';
                    v_Message := 'Column must be 1 same date for 1 file.';

                    EXECUTE IMMEDIATE v_queryCountKey INTO v_Count;
                ELSE
                    v_Count := 0;
                END IF;

                IF (v_Count > 1)
                THEN
                    v_Result := 0;
                    INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, COLUMN_NAME, ERRORMESSAGE)
                    SELECT
                        v_UploadId,
                        v_keyHistory,
                        v_Message
                    FROM dual;
                    COMMIT;
                ELSE
                    -- CHECK DATA TYPE AND NULL
                    SELECT
                    COUNT(1) INTO v_Max
                    FROM TBLU_DOC_TEMP_HEADER
                    WHERE UPLOADID = v_UploadId;

                    -- START LOOP
                    WHILE (v_Col <= v_Max)
                    LOOP
                        SELECT DATA_TYPE, MAX_LENGTH, IS_NULLABLE
                        INTO v_ColType,v_ColMaxLen, v_ColNullStatus
                        FROM GTMP_DESTINATION
                        WHERE NO_URUT = v_Col;

                        SELECT
                            COLUMN_SOURCE INTO v_ColDesc
                        FROM GTMP_SOURCE_HEADER
                        WHERE NO_URUT = v_Col;

                        v_ColName := 'COLUMN_' || TO_CHAR (v_Col);

                        v_Result  := 1;
                        v_Message := 'Successfully';

                        IF (v_ColNullStatus = 'N')
                        THEN
                            ---check data null or not
                            v_queryCountNull := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL WHERE (TRIM(' || v_ColName || ') IS NULL OR ' || v_ColName || ' = '''')';
                            v_Message := 'Column cannot be blank.';
                            v_queryCheckNull := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL WHERE (' || v_ColName || ' IS NULL OR ' || v_ColName || ' = '''')';

                            EXECUTE IMMEDIATE  v_queryCountNull INTO v_Count;

                            IF (v_Count > 0) THEN
                                v_Result := 0;
                                EXECUTE IMMEDIATE 'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_queryCheckNull;
                                COMMIT;
                            END IF;
                        END IF;

                        IF (v_ColType IN ('DATE', 'TIMESTAMP') AND v_Result = 1)
                        THEN
                            v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                                            WHERE (LENGTH(' || v_ColName || ') = 6  AND  FN_ISDATE(' || v_ColName || ' + ''01'', ''YYYYMMDD'') = 0) OR (LENGTH(' || v_ColName || ') <> 6 AND FN_ISDATE(' || v_ColName || ', ''YYYYMMDD'') = 0)';

                            v_Message := 'Error inserting text to data type date.';

                            v_query   := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                                          WHERE (LENGTH(' || v_ColName || ') = 6  AND  FN_ISDATE(' || v_ColName || ' + ''01'', ''YYYYMMDD'') = 0) OR (LENGTH(' || v_ColName || ') <> 6 AND FN_ISDATE(' || v_ColName || ', ''YYYYMMDD'') = 0)';

                            EXECUTE IMMEDIATE  v_querycount INTO v_Count;

                            IF (v_Count > 0)
                            THEN
                                v_Result := 0;

                                EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
                                COMMIT;
                            ELSE
                                v_Result := 1;
                                v_Message := 'Successfully';
                            END IF;
                        ELSIF (v_ColType LIKE '%CHAR' AND v_Result = 1)
                        THEN
                            v_querycount := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                                             WHERE LENGTH(' || v_ColName || ') > ' || TO_CHAR (v_ColMaxLen);
                            v_Message := 'Text length is too long.';
                            v_query := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                                        WHERE LENGTH(' || v_ColName || ') > ' || TO_CHAR (v_ColMaxLen);

                            EXECUTE IMMEDIATE  v_querycount INTO v_Count;

                            IF (v_Count > 0)
                            THEN
                                v_Result := 0;

                                EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
                            ELSE
                                v_Result := 1;
                                v_Message := 'Successfully';
                            END IF;
                        ELSIF (v_ColType = 'NUMBER' AND v_ColMaxLen > 1 AND v_Result = 1)
                        THEN
                            v_querycount := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                                             WHERE FN_ISNUMERIC(' || v_ColName || ') = 0';
                            v_Message := 'Error inserting text to data type numeric.';
                            v_query := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL WHERE FN_ISNUMERIC(' || v_ColName || ') = 0';

                            EXECUTE IMMEDIATE  v_querycount INTO v_Count;

                            IF (v_Count > 0)
                            THEN
                                v_Result := 0;

                                EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
                                COMMIT;
                            ELSE
                                v_Result := 1;
                                v_Message := 'Successfully';
                            END IF;
                        ELSIF (v_ColType = 'NUMBER' AND v_ColMaxLen = 1 AND v_Result = 1) --Bit
                        THEN
                            v_querycount := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                                             WHERE UPPER(' || v_ColName || ') NOT IN (''Y'',''N'')';
                            v_Message := 'Error Inserting Data Boolean, Must Y or N.';
                            v_query := 'SELECT UPLOADID,NO_URUT,''' || v_ColDesc || ''',' || v_ColName || ',''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                                        WHERE UPPER(' || v_ColName || ') NOT IN (''Y'',''N'')';

                            EXECUTE IMMEDIATE  v_querycount INTO v_Count;

                            IF (v_Count > 0)
                            THEN
                                v_Result := 0;

                                EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
                                COMMIT;
                            ELSE
                                v_Result := 1;
                                v_Message := 'Successfully';
                            END IF;
                        END IF;

                        v_Col := v_Col + 1;
                    END LOOP;
                    -- END LOOP
                END IF;
            END IF;
        END IF;
    ELSE --Start data type validation for FORWARD_LOOKING_VAR and FORWARD_LOOKING_VAR_FORECAST
        v_Result := 1;

        --Validate ME Period : Check the first column format
        EXECUTE IMMEDIATE 'INSERT INTO GTMP_TBLU_DOC_TEMP_DETAIL
                                (
                                    NO_URUT,
                                    UPLOADID,
                                    COLUMN_1
                                )
                                SELECT RANK() OVER (ORDER BY ROWID ASC) AS NO_URUT,
                                    UPLOADID,
                                    COLUMN_1
                                FROM TBLU_DOC_TEMP_DETAIL
                                WHERE UPLOADID = ' || TO_CHAR(v_UploadId);

        COMMIT;

        v_querycount:= 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                        WHERE ((LENGTH(COLUMN_1) = 6 AND FN_ISDATE(COLUMN_1 || ''01'', ''YYYYMMDD'') = 0) OR (LENGTH(COLUMN_1) <> 6 AND FN_ISDATE(COLUMN_1, ''YYYYMMDD'') = 0)) ';

        v_Message := 'Error inserting text to data type date.';

        v_query   := 'SELECT UPLOADID,NO_URUT,''Period'',COLUMN_1,''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL
                      WHERE ((LENGTH(COLUMN_1) = 6 AND FN_ISDATE(COLUMN_1 || ''01'', ''YYYYMMDD'') = 0) OR (LENGTH(COLUMN_1) <> 6 AND FN_ISDATE(COLUMN_1, ''YYYYMMDD'') = 0)) ORDER BY NO_URUT';

        EXECUTE IMMEDIATE  v_querycount INTO v_Count;

        IF (v_Count > 0)
        THEN
            v_Result := 0;
            EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
            COMMIT;
        END IF;

        --Validate ME Value start from second column
        --Transpose the Macro Economic column base data to row base for the checking
        SELECT
        COUNT(1) INTO v_Max
        FROM TBLU_DOC_TEMP_HEADER
        WHERE UPLOADID = v_UploadId;

        v_Col := 2;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLU_DOC_TEMP_DETAIL';

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
                                    ''' || UPPER(v_ColName) || ''' ME_CODE,
                                    COLUMN_' || TO_CHAR(v_Col) || ' ME_VAL
                                FROM TBLU_DOC_TEMP_DETAIL
                                WHERE UPLOADID = ' || TO_CHAR(v_UploadId);

            COMMIT;

            v_Col := v_Col + 1;
        END LOOP;

        UPDATE GTMP_TBLU_DOC_TEMP_DETAIL
        SET COLUMN_3 = '0'
        WHERE COLUMN_3 = '.' OR COLUMN_3 IS NULL;
        COMMIT;

        v_querycount := 'SELECT COUNT(*) from GTMP_TBLU_DOC_TEMP_DETAIL
                         WHERE FN_ISNUMERIC(COLUMN_3) = 0';
        v_Message := 'Error inserting text to data type numeric.';
        v_query := 'SELECT UPLOADID,NO_URUT,COLUMN_2,COLUMN_3,''' || v_Message || ''' from GTMP_TBLU_DOC_TEMP_DETAIL WHERE FN_ISNUMERIC(COLUMN_3) = 0 ORDER BY COLUMN_2, NO_URUT';

        EXECUTE IMMEDIATE  v_querycount INTO v_Count;

        IF (v_Count > 0)
        THEN
            v_Result := 0;
            EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_query;
            COMMIT;
        END IF;

        INSERT INTO GTMP_PK
        (
            ORDINAL_POSITION,
            COLUMN_PK,
            COLUMN_DETAIL
        )
        SELECT 1, 'ME_PERIOD', 'COLUMN_1' FROM DUAL
        UNION ALL
        SELECT 2, 'ME_CODE', 'COLUMN_2' FROM DUAL
        UNION ALL
        SELECT 3, 'ME_VAL', 'COLUMN_3' FROM DUAL;

        COMMIT;
    END IF;

    IF v_Result > 0 THEN
        -- CHECK DUPLICATE
        SELECT LISTAGG(GTMP_PK.COLUMN_DETAIL,',')
        WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        INTO v_colPK
        FROM GTMP_PK;

        SELECT LISTAGG(GTMP_PK.COLUMN_PK,',')
        WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        INTO v_ColPKDesc
        FROM GTMP_PK;

        v_queryPK :=
        'SELECT A.UPLOADID, A.NO_URUT, ''' || v_ColPKDesc || ''',B.DUP_COL_VALUE, ''Duplicate Data'' FROM GTMP_TBLU_DOC_TEMP_DETAIL A
            INNER JOIN
            (SELECT ' || REPLACE(v_colPK, ',', '||'',''||') || ' AS DUP_COL_VALUE
             FROM  GTMP_TBLU_DOC_TEMP_DETAIL GROUP BY ' || v_colPK || ' HAVING COUNT(*) > 1'
        || ' )B  ON ' || REPLACE(v_colPK, ',', '||'',''||') || ' = B.DUP_COL_VALUE'
        || ' ORDER BY ' || v_colPK || ',A.NO_URUT ';

        v_querycountPK :=
        'SELECT COUNT(1) FROM ( SELECT ' || v_colPK || '
        FROM  GTMP_TBLU_DOC_TEMP_DETAIL GROUP BY ' || v_colPK || ' HAVING COUNT(*) > 1 ) A';

        EXECUTE IMMEDIATE v_querycountPK INTO v_Count;

        IF (v_Count > 0)
        THEN
            v_Result := 0;

            EXECUTE IMMEDIATE  'INSERT INTO GTMP_TBLU_DOC_TEMP_EXCEPTION (UPLOAD_ID, ROWNUMBER, COLUMN_NAME, COLUMN_VALUE, ERRORMESSAGE) ' || v_queryPK;
            COMMIT;
        ELSE
            v_Result := 1;
            v_Message := 'Successfully';
        END IF;
    END IF;

    --UPDATE VALIDATION STATUS
    SELECT COUNT(*)
    INTO v_Count
    FROM GTMP_TBLU_DOC_TEMP_EXCEPTION
    WHERE UPLOAD_ID = v_UploadId;

    IF (v_Count > 0) THEN
        DELETE FROM TBLU_DOC_TEMP_EXCEPTION
        WHERE UPLOADID = v_UploadId;

        COMMIT;

        INSERT INTO TBLU_DOC_TEMP_EXCEPTION
        (
            PKID,
            UPLOADID,
            ROW_NUMBER,
            COLUMN_NAME,
            VALUE,
            ERROR_MESSAGE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST,
            UPDATEDBY,
            UPDATEDDATE,
            UPDATEDHOST
        )
        SELECT
            0 AS PKID,
            A.UPLOAD_ID,
            A.ROWNUMBER,
            A.COLUMN_NAME,
            A.COLUMN_VALUE,
            A.ERRORMESSAGE,
            B.CREATEDBY,
            B.CREATEDDATE,
            B.CREATEDHOST,
            B.UPDATEDBY,
            B.UPDATEDDATE,
            B.UPDATEDHOST
        FROM GTMP_TBLU_DOC_TEMP_EXCEPTION A
        JOIN TBLT_UPLOAD_POOL B
        ON A.UPLOAD_ID = B.PKID;

        COMMIT;

        UPDATE TBLT_UPLOAD_POOL
        SET STATUS = 'VALIDATION FAILED'
        WHERE PKID = v_UploadId;
    ELSE
        --Check bussiness validation based on mapping name

        IF v_mappingName = 'EXPECTED_CASHFLOW' THEN
            SP_IFRS_VALIDATE_EXP_CASHFLOW(v_UploadId);
        ELSIF v_mappingName = 'Worstcase' THEN
            SP_IFRS_VALIDATE_WORSTCASE(v_UploadId);
        ELSIF v_mappingName = 'BI Collectability Override' THEN
            SP_IFRS_BI_OVERRIDE(v_UploadId);
        ELSIF v_mappingName IN ('FORWARD_LOOKING_VAR','FORWARD_LOOKING_VAR_FORECAST') THEN
            SP_IFRS_VALIDATE_FL_VAR(v_UploadId);
        ELSIF v_mappingName = 'OVERRIDE_PD_AFL' THEN
            SP_IFRS_VALIDATE_OVERRIDE_PD(v_UploadId);
        ELSIF v_mappingName = 'OVERRIDE_LGD_AFL' THEN
            SP_IFRS_VALIDATE_OVERRIDE_LGD(v_UploadId);
        ELSIF v_mappingName = 'WO_TREASURY' THEN
            SP_IFRS_VALIDATE_WO_TREASURY(v_UploadId);
        END IF;

        --RE-UPDATE VALIDATION STATUS AFTER BUSSINESS VALIDATION
        SELECT COUNT(*)
        INTO v_Count
        FROM GTMP_TBLU_DOC_TEMP_EXCEPTION
        WHERE UPLOAD_ID = v_UploadId;

        IF (v_Count > 0) THEN
            DELETE FROM TBLU_DOC_TEMP_EXCEPTION
            WHERE UPLOADID = v_UploadId;

            COMMIT;

            INSERT INTO TBLU_DOC_TEMP_EXCEPTION
            (
                PKID,
                UPLOADID,
                ROW_NUMBER,
                COLUMN_NAME,
                VALUE,
                ERROR_MESSAGE,
                CREATEDBY,
                CREATEDDATE,
                CREATEDHOST,
                UPDATEDBY,
                UPDATEDDATE,
                UPDATEDHOST
            )
            SELECT
                0 AS PKID,
                A.UPLOAD_ID,
                A.ROWNUMBER,
                A.COLUMN_NAME,
                A.COLUMN_VALUE,
                A.ERRORMESSAGE,
                B.CREATEDBY,
                B.CREATEDDATE,
                B.CREATEDHOST,
                B.UPDATEDBY,
                B.UPDATEDDATE,
                B.UPDATEDHOST
            FROM GTMP_TBLU_DOC_TEMP_EXCEPTION A
            JOIN TBLT_UPLOAD_POOL B
            ON A.UPLOAD_ID = B.PKID
            ORDER BY A.COLUMN_NAME, A.ROWNUMBER;

            COMMIT;

            UPDATE TBLT_UPLOAD_POOL
            SET STATUS = 'VALIDATION FAILED'
            WHERE PKID = v_UploadId;
        ELSE
            --check Flag approval
            SELECT
                NEEDAPPROVAL
            into v_flagApproval
            FROM TBLM_MAPPINGRULEHEADER_NEW
            WHERE PKID = (SELECT
                MAPPINGID
            FROM TBLT_UPLOAD_POOL
            WHERE PKID = v_UploadId);

            IF v_flagApproval = 0 THEN
                SELECT MAX(CREATEDBY), MAX(CREATEDDATE), MAX(CREATEDHOST)
                INTO v_CreatedBy, v_CreatedDate, v_CreatedHost
                FROM TBLT_UPLOAD_POOL
                WHERE PKID = v_UploadId;

                SP_IFRS_UPLOAD_APPROVAL(v_UploadId,v_CreatedBy,v_CreatedHost);

                v_Count:=0;
            ELSE
                UPDATE TBLT_UPLOAD_POOL
                SET STATUS = 'PENDING'
                WHERE PKID = v_UploadId;
            END IF;
        END IF;
    END IF;

END;