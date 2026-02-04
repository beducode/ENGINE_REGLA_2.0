CREATE OR REPLACE PROCEDURE SP_IFRS_UPLOAD_APPROVAL
(
    v_uploadId number,
    v_CreatedBy varchar2,
    v_CreatedHost varchar2
)
AS
    v_Max number(10);
    v_Col number(10) := 1;
    v_ColName varchar2(50);
    v_approvedBy varchar2(36) := v_CreatedBy;
    v_approvedHost varchar2(30) := v_CreatedHost;
    v_tableName varchar2(50);
    v_colNumber varchar2(4000);
    v_colMaxLen varchar2(4000);
    v_colDesc varchar2(4000);
    v_colSource varchar2(4000);
    v_query varchar2(4000);
    v_queryCheckUploadId varchar2(4000);
    v_queryDelete varchar2(4000);
    v_queryMaxLen varchar2(4000);
    v_queryTruncate varchar2(4000);
    v_count number;
    v_keyHistory varchar2(100);
    v_queryKeyHistory varchar2(4000);
    v_keyHistoryValue varchar2(4000);
    v_keyHistoryAlias varchar2(4000);
    v_mappingType varchar2(100);
    v_mappingName varchar2(250);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_SOURCE_HEADER';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLU_DOC_TEMP_DETAIL';

    SELECT TABLEDESTINATION, MAPPINGNAME
    INTO v_tableName, v_mappingName
    FROM TBLM_MAPPINGRULEHEADER_NEW
    WHERE PKID = (SELECT
        MAPPINGID
    FROM TBLT_UPLOAD_POOL
    WHERE PKID = v_UploadId);

    IF v_mappingName NOT IN ('FORWARD_LOOKING_VAR','FORWARD_LOOKING_VAR_FORECAST') THEN
        INSERT INTO GTMP_TBLU_DOC_TEMP_DETAIL
        SELECT
            RANK() OVER (ORDER BY ROWID ASC) AS NO_URUT,
            A.*
        FROM TBLU_DOC_TEMP_DETAIL A
        WHERE UPLOADID = v_UploadId;

        COMMIT;
    ELSE
        --Transpose the Macro Economic column base data to row base
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
                                    COLUMN_1 ME_PERIOD,
                                    UPPER(''' || v_ColName || ''') ME_CODE,
                                    COLUMN_' || TO_CHAR(v_Col) || ' ME_VAL
                                FROM TBLU_DOC_TEMP_DETAIL
                                WHERE UPLOADID = ' || TO_CHAR(v_UploadId);

            COMMIT;

            v_Col := v_Col + 1;
        END LOOP;
    END IF;

    INSERT INTO GTMP_SOURCE_HEADER (NO_URUT, COLUMN_SOURCE, COLUMN_ALIAS, DATA_TYPE_DESTINATION, MAX_LENGTH)
    SELECT
        ROW_NUMBER() OVER (ORDER BY COLUMN_ID) NO_URUT,
        COLUMN_NAME,
        'COLUMN_' || TO_CHAR (ROW_NUMBER() OVER (ORDER BY COLUMN_ID)) COLUMN_ALIAS,
        DATA_TYPE,
        CASE WHEN DATA_TYPE = 'NUMBER' THEN
            NVL(DATA_PRECISION,50)
        ELSE
            NVL(DATA_LENGTH, 0)
        END AS MAX_LENGTH
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = v_tableName
    AND COLUMN_NAME NOT IN ('PKID','UPLOADID', 'UPLOADBY', 'UPLOADDATE', 'UPLOADHOST', 'APPROVEDBY', 'APPROVEDDATE', 'APPROVEDHOST');

    COMMIT;

    SELECT COUNT(*)
    INTO v_count
    FROM GTMP_SOURCE_HEADER
    WHERE DATA_TYPE_DESTINATION = 'DATE';

    IF v_count > 0 THEN
        SELECT LISTAGG('NVL(MAX(LENGTH(' || GTMP_SOURCE_HEADER.COLUMN_ALIAS || ')),0) AS ' || GTMP_SOURCE_HEADER.COLUMN_ALIAS,',')
        WITHIN GROUP (ORDER BY NO_URUT)
        INTO v_colMaxLen
        FROM GTMP_SOURCE_HEADER
        WHERE DATA_TYPE_DESTINATION = 'DATE';

        SELECT LISTAGG(GTMP_SOURCE_HEADER.COLUMN_ALIAS,',')
        WITHIN GROUP (ORDER BY NO_URUT)
        INTO v_colSource
        FROM GTMP_SOURCE_HEADER
        WHERE DATA_TYPE_DESTINATION = 'DATE';

        v_queryMaxLen := 'MERGE INTO GTMP_SOURCE_HEADER A
                          USING
                            (
                                SELECT * FROM
                                (
                                    SELECT ' || v_colMaxLen || '
                                    FROM tblu_doc_temp_detail WHERE uploadid = ' || to_char(v_UploadId) || '
                                ) UNPIVOT (MAX_LENGTH FOR COLUMN_NAME IN (' || v_colSource || '))
                            ) B
                          ON (A.COLUMN_ALIAS = B.COLUMN_NAME)
                          WHEN MATCHED THEN
                          UPDATE SET A.MAX_LENGTH = B.MAX_LENGTH';

        EXECUTE IMMEDIATE v_queryMaxLen;

        COMMIT;
    END IF;

    SELECT LISTAGG(CASE WHEN (DATA_TYPE_DESTINATION = 'DATE' AND MAX_LENGTH = 6) THEN
                        'CASE WHEN LENGTH(TRIM(' || COLUMN_ALIAS || ')) = 0 THEN NULL ELSE LAST_DAY(TO_DATE(' || COLUMN_ALIAS || ' || ''01'',''YYYYMMDD'')) END AS ' || COLUMN_ALIAS
                   WHEN DATA_TYPE_DESTINATION = 'DATE' THEN
                        'CASE WHEN LENGTH(TRIM(' || COLUMN_ALIAS || ')) = 0 THEN NULL ELSE TO_DATE(' || COLUMN_ALIAS || ',''YYYYMMDD'') END AS ' || COLUMN_ALIAS
                   WHEN DATA_TYPE_DESTINATION = 'NUMBER' AND MAX_LENGTH = 1 THEN
                        'CASE WHEN UPPER(' || COLUMN_ALIAS || ') = ''Y'' THEN 1 ELSE 0 END AS ' || COLUMN_ALIAS
                   WHEN DATA_TYPE_DESTINATION = 'NUMBER' AND v_mappingName IN ('FORWARD_LOOKING_VAR', 'FORWARD_LOOKING_VAR_FORECAST')  THEN
                        'CASE WHEN ' || COLUMN_ALIAS || ' = ''.'' THEN NULL ELSE ' || COLUMN_ALIAS || ' END AS ' || COLUMN_ALIAS
                   ELSE COLUMN_ALIAS
                   END,',')
    WITHIN GROUP (ORDER BY NO_URUT)
    INTO v_colSource
    FROM GTMP_SOURCE_HEADER;

    SELECT LISTAGG(COLUMN_SOURCE,',')
    WITHIN GROUP (ORDER BY NO_URUT)
    INTO v_colDesc
    FROM GTMP_SOURCE_HEADER;

    v_query := 'INSERT INTO ' || v_tableName || ' (' || v_colDesc || ',UPLOADID,UPLOADBY,UPLOADDATE,UPLOADHOST,APPROVEDBY,APPROVEDDATE,APPROVEDHOST) ' ||
                    'SELECT ' || v_colSource || ',' || TO_CHAR (v_uploadId) || ',B.CREATEDBY,B.CREATEDDATE,B.CREATEDHOST,'''
                    || NVL(v_approvedBy, 'NULL') || ''', CASE WHEN B.CREATEDBY = ''' || NVL(v_approvedBy, 'NULL') || ''' THEN CREATEDDATE ELSE SYSTIMESTAMP END,''' || NVL(v_approvedHost, 'NULL') || '''
                   FROM GTMP_TBLU_DOC_TEMP_DETAIL A
                   INNER JOIN TBLT_UPLOAD_POOL B ON A.UPLOADID = B.PKID
                   ORDER BY NO_URUT ASC';

    COMMIT;

    SELECT
        MAPPINGTYPE INTO v_mappingType
    FROM TBLM_MAPPINGRULEHEADER_NEW
    WHERE PKID = (SELECT MAPPINGID FROM TBLT_UPLOAD_POOL WHERE PKID = v_uploadId);

    IF v_mappingName NOT IN ('FORWARD_LOOKING_VAR','FORWARD_LOOKING_VAR_FORECAST') AND v_mappingType = 'DEL_INS' THEN

        SELECT KEYHISTORY
        INTO v_keyHistory
        FROM TBLM_MAPPINGRULEHEADER_NEW
        WHERE PKID = (SELECT MAPPINGID FROM TBLT_UPLOAD_POOL WHERE PKID = v_uploadId);

        SELECT CASE WHEN DATA_TYPE_DESTINATION = 'DATE' AND MAX_LENGTH = 6 THEN
                  'LAST_DAY(TO_DATE(' || COLUMN_ALIAS || ' || ''01'',''YYYYMMDD'')'
               WHEN DATA_TYPE_DESTINATION = 'DATE' THEN
                  'TO_DATE(' || COLUMN_ALIAS || ',''YYYYMMDD'')'
               ELSE
                  COLUMN_ALIAS
               END INTO v_keyHistoryAlias
        FROM GTMP_SOURCE_HEADER
        WHERE COLUMN_SOURCE = v_keyHistory;

        SELECT CASE WHEN DATA_TYPE_DESTINATION = 'DATE' THEN
                  'select distinct ''to_date('''''' || to_char(' || v_keyHistoryAlias || ',''YYYYMMDD'') || '''''',''''YYYYMMDD'''')'' from GTMP_TBLU_DOC_TEMP_DETAIL'
               ELSE
                  'select distinct ' || v_keyHistoryAlias || ' from GTMP_TBLU_DOC_TEMP_DETAIL'
               END INTO v_queryKeyHistory
        FROM GTMP_SOURCE_HEADER
        WHERE COLUMN_SOURCE = v_keyHistory;

        EXECUTE IMMEDIATE v_queryKeyHistory INTO v_keyHistoryValue;
    END IF;

    v_queryCheckUploadId := 'SELECT COUNT(*) FROM ' || v_tableName || ' WHERE ' || v_keyHistory || ' = ' || v_keyHistoryValue || ' AND ROWNUM <=1';
    v_queryDelete := 'DELETE FROM ' || v_tableName || ' WHERE ' || v_keyHistory || ' = ' || v_keyHistoryValue || '';
    v_queryTruncate := 'TRUNCATE TABLE ' || v_tableName;

    IF v_mappingType = 'DEL_INS'
    THEN
        EXECUTE IMMEDIATE  v_queryCheckUploadId INTO v_count;

        IF v_count > 0
        THEN
            EXECUTE IMMEDIATE  v_queryDelete;
        END IF;
    ELSE
        EXECUTE IMMEDIATE  v_queryTruncate;
    END IF;

    EXECUTE IMMEDIATE  v_query;

    COMMIT;

    IF v_mappingName = 'EXPECTED_CASHFLOW' THEN
        SP_IFRS_IA_SEQUENCE(v_uploadId, '1-JAN-1900');
    ELSIF v_mappingName = 'FORWARD_LOOKING_VAR' THEN
        SP_IFRS_INSERT_FL_VAR(v_uploadId);
    ELSIF v_mappingName = 'ADJUSTMENT_COVID' THEN
        SP_IFRS_INSERT_ADJ_COVID(v_uploadId);
    ELSIF v_mappingName = 'OVERRIDE_PD_AFL' THEN
        SP_IFRS_INSERT_PD_TS_OVERRIDE(v_UploadId);
    ELSIF v_mappingName = 'OVERRIDE_LGD_AFL' THEN
        SP_IFRS_INSERT_LGD_TS_OVERRIDE(v_UploadId);
    END IF;

    IF v_tableName = 'TBLU_RATING_BANK' THEN
        SP_IFRS_INSERT_RTG_BANK_FINAL(v_uploadId);
    END IF;

    UPDATE TBLT_UPLOAD_POOL
    SET STATUS = 'APPROVED',
        APPROVEDBY = v_CreatedBy,
        APPROVEDDATE = CASE WHEN CREATEDBY = v_CreatedBy THEN
                           CREATEDDATE
                       ELSE
                           SYSTIMESTAMP
                       END
    WHERE PKID = v_uploadId;

    COMMIT;
END;