CREATE OR REPLACE PROCEDURE USPS_UPLOADHISTORICALDATA
(
    v_tableDest in VARCHAR2 default ' ',
    v_keyHistory in VARCHAR2 default ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(1000);
    v_mappingType VARCHAR2(100);
    v_ColumnAlias VARCHAR2(1000);
    v_DataType VARCHAR2(100);
BEGIN

    SELECT MAPPINGTYPE
    INTO v_mappingType
    FROM TBLM_MAPPINGRULEHEADER_NEW
    WHERE TABLEDESTINATION = v_tableDest;

    IF v_mappingType = 'DEL_INS' THEN
        SELECT COLUMN_ALIAS, DATA_TYPE
        INTO v_ColumnAlias, v_DataType
        FROM
        (
            SELECT COLUMN_NAME,
                'COLUMN_' || TO_CHAR (ROW_NUMBER() OVER (ORDER BY COLUMN_ID)) COLUMN_ALIAS,
                DATA_TYPE
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = v_tableDest
                AND COLUMN_NAME NOT IN ('PKID','UPLOADID', 'UPLOADBY', 'UPLOADDATE', 'UPLOADHOST', 'APPROVEDBY', 'APPROVEDDATE', 'APPROVEDHOST')
        ) A WHERE COLUMN_NAME =  UPPER(v_keyHistory);

        IF v_DataType = 'DATE' THEN
            v_ColumnAlias := 'TO_CHAR((CASE LENGTH(TRIM(' || v_ColumnAlias || '))
                WHEN 0 THEN NULL
                WHEN 6 THEN LAST_DAY(TO_DATE(' || v_ColumnAlias || ' || ''01'', ''YYYYMMDD''))
                ELSE TO_DATE(' || v_ColumnAlias || ', ''YYYYMMDD'')
            END), ''dd-MON-yyyy'')';
        END IF;

        v_Query :=
            'SELECT DISTINCT ' || v_ColumnAlias || ' AS KEYFIELD,
                B.CREATEDDATE UPLOADDATE,
                B.APPROVEDDATE,
                A.UPLOADID
            FROM TBLU_DOC_TEMP_HEADER A
            JOIN TBLT_UPLOAD_POOL B
            ON A.UPLOADID = B.PKID
            JOIN TBLM_MAPPINGRULEHEADER_NEW C
            ON B.MAPPINGID = C.PKID
            AND B.STATUS = ''APPROVED''
            AND C.TABLEDESTINATION = ''' || v_tableDest || '''
            JOIN TBLU_DOC_TEMP_DETAIL D
            ON A.UPLOADID = D.UPLOADID
            ORDER BY UPLOADDATE DESC';
    ELSE
        v_Query :=
            'SELECT DISTINCT ' || v_keyHistory || ' AS KEYFIELD,
                B.CREATEDDATE UPLOADDATE,
                B.APPROVEDDATE,
                A.UPLOADID
            FROM TBLU_DOC_TEMP_HEADER A
            JOIN TBLT_UPLOAD_POOL B
            ON A.UPLOADID = B.PKID
            JOIN TBLM_MAPPINGRULEHEADER_NEW C
            ON B.MAPPINGID = C.PKID
            AND B.STATUS = ''APPROVED''
            AND C.TABLEDESTINATION = ''' || v_tableDest || '''
            ORDER BY UPLOADDATE DESC';
    END IF;

    DBMS_OUTPUT.PUT_LINE(v_Query);

    OPEN Cur_out FOR v_Query;

END;