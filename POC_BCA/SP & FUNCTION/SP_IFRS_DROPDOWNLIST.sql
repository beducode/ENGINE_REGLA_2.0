CREATE OR REPLACE PROCEDURE SP_IFRS_DROPDOWNLIST
(
    V_IsEndOfTheDay VARCHAR2 DEFAULT 'YES'
)
AS
    V_CURRDATE  DATE;
    V_COLUMN_NAME VARCHAR2(30);
    V_COLUMN_SOURCE VARCHAR2(30);
    V_TABLE_NAME VARCHAR2(30);
    V_TABLE_SOURCE VARCHAR2(30);
    V_STR_SQL VARCHAR2(4000);
    V_QUERY VARCHAR2(4000);
    V_COUNT NUMBER(10);
    V_SEQUENCE NUMBER(10);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_TBLM_DROPDOWNLIST';

    V_QUERY := 'INSERT INTO GTMP_TBLM_DROPDOWNLIST
                (
                    SEQUENCE,
                    TABLE_NAME,
                    TABLE_SOURCE,
                    COLUMN_NAME,
                    COLUMN_SOURCE
                )
                    SELECT ROWNUM,
                    A.PARENTCOMMONCODEVALUE AS TABLE_NAME,
                    B.VALUE3 AS TABLE_SOURCE,
                    A.VALUE1 AS COLUMN_NAME,
                    B.VALUE2 AS COLUMN_SOURCE
                FROM TBLM_COMMONCODEDETAIL A
                JOIN TBLM_COMMONCODEDETAIL B
                ON A.COMMONCODE = ''B31''
                AND B.COMMONCODE = ''B32''
                AND B.PARENTCOMMONCODE = A.COMMONCODE
                AND A.VALUE1 = B.VALUE1
                AND A.PARENTCOMMONCODEVALUE = B.PARENTCOMMONCODEVALUE
                WHERE A.VALUE2 IN (''CHAR'',''VARCHAR2'',''VARCHAR'')';

    IF (V_IsEndOfTheDay != 'YES') THEN
        V_QUERY := V_QUERY || '
                    AND B.VALUE3 NOT IN (''IFRS_MASTER_ACCOUNT'')';
    END IF;

    EXECUTE IMMEDIATE V_QUERY;

    COMMIT;


    SELECT COUNT(*)
    INTO V_COUNT
    FROM TBLM_DROPDOWNLIST;

    SELECT CURRDATE
    INTO V_CURRDATE
    FROM IFRS_PRC_DATE;

    V_SEQUENCE := 1;

    IF (V_COUNT <= 0) THEN
        BEGIN

            SELECT COUNT(*)
            INTO V_COUNT
            FROM GTMP_TBLM_DROPDOWNLIST;

            WHILE (V_SEQUENCE <= V_COUNT) LOOP
                SELECT TABLE_NAME, COLUMN_NAME, TABLE_SOURCE, COLUMN_SOURCE
                INTO V_TABLE_NAME, V_COLUMN_NAME, V_TABLE_SOURCE, V_COLUMN_SOURCE
                FROM GTMP_TBLM_DROPDOWNLIST
                WHERE SEQUENCE = V_SEQUENCE;

                V_STR_SQL := 'INSERT INTO TBLM_DROPDOWNLIST
                              (
                                TABLE_NAME,
                                COLUMN_NAME,
                                VALUE
                              )
                              SELECT DISTINCT ''' || NVL(V_TABLE_NAME, '') || ''',
                                        ''' || NVL(V_COLUMN_NAME, '') || ''' ,
                                ' || NVL(V_COLUMN_SOURCE, '') || '
                              FROM ' || V_TABLE_SOURCE || ' A
                              WHERE NVL('|| V_COLUMN_SOURCE || ', '' '') != '' ''';

                EXECUTE IMMEDIATE V_STR_SQL;

                COMMIT;

                V_SEQUENCE := V_SEQUENCE + 1;
            END LOOP;
    END;
    ELSE
        BEGIN
            SELECT COUNT(*)
            INTO V_COUNT
            FROM GTMP_TBLM_DROPDOWNLIST;

            WHILE (V_SEQUENCE <= V_COUNT) LOOP
                SELECT TABLE_NAME, COLUMN_NAME, TABLE_SOURCE, COLUMN_SOURCE
                INTO V_TABLE_NAME, V_COLUMN_NAME, V_TABLE_SOURCE, V_COLUMN_SOURCE
                FROM GTMP_TBLM_DROPDOWNLIST
                WHERE SEQUENCE = V_SEQUENCE;

                V_STR_SQL := 'INSERT INTO TBLM_DROPDOWNLIST
                              (
                                 TABLE_NAME,
                                 COLUMN_NAME,
                                 VALUE
                              )
                              SELECT DISTINCT ''' || NVL(V_TABLE_NAME, '') || ''',
                                 ''' || NVL(V_COLUMN_NAME, '') || ''' ,
                                 ' || NVL(V_COLUMN_SOURCE, '') || '
                              FROM ' || NVL(V_TABLE_SOURCE, '') || ' A
                              WHERE ' ||
                                CASE WHEN V_TABLE_SOURCE IN ('IFRS_MASTER_ACCOUNT')
                                    THEN 'A.DOWNLOAD_DATE =  ''' || TO_CHAR(V_CURRDATE,'dd-MON-yyyy') || ''' AND '
                                    ELSE ''
                                END
                                || NVL(V_COLUMN_SOURCE, '') || ' NOT IN ( SELECT VALUE FROM TBLM_DROPDOWNLIST WHERE TABLE_NAME = ''' || NVL(V_TABLE_NAME, '') || ''' AND COLUMN_NAME = ''' || NVL(V_COLUMN_NAME, '') || ''')
                                AND NVL('|| V_COLUMN_SOURCE || ', '' '') != '' ''';

                EXECUTE IMMEDIATE V_STR_SQL;

                COMMIT;

                V_SEQUENCE := V_SEQUENCE + 1;
            END LOOP;
        END;
    END IF;

END;