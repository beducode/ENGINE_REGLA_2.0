CREATE OR REPLACE PROCEDURE SP_IFRS_AMORT_ARCHIVE_TABLES
AS
    V_DATE DATE;
    V_MAX_DATE DATE;
    V_COUNT NUMBER(10);
    V_COUNT2 NUMBER(10);
    V_COUNT3 NUMBER(10);
    V_DATA_COUNT NUMBER(10);
    V_PKID NUMBER;
    V_MAX_PKID NUMBER;
    V_SOURCE_TABLE VARCHAR2(30);
    V_DESTINATION_TABLE VARCHAR2(30);
    V_TABLE_SCHEMA VARCHAR2(30);
    V_DATA_RETENTION NUMBER(10);
    V_SPECIFIC_FIELD VARCHAR2(30);
    V_QUERY VARCHAR2(4000);
BEGIN
    SELECT MIN(PKID), MAX(PKID)
    INTO V_PKID, V_MAX_PKID
    FROM IFRS_RETENTION
    WHERE TYPE = 1;

    WHILE V_PKID <= V_MAX_PKID LOOP
        SELECT SOURCE_TABLE, DESTINATION_TABLE, TABLE_SCHEMA, DATA_RETENTION, SPECIFIC_FIELD
        INTO V_SOURCE_TABLE, V_DESTINATION_TABLE, V_TABLE_SCHEMA, V_DATA_RETENTION, V_SPECIFIC_FIELD
        FROM IFRS_RETENTION
        WHERE PKID = V_PKID;

        V_QUERY := 'SELECT COUNT(*) FROM (SELECT DISTINCT ' || V_SPECIFIC_FIELD || ' FROM ' || V_SOURCE_TABLE || ')';
        EXECUTE IMMEDIATE V_QUERY INTO V_DATA_COUNT;

        V_QUERY := 'SELECT MIN(' || V_SPECIFIC_FIELD || '), MAX(' || V_SPECIFIC_FIELD || ') FROM ' || V_SOURCE_TABLE;
        EXECUTE IMMEDIATE V_QUERY INTO V_DATE, V_MAX_DATE;

        WHILE V_DATE <= V_MAX_DATE AND V_DATA_COUNT > V_DATA_RETENTION LOOP
            V_QUERY := 'SELECT COUNT(*) FROM ' || V_SOURCE_TABLE || ' WHERE ' || V_SPECIFIC_FIELD || ' = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'') AND ROWNUM = 1';
            EXECUTE IMMEDIATE V_QUERY INTO V_COUNT;

            IF V_COUNT = 1 THEN
                IF NOT(V_SOURCE_TABLE = 'IFRS_MASTER_ACCOUNT' AND V_DATE = LAST_DAY(V_DATE)) THEN

                    V_QUERY := 'SELECT COUNT(*) FROM IFRS_AMORT_LOG
                                WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')
                                AND OPS = ''INSERT ' || V_DESTINATION_TABLE ||'''';

                    EXECUTE IMMEDIATE V_QUERY INTO V_COUNT2;

                    IF V_COUNT2 > 0 THEN
                        V_QUERY := 'SELECT COUNT(*) FROM ' || V_SOURCE_TABLE || ' WHERE ' || V_SPECIFIC_FIELD || ' = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')';
                        EXECUTE IMMEDIATE V_QUERY INTO V_COUNT3;

                        IF V_COUNT3 > 0 THEN
                            V_QUERY := 'DELETE ' || V_DESTINATION_TABLE ||' WHERE ' || V_SPECIFIC_FIELD || ' = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')';
                            EXECUTE IMMEDIATE V_QUERY;

                            INSERT  INTO IFRS_AMORT_LOG(DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
                            VALUES  ( V_DATE ,
                                  SYSTIMESTAMP ,
                                  'DELETE ' || V_DESTINATION_TABLE ,
                                  'SP_IFRS_AMORT_ARCHIVE_TABLES' ,
                                  ''
                                  );
                            COMMIT;
                        END IF;
                    END IF;

                    V_QUERY := 'INSERT /*+ PARALLEL(12) */ INTO ' || V_DESTINATION_TABLE || '
                                SELECT /*+ PARALLEL(12) */ * FROM ' || V_SOURCE_TABLE || '
                                WHERE ' || V_SPECIFIC_FIELD || ' = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')';

                    EXECUTE IMMEDIATE V_QUERY;

                    INSERT  INTO IFRS_AMORT_LOG(DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
                    VALUES  ( V_DATE ,
                          SYSTIMESTAMP ,
                          'INSERT ' || V_DESTINATION_TABLE ,
                          'SP_IFRS_AMORT_ARCHIVE_TABLES' ,
                          ''
                          );

                    V_QUERY := 'DELETE /*+ PARALLEL(12) */' || V_SOURCE_TABLE || ' WHERE ' || V_SPECIFIC_FIELD || ' = TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')';

                    EXECUTE IMMEDIATE V_QUERY;

                    INSERT  INTO IFRS_AMORT_LOG(DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
                    VALUES  ( V_DATE ,
                          SYSTIMESTAMP ,
                          'DELETE ' || V_SOURCE_TABLE,
                          'SP_IFRS_AMORT_ARCHIVE_TABLES' ,
                          ''
                          );

                    COMMIT;
                END IF;

                V_DATA_COUNT := V_DATA_COUNT - 1;
                V_DATE := V_DATE + 1;
            ELSE
                V_QUERY := 'SELECT NVL(MIN(' || V_SPECIFIC_FIELD || '), TO_DATE(''31 DEC 2100'',''dd MON yyyy'')) FROM ' || V_SOURCE_TABLE || ' WHERE ' || V_SPECIFIC_FIELD || ' > TO_DATE(''' || TO_CHAR(V_DATE, 'DD MON YYYY') || ''',''dd MON yyyy'')';
                EXECUTE IMMEDIATE V_QUERY INTO V_DATE;
            END IF;
        END LOOP;

        SELECT NVL(MIN(PKID),10000000000000)
        INTO V_PKID
        FROM IFRS_RETENTION
        WHERE TYPE = 1
        AND PKID > V_PKID;
    END LOOP;
END;