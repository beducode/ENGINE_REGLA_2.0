CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_VAS_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_VAS_DEV';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLEINSERT3  VARCHAR2(100);
    V_TABLEINSERT4  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLESELECT2  VARCHAR2(100);
    V_TABLESELECT3  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_DETAIL_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_MINZEROBUCKETID NUMBER(10);
	V_MAXZEROBUCKETID NUMBER(10);
	V_MINPD NUMBER;
	V_MAXPD NUMBER;
	V_PREVPD NUMBER;
	V_i NUMBER(10);
	V_BUCKETGROUP VARCHAR2(30);


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);
    V_SYSCODE      VARCHAR2(10);
    V_PRC         VARCHAR2(5);

    -- RESULT QUERY
    V_QUERYS        CLOB;


BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');
    V_SYSCODE := NVL(P_SYSCODE, '0');
    V_MODEL_ID := V_SYSCODE;
    V_PRC := NVL(P_PRC, 'P');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'TMP_IFRS_PD_VAS_SNP_' || V_RUNID;
        V_TABLEINSERT2 := 'TMP_IFRS_PD_VAS_PEF_' || V_RUNID;
        V_TABLEINSERT3 := 'TMP_IFRS_PD_VAS_SNP_FI_' || V_RUNID;
        V_TABLEINSERT4 := 'TMP_IFRS_PD_VAS_PEF_FI_' || V_RUNID;
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLESELECT2 := 'IFRS_BUCKET_HEADER_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_BUCKET_DETAIL_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'TMP_IFRS_PD_VAS_SNP';
        V_TABLEINSERT2 := 'TMP_IFRS_PD_VAS_PEF';
        V_TABLEINSERT3 := 'TMP_IFRS_PD_VAS_SNP_FI';
        V_TABLEINSERT4 := 'TMP_IFRS_PD_VAS_PEF_FI';
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLESELECT2 := 'IFRS_BUCKET_HEADER';
        V_TABLESELECT3 := 'IFRS_BUCKET_DETAIL';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    ----------------------------------------------------------------
    -- PRE-PROCESSING SIMULATION TABLES
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN
        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_VAS_SNP';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_VAS_PEF';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT3);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT3;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT3 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_VAS_SNP_FI';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT4);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT4;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT4 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_VAS_PEF_FI';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLESELECT1;

    ----------------------------------------------------------------
    -- BUILD DYNAMIC QUERY
    ----------------------------------------------------------------

    -- GET PD_RULE_ID FOR VASICEK METHOD
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || '
	(
        EFF_DATE,
        PD_RULE_ID,
        BUCKET_GROUP
	)
	SELECT
	    TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP
	FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A
	WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD =''VAS''
	AND DERIVED_PD_MODEL IS NULL';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    -- ============================== START PD INTERPOLATION FROM SNP ==============================
    BEGIN
        EXECUTE IMMEDIATE 'SELECT BUCKET_GROUP
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
        WHERE PKID =
        (
            SELECT MAX(PKID) FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
            WHERE OPTION_GROUPING = ''SNP''
            AND IS_DELETED = 0
        )' INTO V_BUCKETGROUP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_BUCKETGROUP := NULL;
    END;

    IF V_BUCKETGROUP IS NOT NULL THEN

        V_STR_QUERY := 'BEGIN ' ||
                    'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                    'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                    'END;';
        EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT1;


        V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
        (
            PKID,
            EFF_DATE,
            BUCKET_GROUP,
            BUCKET_ID,
            PD
        )
        SELECT A.BUCKET_ID PKID,
            TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            B.PD
        FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
        LEFT JOIN ' || V_OWNER || '.TBLU_SNP_RATING B
        ON B.RATING_CODE = A.BUCKET_NAME
        WHERE A.BUCKET_GROUP = :1';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;


        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MIN_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_SNP_RATING B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
                B.MIN_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MAX_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_SNP_RATING B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
                B.MAX_VALUE
            ELSE
                A.PD
            END';
        
        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*), MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_COUNT := 0;
                V_MINZEROBUCKETID := NULL;
        END;

        WHILE V_COUNT > 0 LOOP

            BEGIN
                EXECUTE IMMEDIATE 'SELECT MIN(BUCKET_ID)
                FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
                WHERE BUCKET_ID > ' || V_MINZEROBUCKETID || '
                AND NVL(PD,0) > 0' INTO V_MAXZEROBUCKETID;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    V_MAXZEROBUCKETID := NULL;
            END;        

            IF V_MAXZEROBUCKETID IS NULL THEN
                EXIT;
            END IF;
            
            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            WHERE BUCKET_ID = ' || (V_MINZEROBUCKETID - 1) || '' INTO V_MINPD;


            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            WHERE BUCKET_ID = ' || V_MAXZEROBUCKETID || '' INTO V_MAXPD;


            V_i := V_MINZEROBUCKETID;

            WHILE V_i < V_MAXZEROBUCKETID LOOP
                
                EXECUTE IMMEDIATE 'SELECT PD FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ' WHERE BUCKET_ID = ' || (V_i - 1) INTO V_PREVPD;

                V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
                SET PD = :1 + (:2 - :3)/(:4 - (:5 - 1))
                WHERE BUCKET_ID = :6';

                EXECUTE IMMEDIATE V_STR_QUERY USING V_PREVPD, V_MAXPD, V_MINPD, V_MAXZEROBUCKETID, V_MINZEROBUCKETID, V_i;
                COMMIT;

                V_i := V_i + 1;

            END LOOP;

            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;

        END LOOP;
    END IF;
    -- ============================== END PD INTERPOLATION FROM SNP ==============================
    

    -- =========================== START PD INTERPOLATION FROM PEFINDO ===========================
    BEGIN
        EXECUTE IMMEDIATE 'SELECT BUCKET_GROUP
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
        WHERE PKID =
        (
            SELECT MAX(PKID) FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
            WHERE OPTION_GROUPING = ''PEF''
            AND IS_DELETED = 0
        )' INTO V_BUCKETGROUP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_BUCKETGROUP := NULL;
    END;

    IF V_BUCKETGROUP IS NOT NULL THEN
        V_STR_QUERY := 'BEGIN ' ||
                    'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                    'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                    'END;';
        EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT2;


        V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || '
        (
            PKID,
            EFF_DATE,
            BUCKET_GROUP,
            BUCKET_ID,
            PD
        )
        SELECT A.BUCKET_ID PKID,
            TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            B.PD
        FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
        LEFT JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING B
        ON B.RATING_CODE = A.BUCKET_NAME
        WHERE A.BUCKET_GROUP = :1';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        
        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MIN_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
                B.MIN_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;


        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MAX_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
                B.MAX_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_COUNT := 0;
                V_MINZEROBUCKETID := NULL;
        END;

        WHILE V_COUNT > 0 LOOP

            BEGIN
                EXECUTE IMMEDIATE 'SELECT MIN(BUCKET_ID)
                FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
                WHERE BUCKET_ID > :1
                AND NVL(PD,0) > 0' INTO V_MAXZEROBUCKETID USING V_MINZEROBUCKETID;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    V_MAXZEROBUCKETID := NULL;
            END;

            IF V_MAXZEROBUCKETID IS NULL THEN
                EXIT;
            END IF;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
            WHERE BUCKET_ID = :1 - 1' INTO V_MINPD USING V_MINZEROBUCKETID;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
            WHERE BUCKET_ID = :1' INTO V_MAXPD USING V_MAXZEROBUCKETID;

            V_i := V_MINZEROBUCKETID;

            WHILE V_i < V_MAXZEROBUCKETID LOOP

                EXECUTE IMMEDIATE 'SELECT PD
                FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
                WHERE BUCKET_ID = :1-1' INTO V_PREVPD USING V_i;


                V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT2 || '
                SET PD = :1 + (:2 - :3)/(:4 - (:5 - 1))
                WHERE BUCKET_ID = :6';

                EXECUTE IMMEDIATE V_STR_QUERY USING V_PREVPD, V_MAXPD, V_MINPD, V_MAXZEROBUCKETID, V_MINZEROBUCKETID, V_i;
                COMMIT;

                V_i := V_i + 1;
            END LOOP;


            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;

        END LOOP;
    END IF;
    -- =========================== END PD INTERPOLATION FROM PEFINDO ===========================

    -- ============================== START PD INTERPOLATION FROM SNP FI ==============================
    BEGIN
        EXECUTE IMMEDIATE 'SELECT BUCKET_GROUP
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
        WHERE PKID =
        (
            SELECT MAX(PKID) FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
            WHERE OPTION_GROUPING = ''SNPFI''
            AND IS_DELETED = 0
        )' INTO V_BUCKETGROUP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_BUCKETGROUP := NULL;
    END;

    IF V_BUCKETGROUP IS NOT NULL THEN
        V_STR_QUERY := 'BEGIN ' ||
                    'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                    'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                    'END;';
        EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT3;

        V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || '
        (
            PKID,
            EFF_DATE,
            BUCKET_GROUP,
            BUCKET_ID,
            PD
        )
        SELECT A.BUCKET_ID PKID,
            TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            B.PD
        FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
        LEFT JOIN ' || V_OWNER || '.TBLU_SNP_RATING_FI B
        ON B.RATING_CODE = A.BUCKET_NAME
        WHERE A.BUCKET_GROUP = :1';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;


        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MIN_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_SNP_RATING_FI B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
                B.MIN_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;


        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MAX_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_SNP_RATING_FI B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
                B.MAX_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_COUNT := 0;
                V_MINZEROBUCKETID := NULL;
        END;

        WHILE V_COUNT > 0 LOOP
            
            BEGIN
                EXECUTE IMMEDIATE 'SELECT MIN(BUCKET_ID)
                FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
                WHERE BUCKET_ID > :1
                AND NVL(PD,0) > 0' INTO V_MAXZEROBUCKETID USING V_MINZEROBUCKETID;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    V_MAXZEROBUCKETID := NULL;
            END;

            IF V_MAXZEROBUCKETID IS NULL THEN
                EXIT;
            END IF;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
            WHERE BUCKET_ID = :1 - 1' INTO V_MINPD USING V_MINZEROBUCKETID;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
            WHERE BUCKET_ID = :1' INTO V_MAXPD USING V_MAXZEROBUCKETID;

            V_i := V_MINZEROBUCKETID;

            WHILE V_i < V_MAXZEROBUCKETID LOOP

                EXECUTE IMMEDIATE 'SELECT PD
                FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
                WHERE BUCKET_ID = :1 - 1' INTO V_PREVPD USING V_i;

                V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT3 || '
                SET PD = :1 + (:2 - :3)/(:4 - (:5 - 1))
                WHERE BUCKET_ID = :6';

                EXECUTE IMMEDIATE V_STR_QUERY USING V_PREVPD, V_MAXPD, V_MINPD, V_MAXZEROBUCKETID, V_MINZEROBUCKETID, V_i;
                COMMIT;

                V_i := V_i + 1;
            END LOOP;

            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;

        END LOOP;
    END IF;
    -- ============================== END PD INTERPOLATION FROM SNP FI ==============================

    
    -- -- =========================== START PD INTERPOLATION FROM PEFINDO FI ===========================
    BEGIN
        EXECUTE IMMEDIATE 'SELECT BUCKET_GROUP
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
        WHERE PKID =
        (
            SELECT MAX(PKID) FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
            WHERE OPTION_GROUPING = ''PEFFI''
            AND IS_DELETED = 0
        )' INTO V_BUCKETGROUP;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_BUCKETGROUP := NULL;
    END;

    IF V_BUCKETGROUP IS NOT NULL THEN
        V_STR_QUERY := 'BEGIN ' ||
                    'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT4 || '''); ' ||
                    'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT4 || '''); ' ||
                    'END;';
        EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT4;


        V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT4 || '
        (
            PKID,
            EFF_DATE,
            BUCKET_GROUP,
            BUCKET_ID,
            PD
        )
        SELECT A.BUCKET_ID PKID,
            TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            B.PD
        FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
        LEFT JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING_FI B
        ON B.RATING_CODE = A.BUCKET_NAME
        WHERE A.BUCKET_GROUP = :1';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT4 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MIN_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING_FI B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
                B.MIN_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT4 || ' A
        USING
        (
            SELECT A2.BUCKET_ID, A2.MAX_VALUE
            FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A2
            JOIN ' || V_OWNER || '.TBLU_PEFINDO_RATING_FI B2
            ON B2.RATING_CODE = A2.BUCKET_NAME
            WHERE A2.BUCKET_GROUP = :1
        ) B
        ON (A.BUCKET_ID = B.BUCKET_ID)
        WHEN MATCHED THEN
        UPDATE SET
        A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
                B.MAX_VALUE
            ELSE
                A.PD
            END';

        EXECUTE IMMEDIATE V_STR_QUERY USING V_BUCKETGROUP;
        COMMIT;

        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_COUNT := 0;
                V_MINZEROBUCKETID := NULL;
        END;

        WHILE v_count > 0 LOOP
                
            BEGIN
                EXECUTE IMMEDIATE 'SELECT MIN(BUCKET_ID)
                FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
                WHERE BUCKET_ID > :1
                AND NVL(PD,0) > 0' INTO V_MAXZEROBUCKETID USING V_MINZEROBUCKETID;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    V_MAXZEROBUCKETID := NULL;
            END;

            IF V_MAXZEROBUCKETID IS NULL THEN
                EXIT;
            END IF;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
            WHERE BUCKET_ID = :1 - 1' INTO V_MINPD USING V_MINZEROBUCKETID;

            EXECUTE IMMEDIATE 'SELECT PD
            FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
            WHERE BUCKET_ID = :1' INTO V_MAXPD USING V_MAXZEROBUCKETID;

            V_i := V_MINZEROBUCKETID;

            WHILE V_i < V_MAXZEROBUCKETID LOOP

                EXECUTE IMMEDIATE 'SELECT PD
                FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
                WHERE BUCKET_ID = :1-1' INTO V_PREVPD USING V_i;

                V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT4 || '
                SET PD = :1 + (:2 - :3)/(:4 - (:5 - 1))
                WHERE BUCKET_ID = :6';

                EXECUTE IMMEDIATE V_STR_QUERY USING V_PREVPD, V_MAXPD, V_MINPD, V_MAXZEROBUCKETID, V_MINZEROBUCKETID, V_i;
                COMMIT;

                V_i := V_i + 1;
            END LOOP;

            EXECUTE IMMEDIATE 'SELECT COUNT(*),  MIN(BUCKET_ID)
            FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
            WHERE NVL(PD,0) = 0' INTO V_COUNT, V_MINZEROBUCKETID;

        END LOOP;
    END IF;
    -- =========================== END PD INTERPOLATION FROM PEFINDO FI ===========================

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.IFRS_PD_VAS
	WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') 
	AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
	JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
	JOIN ' || V_OWNER || '.' || V_TABLEINSERT2 || ' B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
	JOIN ' || V_OWNER || '.' || V_TABLEINSERT3 || ' B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
	JOIN ' || V_OWNER || '.' || V_TABLEINSERT4 || ' B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF C_RULE%ISOPEN THEN
            CLOSE C_RULE;
        END IF;

        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;