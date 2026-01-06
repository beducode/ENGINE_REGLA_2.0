    CREATE OR REPLACE PROCEDURE IFRS9.SP_IFRS_IMP_INITIAL_CONFIG (
        P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
        P_DOWNLOAD_DATE IN DATE,
        P_PRC           IN VARCHAR2 DEFAULT 'S'
    )
    AUTHID CURRENT_USER
    AS
        -- DATES / COUNTERS
        V_CURRDATE      DATE;
        V_PREVDATE      DATE;
        V_COUNT         NUMBER;

        -- DYNAMIC SQL
        V_STR_QUERY     CLOB;

        -- TABLE NAMES
        V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'IFRS9';
        V_TABLENAME  VARCHAR2(100);

        -- MISC
        V_RETURNROWS    NUMBER;
        V_RETURNROWS2   NUMBER;
        V_TABLEDEST     VARCHAR2(100);
        V_COLUMNDEST    VARCHAR2(100);
        V_SP_NAME       VARCHAR2(100);
        V_OPERATION     VARCHAR2(100);
        V_RUNID VARCHAR2(50);
        V_PRC   VARCHAR2(1);

        -- RESULT QUERY
        V_QUERYS        CLOB;
    BEGIN
        -- SET PROCEDURE NAME
        V_SP_NAME := 'SP_IFRS_IMP_INITIAL_CONFIG';
    
        -- SET IF NULL
        V_RUNID := NVL(P_RUNID, 'S_00000_0000');
        V_PRC   := NVL(P_PRC, 'S');

        -- HANDLE DEFAULT DOWNLOAD DATE
        IF P_DOWNLOAD_DATE IS NULL THEN
            SELECT CURRDATE
            INTO V_CURRDATE
            FROM IFRS9.IFRS_PRC_DATE; 
        ELSE
            V_CURRDATE := P_DOWNLOAD_DATE;
        END IF;

        IF V_PRC = 'S' THEN
            V_TABLENAME  := 'IFRS_LGD_RULES_CONFIG_' || V_RUNID;
        ELSE
            V_TABLENAME  := 'IFRS_LGD_RULES_CONFIG';
        END IF;

    IFRS9.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, 0, SYSDATE);
    COMMIT;

        ----------------------------------------------------------------
        -- PRE-SIMULATION TABLE: CREATE/DROP TEMP TABLES IF V_PRC = 'S'
        ----------------------------------------------------------------
        IF V_PRC = 'S' THEN
            -- DROP TABLE IF EXISTS
            SELECT COUNT(*) INTO V_COUNT
            FROM ALL_TABLES
            WHERE OWNER = V_TAB_OWNER
            AND TABLE_NAME = UPPER(V_TABLENAME);

            IF V_COUNT > 0 THEN
                V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLENAME;
                EXECUTE IMMEDIATE V_STR_QUERY;
            END IF;

            V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLENAME ||
                        ' AS SELECT *  FROM ' || V_TAB_OWNER || '.IFRS_LGD_RULES_CONFIG';
            EXECUTE IMMEDIATE V_STR_QUERY;

        END IF;
        COMMIT;


        V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLENAME;
        V_COLUMNDEST := '-';
        V_OPERATION := 'INSERT';
    
        IFRS9.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
        COMMIT;  
    
        V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLENAME;

        IFRS9.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
        COMMIT;

    END;