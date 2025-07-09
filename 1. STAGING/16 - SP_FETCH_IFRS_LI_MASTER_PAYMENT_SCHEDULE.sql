---- DROP PROCEDURE SP_FETCH_IFRS_LI_MASTER_PAYMENT_SCHEDULE;

CREATE OR REPLACE PROCEDURE SP_FETCH_IFRS_LI_MASTER_PAYMENT_SCHEDULE(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEINSERT1 VARCHAR(100);
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
	GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_LI_STG_PAYM_SCHD_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_LI_STG_PAYM_SCHD';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            DBLINK('ifrs9_stg', 'SELECT CURRDATE, PREVDATE FROM STG_PRC_DATE') AS STG_PRC_DATE(CURRDATE DATE, PREVDATE DATE);
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;

    IF V_CURRDATE IS NULL THEN 
        SELECT CURRDATE INTO V_CURRDATE 
        FROM IFRS_PRC_DATE;
    END IF;

    IF V_PREVDATE IS NULL THEN 
        SELECT PREVDATE INTO V_PREVDATE 
        FROM IFRS_PRC_DATE; 
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_LI_STG_PAYM_SCHD WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,PMTDATE 
            ,OSPRN 
            ,PRINCIPAL 
            ,INTEREST 
        ) SELECT 
            DOWNLOAD_DATE 
	        ,MASTERID 
	        ,PMTDATE 
	        ,CASE 
                WHEN B.PRODUCT_CODE IS NULL 
                THEN OSPRN 
                ELSE OSPRN * (PRINCIPAL_PORTION/100) 
            END AS OSPRN 
	        ,CASE 
                WHEN B.PRODUCT_CODE IS NULL 
                THEN PRINCIPAL 
                ELSE PRINCIPAL * (PRINCIPAL_PORTION/100) 
            END AS PRINCIPAL 
	        ,CASE 
                WHEN B.PRODUCT_CODE IS NULL 
                THEN INTEREST 
                ELSE INTEREST * (INTEREST_PORTION/100) 
            END AS INTEREST 
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM IMA_SCHD_LIABILITIES'') 
        AS A ( 
            DOWNLOAD_DATE DATE 
            ,MASTERID VARCHAR(100) 
            ,PMTDATE DATE 
            ,INTEREST_RATE NUMERIC(12,6) 
            ,OSPRN NUMERIC(32,6) 
            ,PRINCIPAL NUMERIC(32,6) 
            ,INTEREST NUMERIC(32,6) 
            ,PLAFOND NUMERIC(32,6) 
            ,COUNTER INT8 
            ,OUTSTANDING NUMERIC(32,6) 
            ,SOURCE_PROCESS VARCHAR(50) 
        ) LEFT JOIN IFRS_JF_PORTION_PARAM B 
        ON RIGHT(A.MASTERID, 3) = B.PRODUCT_CODE 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCE_PROCESS = ''SCHD_LIABILITIES'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_FETCH_IFRS_LI_MASTER_PAYMENT_SCHEDULE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_FETCH_IFRS_LI_MASTER_PAYMENT_SCHEDULE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;