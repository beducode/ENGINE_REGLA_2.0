---- DROP PROCEDURE SP_FETCH_IFRS_MASTER_WO;

CREATE OR REPLACE PROCEDURE SP_FETCH_IFRS_MASTER_WO(
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
    V_TABLEINSERT VARCHAR(100);
    V_TMPTABLE VARCHAR(100);
    
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
        V_TABLEINSERT := 'IFRS_MASTER_WO_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_MASTER_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IFRS_MASTER_WO';
        V_TABLEINSERT1 := 'IFRS_MASTER_PRODUCT_PARAM';
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

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT || ' AS SELECT * FROM IFRS_MASTER_WO WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCE_TABLE = ''STG_IFRS_MASTER_WO''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCE_TABLE = ''STG_IFRS_MASTER_RECOVERY''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE    
            ,ACCOUNT_NUMBER      
            ,ACCOUNT_STATUS      
            ,CURRENCY      
            ,CUSTOMER_NUMBER      
            ,DATA_SOURCE      
            ,EXCHANGE_RATE       
            ,MASTERID      
            ,BI_COLLECTABILITY    
            ,PRODUCT_CODE     
            ,PRODUCT_GROUP    
            ,PRODUCT_ENTITY     
            ,PRODUCT_TYPE      
            ,WRITEOFF_DATE      
            ,SOURCE_TABLE       
            ,NPL_FLAG      
            ,CO_FLAG      
            ,CREATEDBY      
            ,CREATEDDATE  
            ,WRITEOFF_FLAG
            ,SEGMENT_FLAG 
        ) SELECT DISTINCT 
            DOWNLOAD_DATE,    
            CASE WHEN ISNULL(OLD_ACCOUNT_NUMBER,'') = '' THEN ACCOUNT_NUMBER ELSE OLD_ACCOUNT_NUMBER END AS ACCOUNT_NUMBER,    
            ''W'' AS ACCOUNT_STATUS,    
            CASE WHEN ISNULL(CURRENCY,'') = '' THEN ''IDR'' ELSE CURRENCY END AS CURRENCY,     
            CASE WHEN ISNULL(OLD_CUSTOMER_NUMBER,'') = '' THEN CUSTOMER_NUMBER ELSE OLD_CUSTOMER_NUMBER END AS CUSTOMER_NUMBER,    
            B.DATA_SOURCE AS DATA_SOURCE,    
            1 AS EXCHANGE_RATE,    
            CASE WHEN ISNULL(OLD_ACCOUNT_NUMBER,'') = '' THEN ACCOUNT_NUMBER ELSE OLD_ACCOUNT_NUMBER END AS MASTERID,    
            ''5'' AS BIL_COLLECTABILITY,    
            PRODUCT_CODE,    
            B.PRD_GROUP AS PRODUCT_GROUP,    
            ''C'' AS PRODUCT_ENTITY,  
            B.PRD_TYPE AS PRODUCT_TYPE,                  
            WO_DATE,    
            ''STG_IFRS_MASTER_WO'' AS SOURCE_TABLE,    
            1 AS NPL_FLAG,    
            CO_FLAG,    
            ''IFRS'' CREATEDBY     
            , GETDATE() AS CREATEDDATE  
            , 1 AS WRITEOFF_FLAG
            , ''N/A'' AS SEGMENT_FLAG 
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM STG_IFRS_MASTER_WO'') A
        AS STG_IFRS_MASTER_WO (
            DOWNLOAD_DATE
            ,CUSTOMER_NUMBER
            ,OLD_CUSTOMER_NUMBER
            ,ACCOUNT_NUMBER
            ,OLD_ACCOUNT_NUMBER
            ,PRODUCT_CODE
            ,CURRENCY
            ,WO_DATE
            ,WO_AMOUNT
            ,CO_FLAG
            ,DATA_SOURCE
        )
        LEFT JOIN ' || V_TABLEINSERT1 || ' B
        ON A.PRODUCT_CODE = B.PRD_CODE AND B.IS_DELETE = 0     
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE      
            ,ACCOUNT_NUMBER      
            ,ACCOUNT_STATUS    
            ,CURRENCY      
            ,CUSTOMER_NAME      
            ,CUSTOMER_NUMBER      
            ,DATA_SOURCE      
            ,EXCHANGE_RATE      
            ,MASTERID      
            ,BI_COLLECTABILITY      
            ,PRODUCT_CODE      
            ,PRODUCT_GROUP      
            ,PRODUCT_TYPE      
            ,RECOVERY_AMOUNT      
            ,TOTAL_RECOVERY      
            ,SOURCE_TABLE      
            ,CREATEDBY      
            ,CREATEDDATE 
            ,SEGMENT_FLAG 
        ) SELECT     
            DOWNLOAD_DATE,    
            CASE WHEN ISNULL(OLD_ACCOUNT_NUMBER,'') = '' THEN ACCOUNT_NUMBER ELSE OLD_ACCOUNT_NUMBER END AS ACCOUNT_NUMBER,    
            ''W'' AS ACCOUNT_STATUS,    
            CASE WHEN ISNULL(CURRENCY,'') = '' THEN ''IDR'' ELSE CURRENCY END AS CURRENCY,     
            CUSTOMER_NAME,    
            CASE WHEN ISNULL(OLD_CUSTOMER_NUMBER,'') = '' THEN CUSTOMER_NUMBER ELSE OLD_CUSTOMER_NUMBER END AS CUSTOMER_NUMBER,    
            B.DATA_SOURCE AS DATA_SOURCE,    
            1 AS EXCHANGE_RATE,    
            CASE WHEN ISNULL(OLD_ACCOUNT_NUMBER,'') = '' THEN ACCOUNT_NUMBER ELSE OLD_ACCOUNT_NUMBER END AS MASTERID,    
            ''5'' AS BIL_COLLECTABILITY,    
            PRODUCT_CODE,    
            B.PRD_GROUP AS PRODUCT_GROUP,    
            B.PRD_TYPE AS PRODUCT_TYPE,                 
            ISNULL(RECOVERY_AMOUNT,0) AS RECOVERY_AMOUNT,    
            ISNULL(RECOVERY_AMOUNT,0) AS TOTAL_RECOVERY,    
            ''STG_IFRS_MASTER_RECOVERY'' AS SOURCE_TABLE,    
            ''IFRS'' CREATEDBY     
            , GETDATE() AS CREATEDDATE
            , ''N/A'' AS SEGMENT_FLAG
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM STG_IFRS_MASTER_RECOVERY'') A
        AS STG_IFRS_MASTER_RECOVERY (
            DOWNLOAD_DATE
            ,ACCOUNT_NUMBER
            ,OLD_ACCOUNT_NUMBER
            ,CUSTOMER_NUMBER
            ,OLD_CUSTOMER_NUMBER
            ,CUSTOMER_NAME
            ,CUSTOMER_SEGMENT
            ,WO_DATE
            ,DEFAULT_DATE
            ,EAD_DEFAULT
            ,CURRENCY
            ,RECOVERY_AMOUNT
            ,RECOVERY_DATE
            ,DISCOUNT_RATE
            ,PRODUCT_CODE
            ,DATA_SOURCE
        )
        LEFT JOIN ' || V_TABLEINSERT1 || ' B
        ON A.PRODUCT_CODE = B.PRD_CODE AND B.IS_DELETE = 0     
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_FETCH_IFRS_MASTER_WO | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_FETCH_IFRS_MASTER_WO';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;