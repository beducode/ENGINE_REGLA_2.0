---- DROP PROCEDURE SP_IFRS_SYNC_PRODUCT_PARAM;

CREATE OR REPLACE PROCEDURE SP_IFRS_SYNC_PRODUCT_PARAM(
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
        V_TABLEINSERT := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TMPTABLE := 'TMP_MARKETRATE_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IFRS_PRODUCT_PARAM';
        V_TMPTABLE := 'TMP_MARKETRATE';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT || ' AS SELECT * FROM IFRS_PRODUCT_PARAM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', V_SP_NAME, '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TMPTABLE || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TMPTABLE || ' AS 
        SELECT EFF_DATE, PRD_CODE, MKT_RATE 
        FROM (
            SELECT PKID, PRD_CODE, EFF_DATE, MKT_RATE 
            ,ROW_NUMBER() OVER (PARTITION BY PRD_CODE ORDER BY EFF_DATE DESC) RN 
            FROM IFRS_MASTER_MARKETRATE_PARAM 
            WHERE EFF_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND IS_DELETE = 0 
        ) A 
        WHERE RN = 1 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' 
        ( 
            DATA_SOURCE 
            ,PRD_TYPE 
            ,PRD_TYPE_1 
            ,PRD_CODE 
            ,PRD_GROUP 
            ,MARKET_RATE 
            ,CCY 
            ,AMORT_TYPE 
            ,IS_STAF_LOAN 
            ,IS_IMPAIRED 
            ,PRODUCT_DESCRIPTION 
            ,FEE_MAT_AMT 
            ,COST_MAT_AMT 
            ,EXPECTED_LIFE 
            ,FEE_MAT_TYPE 
            ,COST_MAT_TYPE 
            -- ,BISEGMENT 
            ,FLAG_AL 
            ,REPAY_TYPE_VALUE 
            ,WORKING_PERIOD 
            ,TENOR_TYPE 
        ) SELECT DISTINCT 
            A.DATA_SOURCE 
            ,A.PRD_TYPE 
            ,A.PRD_TYPE_1 
            ,A.PRD_CODE 
            ,A.PRD_GROUP 
            ,COALESCE(B.MKT_RATE, A.MKT_INT_RATE, 0) 
            ,A.CCY 
            ,A.AMORT_TYPE 
            ,CASE WHEN A.STAFF_LOAN_IND = 1 THEN ''Y'' ELSE ''N'' END 
            ,A.IS_IMPAIRED 
            ,A.PRD_DESC 
            ,A.ORG_FEE_MAT_AMT 
            ,A.TXN_COST_MAT_AMT 
            ,A.EXP_LIFE 
            ,A.ORG_FEE_MAT_TYPE 
            ,A.TXN_COST_MAT_TYPE 
            --,BI_SEGMENT 
            ,A.INST_CLS_VALUE 
            ,A.REPAY_TYPE_VALUE 
            ,A.WORKING_PERIOD 
            ,A.TENOR_TYPE 
        FROM IFRS_MASTER_PRODUCT_PARAM A 
        LEFT JOIN ' || V_TMPTABLE || ' B 
        ON (A.PRD_CODE = B.PRD_CODE OR B.PRD_CODE = ''ALL'')  
        WHERE A.INST_CLS_VALUE IN (''A'', ''O'') 
        AND A.IS_DELETE = 0 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', V_SP_NAME, '');

    RAISE NOTICE 'SP_IFRS_SYNC_PRODUCT_PARAM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_SYNC_PRODUCT_PARAM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;