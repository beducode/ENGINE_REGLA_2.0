CREATE OR REPLACE PROCEDUREPROCEDURE SP_IFRS_ACCT_JRNL_INTM_SUMM(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_PARAM_DISABLE_ACCRU_PREV INT;
    V_SL_METHOD VARCHAR(40);
    V_ROUND INT;
    
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
	V_SP_NAME := UPPER(LEFT(FCESIG::REGPROCEDURE::TEXT, POSITION('(' IN FCESIG::REGPROCEDURE::TEXT)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM_SUMM_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_LBM_STAFF_BENEFIT_SUMM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM_SUMM';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT5 := 'IFRS_LBM_STAFF_BENEFIT_SUMM';
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

    SELECT VALUE1 INTO V_SL_METHOD
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SL_METHOD';

    IF V_SL_METHOD IS NULL OR V_SL_METHOD NOT IN ('ECF', 'NO_ECF') THEN 
        V_SL_METHOD := 'ECF';
    END IF;

    V_PARAM_DISABLE_ACCRU_PREV := 0;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_JOURNAL_INTM_SUMM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_JRNL_INTM_SUMM', '');

    --DELETE FIRST  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || 
	' WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
	
    --CREATE TEMPTABLE  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_JRNL1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_JRNL1' || ' 
        (  
            FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,JOURNALCODE  
            ,STATUS  
            ,REVERSE  
            ,N_AMOUNT  
            ,MASTERID  
            ,ACCTNO  
            ,FLAG_CF  
            ,BRANCH  
            ,IS_PNL  
            ,JOURNALCODE2  
            ,PRDTYPE  
        ) SELECT 
            FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,JOURNALCODE  
            ,STATUS  
            ,REVERSE  
            ,N_AMOUNT  
            ,MASTERID  
            ,ACCTNO  
            ,FLAG_CF  
            ,BRANCH  
            ,IS_PNL  
            ,JOURNALCODE2  
            ,PRDTYPE  
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE  
        
        UNION ALL  
        
        SELECT 
            FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,JOURNALCODE  
            ,STATUS  
            ,REVERSE  
            ,N_AMOUNT  
            ,MASTERID  
            ,ACCTNO  
            ,FLAG_CF  
            ,BRANCH  
            ,IS_PNL  
            ,JOURNALCODE2  
            ,PRDTYPE  
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
    
    --INSERT  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (  
            DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,JOURNALCODE  
            ,STATUS  
            ,REVERSE  
            ,N_AMOUNT  
            ,MASTERID  
            ,CREATEDDATE  
            ,ACCTNO  
            ,FLAG_CF  
            ,BRANCH  
            ,IS_PNL  
            ,JOURNALCODE2  
            ,PRDTYPE  
        ) SELECT 
            DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,JOURNALCODE  
            ,STATUS  
            ,REVERSE  
            ,AMOUNT  
            ,MASTERID  
            ,CREATEDDATE  
            ,ACCTNO  
            ,FLAG_CF  
            ,BRANCH  
            ,IS_PNL  
            ,JOURNALCODE2  
            ,PRDTYPE  
        FROM (  
            SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE  
                ,FACNO  
                ,CIFNO  
                ,DATASOURCE  
                ,PRDCODE  
                ,TRXCODE  
                ,CCY  
                ,JOURNALCODE  
                ,STATUS  
                ,REVERSE  
                ,SUM(COALESCE(N_AMOUNT, 0)) AS AMOUNT  
                ,MASTERID  
                ,CURRENT_TIMESTAMP AS CREATEDDATE  
                ,ACCTNO  
                ,FLAG_CF  
                ,BRANCH  
                ,IS_PNL  
                ,JOURNALCODE2  
                ,PRDTYPE  
            FROM ' || 'TMP_JRNL1' || ' 
            GROUP BY 
                FACNO  
                ,CIFNO  
                ,DATASOURCE  
                ,PRDCODE  
                ,TRXCODE  
                ,CCY  
                ,JOURNALCODE  
                ,STATUS  
                ,REVERSE  
                ,MASTERID  
                ,ACCTNO  
                ,FLAG_CF  
                ,BRANCH  
                ,IS_PNL  
                ,JOURNALCODE2  
                ,PRDTYPE  
        ) B ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    -- UPDATE AMORT AMOUNT ON COST FEE SUMM  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_AP' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_AP' || ' 
        (  
            MASTERID  
            ,FLAG_CF  
            ,AMOUNT  
        ) SELECT 
            MASTERID  
            ,FLAG_CF  
            ,SUM(CASE   
                WHEN REVERSE = ''Y''  
                THEN - 1 * N_AMOUNT  
                ELSE N_AMOUNT  
            END) AS AMORT_AMOUNT  
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND JOURNALCODE IN (  
            ''ACCRU''  
            ,''ACCRU_SL''  
            ,''AMORT''  
        )  
        AND TRXCODE <> ''BENEFIT''  
        GROUP BY 
            MASTERID  
            ,FLAG_CF ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET AMORT_FEE = B.AMOUNT  
        FROM ' || 'TMP_AP' || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND B.FLAG_CF = ''F'' ';
    EXECUTE (V_STR_QUERY);
    
    --- RIDWAN FOR AMORT COST  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET AMORT_COST = B.AMOUNT  
        FROM ' || 'TMP_AP' || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND B.FLAG_CF = ''C'' ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_AP' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_AP' || ' 
        (  
            MASTERID  
            ,FLAG_CF  
            ,AMOUNT  
        ) SELECT 
            MASTERID  
            ,FLAG_CF  
            ,SUM(CASE   
                WHEN REVERSE = ''Y''  
                THEN - 1 * N_AMOUNT  
                ELSE N_AMOUNT  
            END) AS AMORT_AMOUNT  
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND JOURNALCODE IN (  
            ''ACCRU''  
            ,''AMORT''  
        )  
        AND TRXCODE = ''BENEFIT''  
        GROUP BY 
            MASTERID  
            ,FLAG_CF ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET AMORT_FEE = B.AMOUNT  
        FROM ' || 'TMP_AP' || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND B.FLAG_CF = ''F'' ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET AMORT_COST = B.AMOUNT  
        FROM ' || 'TMP_AP' || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND B.FLAG_CF = ''C'' ';
    EXECUTE (V_STR_QUERY);
    
    -- UPDATE INITIAL COST FEE  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
            ,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE  
        FROM ' || V_TABLEINSERT1 || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND COALESCE(A.FACILITY_NUMBER, '''') = COALESCE(B.FACNO, '''') ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET 
            INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
            ,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE  
        FROM ' || V_TABLEINSERT1 || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND COALESCE(A.FACILITY_NUMBER, '''') = COALESCE(B.FACNO, '''') ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            INITIAL_BENEFIT = B.AMOUNT_FEE 
            --,INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
            --,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE  
        FROM ' || V_TABLEINSERT5 || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
    
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET 
            INITIAL_BENEFIT = B.AMOUNT_FEE  
            --,INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
            --,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE  
        FROM ' || V_TABLEINSERT5 || ' B  
        WHERE A.MASTERID = B.MASTERID  
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_JRNL_INTM_SUMM', '');

    RAISE NOTICE 'SP_IFRS_ACCT_JRNL_INTM_SUMM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_JRNL_INTM_SUMM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$PROCEDURE$
