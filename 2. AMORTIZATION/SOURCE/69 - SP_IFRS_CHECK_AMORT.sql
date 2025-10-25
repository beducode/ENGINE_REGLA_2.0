CREATE OR REPLACE PROCEDUREPROCEDURE SP_IFRS_CHECK_AMORT(
    IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, 
    IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_PREVMONTH DATE;
    V_CURRMONTH DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_CNT INT;
    
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
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_CHECK_AMORT_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_INTM_SUMM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_CHECK_AMORT';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_INTM_SUMM';
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
    
    V_PREVMONTH := F_EOMONTH(V_PREVDATE, 1, 'M', 'PREV');
    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_CHECK_AMORT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_CHECK_AMORT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT1 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_X' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_X' || ' 
        (  
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,JOURNALCODE  
            ,AMOUNT  
        ) SELECT 
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,CASE   
                WHEN JOURNALCODE IN (  
                    ''ACCRU''  
                    ,''ACCRU_SL''  
                )  
                THEN ''AMORT''  
                ELSE JOURNALCODE  
            END AS JOURNALCODE  
            ,SUM(CASE   
                WHEN REVERSE = ''Y''  
                THEN - 1 * N_AMOUNT  
                ELSE N_AMOUNT  
            END) AS AMOUNT  
        FROM ' || V_TABLEINSERT2 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND JOURNALCODE NOT IN (  
                ''ACRU4''  
                ,''AMRT4''  
            )  
        GROUP BY 
            DOWNLOAD_DATE  
            ,CCY  
            ,CASE   
                WHEN JOURNALCODE IN (  
                    ''ACCRU''  
                    ,''ACCRU_SL''  
                )  
                THEN ''AMORT''  
                ELSE JOURNALCODE  
            END  
            ,MASTERID  
            ,ACCTNO ';
    EXECUTE (V_STR_QUERY);
    -- RAISE NOTICE 'SP_IFRS_CHECK_AMORT | %', V_STR_QUERY;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_X1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_X1' || ' 
        (  
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,JOURNALCODE  
            ,AMOUNT  
        ) SELECT 
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,JOURNALCODE  
            ,AMOUNT  
        FROM ' || 'TMP_X' || '
        WHERE JOURNALCODE = ''DEFA0'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_X2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_X2' || ' 
        (  
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,JOURNALCODE  
            ,AMOUNT  
        ) SELECT 
            DOWNLOAD_DATE  
            ,CCY  
            ,MASTERID  
            ,ACCTNO  
            ,JOURNALCODE  
            ,AMOUNT  
        FROM ' || 'TMP_X' || '
        WHERE JOURNALCODE != ''DEFA0'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (  
            DOWNLOAD_DATE  
            ,MASTERID  
            ,CCY  
            ,DEFA0_AMT  
            ,AMORT_AMT  
            ,UNAMORT_AMT  
            ,ACCOUNT_NUMBER  
        ) SELECT 
            COALESCE(A.DOWNLOAD_DATE, B.DOWNLOAD_DATE) AS DOWNLOAD_DATE2  
            ,COALESCE(A.MASTERID, B.MASTERID) AS MASTERID2  
            ,COALESCE(A.CCY, B.CCY) AS CCY  
            ,A.AMOUNT AS DEFA0_AMT  
            ,B.AMOUNT AS AMORT_AMT  
            ,COALESCE(A.AMOUNT, 0) + COALESCE(B.AMOUNT, 0) AS UNAMORT_AMT  
            ,A.ACCTNO  
        FROM ' || 'TMP_X1' || ' A  
        FULL OUTER JOIN ' || 'TMP_X2' || ' B 
            ON B.MASTERID = A.MASTERID  
            AND A.CCY = B.CCY  
            AND A.ACCTNO = B.ACCTNO ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            IMA_UNAMORT_AMT = COALESCE(B.UNAMORT_FEE_AMT, 0) + COALESCE(B.UNAMORT_COST_AMT, 0) + COALESCE(B.UNAMORT_BENEFIT,0)  
            ,IMA_OUTSTANDING = B.OUTSTANDING  
            ,CONTROL_AMT = COALESCE(A.DEFA0_AMT, 0) + COALESCE(A.AMORT_AMT, 0) -  
                (COALESCE(B.UNAMORT_FEE_AMT, 0) + COALESCE(B.UNAMORT_COST_AMT, 0)+ COALESCE(B.UNAMORT_BENEFIT,0))  
        FROM ' || V_TABLENAME || ' B  
        WHERE B.MASTERID = A.MASTERID  
            AND B.ACCOUNT_NUMBER = A.ACCOUNT_NUMBER  
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || V_TABLEINSERT1 || ' 
        WHERE ABS(CONTROL_AMT) > 1 ';
    EXECUTE (V_STR_QUERY) INTO V_CNT;

    -- IF V_CNT > 0 
    -- THEN 
    --     RAISE EXCEPTION 'SP_IFRS_CHECK_AMORT | CHECK AMORT ERROR';
    -- END IF;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_CHECK_AMORT', '');

    RAISE NOTICE 'SP_IFRS_CHECK_AMORT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_CHECK_AMORT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
