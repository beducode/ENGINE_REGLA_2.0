---- DROP PROCEDURE SP_IFRS_ACCT_SL_ACF_PMTDATE;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_ACF_PMTDATE(
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
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_CLOSED';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ACCRU_PREV';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_ECF';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_STOP_REV';
        V_TABLEINSERT8 := 'IFRS_IMA_AMORT_CURR';
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

    SELECT CAST(VALUE1 AS INT), CAST(VALUE2 AS INT) INTO V_ROUND, V_FUNCROUND
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SCM003';

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_ACCT_SL_STOP_REV WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_ACF_PMTDATE', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (  
            DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,N_UNAMORT_COST  
            ,N_UNAMORT_FEE  
            ,N_AMORT_COST  
            ,N_AMORT_FEE  
            ,N_ACCRU_COST  
            ,N_ACCRU_FEE  
            ,N_ACCRUFULL_COST  
            ,N_ACCRUFULL_FEE  
            ,ECFDATE  
            ,CREATEDDATE  
            ,CREATEDBY  
            ,MASTERID  
            ,ACCTNO  
            ,DO_AMORT  
            ,BRANCH  
            ,ACF_CODE
            ,FLAG_AL  
        ) SELECT 
            M.DOWNLOAD_DATE  
            ,M.FACILITY_NUMBER  
            ,M.CUSTOMER_NUMBER  
            ,M.DATA_SOURCE  
            ,A.N_UNAMORT_COST  
            ,A.N_UNAMORT_FEE  
            ,A.N_AMORT_COST  
            ,A.N_AMORT_FEE  
            ,A.N_UNAMORT_COST - A.UNAMORT_COST_PREV - COALESCE(A.SW_ADJ_COST, 0)  
            ,A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV - COALESCE(A.SW_ADJ_FEE, 0)  
            ,A.N_UNAMORT_COST - A.UNAMORT_COST_PREV - COALESCE(A.SW_ADJ_COST, 0) AS N_ACCRUFULL_COST  
            ,A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV - COALESCE(A.SW_ADJ_FEE, 0) AS N_ACCRUFULL_FEE  
            ,A.DOWNLOAD_DATE  
            ,CURRENT_TIMESTAMP  
            ,''SP_ACCT_SL_ACF_PMTDATE 1''  
            ,M.MASTERID  
            ,M.ACCOUNT_NUMBER  
            ,''Y'' DO_AMORT  
            ,M.BRANCH_CODE  
            ,''1'' ACFCODE
            ,M.FLAG_AL  
        FROM ' || V_TABLEINSERT6 || ' A  
        JOIN (  
            SELECT 
                M.DOWNLOAD_DATE  
                ,M.MASTERID  
                ,M.ACCOUNT_NUMBER  
                ,M.DATA_SOURCE  
                ,M.BRANCH_CODE  
                ,M.FACILITY_NUMBER  
                ,M.CUSTOMER_NUMBER  
                ,M.IAS_CLASS AS FLAG_AL
            FROM ' || V_TABLEINSERT8 || ' M  
            LEFT JOIN (  
                SELECT DISTINCT 
                    DOWNLOAD_DATE  
                    ,MASTERID  
                FROM ' || V_TABLEINSERT1 || '  
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            ) D 
                ON M.DOWNLOAD_DATE = D.DOWNLOAD_DATE  
                AND M.MASTERID = D.MASTERID  
            WHERE M.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
                AND D.MASTERID IS NULL  
        ) M 
            ON M.MASTERID = A.MASTERID  
        WHERE A.PMTDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.PMTDATE <> A.PREVDATE  
            AND A.AMORTSTOPDATE IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
        SELECT MAX(ID) AS ID  
        FROM ' || V_TABLEINSERT3 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        -- EXCLUDE ACCT REGISTERED @ STOP REV 20160619  
        AND MASTERID NOT IN (  
            SELECT MASTERID  
            FROM ' || V_TABLEINSERT7 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        )  
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' 
        (  
            SUM_AMT  
            ,DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,ACCTNO  
            ,MASTERID  
        ) SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT  
            ,A.DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID  
        FROM (  
            SELECT 
                CASE   
                    WHEN A.FLAG_REVERSE = ''Y''  
                    THEN - 1 * A.AMOUNT  
                    ELSE A.AMOUNT  
                END AS N_AMOUNT  
                ,A.ECFDATE DOWNLOAD_DATE  
                ,A.FACNO  
                ,A.CIFNO  
                ,A.DATASOURCE  
                ,A.ACCTNO  
                ,A.MASTERID  
            FROM ' || V_TABLEINSERT4 || ' A  
            WHERE A.FLAG_CF = ''F''  
        ) A  
        GROUP BY 
            A.DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (  
            FACNO  
            ,CIFNO  
            ,DOWNLOAD_DATE  
            ,ECFDATE  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,AMOUNT  
            ,STATUS  
            ,CREATEDDATE  
            ,ACCTNO  
            ,MASTERID  
            ,FLAG_CF  
            ,FLAG_REVERSE  
            ,BRCODE  
            ,SRCPROCESS  
            ,CREATEDBY  
            ,METHOD  
            ,SEQ  
            ,AMOUNT_ORG  
            ,ORG_CCY  
            ,ORG_CCY_EXRATE  
            ,PRDTYPE  
            ,CF_ID  
        ) SELECT 
            A.FACNO  
            ,A.CIFNO  
            ,A.DOWNLOAD_DATE  
            ,A.ECFDATE  
            ,A.DATASOURCE  
            ,B.PRDCODE  
            ,B.TRXCODE  
            ,B.CCY  
            ,CAST(CAST(B.AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_UNAMORT_FEE AS N_AMOUNT  
            ,B.STATUS  
            ,CURRENT_TIMESTAMP  
            ,A.ACCTNO  
            ,A.MASTERID  
            ,B.FLAG_CF  
            ,B.FLAG_REVERSE  
            ,B.BRCODE  
            ,B.SRCPROCESS  
            ,''SLACF01''  
            ,''SL''  
            ,''1''  
            ,B.AMOUNT_ORG  
            ,B.ORG_CCY  
            ,B.ORG_CCY_EXRATE  
            ,B.PRDTYPE  
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT3 || ' A  
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.ECFDATE = A.ECFDATE  
            AND A.MASTERID = B.MASTERID  
            AND B.FLAG_CF = ''F''  
            AND B.STATUS = ''ACT''  
            AND B.METHOD = ''SL''  
        JOIN ' || 'TMP_T1' || ' C 
            ON C.DOWNLOAD_DATE = A.ECFDATE  
            AND C.MASTERID = A.MASTERID  
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.N_UNAMORT_FEE < 0  
            AND A.ID IN (  
                SELECT ID  
                FROM ' || 'TMP_P1' || ' 
            ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T2' || ' 
        (  
            SUM_AMT  
            ,DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,ACCTNO  
            ,MASTERID  
        ) SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT  
            ,A.DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID  
        FROM (  
            SELECT 
                CASE   
                    WHEN A.FLAG_REVERSE = ''Y''  
                    THEN - 1 * A.AMOUNT  
                    ELSE A.AMOUNT  
                END AS N_AMOUNT  
                ,A.ECFDATE DOWNLOAD_DATE  
                ,A.FACNO  
                ,A.CIFNO  
                ,A.DATASOURCE  
                ,A.ACCTNO  
                ,A.MASTERID  
            FROM ' || V_TABLEINSERT4 || ' A  
            WHERE A.FLAG_CF = ''C''  
        ) A  
        GROUP BY 
            A.DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (  
            FACNO  
            ,CIFNO  
            ,DOWNLOAD_DATE  
            ,ECFDATE  
            ,DATASOURCE  
            ,PRDCODE  
            ,TRXCODE  
            ,CCY  
            ,AMOUNT  
            ,STATUS  
            ,CREATEDDATE  
            ,ACCTNO  
            ,MASTERID  
            ,FLAG_CF  
            ,FLAG_REVERSE  
            ,BRCODE  
            ,SRCPROCESS  
            ,CREATEDBY  
            ,METHOD  
            ,SEQ  
            ,AMOUNT_ORG  
            ,ORG_CCY  
            ,ORG_CCY_EXRATE  
            ,PRDTYPE  
            ,CF_ID  
        ) SELECT 
            A.FACNO  
            ,A.CIFNO  
            ,A.DOWNLOAD_DATE  
            ,A.ECFDATE  
            ,A.DATASOURCE  
            ,B.PRDCODE  
            ,B.TRXCODE  
            ,B.CCY  
            ,CAST(CAST(B.AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_UNAMORT_COST AS N_AMOUNT  
            ,B.STATUS  
            ,CURRENT_TIMESTAMP  
            ,A.ACCTNO  
            ,A.MASTERID  
            ,B.FLAG_CF  
            ,B.FLAG_REVERSE  
            ,B.BRCODE  
            ,B.SRCPROCESS  
            ,''SLACF01''  
            ,''SL''  
            ,''1''  
            ,B.AMOUNT_ORG  
            ,B.ORG_CCY  
            ,B.ORG_CCY_EXRATE  
            ,B.PRDTYPE  
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT3 || ' A  
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.ECFDATE = A.ECFDATE  
            AND A.MASTERID = B.MASTERID  
            AND B.FLAG_CF = ''C''  
            AND B.STATUS = ''ACT''  
        JOIN ' || 'TMP_T2' || ' C 
            ON C.DOWNLOAD_DATE = A.ECFDATE  
            AND C.MASTERID = A.MASTERID  
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.N_UNAMORT_COST > 0  
            AND A.ID IN (  
                SELECT ID  
                FROM ' || 'TMP_P1' || ' 
            ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET STATUS = TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMMDD'')  
        WHERE STATUS = ''ACT''  
        AND MASTERID IN (  
            SELECT DISTINCT MASTERID  
            FROM ' || V_TABLEINSERT3 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND DO_AMORT = ''Y''  
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            ,AMORTSTOPREASON = ''END_ACF''  
        WHERE AMORTENDDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND AMORTSTOPDATE IS NULL  
            AND MASTERID NOT IN (  
                SELECT DISTINCT MASTERID  
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            ) ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_ACF_PMTDATE', '');

    RAISE NOTICE 'SP_IFRS_ACCT_SL_ACF_PMTDATE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_SL_ACF_PMTDATE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;