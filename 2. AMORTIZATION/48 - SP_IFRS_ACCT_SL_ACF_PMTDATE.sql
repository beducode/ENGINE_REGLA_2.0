---- DROP PROCEDURE SP_IFRS_ACCT_COST_FEE_SUMM;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_COST_FEE_SUMM(
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
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_STOP_REV';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT5 := 'IFRS_ACCT_CLOSED';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_ACCT_COST_FEE_SUMM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_ACF_PMTDATE', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        ( 
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,CCY 
            ,AMOUNT_FEE 
            ,AMOUNT_COST 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,AMORT_FEE 
            ,AMORT_COST 
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
            ,'SP_ACCT_SL_ACF_PMTDATE 1'  
            ,M.MASTERID  
            ,M.ACCOUNT_NUMBER  
            ,'Y' DO_AMORT  
            ,M.BRANCH_CODE  
            ,'1' ACFCODE
            ,M.FLAG_AL
    FROM IFRS_ACCT_SL_ECF A
    JOIN (
        SELECT 
            M.DOWNLOAD_DATE  
            ,M.MASTERID  
            ,M.ACCOUNT_NUMBER  
            ,M.DATA_SOURCE  
            ,M.BRANCH_CODE  
            ,M.FACILITY_NUMBER  
            ,M.CUSTOMER_NUMBER
        FROM IFRS_ACCT_SL_ACCRU_PREV M
        WHERE M.STATUS = 'ACT'
    ) M ON A.DOWNLOAD_DATE = M.DOWNLOAD_DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_P1 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_P1 || '  
        ( 
          ID 
        ) SELECT  
          MAX(ID) AS ID  
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND MASTERID IN (
            SELECT MASTERID
            FROM ' || V_TABLEINSERT2 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) 
        GROUP BY MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_P1 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T2 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T1 || '  
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
            SELECT CASE
                WHEN FLAG_REVERSE = 'Y' 
                  THEN -1 * AMOUNT
                ELSE AMOUNT
                END AS N_AMOUNT,
            ,A.ECFDATE DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID
            FROM ' || V_TABLEINSERT3 || ' A
            WHERE A.FLAG_CF = 'F'  
        ) A
        GROUP BY A.DOWNLOAD_DATE  
          ,A.FACNO  
          ,A.CIFNO  
          ,A.DATASOURCE  
          ,A.ACCTNO  
          ,A.MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 ||
        ' ( 
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
            ) 
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
            ,'SLACF01'  
            ,'SL'  
            ,'1'  
            ,B.AMOUNT_ORG  
            ,B.ORG_CCY  
            ,B.ORG_CCY_EXRATE  
            ,B.PRDTYPE  
            ,B.CF_ID
        FROM ' || V_TABLEINSERT1 || ' A
        JOIN ' || V_TABLEINSERT3 || ' B ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'F'
        AND B.STATUS = 'ACT'
        AND B.METHOD = 'SL'
        JOIN TMP_T1 C ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.N_UNAMORT_FEE < 0
        AND A.ID IN (
            SELECT ID
            FROM ' || TMP_P1 || '
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T2 ||
        ' ( 
            SUM_AMT  
            ,DOWNLOAD_DATE  
            ,FACNO  
            ,CIFNO  
            ,DATASOURCE  
            ,ACCTNO  
            ,MASTERID  
            ) 
        ) SELECT  
            SUM(A.N_AMOUNT) AS SUM_AMT  
            ,A.DOWNLOAD_DATE  
            ,A.FACNO  
            ,A.CIFNO  
            ,A.DATASOURCE  
            ,A.ACCTNO  
            ,A.MASTERID
        FROM (
            SELECT CASE
                WHEN FLAG_REVERSE = 'Y' 
                  THEN -1 * AMOUNT
                ELSE AMOUNT
                END AS N_AMOUNT,
            ,A.ECFDATE DOWNLOAD_DATE  
            ,A.FACNO
            ,A.CIFNO
            ,A.DATASOURCE
            ,A.ACCTNO
            ,A.MASTERID
            FROM ' || V_TABLEINSERT3 || ' A
            WHERE A.FLAG_CF = 'C'
        ) A
        GROUP BY A.DOWNLOAD_DATE  
          ,A.FACNO  
          ,A.CIFNO  
          ,A.DATASOURCE  
          ,A.ACCTNO  
          ,A.MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T2 ||
        ' ( 
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
            ) 
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
            ,'SLACF01'  
            ,'SL'  
            ,'1'  
            ,B.AMOUNT_ORG  
            ,B.ORG_CCY  
            ,B.ORG_CCY_EXRATE  
            ,B.PRDTYPE  
            ,B.CF_ID
        FROM ' || V_TABLEINSERT1 || ' A
        JOIN ' || V_TABLEINSERT3 || ' B ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'C'
        AND B.STATUS = 'ACT'
        JOIN TMP_T2 C ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.N_UNAMORT_COST > 0
        AND A.ID IN (
            SELECT ID
            FROM ' || TMP_P1 || '
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 ||
        '
        SET STATUS = ' ||  CONVERT(VARCHAR(8), V_CURRDATE, 112) || '
        WHERE STATUS = ''ACT''
        AND MASTERID IN (
            SELECT DISTINCT MASTERID
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND DO_AMORT = ''Y''
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 ||
        '
        SET AMORTSTOPDATE = ' ||  CONVERT(VARCHAR(8), V_CURRDATE, 112) || '
        ,AMORTSTOPREASON = 'END_ACF'
        AND AMORTENDDATE = ' ||  CONVERT(VARCHAR(8), V_CURRDATE, 112) || '
        WHERE AMORTSTOPDATE IS NULL
        AND MASTERID NOT IN (
            SELECT DISTINCT MASTERID
            FROM ' || V_TABLEINSERT5 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        )

        ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_ACF_PMTDATE', '');

    RAISE NOTICE 'SP_IFRS_ACCT_COST_FEE_SUMM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_COST_FEE_SUMM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;