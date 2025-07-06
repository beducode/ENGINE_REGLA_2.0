---- DROP PROCEDURE SP_IFRS_LBM_ACCT_EIR_ACF_PMTDT;

CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_ACF_PMTDT(
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
    V_TMPTABLE1 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_NUM INT;
    
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
        V_TABLEINSERT1 := 'IFRS_LBM_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT4 := 'TMP_P1_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_LBM_ACCT_EIR_ACF';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ECF';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT4 := 'TMP_P1';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV';
        V_TABLEINSERT6 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF';
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

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', '');
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'CLEAN UP');

    -- PREPARE INDEX  
    --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_ECF_IDX1';  
    --EXECUTE IMMEDIATE 'DROP INDEX IFRS_IMA_AMORT_CURR_IDX1';  
    --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_ECF_IDX1 ON ' || V_TABLEINSERT2 || ' (MASTERID,PREV_PMT_DATE,PMT_DATE,AMORTSTOPDATE)';  
    --EXECUTE IMMEDIATE 'CREATE INDEX IFRS_IMA_AMORT_CURR_IDX1 ON ' || V_TABLEINSERT2 ||' (MASTERID)';  
    -- INSERT ACF  

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
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
        ,FLAG_AL -- AR : ADDITIONAL ASSET / LIAB FLAG  
        ,N_ACCRU_NOCF -- FROM NO COST FEE ECF  
        ,N_UNAMORT_NOCF  
        ,N_UNAMORT_PREV_NOCF  
        )
        SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
             ,M.FACILITY_NUMBER  
            ,M.CUSTOMER_NUMBER  
            ,M.DATA_SOURCE  
            ,A.N_COST_UNAMORT_AMT  
            ,A.N_FEE_UNAMORT_AMT  
            ,C.N_COST_UNAMORT_AMT - A.N_COST_UNAMORT_AMT  
            ,C.N_FEE_UNAMORT_AMT - A.N_FEE_UNAMORT_AMT  
            ,A.N_COST_AMORT_AMT - COALESCE(A.SW_ADJ_COST, 0)
            ,A.N_FEE_AMORT_AMT - COALESCE(A.SW_ADJ_FEE, 0)
            ,A.N_COST_AMORT_AMT - COALESCE(A.SW_ADJ_COST, 0) AS N_ACCRUFULL_COST 
            ,A.N_FEE_AMORT_AMT - COALESCE(A.SW_ADJ_FEE, 0) AS N_ACCRUFULL_FEE  
            ,A.DOWNLOAD_DATE
            ,CURRENT_TIMESTAMP
            ,''SP_ACCT_EIR_ACF_PMTDATE 1''
            ,M.MASTERID  
            ,M.ACCOUNT_NUMBER  
            ,''Y'' DO_AMORT  
            ,M.BRANCH_CODE  
            ,''1'' ACFCODE  
            ,M.IAS_CLASS  
            ,A.NOCF_AMORT_AMT  
            ,A.NOCF_UNAMORT_AMT  
            ,A.NOCF_UNAMORT_AMT_PREV
        FROM ' || V_TABLEINSERT2  || ' A
        JOIN (
            SELECT 
                M.DATA_SOURCE  
                ,M.BRANCH_CODE  
                ,M.MASTERID  
                ,M.ACCOUNT_NUMBER  
                ,M.FACILITY_NUMBER  
                ,M.CUSTOMER_NUMBER  
                ,M.IAS_CLASS
                FROM ' || V_TABLEINSERT2  || ' M
                LEFT JOIN (
                    SELECT 
                        DOWNLOAD_DATE 
                        ,MASTERID
                        FROM ' || IFRS_ACCT_CLOSED || ' M
                        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                        
                        ) D ON M.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                        AND M.MASTERID = D.MASTERID
                        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                        AND D.MASTERID IS NULL
                 ) M ON M.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT2  || ' C ON C.AMORTSTOPDATE IS NULL
            AND C.MASTERID = M.MASTERID 
            AND C.PMT_DATE = C.PREV_PMT_DATE  
        WHERE A.PMT_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.PMT_DATE <> A.PREV_PMT_DATE
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE

            /* REMARKS 20180824
            --TEST KALO SWITCH JGN BIKIN AMORT LAGI  
            AND A.MASTERID NOT IN (  
            SELECT MASTERID  
            FROM ' || IFRS_ACCT_SWITCH ||'  
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            )
            20180824*/  
            
            /* REMARKS.. TUNNING SCRIPT 20160602  
            FROM ' || V_TABLEINSERT3 ||' M
            JOIN ' || V_TABLEINSERT2  ||' A ON A.AMORTSTOPDATE IS NULL  
                AND A.MASTERID=M.MASTERID  
                AND A.PMT_DATE=''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
                AND A.PMT_DATE<>A.PREV_PMT_DATE
            JOIN ' || V_TABLEINSERT2  ||' C ON C.AMORTSTOPDATE IS NULL  
                AND C.MASTERID=M.MASTERID  
                AND C.PMT_DATE=C.PREV_PMT_DATE  
            WHERE  
                --DONT DO IF CLOSED  
                M.MASTERID NOT IN (SELECT MASTERID FROM IFRS_ACCT_CLOSED WHERE DOWNLOAD_DATE=''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)  
            END REMARKS.. TUNNING SCRIPT 20160602*/
        ';
        EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'ACF INSERTED');

    -- GET EIR_ACF MAX(ID) TO PROCESS  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (ID)  
        SELECT MAX(ID) AS ID  
        FROM ' || V_TABLEINSERT1 || ' A  
        -- EXCLUDE ACCT REGISTERED @ STOP REV  
        --LEFT JOIN (SELECT DISTINCT MASTERID FROM PSAK_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ) B  
        --ON A.MASTERID = B.MASTERID  
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        --AND B.MASTERID IS NULL  
        -- EXCLUDE ACCT REGISTERED @ STOP REV  
        --AND MASTERID NOT IN (SELECT MASTERID FROM PSAK_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)   
        GROUP BY A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'P1');

    --FEE 1  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T1 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (
        SUM_AMT  
        ,DOWNLOAD_DATE  
        ,FACNO  
        ,CIFNO  
        ,DATASOURCE  
        ,ACCTNO  
        ,MASTERID
        )  
        SELECT SUM(A.N_AMOUNT) AS SUM_AMT  
        ,A.DOWNLOAD_DATE  
        ,A.FACNO  
        ,A.CIFNO  
        ,A.DATASOURCE  
        ,A.ACCTNO  
        ,A.MASTERID  
        FROM (  
        SELECT CASE   
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
        FROM ' || V_TABLEINSERT6 || ' A 
        WHERE A.FLAG_CF = ''F'' AND A.STATUS = ''ACT''  
        ) A  
        GROUP BY A.DOWNLOAD_DATE  
        ,A.FACNO  
        ,A.CIFNO  
        ,A.DATASOURCE  
        ,A.ACCTNO  
        ,A.MASTERID 
     ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'T1 FEE');

    -- UPDATE SUM_AMT  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || '
        SET SUM_AMT = B.SUM_AMT  
        FROM ' || V_TABLEINSERT4 || ' B
        WHERE B.MASTERID = ' || V_TABLEINSERT6 || '.MASTERID  
        AND B.DOWNLOAD_DATE = ' || V_TABLEINSERT6 || '.ECFDATE  
        AND ' || V_TABLEINSERT6 || '.FLAG_CF = ''F''
     ';
    EXECUTE (V_STR_QUERY);
    
    --PREPARE INDEX  
    --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_COST_FEE_ECF_IDX1';  
    --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_ACF_IDX1';  
    --EXECUTE IMMEDIATE 'DROP INDEX TMP_T1_IDX1';  
    --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_COST_FEE_ECF_IDX1 ON V_TABLEINSERT6(MASTERID,ECFDATE,FLAG_CF)';  
    --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_ACF_IDX1 ON V_TABLEINSERT1(MASTERID,ECFDATE)';  
    --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T1_IDX1 ON TMP_T1(MASTERID,DOWNLOAD_DATE)';  
    -- FEE 1  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '
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
        )
        SELECT A.FACNO  
        ,A.CIFNO  
        ,A.DOWNLOAD_DATE  
        ,A.ECFDATE  
        ,A.DATASOURCE  
        ,B.PRDCODE  
        ,B.TRXCODE  
        ,B.CCY  
        ,ROUND(CAST(CAST(B.AMOUNT AS FLOAT) / CAST(B.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_UNAMORT_FEE, ''' || CAST(V_ROUND AS VARCHAR(10)) || ''', ''' || CAST(V_FUNCROUND AS VARCHAR(10)) || ''') AS N_AMOUNT  
        ,B.STATUS  
        ,CURRENT_TIMESTAMP  
        ,A.ACCTNO  
        ,A.MASTERID  
        ,B.FLAG_CF  
        ,B.FLAG_REVERSE  
        ,B.BRCODE  
        ,B.SRCPROCESS  
        ,''EIRACF01''  
        ,''EIR''  
        ,''1''  
        ,B.AMOUNT_ORG  
        ,B.ORG_CCY  
        ,B.ORG_CCY_EXRATE  
        ,B.PRDTYPE  
        ,B.CF_ID    
        FROM ' || V_TABLEINSERT1 || ' A
        JOIN ' || V_TABLEINSERT6 || ' B ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = ''F''
        AND B.STATUS = ''ACT''
        AND B.METHOD = ''EIR''
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND (  
        (  
            A.N_UNAMORT_FEE < 0  
            AND A.FLAG_AL = ''A''  
            )
        OR (
            A.N_UNAMORT_FEE > 0  
            AND A.FLAG_AL = ''L''  
            )  
        )
        AND A.ID IN (
        SELECT ID  
        FROM V_TABLEINSERT4  
        )
            
     ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'FEE PREV');

    --COST 1 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T2 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T2 || '
        (
        SUM_AMT  
        ,DOWNLOAD_DATE  
        ,FACNO  
        ,CIFNO  
        ,DATASOURCE  
        ,ACCTNO  
        ,MASTERID
        )
        SELECT SUM(A.N_AMOUNT) AS SUM_AMT  
        ,A.DOWNLOAD_DATE  
        ,A.FACNO  
        ,A.CIFNO  
        ,A.DATASOURCE  
        ,A.ACCTNO  
        ,A.MASTERID  
        FROM (
        SELECT CASE   
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
        FROM ' || V_TABLEINSERT6 || ' A  
        WHERE A.FLAG_CF = ''C'' AND A.STATUS = ''ACT''  
        ) A
        GROUP BY A.DOWNLOAD_DATE  
        ,A.FACNO
        ,A.CIFNO
        ,A.DATASOURCE
        ,A.ACCTNO
        ,A.MASTERID
     ';
    EXECUTE (V_STR_QUERY);

    --EXECUTE IMMEDIATE 'DROP INDEX TMP_T2_IDX1';  
    --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T2_IDX1 ON TMP_T2(MASTERID,DOWNLOAD_DATE)';  
    -- UPDATE SUM_AMT 

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'T2');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || '
        SET SUM_AMT = B.SUM_AMT  
        FROM ' || TMP_T2 || ' B
        WHERE B.MASTERID = ' || V_TABLEINSERT6 || '.MASTERID  
        AND B.DOWNLOAD_DATE = ' || V_TABLEINSERT6 || '.ECFDATE  
        AND ' || V_TABLEINSERT6 || '.FLAG_CF = ''C''
     ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '
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
        )
        SELECT A.FACNO  
        ,A.CIFNO  
        ,A.DOWNLOAD_DATE  
        ,A.ECFDATE  
        ,A.DATASOURCE  
        ,B.PRDCODE  
        ,B.TRXCODE  
        ,B.CCY  
        ,ROUND(CAST(CAST(B.AMOUNT AS FLOAT) / CAST(B.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_UNAMORT_FEE, ''' || CAST(V_ROUND AS VARCHAR(10)) || ''', ''' || CAST(V_FUNCROUND AS VARCHAR(10)) || ''') AS N_AMOUNT  
        ,B.STATUS  
        ,CURRENT_TIMESTAMP  
        ,A.ACCTNO  
        ,A.MASTERID  
        ,B.FLAG_CF  
        ,B.FLAG_REVERSE  
        ,B.BRCODE  
        ,B.SRCPROCESS  
        ,''EIRACF01''  
        ,''EIR''  
        ,''1''  
        ,B.AMOUNT_ORG  
        ,B.ORG_CCY  
        ,B.ORG_CCY_EXRATE  
        ,B.PRDTYPE  
        ,B.CF_ID  
        FROM ' || V_TABLEINSERT1 || ' A  
        JOIN ' || V_TABLEINSERT6 || ' B ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = ''C''
        AND B.STATUS = ''ACT''
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND (  
        (  
            A.N_UNAMORT_COST < 0  
            AND A.FLAG_AL = ''A''  
            )
        OR (
            A.N_UNAMORT_COST > 0 
            AND A.FLAG_AL = ''L''  
            )
        )
        AND A.ID IN (
        SELECT ID
        FROM ' || V_TABLEINSERT4  || '
        )
     ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'COST PREV');

    -- AMORT ACRU --JOURNAL SHOULD DO THE REST  

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || '
        SET STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE STATUS = ''ACT''
        AND MASTERID IN (
            SELECT DISTINCT MASTERID
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND DO_AMORT = ''Y''
        )
     ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'ACRU PRV UPD');

    -- STOP EIR ECF END TODAY  

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2  || '
        SET AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,AMORTSTOPDATE_PREV = 'END_ACF'
        WHERE ENDAMORTDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATET 
            AND AMORTSTOPDATE IS NULL
            AND MASTERID NOT IN (
            SELECT  MASTERID
            FROM ' || IFRS_ACCT_CLOSED || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
     ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDATE', 'STOP ECF');


    -- AMORT ACRU --JOURNAL SHOULD DO THE REST  
    V_STR_QUERY := '';

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDT', '');

    RAISE NOTICE 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT6;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_LBM_ACCT_EIR_ACF_PMTDT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT6 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;