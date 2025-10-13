---- DROP PROCEDURE SP_IFRS_LBM_ACCT_EIR_JRNL_INTM;

CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_JRNL_INTM(
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
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_PARAM_DISABLE_ACCRU_PREV INT;
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
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'TMP_T5_' || P_RUNID || '';
        V_TABLEINSERT6 := 'TMP_T6_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_GAIN_LOSS_' || P_RUNID || '';
        V_TABLEINSERT11 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT12 := 'IFRS_LBM_ACCT_SWITCH_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_ACF';
        V_TABLEINSERT5 := 'TMP_T5';
        V_TABLEINSERT6 := 'TMP_T6';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_STOP_REV';
        V_TABLEINSERT8 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV';
        V_TABLEINSERT9 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_GAIN_LOSS';
        V_TABLEINSERT11 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV';
        V_TABLEINSERT12 := 'IFRS_LBM_ACCT_SWITCH';
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

    SELECT CAST(VALUE1 AS INT) INTO V_ROUND
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SCM003';

    V_PARAM_DISABLE_ACCRU_PREV := 0;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM TMP_T5 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT6 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT6 || ' AS SELECT * FROM TMP_T6 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '');
/*
    --DELETE FIRST 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SUBSTRING(SOURCEPROCESS, 1, 3) = ''EIR'' ';
    EXECUTE (V_STR_QUERY); 

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '1');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,IS_PNL                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID    
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRD_CODE                
            ,TRX_CODE                
            ,CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
            ELSE AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM PNL 1''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,''Y'' IS_PNL                
            ,PRD_TYPE                
            ,''ITRCG''                
            ,CF_ID     
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''PNL'' 
            AND METHOD = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '2');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE           
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,IS_PNL                
            ,PRDTYPE                
            ,JOURNALCODE2          
            ,CF_ID     
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRD_CODE                
            ,TRX_CODE                
            ,CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
            ELSE AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM PNL 1''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,''Y'' IS_PNL                
            ,PRD_TYPE                
            ,''ITRCG''                
            ,CF_ID     
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''PNL'' 
            AND METHOD = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '3');
*/

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
           FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID     
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,- 1 * (                
            CASE                 
                WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END
            )                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM PNL 3''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,PRDTYPE                
            ,''ACCRU''                
            ,CF_ID 
        FROM ' || V_TABLEINSERT8 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''PNL'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '4');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID                
            ,METHOD    
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE               
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,- 1 * (                
            CASE                 
                WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END                
            )                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM PNL 4''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,PRDTYPE                
            ,''ACCRU''                
            ,CF_ID                
            ,METHOD  
        FROM (                
            SELECT ACCTNO                
            ,SUM(AMOUNT) AS AMOUNT                
            ,PRDTYPE                
            ,BRCODE                
            ,CCY                
            ,CF_ID                
            ,CIFNO                
            ,DATASOURCE                
            ,DOWNLOAD_DATE                
            ,FACNO                
            ,FLAG_CF                
            ,FLAG_REVERSE                
            ,PRDCODE                
            ,TRXCODE                
            ,MASTERID                
            ,METHOD                
            ,STATUS   
            FROM ' || V_TABLEINSERT8 || '                
            WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE             
                AND STATUS = ''PNL2''                
            GROUP BY ACCTNO                
            ,PRDTYPE                
            ,BRCODE                
            ,CCY                
            ,CF_ID                
            ,CIFNO                
            ,DATASOURCE                
            ,DOWNLOAD_DATE                
            ,FACNO                
            ,FLAG_CF                
            ,FLAG_REVERSE                
            ,PRDCODE                
            ,TRXCODE                
            ,MASTERID                
            ,METHOD                
            ,STATUS                
            ) A   
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''PNL'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '5');

/*
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRD_CODE                
            ,TRX_CODE                
            ,CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
            ELSE AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR ACT 1''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,PRD_TYPE                
            ,''ITRCG''                
            ,CF_ID   
        FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''ACT'' 
            AND METHOD = ''EIR''';
    EXECUTE (V_STR_QUERY);
*/

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '6');

    --HRD DEFA0 COME FROM DIFFERENT TABLE           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (        
            FACNO        
            ,CIFNO        
            ,DOWNLOAD_DATE        
            ,DATASOURCE        
            ,PRDCODE        
            ,TRXCODE        
            ,CCY        
            ,JOURNALCODE        
            ,STATUS        
            ,REVERSE        
            ,N_AMOUNT        
            ,CREATEDDATE        
            ,SOURCEPROCESS        
            ,ACCTNO        
            ,MASTERID        
            ,FLAG_CF        
            ,BRANCH        
            ,PRDTYPE        
            ,JOURNALCODE2        
            ,CF_ID        
        ) SELECT 
            FACNO        
            ,CIFNO        
            ,ECFDATE        
            ,DATASOURCE        
            ,PRDCODE        
            ,TRXCODE        
            ,CCY        
            ,''DEFA0''        
            ,''ACT''        
            ,''N''        
            ,CASE         
            WHEN FLAG_REVERSE = ''Y''        
                THEN - 1 * AMOUNT        
            ELSE AMOUNT        
            END        
            ,CURRENT_TIMESTAMP        
            ,''EIR LBM HRD 1''        
            ,ACCTNO        
            ,MASTERID        
            ,FLAG_CF        
            ,BRCODE        
            ,PRDTYPE        
            ,''ITRCG''        
            ,CF_ID        
        FROM ' || 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF' || '
        WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE        
        AND STATUS IN (''ACT'', ''REV'' )       
        AND TRXCODE = ''BENEFIT''      
        AND COALESCE(CREATEDBY,'''') <> ''EIR_SWITCH'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '7');

    --REVERSE ACCRUAL                  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            FACNO                
            ,CIFNO  
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,''Y''                
            ,N_AMOUNT                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM REV ACCRU''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE                
            ,CF_ID    
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS = ''ACT'' 
            AND TRXCODE = ''BENEFIT''
            AND JOURNALCODE IN (
                ''ACCRU'', ''ACRU4''
            ) 
            AND REVERSE = ''N'' 
            AND SUBSTRING(SOURCEPROCESS, 1, 3) = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '8');

    --ACCRU FEE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,N_AMOUNT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID   
        ) SELECT 
           FACNO                
            ,CIFNO                
            ,ECFDATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,SUM(CASE                 
                WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END) AS N_AMOUNT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID    
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE FLAG_CF = ''F'' AND TRXCODE = ''BENEFIT'' AND STATUS = ''ACT'' 
        GROUP BY 
            FACNO                
            ,CIFNO                
            ,ECFDATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,FLAG_REVERSE                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '9');
 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,SUM_AMT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE   
        ) SELECT 
            FACNO                
            ,CIFNO                
                ,DOWNLOAD_DATE                
                ,DATASOURCE                
                ,SUM(N_AMOUNT) AS SUM_AMT                
                ,ACCTNO                
                ,MASTERID                
                ,BRCODE  
        FROM ' || V_TABLEINSERT5 || ' D
        GROUP BY 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '10');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''ACCRU''                
            ,''ACT''                
            ,''N''                
            ,ROUND(A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)), ' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM ACCRU FEE 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''F''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ACCRU''                
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND A.MASTERID = B.MASTERID
        JOIN ' || V_TABLEINSERT6 || ' C 
            ON C.MASTERID = A.MASTERID   
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '11');        

    --AMORT FEE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS            
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,ROUND(A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)),' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT FEE 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''F''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ACCRU''                
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND A.MASTERID = B.MASTERID
        JOIN ' || V_TABLEINSERT6 || ' C 
            ON C.MASTERID = A.MASTERID   
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DO_AMORT = ''Y'' 
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '12');

    --STOP REV DEFA0 FEE 20160619          
 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE               
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''N''                
            ,ROUND(- 1 * A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)), ' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM DEFA0 FEE 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''F''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ITRCG''                
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND A.MASTERID = B.MASTERID
        JOIN ' || V_TABLEINSERT6 || ' C ON 
            C.MASTERID = A.MASTERID                
            AND A.ECFDATE = C.DOWNLOAD_DATE   
        --ONLY FOR STOP REV 
        JOIN (
            SELECT DISTINCT MASTERID
            FROM ' || V_TABLEINSERT7 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) D ON A.MASTERID = D.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DO_AMORT = ''Y'' 
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '13');

    --ACCRU COST           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT5 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,N_AMOUNT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID   
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,ECFDATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,SUM(CASE                 
                WHEN FLAG_REVERSE = ''Y''            
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END) AS N_AMOUNT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID  
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE FLAG_CF = ''C'' AND TRXCODE = ''BENEFIT'' AND STATUS = ''ACT''
        GROUP BY 
            FACNO                
            ,CIFNO                
            ,ECFDATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE                
            ,PRDTYPE                
            ,CF_ID                
            ,FLAG_REVERSE 
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '14');

    --ACCRU COST           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT6 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,SUM_AMT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE   
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,SUM(N_AMOUNT) AS SUM_AMT                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE  
        FROM ' || V_TABLEINSERT5 || ' D
        GROUP BY 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,ACCTNO                
            ,MASTERID                
            ,BRCODE 
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '15');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''ACCRU''                
            ,''ACT''                
            ,''N''                
            ,ROUND(A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)), ' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM ACCRU COST 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''C''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ACCRU''                
            ,B.CF_ID   
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND A.MASTERID = B.MASTERID
        JOIN ' || V_TABLEINSERT6 || ' C 
            ON C.MASTERID = A.MASTERID
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''N''
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '16');
    
    --AMORT COST                       
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID      
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,ROUND(A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)), ' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT COST 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''C''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ACCRU''                
            ,B.CF_ID  
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT6 || ' C 
            ON C.MASTERID = A.MASTERID
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '17');
    
    --AMORT COST                       
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID      
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''N''                
            ,ROUND(- 1 * A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS DOUBLE PRECISION) / CAST(C.SUM_AMT AS DOUBLE PRECISION) AS DECIMAL(32, 20)), ' || V_ROUND || ')                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT COST 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''C''                
            ,B.BRCODE                
            ,B.PRDTYPE                
            ,''ITRCG''                
            ,B.CF_ID   
        FROM ' || V_TABLEINSERT4 || ' A 
        JOIN ' || V_TABLEINSERT5 || ' B 
            ON B.DOWNLOAD_DATE = A.ECFDATE   
            AND B.MASTERID = A.MASTERID 
        JOIN ' || V_TABLEINSERT6 || ' C 
            ON C.MASTERID = A.MASTERID 
            AND A.ECFDATE = C.DOWNLOAD_DATE 
        --STOPREV 
        JOIN (
            SELECT DISTINCT MASTERID
            FROM ' || V_TABLEINSERT7 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) D ON A.MASTERID = D.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '18');
    
    -- 20160407 DANIEL S : SET BLK BEFORE ACCRU PREV CODE                  
    -- UPDATE STATUS ACCRU PREV FOR EIR STOP REV               
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT11 || ' 
        SET STATUS = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || ''' || ''BLK'' 
        FROM ' || V_TABLEINSERT11 || ' A
        JOIN ' || V_TABLEINSERT7 || ' E ON E.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.MASTERID = E.MASTERID
        JOIN ' || V_TABLEINSERT11 || ' C ON C.MASTERID = A.MASTERID 
        AND C.STATUS = ''ACT''
        AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '19');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID                
            ,METHOD   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.CCY                
            ,''ACCRU''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN C.FLAG_REVERSE = ''Y''                
                THEN - 1 * C.AMOUNT                
            ELSE C.AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM ACCRU PREV''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,C.FLAG_CF                
            ,A.BRANCH                
            ,C.PRDTYPE                
            ,''ACCRU''                
            ,C.CF_ID                
            ,C.METHOD
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT11 || ' C ON C.MASTERID = A.MASTERID
        AND C.STATUS = ''ACT''
        AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.DO_AMORT = ''N'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '20');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID                
            ,METHOD   
        ) SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
                WHEN C.FLAG_REVERSE = ''Y''                
                THEN - 1 * C.AMOUNT                
                ELSE C.AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT PREV''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,C.FLAG_CF                
            ,A.BRANCH                
            ,C.PRDTYPE                
            ,''ACCRU''                
            ,C.CF_ID                
            ,C.METHOD
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT11 || ' C ON C.MASTERID = A.MASTERID
        AND C.STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.DO_AMORT = ''Y''
        AND A.MASTERID NOT IN (
            SELECT DISTINCT MASTERID 
            FROM ' || V_TABLEINSERT12 || ' 
            WHERE DOWNLOAD_DATE = C.DOWNLOAD_DATE
            
        )';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '21');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT            
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID                
            ,METHOD   
        ) SELECT 
            C.FACNO                
            ,C.CIFNO                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                
            ,C.DATASOURCE                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''             
            ,CASE    
            WHEN C.FLAG_REVERSE = ''Y''                
                THEN - 1 * C.AMOUNT                
            ELSE C.AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT PREV2''                
            ,C.ACCTNO                
            ,C.MASTERID                
            ,C.FLAG_CF                
            ,P.BRANCH_CODE                
            ,C.PRDTYPE                
            ,''ACCRU''                
            ,C.CF_ID                
            ,C.METHOD
        FROM (
            SELECT ACCTNO                
            ,AMORTDATE                
            ,SUM(AMOUNT) AS AMOUNT                
            ,PRDTYPE                
            ,CCY                
            ,CF_ID                
            ,CIFNO                
            ,DATASOURCE                
            ,DOWNLOAD_DATE                
            ,FACNO                
            ,FLAG_CF                
            ,FLAG_REVERSE                
            ,PRDCODE                
            ,TRXCODE                
            ,MASTERID                
            ,METHOD
            ,STATUS
            FROM ' || V_TABLEINSERT11 || ' 
            WHERE DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY ACCTNO                
            ,AMORTDATE                
            ,PRDTYPE                
            ,CCY                
            ,CF_ID                
            ,CIFNO                
            ,DATASOURCE                
            ,DOWNLOAD_DATE                
            ,FACNO                
            ,FLAG_CF                
            ,FLAG_REVERSE                
            ,PRDCODE                
            ,TRXCODE                
            ,MASTERID                
            ,METHOD                
            ,STATUS
        ) C
        JOIN ' || V_TABLEINSERT9 || ' P ON P.MASTERID = C.MASTERID
        --20180310 CHANGE FROM ECF TO ACF
        LEFT JOIN ' || V_TABLEINSERT4 || ' A ON A.MASTERID = C.MASTERID
        AND C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE C.STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND C.MASTERID IS NULL
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '22');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            A.PREV_FACNO                
            ,A.PREV_CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.PREV_DATASOURCE                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN C.FLAG_REVERSE = ''Y''                
                THEN - 1 * C.AMOUNT                
            ELSE C.AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM ACRU SW''                
            ,A.PREV_ACCTNO                
            ,A.PREV_MASTERID                
            ,C.FLAG_CF              
            ,A.PREV_BRCODE                
            ,C.PRDTYPE                
            ,''ACCRU''                
            ,C.CF_ID
        FROM ' || V_TABLEINSERT12 || ' A
        JOIN ' || V_TABLEINSERT11 || ' C 
        ON C.MASTERID = A.PREV_MASTERID
            AND C.STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
            AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.PREV_EIR_ECF = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '23');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''Y''                
            ,1 * (                
            CASE                 
                WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END                
            )                
            ,CURRENT_TIMESTAMP                
            ,''EIR_REV_SWITCH''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,PRDTYPE                
            ,''ITRCG''                
            ,CF_ID
        FROM ' || V_TABLEINSERT8 || ' A
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND STATUS = ''REV'' AND CREATEDBY = ''EIR_SWITCH''
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '24');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
             FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE           
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT 
            FACNO                
            ,CIFNO                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''Y''                
            ,1 * (                
            CASE                 
                WHEN FLAG_REVERSE = ''Y''                
                THEN - 1 * AMOUNT                
                ELSE AMOUNT                
                END                
            )                
            ,CURRENT_TIMESTAMP                
            ,''EIR_REV_SWITCH''                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRCODE                
            ,PRDTYPE                
            ,''ITRCG''                
            ,CF_ID
        FROM ' || V_TABLEINSERT8 || ' A
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
        AND STATUS = ''REV2'' AND CREATEDBY = ''EIR_SWITCH''
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '25');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT FACNO                
        ,CIFNO                
        ,DOWNLOAD_DATE                
        ,DATASOURCE                
        ,PRDCODE                
        ,TRXCODE                
        ,CCY                
        ,''DEFA0''                
        ,''ACT''                
        ,''N''                
        ,1 * (                
        CASE                 
            WHEN FLAG_REVERSE = ''Y''                
            THEN - 1 * AMOUNT                
            ELSE AMOUNT                
            END                
        )                
        ,CURRENT_TIMESTAMP                
        ,''EIR_SWITCH''                
        ,ACCTNO                
        ,MASTERID              
        ,FLAG_CF                
        ,BRCODE                
        ,PRDTYPE                
        ,''ITRCG''                
        ,CF_ID  
        FROM ' || V_TABLEINSERT8 || ' A
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''ACT''
            AND SEQ = ''0''';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '26');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT A.FACNO                
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRODUCT_CODE                
            ,''EIR_NOCF'' TRXCODE                
            ,B.CURRENCY                
            ,''ACRU4''                
            ,''ACT''                
            ,''N''                
            ,A.N_UNAMORT_PREV_NOCF + A.N_ACCRU_NOCF                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM ACCRU NOCF''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''S''                                 
            ,B.BRANCH_CODE                
            ,B.PRODUCT_TYPE                
            ,''ACRU4''                
            ,NULL  
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT9 || ' B ON B.MASTERID = A.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''N''
            AND A.N_ACCRU_NOCF IS NOT NULL
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '27');
    
    --EIR ACCRU PREV                           
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID   
        ) SELECT A.FACNO                              
            ,A.CIFNO                
            ,A.DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,B.PRODUCT_CODE                
            ,''EIR_NOCF'' TRXCODE                
            ,B.CURRENCY                
            ,''AMRT4''                
            ,''ACT''                
            ,''N''                
            ,A.N_ACCRU_NOCF                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT NOCF''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''N''                
            ,B.BRANCH_CODE                
            ,B.PRODUCT_TYPE                
            ,''AMRT4''                
            ,NULL   
        FROM ' || V_TABLEINSERT4 || ' A
        JOIN ' || V_TABLEINSERT9 || ' B ON B.MASTERID = A.MASTERID
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''Y''
            AND A.N_ACCRU_NOCF IS NOT NULL
        ';
    EXECUTE (V_STR_QUERY);
    
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '28');
    
    -- PNL FOR NO COST FEE ECF FOR CLOSED ACCOUNT AND EVENT CHANGE        

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE TMP_NOCF';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMP_NOCF (MASTERID)
        SELECT DISTINCT MASTERID
        FROM IFRS_ACCT_CLOSED
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '29');
    
    -- PNL FOR NO COST FEE ECF FOR CLOSED ACCOUNT AND EVENT CHANGE        

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT1 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE      
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID
        )
        SELECT A.FACNO                
            ,A.CIFNO                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,COALESCE(B.PRODUCT_CODE, C.PRODUCT_CODE)                
            ,''EIR_NOCF'' TRXCODE                
            ,COALESCE(B.CURRENCY, C.CURRENCY)                
            ,''AMRT4''                
            ,''ACT''                
            ,''Y''                
            ,CASE                 
            WHEN A.DO_AMORT = ''Y''                
                THEN A.N_UNAMORT_NOCF                
            ELSE A.N_UNAMORT_PREV_NOCF                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM AMORT NOCF''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,''N''                
            ,COALESCE(B.BRANCH_CODE, C.BRANCH_CODE)                
            ,COALESCE(B.PRODUCT_TYPE, C.PRODUCT_TYPE)                
            ,''AMRT4''                
            ,NULL --CFID
        FROM ' || V_TABLEINSERT4 || ' A
        LEFT JOIN ' || V_TABLEINSERT9  || ' B ON B.MASTERID = A.MASTERID
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        LEFT JOIN IFRS_IMA_AMORT_PREV C ON C.MASTERID = A.MASTERID
            AND C.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.ID IN (
            SELECT MAX(ID)
            FROM ' || V_TABLEINSERT4 || '
            WHERE DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            AND DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND MASTERID IN (
                SELECT MASTERID
                FROM TMP_NOCF
            )
        GROUP BY MASTERID
        )
        AND CASE 
            WHEN A.DO_AMORT = ''Y''
                THEN A.N_UNAMORT_NOCF
            ELSE A.N_UNAMORT_PREV_NOCF
            END <> 0
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '30');
    
    -- PNL FOR NO COST FEE ECF FOR CLOSED ACCOUNT AND EVENT CHANGE        

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID
        )
        SELECT 
            A.FACNO                
            ,A.CIFNO                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                
            ,A.DATASOURCE                
            ,A.PRDCODE                
            ,A.TRXCODE                
            ,A.CCY                
            ,''DEFA0''                
            ,''ACT''                
            ,''Y''                
            ,CASE                 
            WHEN FLAG_REVERSE = ''Y''                
            THEN - 1 * AMOUNT                
            ELSE AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM STOP REV 1''                
            ,A.ACCTNO                
            ,A.MASTERID                
            ,A.FLAG_CF                
            ,A.BRCODE                
            ,A.PRDTYPE                
            ,''ITRCG''                
            ,A.CF_ID
        FROM ' || V_TABLEINSERT8 || ' A
        JOIN VW_LAST_EIR_CF_PREV_YEST C ON C.MASTERID = A.MASTERID
            AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
            AND COALESCE(C.SEQ, '''') = COALESCE(A.SEQ, '''')
        JOIN ' || V_TABLEINSERT7  || ' B ON B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.MASTERID = A.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.STATUS = ''ACT''
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '31');
    
    -- 20160407 AMORT YESTERDAY ACCRU                  
    -- BLOCK ACCRU PREV GENERATION ON SL_ECF
    
    V_STR_QUERY := '';
    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN 
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
                FACNO                
                ,CIFNO                
                ,DOWNLOAD_DATE                
                ,DATASOURCE                
                ,PRDCODE                
                ,TRXCODE                
                ,CCY                
                ,JOURNALCODE                
                ,STATUS                
                ,REVERSE                
                ,N_AMOUNT                
                ,CREATEDDATE                
                ,SOURCEPROCESS                
                ,ACCTNO                
                ,MASTERID                
                ,FLAG_CF                
                ,BRANCH                
                ,PRDTYPE                
                ,JOURNALCODE2                
                ,CF_ID
            )
            SELECT FACNO                
                ,CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                
                ,DATASOURCE                
                ,PRDCODE                
                ,TRXCODE                
                ,CCY                
                ,''AMORT''                
                ,STATUS                
                ,''N''                
                ,N_AMOUNT                
                ,CURRENT_TIMESTAMP                
                ,''EIR LBM STOP REV 2''                
                ,ACCTNO                
                ,X.MASTERID                
                ,FLAG_CF                
                ,BRANCH                
                ,PRDTYPE                
                ,''ACCRU''                
                ,CF_ID
            FROM ' || V_TABLEINSERT1 || ' X
            INNER JOIN (
                SELECT DISTINCT MASTERID
                FROM ' || V_TABLEINSERT7  || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ) Y ON X.MASTERID = Y.MASTERID
            WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
                AND STATUS = ''ACT''
                AND JOURNALCODE = ''ACCRU''                
                AND TRXCODE = ''BENEFIT''                
                AND REVERSE = ''N''                
                AND SUBSTRING(SOURCEPROCESS, 1, 3) = ''EIR'''
		END;
    ELSE         
    	-- REVERSE ACCRU 
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
                FACNO                
                ,CIFNO                
                ,DOWNLOAD_DATE                
                ,DATASOURCE                
                ,PRDCODE                
                ,TRXCODE                
                ,CCY                
                ,JOURNALCODE                
                ,STATUS                
                ,REVERSE                
                ,N_AMOUNT                
                ,CREATEDDATE                
                ,SOURCEPROCESS                
                ,ACCTNO                
                ,MASTERID                
                ,FLAG_CF                
                ,BRANCH                
                ,PRDTYPE                
                ,JOURNALCODE2                
                ,CF_ID 
            )
            SELECT FACNO                
                ,CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE                
                ,DATASOURCE                
                ,PRDCODE                
                ,TRXCODE                
                ,CCY                
                ,''DEFA0''                
                ,STATUS                
                ,''Y''                
                ,- 1 * N_AMOUNT                
                ,CURRENT_TIMESTAMP                
                ,''EIR LBM STOP REV 2''                
                ,ACCTNO                
                ,MASTERID                
                ,FLAG_CF                
                ,BRANCH                
                ,PRDTYPE                
                ,''ITRCG''                
                ,CF_ID
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND STATUS = ''ACT''
                AND JOURNALCODE = ''ACCRU''                
                AND TRXCODE = ''BENEFIT''                
                AND REVERSE = ''N''                
                AND SUBSTRING(SOURCEPROCESS, 1, 3) = ''EIR''                
                AND MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || V_TABLEINSERT7 || '                
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
            FACNO                
            ,CIFNO                
            ,DOWNLOAD_DATE                
            ,DATASOURCE                
            ,PRDCODE                
            ,TRXCODE                
            ,CCY                
            ,JOURNALCODE                
            ,STATUS                
            ,REVERSE                
            ,N_AMOUNT                
            ,CREATEDDATE                
            ,SOURCEPROCESS                
            ,ACCTNO                
            ,MASTERID                
            ,FLAG_CF                
            ,BRANCH                
            ,PRDTYPE                
            ,JOURNALCODE2                
            ,CF_ID                
            ,METHOD
        )
        SELECT A.FACILITY_NUMBER                
            ,A.CUSTOMER_NUMBER                
            ,A.DOWNLOAD_DATE                
            ,A.DATA_SOURCE                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.CCY                
            ,''AMORT''                
            ,''ACT''                
            ,''N''                
            ,CASE                 
            WHEN C.FLAG_REVERSE = ''Y''                
                THEN - 1 * C.AMOUNT                
            ELSE C.AMOUNT                
            END                
            ,CURRENT_TIMESTAMP                
            ,''EIR LBM GAIN LOSS''                
            ,A.ACCOUNT_NUMBER                
            ,A.MASTERID                
            ,C.FLAG_CF                
            ,A.BRANCH_CODE                
            ,C.PRDTYPE                
            ,''ACCRU''                
            ,C.CF_ID                
            ,C.METHOD
        FROM ' || V_TABLEINSERT9 || ' A
        JOIN ' || V_TABLEINSERT10  || ' C ON C.MASTERID = A.MASTERID
            AND C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);
    
    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_ACCT_EIR_JOURNAL_INTM', '');

    RAISE NOTICE 'SP_IFRS_LBM_ACCT_EIR_JRNL_INTM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_LBM_ACCT_EIR_JRNL_INTM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;