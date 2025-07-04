---- DROP PROCEDURE SP_IFRS_ACCT_EIR_ECF_ALIGN4;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_ECF_ALIGN4(
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
        V_TABLEINSERT1 := 'IFRS_CFID_JOURNAL_INTM_SUMM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_CFID_JOURNAL_INTM_SUMM';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF_NOCF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_CFID_JOURNAL_INTM_SUMM', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_CFID1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_CFID2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_CFID1' || '(
        MASTERID
        ,CF_ID
        ,ITRCS_AMT
        ,AMORT_FEE
        )
        SELECT 
            MASTERID
            ,CF_ID
            ,ITRCG_AMT
            ,AMORT_AMT
        FROM ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
    ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_CFID1' || '(
        MASTERID
        ,CF_ID
        ,ITRCS_AMT
        ,AMORT_FEE
        )
        SELECT 
            MASTERID
            ,CF_ID
            ,SUM(CASE WHEN JOURNALCODE2 IN ('ITRCG','ITRCG_SL') THEN CASE WHEN REVERSE = 'N' THEN N_AMOUNT ELSE -1 * N_AMOUNT END ELSE 0 END) AS ITRCS_AMT
            ,SUM(CASE WHEN JOURNALCODE2 IN ('ACCRU','ACCRU_SL') THEN CASE WHEN REVERSE = 'N' THEN N_AMOUNT ELSE -1 * N_AMOUNT END ELSE 0 END) AS AMORT_FE
        FROM ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '
        GROUP BY 
            MASTERID
            ,CF_ID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_CFID2' || '(
        MASTERID
        ,CF_ID
        ,ITRCS_AMT
        ,AMORT_FEE
        )
        SELECT 
            MASTERID
            ,CF_ID
            ,SUM(ITRCG_AMT)
            ,SUM(AMORT_AMT)
        FROM ' || TMP_CFID1 || '
        GROUP BY 
            MASTERID
            ,CF_ID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '(
        DOWNLOAD_DATE
        ,MASTERID
        ,CF_ID
        ,ITRCG_AMT
        ,AMORT_AMT
        ,UNAMORT_AMT
        ,CREATEDDATE
        )
        SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,MASTERID
            ,CF_ID
            ,ITRCS_AMT
            ,AMORT_FEE
            ,(ITRCG_AMT+AMORT_AMT)
            ,CURRENT_TIMESTAMP AS CREATEDDATE
        FROM ' || TMP_CFID2 || '';
    EXECUTE (V_STR_QUERY);

    

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_CFID_JOURNAL_INTM_SUMM', '');
        
    ---------- ====== BODY ======

END;